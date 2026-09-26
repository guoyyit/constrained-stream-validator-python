"""Structured-format constraint: streaming JSON well-formedness check.

A deterministic pushdown automaton driven one character at a time, so a prefix
can be validated without buffering the whole response.  Per-request state is a
small stack; each entry needs at least two characters of input, so stack depth
is bounded by half the service length cap.

Optional ``required_keys`` are checked when the top-level object closes.
"""

from __future__ import annotations

import regex

from ..models import ErrorCode
from .base import Constraint, ConstraintState, CharResult

_VALUE = "value"
_STRING = "str"
_ESCAPE = "esc"
_UNICODE = "uni"
_KEY = "key"
_KEY_ESCAPE = "kesc"
_KEY_UNICODE = "kuni"
_COLON = "colon"            # after key, before ':'
_OBJECT_OPEN = "obj0"       # expecting '}' or first key
_OBJECT_KEY = "obj2"        # expecting another key after comma
_MEMBER_DONE = "obj1"       # expecting ',' or '}'
_ARRAY_OPEN = "arr0"        # expecting ']' or first element
_ARRAY_NEXT = "arr2"        # element required after comma
_ELEMENT_DONE = "arr1"      # expecting ',' or ']'
_VALUE_DONE = "done"        # complete value, continuation depends on stack
_NUMBER = "num"
_LITERAL = "lit"

_WS_MODES = {_VALUE, _OBJECT_OPEN, _OBJECT_KEY, _MEMBER_DONE, _ARRAY_OPEN, _ARRAY_NEXT,
             _ELEMENT_DONE, _VALUE_DONE, _COLON, _NUMBER}
# In _NUMBER mode whitespace terminates the pending number (handled there).
_HEX = set("0123456789abcdefABCDEF")
_LITERALS = {"t": "true", "f": "false", "n": "null"}
_NUM_RX = regex.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")


class JsonState(ConstraintState):
    def __init__(self, required_keys: tuple[str, ...] | None) -> None:
        self._stack: list[str] = []
        self._seen_keys: list[set[str]] = []
        self._mode: str = _VALUE
        self._hex_left = 0
        self._key_chars: list[str] = []
        self._num_buf: list[str] = []
        self._lit_word = ""
        self._lit_idx = 0
        self._root_seen = False
        self._required = required_keys

    def _bad(self, msg: str) -> CharResult:
        return CharResult(ok=False, code=ErrorCode.INVALID_JSON, message=msg)

    def _complete_value(self) -> None:
        # A finished value behaves differently depending on its container.
        if not self._stack:
            self._mode = _VALUE_DONE
        elif self._stack[-1] == "{":
            self._mode = _MEMBER_DONE
        else:
            self._mode = _ELEMENT_DONE

    def _close_container(self, ch: str) -> CharResult:
        if not self._stack:
            return self._bad(f"unmatched {ch!r}")
        top = self._stack[-1]
        if ch == "}":
            if top != "{":
                return self._bad("expected ']' to close array")
            if self._required is not None and len(self._stack) == 1:
                missing = [k for k in self._required if k not in self._seen_keys[-1]]
                self._stack.pop()
                self._seen_keys.pop()
                if missing:
                    return self._bad(f"missing required top-level keys: {missing}")
            else:
                self._stack.pop()
                self._seen_keys.pop()
        else:
            if top != "[":
                return self._bad("expected '}' to close object")
            self._stack.pop()
        self._complete_value()
        return CharResult(ok=True)

    def feed(self, ch: str) -> CharResult:
        mode = self._mode

        # Whitespace is legal in structural modes (including after a finished
        # root value).  In _NUMBER mode it terminates a pending number and is
        # handled by that method; inside strings/literals it is content.
        if ch in " \t\n\r" and mode in _WS_MODES and mode != _NUMBER:
            return CharResult(ok=True)

        if mode == _STRING:
            return self._in_string(ch, key=False)
        if mode == _ESCAPE:
            return self._in_escape(ch, key=False)
        if mode == _UNICODE:
            return self._in_unicode(ch, key=False)
        if mode == _KEY:
            return self._in_string(ch, key=True)
        if mode == _KEY_ESCAPE:
            return self._in_escape(ch, key=True)
        if mode == _KEY_UNICODE:
            return self._in_unicode(ch, key=True)

        if mode == _COLON:
            if ch != ":":
                return self._bad(f"expected ':' after key, got {ch!r}")
            self._mode = _VALUE
            return CharResult(ok=True)

        if mode == _OBJECT_KEY:
            if ch != '"':
                return self._bad(f"expected object key after comma, got {ch!r}")
            self._key_chars = []
            self._mode = _KEY
            return CharResult(ok=True)

        if mode == _OBJECT_OPEN:
            if ch == "}":
                return self._close_container(ch)
            if ch != '"':
                return self._bad(f"expected object key or '}}', got {ch!r}")
            self._key_chars = []
            self._mode = _KEY
            return CharResult(ok=True)

        if mode == _MEMBER_DONE:
            if ch == ",":
                self._mode = _OBJECT_KEY
                return CharResult(ok=True)
            if ch == "}":
                return self._close_container(ch)
            return self._bad(f"expected ',' or '}}', got {ch!r}")

        if mode == _ARRAY_OPEN:
            if ch == "]":
                return self._close_container(ch)
            return self._start_value(ch)

        if mode == _ELEMENT_DONE:
            if ch == ",":
                self._mode = _ARRAY_NEXT
                return CharResult(ok=True)
            if ch == "]":
                return self._close_container(ch)
            return self._bad(f"expected ',' or ']', got {ch!r}")

        if mode == _VALUE_DONE:
            if not self._stack:
                return self._bad(f"unexpected {ch!r} after root value")
            top = self._stack[-1]
            if top == "{":
                if ch == ",":
                    self._mode = _OBJECT_KEY
                    return CharResult(ok=True)
                if ch == "}":
                    return self._close_container(ch)
                return self._bad(f"expected ',' or '}}', got {ch!r}")
            if ch == ",":
                self._mode = _ARRAY_NEXT
                return CharResult(ok=True)
            if ch == "]":
                return self._close_container(ch)
            return self._bad(f"expected ',' or ']', got {ch!r}")

        if mode == _ARRAY_NEXT:
            return self._start_value(ch)

        if mode == _NUMBER:
            return self._in_number(ch)
        if mode == _LITERAL:
            return self._in_literal(ch)
        if mode == _VALUE:
            return self._start_value(ch)
        return self._bad(f"parser stuck: {mode}")  # pragma: no cover

    # -- value start -------------------------------------------------------
    def _start_value(self, ch: str) -> CharResult:
        if self._root_seen and not self._stack:
            return self._bad("multiple root values are not allowed")
        if ch == '"':
            self._mode = _STRING
        elif ch == "{":
            self._stack.append("{")
            self._seen_keys.append(set())
            self._mode = _OBJECT_OPEN  # may close empty or take first key
        elif ch == "[":
            self._stack.append("[")
            self._mode = _ARRAY_OPEN
        elif ch in "-0123456789":
            self._num_buf = [ch]
            self._mode = _NUMBER
        elif ch in _LITERALS:
            self._lit_word = _LITERALS[ch]
            self._lit_idx = 1
            self._mode = _LITERAL
        else:
            return self._bad(f"illegal start of value: {ch!r}")
        self._root_seen = True
        return CharResult(ok=True)

    # -- strings -----------------------------------------------------------
    def _in_string(self, ch: str, *, key: bool) -> CharResult:
        if ch == '"':
            if key:
                name = "".join(self._key_chars)
                if name in self._seen_keys[-1]:
                    return self._bad(f"duplicate object key {name!r}")
                self._seen_keys[-1].add(name)
                self._mode = _COLON
            else:
                self._complete_value()
            return CharResult(ok=True)
        if ch == "\\":
            self._mode = _KEY_ESCAPE if key else _ESCAPE
            if key:
                self._key_chars.append(ch)
            return CharResult(ok=True)
        if ord(ch) < 0x20:
            return self._bad("unescaped control character in string")
        if key:
            self._key_chars.append(ch)
        return CharResult(ok=True)

    def _in_escape(self, ch: str, *, key: bool) -> CharResult:
        if ch not in '"\\/bfnrtu':
            return self._bad(f"invalid escape \\{ch}")
        if key:
            self._key_chars.append(ch)
        if ch == "u":
            self._hex_left = 4
            self._mode = _KEY_UNICODE if key else _UNICODE
        else:
            self._mode = _KEY if key else _STRING
        return CharResult(ok=True)

    def _in_unicode(self, ch: str, *, key: bool) -> CharResult:
        if ch not in _HEX:
            return self._bad("invalid \\u escape, hex digit expected")
        self._hex_left -= 1
        if key:
            self._key_chars.append(ch)
        if self._hex_left == 0:
            self._mode = _KEY if key else _STRING
        return CharResult(ok=True)

    # -- numbers -----------------------------------------------------------
    def _in_number(self, ch: str) -> CharResult:
        if ch in "0123456789.eE+-":
            self._num_buf.append(ch)
            if self._num_regex_ok(partial=True):
                return CharResult(ok=True)
            self._num_buf.pop()
        # Current character terminates the number; number must be complete.
        if not self._num_buf or not self._num_regex_ok(partial=False):
            return self._bad(f"invalid number {''.join(self._num_buf)!r}")
        self._num_buf = []
        self._complete_value()
        if ch in " \t\n\r":
            return CharResult(ok=True)
        return self.feed(ch)

    def _num_regex_ok(self, *, partial: bool) -> bool:
        # fullmatch: every character accumulated so far must belong to one
        # (possibly still incomplete) number; '01' is rejected while '0.' or
        # '1e' remain valid streaming prefixes.
        return _NUM_RX.fullmatch("".join(self._num_buf), partial=partial) is not None

    # -- literals ----------------------------------------------------------
    def _in_literal(self, ch: str) -> CharResult:
        word = self._lit_word
        idx = self._lit_idx
        if idx >= len(word) or ch != word[idx]:
            return self._bad(f"invalid literal {word[:idx]!r}{ch!r}")
        self._lit_idx += 1
        if self._lit_idx == len(word):
            self._complete_value()
        return CharResult(ok=True)

    # -- lifecycle ---------------------------------------------------------
    def rollback(self) -> None:
        # Failed feeds are atomic and never mutate state.
        return None

    def finish(self) -> CharResult | None:
        if not self._root_seen:
            return CharResult(ok=False, code=ErrorCode.INVALID_JSON,
                              message="empty input is not valid JSON")
        if self._stack:
            return CharResult(ok=False, code=ErrorCode.INCOMPLETE_OUTPUT,
                              message="unclosed JSON container")
        # A number is finalized lazily when a delimiter arrives; at EOF a
        # fully-formed pending number is a legal end.
        if self._mode == _NUMBER and self._num_buf and self._num_regex_ok(partial=False):
            return None
        if self._mode != _VALUE_DONE:
            return CharResult(ok=False, code=ErrorCode.INCOMPLETE_OUTPUT,
                              message="incomplete JSON value")
        return None


class JsonConstraint(Constraint):
    type_name = "json"

    def __init__(
        self,
        name: str,
        version: int,
        *,
        required_keys: list[str] | None = None,
        fatal: bool = True,
    ) -> None:
        super().__init__(name, version, fatal=fatal)
        self.required_keys = tuple(required_keys) if required_keys else None

    def new_state(self) -> ConstraintState:
        return JsonState(self.required_keys)

    def to_dict(self) -> dict:
        return {
            "type": self.type_name,
            "name": self.name,
            "version": self.version,
            "required_keys": list(self.required_keys) if self.required_keys else [],
            "fatal": self.fatal,
        }
