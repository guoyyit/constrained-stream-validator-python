"""Streaming constraint validators.

Each validator is a pure incremental state machine. Length, enum and
structured(JSON) validators keep only bounded structural state. The regex
validator retains the accepted prefix; its memory is bounded by the session's
``max_length`` cap (documented compatibility boundary).
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import regex

from .models import ConstraintSpec, ConstraintType, ErrorCode, ValidationError


class StreamingConstraint(ABC):
    """Abstract incremental constraint."""

    consumed: int = 0

    @abstractmethod
    def feed(self, text: str) -> tuple[str, ValidationError | None]:
        """Consume the next text fragment.

        Returns ``(accepted_prefix, error)``. ``accepted_prefix`` is the longest
        prefix of ``text`` that remains valid given prior state. State advances
        only by the accepted prefix. A non-None ``error`` is terminal: no further
        text can ever become valid.
        """

    @abstractmethod
    def finish(self) -> ValidationError | None:
        """Signal end-of-stream; validate completeness."""

    def error(
        self, code: ErrorCode, message: str, position: int | None = None
    ) -> ValidationError:
        return ValidationError(
            code=code,
            message=message,
            position=self.consumed if position is None else position,
            consumed_prefix="",
        )


# ---------------------------------------------------------------------------
# Length
# ---------------------------------------------------------------------------

class LengthConstraint(StreamingConstraint):
    """Output length must not exceed ``max`` characters."""

    def __init__(self, max_length: int) -> None:
        if max_length < 0:
            raise ValueError("max_length must be >= 0")
        self.max_length = max_length
        self.consumed = 0

    def feed(self, text: str) -> tuple[str, ValidationError | None]:
        room = self.max_length - self.consumed
        if len(text) <= room:
            self.consumed += len(text)
            return text, None
        accepted = text[:room]
        self.consumed += len(accepted)
        err = ValidationError(
            code=ErrorCode.LENGTH_LIMIT_EXCEEDED,
            message=f"output exceeds maximum length {self.max_length}",
            position=self.max_length,
        )
        return accepted, err

    def finish(self) -> ValidationError | None:
        return None


# ---------------------------------------------------------------------------
# Enum
# ---------------------------------------------------------------------------

class EnumConstraint(StreamingConstraint):
    """Output must be exactly one of a fixed set of allowed values."""

    def __init__(self, values: list[str]) -> None:
        if not values:
            raise ValueError("enum requires at least one value")
        self.values = tuple(values)
        self.consumed = 0
        self._buffer = ""

    def feed(self, text: str) -> tuple[str, ValidationError | None]:
        accepted_chars = 0
        for ch in text:
            candidate = self._buffer + ch
            if any(v.startswith(candidate) for v in self.values):
                self._buffer = candidate
                accepted_chars += 1
                continue
            position = self.consumed + accepted_chars
            self.consumed += accepted_chars
            err = ValidationError(
                code=ErrorCode.CONSTRAINT_VIOLATION,
                message=f"value is not a prefix of any allowed enum value "
                f"(near {candidate!r})",
                position=position,
            )
            return text[:accepted_chars], err
        self.consumed += accepted_chars
        return text, None

    def finish(self) -> ValidationError | None:
        if self._buffer in self.values:
            return None
        return ValidationError(
            code=ErrorCode.CONSTRAINT_VIOLATION,
            message=f"incomplete enum value {self._buffer!r}",
            position=self.consumed,
        )


# ---------------------------------------------------------------------------
# Regex (fullmatch semantics, partial-match incremental viability)
# ---------------------------------------------------------------------------

class RegexConstraint(StreamingConstraint):
    """Output must fully match ``pattern`` (Python ``regex`` dialect)."""

    def __init__(self, pattern: str) -> None:
        try:
            self._pattern = regex.compile(pattern)
        except regex.error as exc:  # pragma: no cover - defensive
            raise ValueError(f"invalid regex: {exc}") from exc
        self.consumed = 0
        self._buffer = ""

    def _viable(self, candidate: str) -> bool:
        m = self._pattern.fullmatch(candidate, partial=True)
        return m is not None

    def feed(self, text: str) -> tuple[str, ValidationError | None]:
        accepted_chars = 0
        for ch in text:
            candidate = self._buffer + ch
            if self._viable(candidate):
                self._buffer = candidate
                accepted_chars += 1
                continue
            position = self.consumed + accepted_chars
            self.consumed += accepted_chars
            err = ValidationError(
                code=ErrorCode.CONSTRAINT_VIOLATION,
                message="output cannot satisfy regular expression",
                position=position,
            )
            return text[:accepted_chars], err
        self.consumed += accepted_chars
        return text, None

    def finish(self) -> ValidationError | None:
        if self._pattern.fullmatch(self._buffer) is not None:
            return None
        return ValidationError(
            code=ErrorCode.CONSTRAINT_VIOLATION,
            message="output does not fully match regular expression",
            position=self.consumed,
        )
        return None


# ---------------------------------------------------------------------------
# Structured: streaming JSON prefix validator
# ---------------------------------------------------------------------------

_OBJECT = 1
_ARRAY = 2

_WHITESPACE = " \t\n\r"
_LITERALS = ("true", "false", "null")
_NUMBER_PREFIX = r"-?(?:0|[1-9][0-9]*)?(?:\.[0-9]*)?(?:[eE][+-]?[0-9]*)?"
_NUMBER_FULL = r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"

import re as _re

_NUMBER_PREFIX_RE = _re.compile(_NUMBER_PREFIX)
_NUMBER_FULL_RE = _re.compile(_NUMBER_FULL)


class JsonConstraint(StreamingConstraint):
    """Validate a JSON value character by character.

    Every accepted prefix is itself a prefix of at least one complete JSON
    document (no backtracking over already accepted characters). ``finish``
    requires a complete value. Only bounded structural state (depth + the
    in-progress token) is retained.
    """

    def __init__(self, max_depth: int = 32) -> None:
        self.max_depth = max_depth
        self.consumed = 0
        self._stack: list[int] = []          # _OBJECT / _ARRAY
        self._empty: list[bool] = []         # container still empty
        # high-level state
        self._want_value = True              # at a position where a value may start
        self._want_key = False               # object awaiting a key string
        self._want_colon = False
        self._after_value = False            # value completed, delimiter expected
        # in-progress token state
        self._in_string = False
        self._string_is_key = False
        self._escape = False
        self._literal: str | None = None
        self._number = ""
        self._root_started = False
        self._root_done = False

    def feed(self, text: str) -> tuple[str, ValidationError | None]:
        accepted = 0
        for ch in text:
            if not self._step(ch):
                position = self.consumed + accepted
                self.consumed += accepted
                return text[:accepted], ValidationError(
                    code=ErrorCode.CONSTRAINT_VIOLATION,
                    message=f"invalid JSON at character {ch!r}",
                    position=position,
                )
            accepted += 1
        self.consumed += accepted
        return text, None

    # -- token transitions --------------------------------------------------

    def _start_value_token(self, ch: str) -> bool:
        if ch == '"':
            self._in_string = True
            self._string_is_key = False
            return True
        if ch == "{":
            return self._open_container(_OBJECT)
        if ch == "[":
            return self._open_container(_ARRAY)
        if ch in "-0123456789":
            self._number = ch
            return True
        if any(lit.startswith(ch) for lit in _LITERALS):
            self._literal = ch
            if self._literal in _LITERALS:
                self._complete_scalar()
            return True
        return False

    def _open_container(self, kind: int) -> bool:
        if len(self._stack) >= self.max_depth:
            return False
        self._stack.append(kind)
        self._empty.append(True)
        self._want_value = kind == _ARRAY
        self._want_key = kind == _OBJECT
        self._after_value = False
        return True

    def _complete_scalar(self) -> None:
        self._literal = None
        self._number = ""
        self._value_done()

    def _value_done(self) -> None:
        self._want_value = False
        self._after_value = True
        if not self._stack:
            self._root_done = True

    def _close_container(self, kind: int) -> bool:
        if not self._stack or self._stack[-1] != kind:
            return False
        # empty containers are allowed to close at any time
        self._stack.pop()
        self._empty.pop()
        if self._stack:
            self._empty[-1] = False
        self._want_key = False
        self._want_value = False
        self._after_value = True
        if not self._stack:
            self._root_done = True
        return True

    # -- single-character step ----------------------------------------------

    def _step(self, ch: str) -> bool:
        if self._root_done:
            return ch in _WHITESPACE

        if self._in_string:
            return self._step_string(ch)
        if self._literal is not None:
            token = self._literal + ch
            if any(lit.startswith(token) for lit in _LITERALS):
                self._literal = token
                if token in _LITERALS:
                    self._complete_scalar()
                return True
            return False
        if self._number:
            token = self._number + ch
            if _NUMBER_PREFIX_RE.fullmatch(token):
                self._number = token
                self._after_value = bool(_NUMBER_FULL_RE.fullmatch(token))
                return True
            # number token ends here; ch must be delimiter/close
            if not _NUMBER_FULL_RE.fullmatch(self._number):
                return False
            self._number = ""
            self._value_done()
            # fall through to process ch as delimiter

        if ch in _WHITESPACE:
            return True
        if self._want_colon:
            self._want_colon = False
            if ch != ":":
                return False
            self._want_value = True
            return True
        if self._want_key:
            if ch == "}" and self._empty[-1]:
                return self._close_container(_OBJECT)
            if ch == '"':
                self._in_string = True
                self._string_is_key = True
                self._want_key = False
                self._empty[-1] = False
                return True
            return False
        if self._want_value:
            if ch == "]" and self._stack and self._empty[-1]:
                return self._close_container(_ARRAY)
            self._root_started = True
            if self._stack:
                self._empty[-1] = False
            return self._start_value_token(ch)
        if self._after_value and self._stack:
            kind = self._stack[-1]
            if ch == ",":
                if kind == _OBJECT:
                    self._want_key = True
                else:
                    self._want_value = True
                self._after_value = False
                return True
            close = "}" if kind == _OBJECT else "]"
            if ch == close:
                return self._close_container(kind)
            return False
        return False

    def _step_string(self, ch: str) -> bool:
        if self._escape:
            self._escape = False
            return True
        if ch == "\\":
            self._escape = True
            return True
        if ch != '"':
            return True
        self._in_string = False
        if self._string_is_key:
            self._want_colon = True
        else:
            self._value_done()
        return True

    def finish(self) -> ValidationError | None:
        # A number ending without delimiter is valid if the token is complete.
        if self._number and _NUMBER_FULL_RE.fullmatch(self._number):
            self._number = ""
            self._value_done()
        if (
            self._in_string
            or self._escape
            or self._literal is not None
            or self._number
            or self._stack
            or not self._root_started
            or not self._root_done
        ):
            return ValidationError(
                code=ErrorCode.CONSTRAINT_VIOLATION,
                message="incomplete JSON document",
                position=self.consumed,
            )
        return None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def build_constraint(spec: ConstraintSpec) -> StreamingConstraint:
    if spec.type == ConstraintType.LENGTH:
        try:
            max_length = int(spec.params["max"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("length constraint requires integer 'max'") from exc
        return LengthConstraint(max_length)
    if spec.type == ConstraintType.ENUM:
        values = spec.params.get("values")
        if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
            raise ValueError("enum constraint requires non-empty list[str] 'values'")
        return EnumConstraint(values)
    if spec.type == ConstraintType.REGEX:
        pattern = spec.params.get("pattern")
        if not isinstance(pattern, str):
            raise ValueError("regex constraint requires string 'pattern'")
        return RegexConstraint(pattern)
    if spec.type == ConstraintType.STRUCTURED:
        kind = spec.params.get("kind", "json")
        if kind != "json":
            raise ValueError(f"unsupported structured format: {kind!r}")
        max_depth = int(spec.params.get("max_depth", 32))
        return JsonConstraint(max_depth=max_depth)
    raise ValidationError(
        code=ErrorCode.CONSTRAINT_TYPE_UNKNOWN,
        message=f"unknown constraint type {spec.type!r}",
    )
