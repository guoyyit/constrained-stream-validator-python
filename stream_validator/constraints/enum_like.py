"""Enum-field constraint: the output must equal one of a fixed set of labels.

Streaming checking uses a read-only character trie shared by every request;
per-request state only remembers the current trie node and the consumed
length, so memory per request is O(stream length * 1) stack-free.
"""

from __future__ import annotations

from ..models import ErrorCode
from .base import Constraint, ConstraintState, CharResult

_TERMINAL = "\0terminal\0"


def _build_trie(values: tuple[str, ...]) -> dict:
    root: dict = {}
    for value in values:
        node = root
        for ch in value:
            node = node.setdefault(ch, {})
        node[_TERMINAL] = True
    return root


class EnumState(ConstraintState):
    def __init__(self, trie: dict) -> None:
        self._root = trie
        self._node = trie
        self._consumed = 0

    def feed(self, ch: str) -> CharResult:
        nxt = self._node.get(ch)
        if not isinstance(nxt, dict):
            return CharResult(
                ok=False,
                code=ErrorCode.ENUM_MISMATCH,
                message=f"no allowed enum value continues with {ch!r}",
            )
        self._node = nxt
        self._consumed += 1
        # When the node is terminal and has no children the output can only
        # end here; any further character must be rejected.
        final = _TERMINAL in self._node and len(self._node) == 1
        return CharResult(ok=True, final=final)

    def rollback(self) -> None:
        # Illegal feeds are rejected atomically, so nothing to undo in normal
        # flow; provided for stateful tooling.
        self._consumed = max(0, self._consumed - 1)

    def finish(self) -> CharResult | None:
        if _TERMINAL in self._node:
            return None
        return CharResult(
            ok=False,
            code=ErrorCode.INCOMPLETE_OUTPUT,
            message="input ends on a prefix that is not one of the allowed enum values",
        )

    @property
    def consumed(self) -> int:
        return self._consumed


class EnumConstraint(Constraint):
    type_name = "enum"

    def __init__(self, name: str, version: int, values: list[str], *, fatal: bool = True) -> None:
        super().__init__(name, version, fatal=fatal)
        clean = tuple(dict.fromkeys(v for v in values if v))
        if not clean:
            raise ValueError("enum constraint requires at least one non-empty value")
        self.values = clean
        self._trie = _build_trie(clean)

    def new_state(self) -> ConstraintState:
        return EnumState(self._trie)

    def to_dict(self) -> dict:
        return {
            "type": self.type_name,
            "name": self.name,
            "version": self.version,
            "values": list(self.values),
            "fatal": self.fatal,
        }
