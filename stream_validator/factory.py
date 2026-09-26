"""Constraint construction from plain configuration dicts."""

from __future__ import annotations

from .constraints.base import Constraint
from .constraints.enum_like import EnumConstraint
from .constraints.json_like import JsonConstraint
from .constraints.length import MaxLengthConstraint
from .constraints.regex_like import RegexConstraint


def build_constraint(name: str, version: int, spec: dict) -> Constraint:
    """Create a typed constraint from a ``spec``.

    Supported shapes::

        {"type": "max_length", "max_length": 1024}
        {"type": "regex", "pattern": "[0-9]+", "max_length": 10000}
        {"type": "enum", "values": ["a", "b"]}
        {"type": "json", "required_keys": ["id"]}
    """
    if not isinstance(spec, dict):
        raise TypeError("constraint spec must be an object")
    ctype = spec.get("type")
    fatal = bool(spec.get("fatal", True))
    if ctype == "max_length":
        max_length = spec.get("max_length")
        if not isinstance(max_length, int) or isinstance(max_length, bool) or max_length < 0:
            raise ValueError("max_length must be a non-negative integer")
        return MaxLengthConstraint(name, version, max_length, fatal=fatal)
    if ctype == "regex":
        pattern = spec.get("pattern")
        if not isinstance(pattern, str) or not pattern:
            raise ValueError("regex pattern must be a non-empty string")
        max_length = spec.get("max_length", 1 << 20)
        if not isinstance(max_length, int) or isinstance(max_length, bool) or max_length <= 0:
            raise ValueError("max_length must be a positive integer")
        return RegexConstraint(name, version, pattern, max_length=max_length, fatal=fatal)
    if ctype == "enum":
        values = spec.get("values")
        if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
            raise ValueError("enum values must be a list of strings")
        return EnumConstraint(name, version, values, fatal=fatal)
    if ctype == "json":
        required = spec.get("required_keys")
        if required is not None and (
            not isinstance(required, list) or not all(isinstance(v, str) for v in required)
        ):
            raise ValueError("required_keys must be a list of strings or null")
        return JsonConstraint(name, version, required_keys=required, fatal=fatal)
    raise ValueError(f"unknown constraint type: {ctype!r}")
