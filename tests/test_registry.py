"""Tests for versioned constraint registry."""

import pytest

from stream_validator.models import ConstraintSpec, ErrorCode, ValidationError
from stream_validator.models import ConstraintType as T
from stream_validator.registry import ConstraintRegistry


def test_register_assigns_monotonic_versions():
    reg = ConstraintRegistry()
    v1 = reg.register("c", ConstraintSpec(T.LENGTH, {"max": 10}))
    v2 = reg.register("c", ConstraintSpec(T.LENGTH, {"max": 20}))
    assert (v1.version, v2.version) == (1, 2)
    assert reg.latest_version("c") == 2
    assert reg.get("c").spec.params["max"] == 20
    assert reg.get("c", 1).spec.params["max"] == 10


def test_old_version_remains_usable_after_update():
    reg = ConstraintRegistry()
    v1 = reg.register("c", ConstraintSpec(T.LENGTH, {"max": 2}))
    reg.register("c", ConstraintSpec(T.LENGTH, {"max": 9}))
    validator = v1.build_validator()
    accepted, err = validator.feed("abc")
    assert accepted == "ab"
    assert err.code is ErrorCode.LENGTH_LIMIT_EXCEEDED


def test_get_unknown_constraint_raises():
    reg = ConstraintRegistry()
    with pytest.raises(ValidationError) as ei:
        reg.get("missing")
    assert ei.value.code is ErrorCode.UNKNOWN_CONSTRAINT


def test_get_unknown_version_raises():
    reg = ConstraintRegistry()
    reg.register("c", ConstraintSpec(T.LENGTH, {"max": 1}))
    with pytest.raises(ValidationError):
        reg.get("c", 99)


def test_register_invalid_spec_fails_fast():
    reg = ConstraintRegistry()
    with pytest.raises(ValueError):
        reg.register("bad", ConstraintSpec(T.LENGTH, {}))
    assert "bad" not in reg.names()


def test_each_build_validator_is_independent_instance():
    reg = ConstraintRegistry()
    v = reg.register("c", ConstraintSpec(T.ENUM, {"values": ["OK"]}))
    a, b = v.build_validator(), v.build_validator()
    a.feed("O")
    accepted_b, err_b = b.feed("OK")
    assert accepted_b == "OK" and err_b is None
