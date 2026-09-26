"""Versioned registry and constraint version-switching behaviour."""

import pytest

from stream_validator.constraints.length import MaxLengthConstraint
from stream_validator.constraints.regex_like import RegexConstraint
from stream_validator.registry import ConstraintRegistry, ConstraintNotFoundError


def test_versions_auto_increment_per_name():
    reg = ConstraintRegistry()
    assert reg.register("a", MaxLengthConstraint("a", 0, 1)) == 1
    assert reg.register("a", MaxLengthConstraint("a", 0, 2)) == 2
    assert reg.register("b", MaxLengthConstraint("b", 0, 9)) == 1
    assert reg.latest_version("a") == 2
    assert reg.list_versions("a") == [1, 2]


def test_latest_resolution_and_pin():
    reg = ConstraintRegistry()
    reg.register("a", MaxLengthConstraint("a", 0, 1))
    reg.register("a", MaxLengthConstraint("a", 0, 2))
    assert reg.get("a").max_length == 2
    assert reg.get("a", 1).max_length == 1


def test_missing_name_and_version():
    reg = ConstraintRegistry()
    with pytest.raises(ConstraintNotFoundError):
        reg.get("ghost")
    reg.register("a", MaxLengthConstraint("a", 0, 1))
    with pytest.raises(ConstraintNotFoundError):
        reg.get("a", 99)


def test_pinned_request_unaffected_by_update():
    """Requirement 1: in-flight requests keep using their original version."""
    reg = ConstraintRegistry()
    reg.register("a", MaxLengthConstraint("a", 0, 2))
    pinned = reg.get("a")  # session grabs v1 reference

    reg.register("a", RegexConstraint("a", 0, "x+"))
    # registry now serves v2 to new requests
    assert reg.get("a").type_name == "regex"
    # old pinned object still enforces the old semantics
    state = pinned.new_state()
    assert state.feed("a").ok and state.feed("b").ok
    assert not state.feed("c").ok


def test_remove_keeps_pinned_references_alive():
    reg = ConstraintRegistry()
    reg.register("a", MaxLengthConstraint("a", 0, 3))
    pinned = reg.get("a")
    reg.remove("a")
    with pytest.raises(ConstraintNotFoundError):
        reg.get("a")
    assert pinned.new_state().feed("z").ok


def test_remove_missing_raises():
    reg = ConstraintRegistry()
    with pytest.raises(ConstraintNotFoundError):
        reg.remove("ghost")


def test_describe_lists_all_versions():
    reg = ConstraintRegistry()
    reg.register("a", MaxLengthConstraint("a", 0, 1))
    reg.register("a", MaxLengthConstraint("a", 0, 2))
    desc = reg.describe()
    assert set(desc["a"]["versions"]) == {1, 2}
    assert desc["a"]["latest"] == 2
