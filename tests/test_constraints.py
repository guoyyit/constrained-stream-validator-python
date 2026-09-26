"""Unit tests for the four streaming constraint types."""

import pytest

from stream_validator.constraints import build_constraint
from stream_validator.models import ConstraintSpec, ErrorCode, ValidationError
from stream_validator.models import ConstraintType as T


def spec(t, **params):
    return ConstraintSpec(type=t, params=params)


# -- length ----------------------------------------------------------------

def test_length_accepts_within_limit():
    c = build_constraint(spec(T.LENGTH, max=5))
    accepted, err = c.feed("ab")
    assert accepted == "ab" and err is None
    accepted, err = c.feed("cde")
    assert accepted == "cde" and err is None
    assert c.finish() is None


def test_length_rejects_beyond_limit_with_position():
    c = build_constraint(spec(T.LENGTH, max=3))
    accepted, err = c.feed("abcdef")
    assert accepted == "abc"
    assert err is not None
    assert err.code is ErrorCode.LENGTH_LIMIT_EXCEEDED
    assert err.position == 3


def test_length_zero():
    c = build_constraint(spec(T.LENGTH, max=0))
    accepted, err = c.feed("x")
    assert accepted == "" and err.code is ErrorCode.LENGTH_LIMIT_EXCEEDED


# -- enum ------------------------------------------------------------------

def testenum_accepts_exact_value_incrementally():
    c = build_constraint(spec(T.ENUM, values=["RED", "ROUND"]))
    for part in ("RO", "UN", "D"):
        accepted, err = c.feed(part)
        assert accepted == part and err is None
    assert c.finish() is None


def test_enum_rejects_dead_branch():
    c = build_constraint(spec(T.ENUM, values=["RED", "ROUND"]))
    accepted, err = c.feed("RO")
    assert err is None
    accepted, err = c.feed("X")
    assert accepted == ""
    assert err.code is ErrorCode.CONSTRAINT_VIOLATION
    assert err.position == 2


def test_enum_incomplete_at_finish():
    c = build_constraint(spec(T.ENUM, values=["RED"]))
    c.feed("RE")
    err = c.finish()
    assert err is not None and err.position == 2


def test_enum_invalid_params():
    with pytest.raises(ValueError):
        build_constraint(spec(T.ENUM, values=[]))


# -- regex -----------------------------------------------------------------

def test_regex_partial_match_allows_longer_prefix():
    c = build_constraint(spec(T.REGEX, pattern=r"[a-z]+[0-9]+"))
    accepted, err = c.feed("ab")
    assert accepted == "ab" and err is None
    accepted, err = c.feed("12")
    assert accepted == "12" and err is None
    assert c.finish() is None


def test_regex_rejects_impossible_character():
    c = build_constraint(spec(T.REGEX, pattern=r"[a-z]+"))
    accepted, err = c.feed("ab!")
    assert accepted == "ab"
    assert err.code is ErrorCode.CONSTRAINT_VIOLATION
    assert err.position == 2


def test_regex_incomplete_at_finish():
    c = build_constraint(spec(T.REGEX, pattern=r"[0-9]{4}"))
    c.feed("12")
    err = c.finish()
    assert err is not None


# -- json ------------------------------------------------------------------

@pytest.mark.parametrize(
    "text",
    [
        '{"a": [1, true], "b": null}',
        '{}',
        '[]',
        '[[]]',
        '  {"k": "v\\"q"}  ',
        '-0.5e-3',
        '12',
        '"hello"',
        'true',
        'false',
        'null',
        '[[[[[]]]]]',
    ],
)
def test_json_valid(text):
    c = build_constraint(spec(T.STRUCTURED))
    accepted, err = c.feed(text)
    assert err is None
    assert accepted == text
    assert c.finish() is None


@pytest.mark.parametrize(
    "text,bad_at",
    [
        ('{"a": 1,}', 8),
        ('{"a":x', 5),
        ('12x', 2),
        ('01', 1),
        ('{"a":1}x', 7),
        ('[1,,2]', 3),
        ('{:1}', 1),
        ('{"a" 1}', 5),
    ],
)
def test_json_invalid_reports_position(text, bad_at):
    c = build_constraint(spec(T.STRUCTURED))
    accepted, err = c.feed(text)
    assert err is not None
    assert err.position == bad_at
    assert accepted == text[:bad_at]


def test_json_incomplete_at_finish():
    c = build_constraint(spec(T.STRUCTURED))
    c.feed('{"a":')
    err = c.finish()
    assert err is not None and err.position == 5


def test_json_empty_input_incomplete():
    c = build_constraint(spec(T.STRUCTURED))
    assert c.finish() is not None


def test_json_depth_limit():
    c = build_constraint(spec(T.STRUCTURED, max_depth=2))
    accepted, err = c.feed("[[[]]]")
    assert err is not None
    assert accepted == "[["


def test_json_chunked_feed_is_consistent_with_whole():
    whole = '{"items": [1, 2, 3], "ok": true}'
    c1 = build_constraint(spec(T.STRUCTURED))
    for i in range(0, len(whole), 3):
        _accepted, err = c1.feed(whole[i : i + 3])
        assert err is None
    assert c1.finish() is None


def test_unknown_constraint_type():
    with pytest.raises(ValidationError) as ei:
        build_constraint(ConstraintSpec(type="bogus", params={}))  # type: ignore[arg-type]
    assert ei.value.code is ErrorCode.CONSTRAINT_TYPE_UNKNOWN
