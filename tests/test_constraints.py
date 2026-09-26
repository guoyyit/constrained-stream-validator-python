"""Unit tests for the four built-in constraint types."""

import pytest

from stream_validator.constraints.enum_like import EnumConstraint
from stream_validator.constraints.json_like import JsonConstraint
from stream_validator.constraints.length import MaxLengthConstraint
from stream_validator.constraints.regex_like import RegexConstraint
from stream_validator.factory import build_constraint
from stream_validator.models import ErrorCode


# ---------------------------------------------------------------- max length
class TestMaxLength:
    def test_within_limit(self):
        s = MaxLengthConstraint("c", 1, 3).new_state()
        assert all(s.feed(ch).ok for ch in "abc")
        assert s.finish() is None

    def test_over_limit_reports_error(self):
        s = MaxLengthConstraint("c", 1, 2).new_state()
        s.feed("a"); s.feed("b")
        r = s.feed("c")
        assert not r.ok and r.code is ErrorCode.LENGTH_EXCEEDED

    def test_failed_feed_is_atomic(self):
        s = MaxLengthConstraint("c", 1, 1).new_state()
        s.feed("a")
        assert not s.feed("b").ok
        # state still usable and still at the same boundary
        assert not s.feed("c").ok
        assert s.finish() is None

    def test_zero_length(self):
        s = MaxLengthConstraint("c", 1, 0).new_state()
        assert not s.feed("x").ok

    def test_invalid_construction(self):
        with pytest.raises(ValueError):
            MaxLengthConstraint("c", 1, -1)


# ---------------------------------------------------------------- enum
class TestEnum:
    def test_exact_match(self):
        s = EnumConstraint("c", 1, ["ok", "okay"]).new_state()
        for ch in "okay":
            assert s.feed(ch).ok
        assert s.finish() is None

    def test_prefix_is_incomplete_not_invalid(self):
        s = EnumConstraint("c", 1, ["yes"]).new_state()
        assert s.feed("y").ok and s.feed("e").ok
        f = s.finish()
        assert f is not None and f.code is ErrorCode.INCOMPLETE_OUTPUT

    def test_dead_branch(self):
        s = EnumConstraint("c", 1, ["yes"]).new_state()
        s.feed("y")
        r = s.feed("z")
        assert not r.ok and r.code is ErrorCode.ENUM_MISMATCH

    def test_states_are_isolated(self):
        c = EnumConstraint("c", 1, ["ab", "ac"])
        s1 = c.new_state(); s2 = c.new_state()
        s1.feed("a")
        assert s2.feed("a").ok and s2.feed("c").ok
        assert s2.finish() is None
        assert s1.feed("b").ok

    def test_duplicate_values_deduped(self):
        c = EnumConstraint("c", 1, ["x", "x"])
        assert c.values == ("x",)

    def test_empty_values_rejected(self):
        with pytest.raises(ValueError):
            EnumConstraint("c", 1, ["", ""])


# ---------------------------------------------------------------- regex
class TestRegex:
    def test_full_match(self):
        s = RegexConstraint("c", 1, r"\d{3}-[a-z]+").new_state()
        for ch in "123-ab":
            assert s.feed(ch).ok
        assert s.finish() is None

    def test_dead_prefix(self):
        s = RegexConstraint("c", 1, r"\d+").new_state()
        assert s.feed("1").ok
        r = s.feed("x")
        assert not r.ok and r.code is ErrorCode.REGEX_MISMATCH

    def test_incomplete_at_end(self):
        s = RegexConstraint("c", 1, r"\d{3}").new_state()
        for ch in "12":
            assert s.feed(ch).ok
        f = s.finish()
        assert f is not None and f.code is ErrorCode.REGEX_MISMATCH

    def test_length_cap(self):
        c = RegexConstraint("c", 1, r".*", max_length=2)
        s = c.new_state()
        assert s.feed("a").ok and s.feed("b").ok
        r = s.feed("c")
        assert not r.ok and r.code is ErrorCode.LENGTH_EXCEEDED

    def test_invalid_pattern(self):
        with pytest.raises(ValueError):
            RegexConstraint("c", 1, "(unclosed")


# ---------------------------------------------------------------- json
VALID_JSON = [
    "{}", "[]", "null", "true", "false", "123", "-0.5e-2",
    '"a string"', '[1,2,3]', '{"a": 1, "b": [true, null, {}]}',
    '  { "k" : [ 1 , 2 ] } ',
    '"escaped \\" quote"', '{"utf": "\\u0041"}',
]
INVALID_JSON = [
    "{", "}", "[", "01", "{a:1}", "{'a':1}", "[1,]",
    '{"a":1,}', "[1,,2]", "tru", "1.2.3", "nulx",
    '{"a":1}]', '"unterminated', "{'}->", "12e", "-",
    '{"a":1}x', "{'a':01}", '{"a":1e+}', "{,}", "[,1]",
    '"bare" x', "[ true false ]",
]


class TestJson:
    @pytest.mark.parametrize("text", VALID_JSON)
    def test_valid(self, text):
        s = JsonConstraint("c", 1).new_state()
        for ch in text:
            r = s.feed(ch)
            assert r.ok, (ch, r.message)
        assert s.finish() is None

    @pytest.mark.parametrize("text", INVALID_JSON)
    def test_invalid_or_incomplete(self, text):
        s = JsonConstraint("c", 1).new_state()
        for ch in text:
            r = s.feed(ch)
            if not r.ok:
                return
        assert s.finish() is not None

    @pytest.mark.parametrize("text", VALID_JSON)
    def test_every_prefix_is_streaming_valid(self, text):
        for n in range(len(text)):
            s = JsonConstraint("c", 1).new_state()
            for ch in text[:n]:
                assert s.feed(ch).ok

    def test_required_keys_present(self):
        s = JsonConstraint("c", 1, required_keys=["id", "name"]).new_state()
        for ch in '{"id": 1, "name": "x"}':
            assert s.feed(ch).ok
        assert s.finish() is None

    def test_required_keys_missing(self):
        s = JsonConstraint("c", 1, required_keys=["id"]).new_state()
        result = None
        for ch in '{"name": "x"}':
            result = s.feed(ch)
        # violation is reported when the top-level object closes
        assert not result.ok
        assert result.code is ErrorCode.INVALID_JSON
        assert "'id'" in result.message


# ---------------------------------------------------------------- factory
@pytest.mark.parametrize("spec", [
    {"type": "max_length", "max_length": 3},
    {"type": "regex", "pattern": "ab+"},
    {"type": "enum", "values": ["a"]},
    {"type": "json", "required_keys": []},
])
def test_factory_builds(spec):
    c = build_constraint("n", 2, spec)
    assert c.version == 2
    c.new_state()


@pytest.mark.parametrize("spec", [
    {"type": "unknown"},
    {"type": "max_length", "max_length": -1},
    {"type": "max_length", "max_length": "3"},
    {"type": "enum", "values": []},
    {"type": "enum", "values": [1]},
    {"type": "regex", "pattern": ""},
    {"type": "json", "required_keys": "nope"},
])
def test_factory_rejects(spec):
    with pytest.raises((ValueError, TypeError)):
        build_constraint("n", 1, spec)
