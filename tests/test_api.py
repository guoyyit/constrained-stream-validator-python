"""End-to-end HTTP API tests."""

import pytest
from fastapi.testclient import TestClient

from stream_validator.api import create_app
from stream_validator.manager import SessionManager
from stream_validator.registry import ConstraintRegistry


@pytest.fixture
def client():
    reg = ConstraintRegistry()
    mgr = SessionManager(reg, reap_interval=None)
    app = create_app(mgr, reg)
    with TestClient(app) as c:
        yield c


def _register(client, name, spec, version=None):
    r = client.put(f"/constraints/{name}", json=spec)
    assert r.status_code == 200, r.text
    if version is not None:
        assert r.json()["version"] == version
    return r.json()


class TestConstraints:
    def test_health(self, client):
        assert client.get("/health").json() == {"ok": True}

    def test_register_list_get(self, client):
        _register(client, "c", {"type": "max_length", "config": {"max_length": 3}}, 1)
        _register(client, "c", {"type": "enum", "config": {"values": ["a"]}}, 2)
        listed = client.get("/constraints").json()
        assert listed["c"]["latest"] == 2
        assert client.get("/constraints/c").json()["type"] == "enum"
        assert client.get("/constraints/c?version=1").json()["max_length"] == 3

    def test_register_invalid_spec_400(self, client):
        r = client.put("/constraints/bad",
                       json={"type": "enum", "config": {"values": []}})
        assert r.status_code == 400
        assert r.json()["error"]["code"] == "bad_request"

    def test_unknown_constraint_404(self, client):
        assert client.get("/constraints/nope").status_code == 404


class TestSessions:
    def test_full_drop_flow(self, client):
        _register(client, "ans", {"type": "enum", "config": {"values": ["yes"]}})
        sid = client.post("/sessions",
                          json={"constraint": "ans", "policy": "drop"}).json()["session_id"]
        r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "yeXs"})
        assert r.status_code == 200
        body = r.json()
        assert body["emitted"] == "yes" and body["dropped_chars"] == 1
        r = client.post(f"/sessions/{sid}/chunks", json={"seq": 1, "final": True})
        assert r.json()["status"] == "finalized"

    def test_abort_returns_error_code_offset_prefix(self, client):
        _register(client, "cap", {"type": "max_length", "config": {"max_length": 4}})
        sid = client.post("/sessions",
                          json={"constraint": "cap", "policy": "abort"}).json()["session_id"]
        r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "abcdef"})
        assert r.status_code == 422
        v = r.json()["violation"]
        assert v["code"] == "length_exceeded"
        assert v["offset"] == 4
        assert v["consumed_prefix"] == "abcd"

    def test_json_constraint_flow(self, client):
        _register(client, "doc", {"type": "json", "config": {"required_keys": ["id"]}})
        sid = client.post("/sessions",
                          json={"constraint": "doc", "policy": "abort"}).json()["session_id"]
        r = client.post(f"/sessions/{sid}/chunks",
                        json={"seq": 0, "text": '{"id": 42}', "final": True})
        assert r.status_code == 200 and r.json()["status"] == "finalized"

    def test_missing_required_key_422(self, client):
        _register(client, "doc", {"type": "json", "config": {"required_keys": ["id"]}})
        sid = client.post("/sessions",
                          json={"constraint": "doc", "policy": "abort"}).json()["session_id"]
        r = client.post(f"/sessions/{sid}/chunks",
                        json={"seq": 0, "text": '{"x": 1}', "final": True})
        assert r.status_code == 422
        assert r.json()["violation"]["code"] == "invalid_json"

    def test_version_switch_over_http(self, client):
        _register(client, "v", {"type": "max_length", "config": {"max_length": 2}}, 1)
        _register(client, "v", {"type": "max_length", "config": {"max_length": 8}}, 2)
        old = client.post("/sessions",
                          json={"constraint": "v", "policy": "abort", "version": 1}).json()
        new = client.post("/sessions",
                          json={"constraint": "v", "policy": "abort"}).json()
        assert client.post(f"/sessions/{old['session_id']}/chunks",
                           json={"seq": 0, "text": "abc"}).status_code == 422
        r = client.post(f"/sessions/{new['session_id']}/chunks",
                        json={"seq": 0, "text": "abc"})
        assert r.status_code == 200

    def test_create_with_unknown_constraint(self, client):
        r = client.post("/sessions", json={"constraint": "ghost", "policy": "abort"})
        assert r.status_code == 404

    def test_duplicate_chunk_idempotent(self, client):
        _register(client, "cap", {"type": "max_length", "config": {"max_length": 10}})
        sid = client.post("/sessions",
                          json={"constraint": "cap", "policy": "abort"}).json()["session_id"]
        assert client.post(f"/sessions/{sid}/chunks",
                           json={"seq": 0, "text": "ab"}).status_code == 200
        r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "ab"})
        assert r.json()["status"] == "duplicate"
        assert client.get(f"/sessions/{sid}").json()["consumed_chars"] == 2

    def test_out_of_order_buffered(self, client):
        _register(client, "cap", {"type": "max_length", "config": {"max_length": 20}})
        sid = client.post("/sessions",
                          json={"constraint": "cap", "policy": "abort"}).json()["session_id"]
        r = client.post(f"/sessions/{sid}/chunks", json={"seq": 1, "text": "B"})
        assert r.json()["status"] == "buffered"
        client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "A"})
        assert client.get(f"/sessions/{sid}").json()["prefix_preview"] == "AB"

    def test_cancel_and_late_chunk(self, client):
        _register(client, "cap", {"type": "max_length", "config": {"max_length": 20}})
        sid = client.post("/sessions",
                          json={"constraint": "cap", "policy": "abort"}).json()["session_id"]
        assert client.post(f"/sessions/{sid}/cancel").status_code == 200
        r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "x"})
        assert r.status_code == 409 and r.json()["violation"]["code"] == "cancelled"
        assert client.post(f"/sessions/{sid}/cancel").status_code == 404

    def test_unknown_session_routes(self, client):
        assert client.get("/sessions/nope").status_code == 404
        r = client.post("/sessions/nope/chunks", json={"seq": 0, "text": "x"})
        assert r.status_code == 404

    def test_stats_endpoint(self, client):
        _register(client, "cap", {"type": "max_length", "config": {"max_length": 1}})
        sid = client.post("/sessions",
                          json={"constraint": "cap", "policy": "abort"}).json()["session_id"]
        client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "ab"})
        stats = client.get("/stats").json()
        assert stats["total_created"] == 1
        assert stats["total_aborted"] == 1
        assert stats["total_chars_emitted"] == 1

    def test_bad_seq_rejected(self, client):
        _register(client, "cap", {"type": "max_length", "config": {"max_length": 5}})
        sid = client.post("/sessions",
                          json={"constraint": "cap", "policy": "abort"}).json()["session_id"]
        r = client.post(f"/sessions/{sid}/chunks", json={"seq": -1, "text": "x"})
        assert r.status_code == 422  # pydantic validation error


class TestNonFatal:
    def test_nonfatal_constraint_warns_over_http(self, client):
        r = client.put("/constraints/soft",
                       json={"type": "max_length",
                             "config": {"max_length": 2, "fatal": False}})
        assert r.status_code == 200
        sid = client.post("/sessions",
                          json={"constraint": "soft", "policy": "abort"}).json()["session_id"]
        r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "abcd"})
        # warnings do not fail the request at HTTP level
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "warned"
        assert body["warning_count"] == 2
        assert body["emitted"] == "ab"
        assert client.get(f"/sessions/{sid}").json()["warning_count"] == 2
