"""End-to-end HTTP tests with FastAPI TestClient."""

import pytest
from fastapi.testclient import TestClient

from stream_validator.service import create_app


@pytest.fixture()
def client():
    return TestClient(create_app())


def _register(client, name, ctype, **params):
    r = client.post(f"/constraints/{name}", json={"type": ctype, "params": params})
    assert r.status_code == 200, r.text
    return r.json()["version"]


def _create(client, constraint, **kw):
    body = {"constraint": constraint, **kw}
    r = client.post("/sessions", json=body)
    assert r.status_code == 201, r.text
    return r.json()["session_id"]


def test_full_legal_flow(client):
    _register(client, "word", "regex", pattern="[a-z]+")
    sid = _create(client, "word")
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "abc"})
    assert r.status_code == 200 and r.json()["accepted_text"] == "abc"
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 3, "text": "de", "final": True})
    assert r.json()["accepted_text"] == "de"
    r = client.post(f"/sessions/{sid}/finish")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "finished" and body["output"] == "abcde"


def test_violation_abort_returns_error_details(client):
    _register(client, "word", "regex", pattern="[a-z]+")
    sid = _create(client, "word", policy="abort")
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "ab1"})
    body = r.json()
    assert r.status_code == 409
    assert body["error"]["code"] == "constraint_violation"
    assert body["error"]["position"] == 2
    assert body["error"]["consumed_prefix"] == "ab"
    assert body["accepted_text"] == "ab"


def test_truncate_policy(client):
    _register(client, "word", "regex", pattern="[a-z]+")
    sid = _create(client, "word", policy="truncate")
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "ab1cd"})
    assert r.status_code == 409
    assert r.json()["accepted_text"] == "ab"
    r = client.post(f"/sessions/{sid}/finish")
    assert r.json()["status"] == "truncated"


def test_skip_policy(client):
    _register(client, "word", "regex", pattern="[a-z]+")
    sid = _create(client, "word", policy="skip")
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "ab1cd"})
    body = r.json()
    assert body["accepted_text"] == "abcd"
    assert body["rejected_text"] == "1"


def test_out_of_order_and_duplicate_chunks(client):
    _register(client, "word", "regex", pattern="abcdef")
    sid = _create(client, "word")
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 3, "text": "def"})
    assert r.json()["accepted_text"] == ""
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 3, "text": "def"})
    assert r.status_code == 200  # duplicate tolerated
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "abc"})
    assert r.json()["accepted_text"] == "abcdef"
    stats = client.get(f"/sessions/{sid}/stats").json()
    assert stats["chunks_duplicate"] == 1


def test_cancel_drops_later_chunks(client):
    _register(client, "word", "regex", pattern="[a-z]+")
    sid = _create(client, "word")
    client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "ab"})
    r = client.post(f"/sessions/{sid}/cancel")
    assert r.status_code == 200
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 2, "text": "cd"})
    assert r.status_code == 409 and r.json()["error"]["code"] == "cancelled"
    r = client.post(f"/sessions/{sid}/finish")
    assert r.json()["status"] == "cancelled"
    stats = client.get(f"/sessions/{sid}/stats").json()
    assert stats["chunks_received"] == 1


def test_constraint_version_switch(client):
    v1 = _register(client, "c", "length", max=2)
    v2 = _register(client, "c", "length", max=5)
    assert (v1, v2) == (1, 2)
    old = _create(client, "c", constraint_version=1)
    new = _create(client, "c")  # latest
    r_old = client.post(f"/sessions/{old}/chunks", json={"seq": 0, "text": "abc"})
    r_new = client.post(f"/sessions/{new}/chunks", json={"seq": 0, "text": "abc"})
    assert r_old.status_code == 409
    assert r_new.status_code == 200


def test_unknown_constraint_and_bad_policy(client):
    r = client.post("/sessions", json={"constraint": "nope"})
    assert r.status_code == 400 and r.json()["error_code"] == "unknown_constraint"
    _register(client, "c", "length", max=1)
    r = client.post("/sessions", json={"constraint": "c", "policy": "bogus"})
    assert r.status_code == 400


def test_length_limit_and_global_stats(client):
    _register(client, "c", "length", max=3)
    sid = _create(client, "c")
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "abcdef"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "length_limit_exceeded"
    g = client.get("/stats").json()
    assert g["total_violations"] >= 1
    assert g["total_bytes_out"] == 3


def test_structured_constraint(client):
    _register(client, "json", "structured", kind="json")
    sid = _create(client, "json")
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": '{"a": 1'})
    assert r.status_code == 200
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 7, "text": "}"})
    assert r.status_code == 200
    r = client.post(f"/sessions/{sid}/finish")
    assert r.json()["status"] == "finished"


def test_enum_constraint(client):
    _register(client, "e", "enum", values=["RED", "ROUND"])
    sid = _create(client, "e")
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 0, "text": "RO"})
    assert r.status_code == 200
    r = client.post(f"/sessions/{sid}/chunks", json={"seq": 2, "text": "X"})
    assert r.status_code == 409
    assert r.json()["error"]["position"] == 2
