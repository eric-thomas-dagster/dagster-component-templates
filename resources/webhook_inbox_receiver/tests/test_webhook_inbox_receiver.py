"""Real, committed tests for the webhook inbox receiver -- a standalone HTTP process, not a
Dagster component. Starts a REAL http.server instance on an ephemeral port in a background
thread and makes REAL HTTP requests against it with urllib.request (no mocking of the server
or its signature/JSON logic -- this repo's convention is to mock only an unavoidable paid/
external call, and there is none here).
"""
import hashlib
import hmac
import json
import threading
import urllib.error
import urllib.request

import pytest


@pytest.fixture()
def running_server(mod, tmp_path):
    """Starts a real server bound to port 0 (OS picks a free ephemeral port) and tears it
    down after the test. Yields (base_url, config, inbox_path)."""
    inbox_path = str(tmp_path / "inbox.jsonl")

    def _start(**config_overrides):
        config = mod.ReceiverConfig(inbox_path=inbox_path, **config_overrides)
        server = mod.build_server("127.0.0.1", 0, config)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, f"http://127.0.0.1:{port}", config

    servers = []

    def factory(**config_overrides):
        server, base_url, config = _start(**config_overrides)
        servers.append(server)
        return base_url, config

    yield factory, inbox_path

    for server in servers:
        server.shutdown()
        server.server_close()


def _post(base_url, path, body_bytes, headers=None):
    req = urllib.request.Request(
        base_url + path, data=body_bytes, method="POST", headers=headers or {"Content-Type": "application/json"}
    )
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def _read_inbox_lines(inbox_path):
    import os

    if not os.path.exists(inbox_path):
        return []
    with open(inbox_path, "r", encoding="utf-8") as f:
        return [line for line in f.read().split("\n") if line.strip()]


# --- unauthenticated mode -----------------------------------------------------------------

def test_no_secret_configured_accepts_unconditionally(running_server):
    factory, inbox_path = running_server
    base_url, _config = factory()

    status, body = _post(base_url, "/webhook", json.dumps({"job_id": "abc123", "status": "done"}).encode())

    assert status == 200
    assert json.loads(body)["status"] == "accepted"
    lines = _read_inbox_lines(inbox_path)
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["body"] == {"job_id": "abc123", "status": "done"}
    assert record["path"] == "/webhook"
    assert "received_at" in record and "remote_addr" in record


def test_unknown_path_rejected_404(running_server):
    factory, inbox_path = running_server
    base_url, _config = factory()

    status, _body = _post(base_url, "/not-the-webhook-path", b'{"a": 1}')

    assert status == 404
    assert _read_inbox_lines(inbox_path) == []


def test_non_json_body_rejected_400(running_server):
    factory, inbox_path = running_server
    base_url, _config = factory()

    status, body = _post(base_url, "/webhook", b"not json at all")

    assert status == 400
    assert "invalid JSON body" in json.loads(body)["error"]
    assert _read_inbox_lines(inbox_path) == []


def test_empty_body_rejected_400(running_server):
    factory, inbox_path = running_server
    base_url, _config = factory()

    status, _body = _post(base_url, "/webhook", b"")

    assert status == 400
    assert _read_inbox_lines(inbox_path) == []


# --- HMAC signature verification -----------------------------------------------------------

def _sign(secret: str, body: bytes, prefix: str = "") -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return prefix + digest


def test_valid_signature_accepted_and_written(running_server):
    factory, inbox_path = running_server
    base_url, _config = factory(
        signature_header_name="X-Signature-256",
        signature_secret="supersecret",
        signature_prefix="sha256=",
    )
    body = json.dumps({"job_id": "xyz"}).encode()
    sig = _sign("supersecret", body, prefix="sha256=")

    status, resp_body = _post(base_url, "/webhook", body, headers={"X-Signature-256": sig, "Content-Type": "application/json"})

    assert status == 200
    lines = _read_inbox_lines(inbox_path)
    assert len(lines) == 1
    assert json.loads(lines[0])["body"] == {"job_id": "xyz"}


def test_invalid_signature_rejected_401_and_nothing_written(running_server):
    factory, inbox_path = running_server
    base_url, _config = factory(
        signature_header_name="X-Signature-256",
        signature_secret="supersecret",
        signature_prefix="sha256=",
    )
    body = json.dumps({"job_id": "xyz"}).encode()
    wrong_sig = _sign("wrong-secret", body, prefix="sha256=")

    status, resp_body = _post(base_url, "/webhook", body, headers={"X-Signature-256": wrong_sig, "Content-Type": "application/json"})

    assert status == 401
    assert "signature verification failed" in json.loads(resp_body)["error"]
    assert _read_inbox_lines(inbox_path) == []


def test_missing_signature_header_rejected_401_when_secret_configured(running_server):
    factory, inbox_path = running_server
    base_url, _config = factory(signature_header_name="X-Signature-256", signature_secret="supersecret")
    body = json.dumps({"a": 1}).encode()

    status, _resp_body = _post(base_url, "/webhook", body)

    assert status == 401
    assert _read_inbox_lines(inbox_path) == []


def test_signature_verified_before_json_parsing_rejects_bad_sig_on_non_json_body(running_server):
    """Signature check happens even on a non-JSON body -- an attacker can't use a malformed
    body to probe whether verification is enabled, and a bad-sig request never reaches the
    JSON parser."""
    factory, inbox_path = running_server
    base_url, _config = factory(signature_header_name="X-Signature-256", signature_secret="supersecret")

    status, resp_body = _post(base_url, "/webhook", b"not json", headers={"X-Signature-256": "deadbeef"})

    assert status == 401
    assert "signature" in json.loads(resp_body)["error"]


# --- concurrency -------------------------------------------------------------------------

def test_concurrent_posts_do_not_corrupt_inbox_file(running_server):
    factory, inbox_path = running_server
    base_url, _config = factory()

    n_threads = 20
    errors = []

    def worker(i):
        try:
            status, _ = _post(base_url, "/webhook", json.dumps({"i": i}).encode())
            if status != 200:
                errors.append((i, status))
        except Exception as e:  # noqa: BLE001
            errors.append((i, str(e)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert errors == []
    lines = _read_inbox_lines(inbox_path)
    assert len(lines) == n_threads
    seen_ids = set()
    for line in lines:
        record = json.loads(line)  # every single line must parse cleanly -- no interleaved writes
        seen_ids.add(record["body"]["i"])
    assert seen_ids == set(range(n_threads))


# --- verify_signature unit coverage --------------------------------------------------------

def test_verify_signature_rejects_missing_header(mod):
    config = mod.ReceiverConfig(inbox_path="unused", signature_secret="s")
    assert mod.verify_signature(config, b"body", None) is False
    assert mod.verify_signature(config, b"body", "") is False


def test_verify_signature_unknown_algorithm_rejects(mod):
    config = mod.ReceiverConfig(inbox_path="unused", signature_secret="s", signature_algorithm="not_a_real_algo")
    assert mod.verify_signature(config, b"body", "whatever") is False


def test_verify_signature_strips_configured_prefix(mod):
    config = mod.ReceiverConfig(inbox_path="unused", signature_secret="s", signature_prefix="sha256=")
    digest = hmac.new(b"s", b"payload", hashlib.sha256).hexdigest()
    assert mod.verify_signature(config, b"payload", "sha256=" + digest) is True
    assert mod.verify_signature(config, b"payload", "sha256=not-the-right-digest") is False
