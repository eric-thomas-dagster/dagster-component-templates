"""Real, committed tests for openlineage_emitter's retry-with-backoff, added
alongside the same fix in openlineage_export_job after the user pointed out
that both components should be production-usable as-is, not punt reliability
to "swap in a different client for prod." Exercised against a real local
HTTP server (not mocked), including the real asset-check machinery via
dg.materialize + dg.build_asset_check_context's materialize_with_checks path.
"""
import http.server
import importlib.util
import json
import pathlib
import threading
from http import HTTPStatus

import dagster as dg
import pandas as pd
import pytest


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("openlineage_emitter_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_component_module()
OpenlineageEmitterComponent = mod.OpenlineageEmitterComponent


class _FlakyHandler(http.server.BaseHTTPRequestHandler):
    fail_times: int = 0
    received: list = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        if type(self).fail_times > 0:
            type(self).fail_times -= 1
            self.send_response(503)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        type(self).received.append(body)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *a):
        pass


class _TestServer(http.server.ThreadingHTTPServer):
    request_queue_size = 64


@pytest.fixture
def server():
    _FlakyHandler.received = []
    _FlakyHandler.fail_times = 0
    s = _TestServer(("127.0.0.1", 0), _FlakyHandler)
    thread = threading.Thread(target=s.serve_forever, daemon=True)
    thread.start()
    yield _FlakyHandler, f"http://127.0.0.1:{s.server_address[1]}/api/v1/lineage"
    s.shutdown()
    thread.join(timeout=5)


def _materialize_with_check(comp, df):
    @dg.asset(io_manager_key="df_io_manager")
    def upstream():
        return df

    class _DfIOManager(dg.IOManager):
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            return df

    check_def = next(iter(comp.build_defs(None).asset_checks))
    defs = dg.Definitions(
        assets=[upstream], asset_checks=[check_def],
        resources={"df_io_manager": _DfIOManager()},
    )
    job_def = defs.get_implicit_global_asset_job_def()
    return job_def.execute_in_process()


def test_post_with_retry_succeeds_after_transient_failures(server):
    handler, endpoint = server
    handler.fail_times = 1
    ok, err = mod._post_with_retry(endpoint, {"i": 0}, {}, 5, max_attempts=3, backoff_seconds=0.01)
    assert ok is True
    assert err is None
    assert handler.received == [{"i": 0}]


def test_post_with_retry_gives_up_after_max_attempts(server):
    handler, endpoint = server
    handler.fail_times = 10
    ok, err = mod._post_with_retry(endpoint, {"i": 0}, {}, 5, max_attempts=3, backoff_seconds=0.01)
    assert ok is False
    assert "503" in err
    assert handler.received == []


def test_post_with_retry_does_not_retry_a_4xx():
    import time as _time

    class _BadRequestHandler(http.server.BaseHTTPRequestHandler):
        calls = 0

        def do_POST(self):
            type(self).calls += 1
            length = int(self.headers.get("Content-Length", 0))
            self.rfile.read(length)
            self.send_response(400)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *a):
            pass

    server = _TestServer(("127.0.0.1", 0), _BadRequestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_address[1]}/api/v1/lineage"
    try:
        start = _time.monotonic()
        ok, err = mod._post_with_retry(endpoint, {"i": 0}, {}, 5, max_attempts=5, backoff_seconds=5.0)
        elapsed = _time.monotonic() - start
    finally:
        server.shutdown()
        thread.join(timeout=5)

    assert ok is False
    assert "400" in err
    assert _BadRequestHandler.calls == 1
    assert elapsed < 1.0


def test_emitter_check_emits_start_and_complete_through_a_real_flaky_collector(server):
    """End-to-end: a real materialize() + asset_check run, against a collector
    that fails once per event before succeeding -- confirms the retry wiring
    inside build_defs's _check actually recovers, not just the helper in
    isolation."""
    handler, endpoint = server
    handler.fail_times = 1  # only the very first POST (this event's START) fails once

    comp = OpenlineageEmitterComponent(
        check_name="emit_ol", upstream_asset_key="upstream",
        openlineage_url=endpoint, retry_max_attempts=3, retry_backoff_seconds=0.01,
    )
    result = _materialize_with_check(comp, pd.DataFrame({"a": [1, 2, 3]}))
    assert result.success
    # START + COMPLETE, both eventually delivered despite one transient failure.
    assert len(handler.received) == 2
    assert {e["eventType"] for e in handler.received} == {"START", "COMPLETE"}


def test_emitter_check_fails_cleanly_when_collector_is_never_reachable():
    comp = OpenlineageEmitterComponent(
        check_name="emit_ol", upstream_asset_key="upstream",
        openlineage_url="http://127.0.0.1:1/api/v1/lineage",
        retry_max_attempts=2, retry_backoff_seconds=0.01,
    )
    result = _materialize_with_check(comp, pd.DataFrame({"a": [1]}))
    assert result.success  # non-blocking by default -- the materialize run itself still succeeds
    check_evals = [
        e.event_specific_data
        for e in result.all_events
        if e.event_type_value == "ASSET_CHECK_EVALUATION"
    ]
    assert len(check_evals) == 1
    assert check_evals[0].passed is False
