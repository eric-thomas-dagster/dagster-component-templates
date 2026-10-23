"""Real, committed tests for SparkLivyBatchAssetComponent.

Stands up a real local HTTP server implementing the exact Livy REST API
shapes (verified directly against the official reference, not from memory
-- see component.py's module docstring: POST /batches, GET /batches/{id},
GET /batches/{id}/state, GET /batches/{id}/log), and points the component
at it -- real HTTP requests over the real `requests` library.
"""
import http.server
import json
import threading

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class _TestServer(http.server.ThreadingHTTPServer):
    request_queue_size = 64
    daemon_threads = True


class _LivyHandler(http.server.BaseHTTPRequestHandler):
    state_sequence = ["running", "success"]
    batch_id = 7
    submit_requests = []
    state_requests = []

    def _send_json(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        if self.path == "/batches":
            self.__class__.submit_requests.append(body)
            self._send_json(201, {"id": self.__class__.batch_id, "state": "starting", "appId": None, "log": []})
            return
        self._send_json(404, {"msg": "not found"})

    def do_GET(self):
        if self.path == f"/batches/{self.__class__.batch_id}/state":
            seq = self.__class__.state_sequence
            idx = len(self.__class__.state_requests)
            state = seq[idx] if idx < len(seq) else seq[-1]
            self.__class__.state_requests.append(state)
            self._send_json(200, {"id": self.__class__.batch_id, "state": state})
            return
        if self.path == f"/batches/{self.__class__.batch_id}":
            self._send_json(200, {"id": self.__class__.batch_id, "state": self.__class__.state_sequence[-1], "appId": "application_123_0001", "log": []})
            return
        if self.path.startswith(f"/batches/{self.__class__.batch_id}/log"):
            self._send_json(200, {"id": self.__class__.batch_id, "from": 0, "size": 2, "log": ["line one", "ERROR: boom"]})
            return
        self._send_json(404, {"msg": "not found"})

    def log_message(self, fmt, *args):
        pass


@pytest.fixture
def livy_server():
    _LivyHandler.submit_requests = []
    _LivyHandler.state_requests = []
    _LivyHandler.state_sequence = ["running", "success"]
    _LivyHandler.batch_id = 7
    server = _TestServer(("127.0.0.1", 0), _LivyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join()


def _url(server):
    return f"http://127.0.0.1:{server.server_address[1]}"


def _comp(server, **kw):
    args = dict(
        asset_name="out", livy_url=_url(server), file="s3://bucket/job.py",
        poll_interval_seconds=0.01,
    )
    args.update(kw)
    return mod.SparkLivyBatchAssetComponent(**args)


def test_submits_and_polls_to_success(livy_server):
    comp = _comp(livy_server)
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    assert _LivyHandler.submit_requests[0]["file"] == "s3://bucket/job.py"

    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["batch_id"].value == 7
    assert meta["state"].value == "success"
    assert meta["app_id"].value == "application_123_0001"


def test_optional_spark_params_are_sent_in_the_submit_body(livy_server):
    comp = _comp(
        livy_server, class_name="com.acme.Main", args=["--date", "2024-01-01"],
        driver_memory="2g", executor_memory="4g", num_executors=3, spark_conf={"spark.sql.shuffle.partitions": "10"},
    )
    dg.materialize(list(comp.build_defs(None).assets))
    body = _LivyHandler.submit_requests[0]
    assert body["className"] == "com.acme.Main"
    assert body["args"] == ["--date", "2024-01-01"]
    assert body["driverMemory"] == "2g"
    assert body["executorMemory"] == "4g"
    assert body["numExecutors"] == 3
    assert body["conf"] == {"spark.sql.shuffle.partitions": "10"}


def test_batch_failure_raises_with_log_tail(livy_server):
    _LivyHandler.state_sequence = ["running", "dead"]
    comp = _comp(livy_server)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    err = str(failure.event_specific_data.error)
    assert "dead" in err
    assert "ERROR: boom" in err


def test_batch_failure_does_not_raise_when_disabled(livy_server):
    _LivyHandler.state_sequence = ["error"]
    comp = _comp(livy_server, fail_on_batch_failure=False)
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["state"].value == "error"


def test_timeout_raises_when_stuck(livy_server):
    _LivyHandler.state_sequence = ["running"] * 5000
    comp = _comp(livy_server, timeout_seconds=1)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "did not reach a terminal state" in str(failure.event_specific_data.error)


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(livy_server):
    comp = _comp(livy_server, partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    pdef = list(comp.build_defs(None).assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
