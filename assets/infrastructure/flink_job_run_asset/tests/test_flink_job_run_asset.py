"""Real, committed tests for FlinkJobRunAssetComponent.

Stands up a real local HTTP server implementing the exact Flink JobManager
REST API shapes (verified directly against the official reference, not from
memory -- see component.py's module docstring: POST /jars/upload, POST
/jars/{jarid}/run, GET /jobs/{jobid}), and points the component at it --
real HTTP requests over the real `requests` library, mocking only the
far end (an actual Flink cluster, which this test can't spin up).
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


class _FlinkHandler(http.server.BaseHTTPRequestHandler):
    # Class-level, set per-test by the fixture.
    state_sequence = ["RUNNING", "FINISHED"]
    jobid = "abc123"
    upload_requests = []
    run_requests = []
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
        body = self.rfile.read(length) if length else b""
        if self.path == "/jars/upload":
            self.__class__.upload_requests.append({"content_type": self.headers.get("Content-Type", "")})
            self._send_json(200, {"filename": "/tmp/flink-web-upload/uuid123_myjob.jar", "status": "success"})
            return
        if self.path.startswith("/jars/") and self.path.endswith("/run"):
            jar_id = self.path.split("/")[2]
            parsed = json.loads(body) if body else {}
            self.__class__.run_requests.append({"jar_id": jar_id, "body": parsed})
            self._send_json(200, {"jobid": self.__class__.jobid, "applicationId": None})
            return
        self._send_json(404, {"errors": ["not found"]})

    def do_GET(self):
        if self.path == f"/jobs/{self.__class__.jobid}":
            seq = self.__class__.state_sequence
            idx = len(self.__class__.state_requests)
            state = seq[idx] if idx < len(seq) else seq[-1]
            self.__class__.state_requests.append(state)
            self._send_json(200, {"state": state})
            return
        self._send_json(404, {"errors": ["not found"]})

    def log_message(self, fmt, *args):
        pass


@pytest.fixture
def flink_server():
    _FlinkHandler.upload_requests = []
    _FlinkHandler.run_requests = []
    _FlinkHandler.state_requests = []
    _FlinkHandler.state_sequence = ["RUNNING", "FINISHED"]
    _FlinkHandler.jobid = "abc123"
    server = _TestServer(("127.0.0.1", 0), _FlinkHandler)
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
        asset_name="out", jobmanager_url=_url(server), jar_id="myjob.jar",
        poll_interval_seconds=0.01, wait_mode="until_finished",
    )
    args.update(kw)
    return mod.FlinkJobRunAssetComponent(**args)


def test_run_with_existing_jar_id_submits_and_polls_to_finished(flink_server):
    comp = _comp(flink_server)
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    assert not _FlinkHandler.upload_requests  # jar_id given directly, no upload
    assert _FlinkHandler.run_requests[0]["jar_id"] == "myjob.jar"

    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["job_id"].value == "abc123"
    assert meta["state"].value == "FINISHED"


def test_jar_path_uploads_first_and_uses_the_returned_filename(flink_server, tmp_path):
    jar = tmp_path / "myjob.jar"
    jar.write_bytes(b"fake jar bytes")
    comp = _comp(flink_server, jar_id=None, jar_path=str(jar))
    dg.materialize(list(comp.build_defs(None).assets))
    assert len(_FlinkHandler.upload_requests) == 1
    assert "multipart" in _FlinkHandler.upload_requests[0]["content_type"]
    assert _FlinkHandler.run_requests[0]["jar_id"] == "uuid123_myjob.jar"


def test_program_args_and_parallelism_are_sent_in_the_run_body(flink_server):
    comp = _comp(flink_server, program_args=["--date", "2024-01-01"], parallelism=4, entry_class="com.acme.Main")
    dg.materialize(list(comp.build_defs(None).assets))
    body = _FlinkHandler.run_requests[0]["body"]
    assert body["programArgsList"] == ["--date", "2024-01-01"]
    assert body["parallelism"] == 4
    assert body["entryClass"] == "com.acme.Main"


def test_job_failure_raises_by_default(flink_server):
    _FlinkHandler.state_sequence = ["RUNNING", "FAILED"]
    comp = _comp(flink_server)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "FAILED" in str(failure.event_specific_data.error)


def test_job_failure_does_not_raise_when_disabled(flink_server):
    _FlinkHandler.state_sequence = ["FAILED"]
    comp = _comp(flink_server, fail_on_job_failure=False)
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["state"].value == "FAILED"


def test_wait_mode_until_running_stops_polling_once_running_for_a_streaming_job(flink_server):
    _FlinkHandler.state_sequence = ["CREATED", "RUNNING", "RUNNING", "RUNNING"]
    comp = _comp(flink_server, wait_mode="until_running")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    # Only polled until RUNNING (index 1), not through the rest of the sequence.
    assert len(_FlinkHandler.state_requests) == 2
    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["state"].value == "RUNNING"


def test_wait_mode_fire_and_forget_never_polls(flink_server):
    comp = _comp(flink_server, wait_mode="fire_and_forget")
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    assert _FlinkHandler.state_requests == []


def test_both_jar_path_and_jar_id_is_rejected(flink_server):
    comp = _comp(flink_server, jar_id="a.jar", jar_path="/tmp/b.jar")
    with pytest.raises(ValueError, match="exactly one"):
        comp.build_defs(None)


def test_neither_jar_path_nor_jar_id_is_rejected(flink_server):
    comp = _comp(flink_server, jar_id=None)
    with pytest.raises(ValueError, match="exactly one"):
        comp.build_defs(None)


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(flink_server):
    comp = _comp(flink_server, partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    pdef = list(comp.build_defs(None).assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
