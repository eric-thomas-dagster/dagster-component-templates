"""Real, committed tests for ComfyUIWorkflowRunAssetComponent.

Stands up a real local HTTP server implementing the exact ComfyUI API
shapes (verified directly against the official reference, not from memory
-- POST /prompt, GET /history/{prompt_id}, GET /view), and points the
component at it -- real HTTP requests over the real `requests` library.
"""
import http.server
import json
import threading
from urllib.parse import urlparse, parse_qs

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()

WORKFLOW = {
    "3": {"class_type": "KSampler", "inputs": {"seed": 0}},
    "6": {"class_type": "CLIPTextEncode", "inputs": {"text": "a dog"}},
}


class _TestServer(http.server.ThreadingHTTPServer):
    request_queue_size = 64
    daemon_threads = True


class _ComfyHandler(http.server.BaseHTTPRequestHandler):
    prompt_id = "abc-123"
    node_errors = {}
    completed_after_n_polls = 1
    status_str = "success"
    images = [{"filename": "ComfyUI_00001_.png", "subfolder": "", "type": "output"}]
    submit_requests = []
    history_polls = []

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
        if self.path == "/prompt":
            self.__class__.submit_requests.append(body)
            self._send_json(200, {"prompt_id": self.__class__.prompt_id, "number": 1, "node_errors": self.__class__.node_errors})
            return
        self._send_json(404, {})

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == f"/history/{self.__class__.prompt_id}":
            self.__class__.history_polls.append(True)
            completed = len(self.__class__.history_polls) >= self.__class__.completed_after_n_polls
            self._send_json(200, {
                self.__class__.prompt_id: {
                    "outputs": {"9": {"images": self.__class__.images}} if completed else {},
                    "status": {"status_str": self.__class__.status_str if completed else "running", "completed": completed, "messages": []},
                }
            })
            return
        if parsed.path == "/view":
            qs = parse_qs(parsed.query)
            payload = f"fake-png-bytes-for-{qs.get('filename', ['?'])[0]}".encode()
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self._send_json(404, {})

    def log_message(self, fmt, *args):
        pass


@pytest.fixture
def comfy_server():
    _ComfyHandler.prompt_id = "abc-123"
    _ComfyHandler.node_errors = {}
    _ComfyHandler.completed_after_n_polls = 1
    _ComfyHandler.status_str = "success"
    _ComfyHandler.images = [{"filename": "ComfyUI_00001_.png", "subfolder": "", "type": "output"}]
    _ComfyHandler.submit_requests = []
    _ComfyHandler.history_polls = []
    server = _TestServer(("127.0.0.1", 0), _ComfyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join()


def _url(server):
    return f"http://127.0.0.1:{server.server_address[1]}"


def _comp(server, tmp_path, **kw):
    args = dict(
        asset_name="out", comfyui_url=_url(server), workflow=dict(WORKFLOW),
        output_dir=str(tmp_path / "out"), poll_interval_seconds=0.01,
    )
    args.update(kw)
    return mod.ComfyUIWorkflowRunAssetComponent(**args)


def test_submits_polls_and_downloads_the_image(comfy_server, tmp_path):
    comp = _comp(comfy_server, tmp_path)
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    assert _ComfyHandler.submit_requests[0]["prompt"] == WORKFLOW

    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["prompt_id"].value == "abc-123"
    assert meta["image_count"].value == 1
    saved_path = meta["first_image"].value
    assert (tmp_path / "out" / "ComfyUI_00001_.png").exists()
    with open(saved_path, "rb") as f:
        assert f.read() == b"fake-png-bytes-for-ComfyUI_00001_.png"


def test_node_overrides_set_specific_node_inputs(comfy_server, tmp_path):
    comp = _comp(comfy_server, tmp_path, node_overrides={"6": {"text": "a cat in space"}})
    dg.materialize(list(comp.build_defs(None).assets))
    submitted = _ComfyHandler.submit_requests[0]["prompt"]
    assert submitted["6"]["inputs"]["text"] == "a cat in space"
    assert submitted["3"]["inputs"]["seed"] == 0  # untouched node


def test_node_overrides_substitute_partition_tokens(comfy_server, tmp_path):
    comp = _comp(
        comfy_server, tmp_path, node_overrides={"6": {"text": "a photo from {partition_key}"}},
        partition_type="daily", partition_start="2024-01-01",
    )
    dg.materialize(list(comp.build_defs(None).assets), partition_key="2024-01-02")
    submitted = _ComfyHandler.submit_requests[0]["prompt"]
    assert submitted["6"]["inputs"]["text"] == "a photo from 2024-01-02"


def test_node_overrides_for_unknown_node_id_is_rejected(comfy_server, tmp_path):
    comp = _comp(comfy_server, tmp_path, node_overrides={"99": {"text": "x"}})
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "99" in str(failure.event_specific_data.error)


def test_node_errors_at_submission_raise_before_polling(comfy_server, tmp_path):
    _ComfyHandler.node_errors = {"6": {"errors": [{"message": "Required input is missing"}]}}
    comp = _comp(comfy_server, tmp_path)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "Required input is missing" in str(failure.event_specific_data.error)
    assert _ComfyHandler.history_polls == []  # never polled -- rejected at submission


def test_execution_error_status_raises(comfy_server, tmp_path):
    _ComfyHandler.status_str = "error"
    comp = _comp(comfy_server, tmp_path)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "error" in str(failure.event_specific_data.error)


def test_polls_until_completed(comfy_server, tmp_path):
    _ComfyHandler.completed_after_n_polls = 3
    comp = _comp(comfy_server, tmp_path)
    result = dg.materialize(list(comp.build_defs(None).assets))
    assert result.success
    assert len(_ComfyHandler.history_polls) == 3


def test_timeout_raises_when_never_completes(comfy_server, tmp_path):
    _ComfyHandler.completed_after_n_polls = 10_000
    comp = _comp(comfy_server, tmp_path, timeout_seconds=1)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "did not complete" in str(failure.event_specific_data.error)


def test_workflow_and_workflow_path_both_set_is_rejected(comfy_server, tmp_path):
    comp = _comp(comfy_server, tmp_path, workflow_path="/tmp/does_not_matter.json")
    with pytest.raises(ValueError, match="exactly one"):
        comp.build_defs(None)


def test_workflow_path_loads_a_real_json_file(comfy_server, tmp_path):
    wf_file = tmp_path / "workflow.json"
    wf_file.write_text(json.dumps(WORKFLOW))
    comp = _comp(comfy_server, tmp_path, workflow=None, workflow_path=str(wf_file))
    dg.materialize(list(comp.build_defs(None).assets))
    assert _ComfyHandler.submit_requests[0]["prompt"] == WORKFLOW


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(comfy_server, tmp_path):
    comp = _comp(comfy_server, tmp_path, partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    pdef = list(comp.build_defs(None).assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
