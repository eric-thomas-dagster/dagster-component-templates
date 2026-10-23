"""matillion_run_asset's backfill_policy was missing entirely (field + wiring). A single_run backfill
hands the asset a partition-key RANGE; matillion_run_asset already builds its {partition_key} tokens
via the canonical range-safe _partition_vars helper, so wiring single_run should make one job trigger
cover the whole range via a comma-joined partition_key variable. Real dg.materialize() run against a
real local HTTP server standing in for the Matillion REST API; nothing is mocked."""
import http.server
import json
import threading

import dagster as dg

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


class _Handler(http.server.BaseHTTPRequestHandler):
    triggered_payload = None

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        _Handler.triggered_payload = json.loads(self.rfile.read(length) or b"{}")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"id": 1}).encode())

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"state": "SUCCESS"}).encode())

    def log_message(self, *a):
        pass


def _server():
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def _asset(httpd, monkeypatch, backfill_policy=None):
    monkeypatch.setenv("MATILLION_URL", f"http://127.0.0.1:{httpd.server_port}")
    monkeypatch.setenv("MATILLION_USER", "u")
    monkeypatch.setenv("MATILLION_PASS", "p")
    mod = load_component_module()
    comp = mod.MatillionRunAssetComponent(
        asset_key="infra/job",
        project="MyProject",
        job_name="Load Orders",
        instance_url_env_var="MATILLION_URL",
        username_env_var="MATILLION_USER",
        password_env_var="MATILLION_PASS",
        variables={"env": "{partition_key}"},
        poll_interval_seconds=0.01,
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    return next(a for a in defs.assets if hasattr(a, "node_def"))


def test_backfill_policy_single_run_sets_a_real_backfill_policy(monkeypatch):
    httpd = _server()
    try:
        asset = _asset(httpd, monkeypatch, backfill_policy="single_run")
        assert asset.backfill_policy == dg.BackfillPolicy.single_run()
    finally:
        httpd.shutdown()


def test_single_run_over_a_range_joins_partition_keys_into_one_job_trigger(monkeypatch):
    httpd = _server()
    try:
        asset = _asset(httpd, monkeypatch, backfill_policy="single_run")
        result = dg.materialize([asset], tags={
            "dagster/asset_partition_range_start": "2024-01-01",
            "dagster/asset_partition_range_end": "2024-01-03",
        })
        assert result.success
        assert _Handler.triggered_payload == {"variables": {"env": "2024-01-01,2024-01-02,2024-01-03"}}
    finally:
        httpd.shutdown()
