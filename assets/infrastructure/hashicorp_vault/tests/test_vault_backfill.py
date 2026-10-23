"""hashicorp_vault's backfill_policy was missing entirely (field + wiring). A single_run backfill
hands the asset a partition-key RANGE; hashicorp_vault already builds its {partition_key} tokens via
the canonical range-safe _partition_vars helper, so wiring single_run should make one asset run fetch
a secret whose PATH is templated with a comma-joined partition_key covering the whole range. Real
dg.materialize() run against a real local HTTP server standing in for Vault's KV v2 API; nothing is
mocked."""
import http.server
import json
import threading

import dagster as dg

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


class _Handler(http.server.BaseHTTPRequestHandler):
    requested_path = None

    def do_GET(self):
        _Handler.requested_path = self.path
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"data": {"data": {"password": "shh"}}}).encode())

    def log_message(self, *a):
        pass


def _server():
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def _asset(httpd, monkeypatch, backfill_policy=None):
    monkeypatch.setenv("VAULT_URL", f"http://127.0.0.1:{httpd.server_port}")
    monkeypatch.setenv("VAULT_TOKEN", "t")
    mod = load_component_module()
    comp = mod.VaultAssetComponent(
        asset_name="infra",
        vault_url_env_var="VAULT_URL",
        token_env_var="VAULT_TOKEN",
        secrets=[{"path": "secret/data/myapp/{partition_key}", "key": "password", "env_var": "DB_PASSWORD"}],
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


def test_single_run_over_a_range_joins_partition_keys_into_the_secret_path(monkeypatch):
    httpd = _server()
    try:
        asset = _asset(httpd, monkeypatch, backfill_policy="single_run")
        result = dg.materialize([asset], tags={
            "dagster/asset_partition_range_start": "2024-01-01",
            "dagster/asset_partition_range_end": "2024-01-03",
        })
        assert result.success
        assert _Handler.requested_path == "/v1/secret/data/myapp/2024-01-01,2024-01-02,2024-01-03"
    finally:
        httpd.shutdown()
