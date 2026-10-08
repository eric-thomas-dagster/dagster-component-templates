"""Tests for CastordocExportJobComponent.

Covers: the self-contained asset-graph walk (`_build_payload_from_repo`,
exercised against a real `dg.Definitions(...).get_repository_def()` --
not a hand-rolled fake -- since `context.repository_def` is only ever
populated when a job is executed as part of a loaded repository, not via
bare `job.execute_in_process()`), the structural hash, and the same
Castor-specific `_transform`/`_resolve_table_id`/`_push` enrichment logic
as `lineage_to_castordoc` (duplicated inline here, mirroring how
`alation_export_job` duplicates `lineage_to_alation`'s helpers).

Mocks only the network boundary (`requests.post`).
"""
import os

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class _FakeLog:
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass


def _op_from_url(url):
    return url.split("op=", 1)[1]


# ── _build_payload_from_repo / _hash_structural ────────────────────────────

def test_build_payload_from_repo_walks_real_asset_graph(mod):
    @dg.asset(group_name="staging", kinds={"sql"}, description="Raw orders")
    def orders():
        return 1

    @dg.asset(deps=[dg.AssetKey("orders")], group_name="ml", kinds={"python"})
    def ml_model():
        return 2

    repo_def = dg.Definitions(assets=[orders, ml_model]).get_repository_def()
    payload = mod._build_payload_from_repo(repo_def)

    assert payload["sync_metadata"]["total_nodes"] == 2
    assert payload["sync_metadata"]["total_edges"] == 1
    assert {"upstream": "orders", "downstream": "ml_model"} in payload["edges"]
    by_key = {n["asset_key_string"]: n for n in payload["nodes"]}
    assert by_key["orders"]["group"] == "staging"
    assert by_key["orders"]["kinds"] == ["sql"]
    assert by_key["orders"]["description"] == "Raw orders"


def test_hash_structural_is_stable_and_ignores_sync_metadata(mod):
    repo_def = dg.Definitions(assets=[_trivial_asset()]).get_repository_def()
    payload1 = mod._build_payload_from_repo(repo_def)
    payload2 = mod._build_payload_from_repo(repo_def)
    assert payload1["sync_metadata"]["synced_at"] != "" and payload2["sync_metadata"]["synced_at"] != ""
    assert mod._hash_structural(payload1) == mod._hash_structural(payload2)


def _trivial_asset():
    @dg.asset
    def a():
        return 1
    return a


# ── _transform (pure) ──────────────────────────────────────────────────────

def test_transform_uses_last_asset_key_segment_as_match_name(mod):
    payload = {
        "nodes": [{"asset_key": ["staging", "orders"], "asset_key_string": "staging/orders",
                    "group": "staging", "kinds": ["sql"], "description": "d"}],
        "edges": [],
    }
    transformed = mod._transform(payload)
    assert transformed["nodes"][0]["match_name"] == "orders"


# ── _resolve_table_id / _push (mirrors lineage_to_castordoc) ───────────────

def _sample_payload():
    return {
        "source_system": {"platform": "dagster"},
        "sync_metadata": {"total_nodes": 2, "total_edges": 1},
        "nodes": [
            {"asset_key": ["staging", "orders"], "asset_key_string": "staging/orders",
             "group": "staging", "kinds": ["sql"], "description": "Raw orders"},
            {"asset_key": ["ml_model"], "asset_key_string": "ml_model",
             "group": "ml", "kinds": ["python"], "description": "No warehouse table"},
        ],
        "edges": [{"upstream": "staging/orders", "downstream": "ml_model"}],
    }


def _dispatch_fake_post(calls, tables_by_match_name):
    def fake_post(url, headers=None, json=None, timeout=None):
        op = _op_from_url(url)
        calls.append((op, json["variables"]))
        if op == "getTables":
            name = json["variables"]["scope"]["nameContains"]
            table_id = tables_by_match_name.get(name)
            rows = [{"id": table_id, "name": name}] if table_id else []
            return FakeResponse({"data": {"getTables": {"data": rows}}})
        if op == "updateTableDescriptions":
            return FakeResponse({"data": {"updateTableDescriptions": [{"id": d["id"]} for d in json["variables"]["data"]]}})
        if op == "attachTags":
            return FakeResponse({"data": {"attachTags": True}})
        if op == "upsertDataQualities":
            return FakeResponse({"data": {"upsertDataQualities": [{"id": "qc-1"}]}})
        if op == "upsertLineages":
            return FakeResponse({"data": {"upsertLineages": [{"id": "l-1"}]}})
        raise AssertionError(f"unexpected op {op}")
    return fake_post


def test_resolve_table_id_filters_substring_decoy_to_exact_match(mod):
    def fake_post(url, headers=None, json=None, timeout=None):
        assert headers["Authorization"] == "Token sekret"
        return FakeResponse({"data": {"getTables": {"data": [
            {"id": "t-decoy", "name": "stg_orders"},
            {"id": "t-real", "name": "Orders"},
        ]}}})

    mod.requests.post = fake_post
    table_id = mod._resolve_table_id("https://x", {"Authorization": "Token sekret"}, "orders")
    assert table_id == "t-real"


def test_push_raises_without_token_env(mod):
    os.environ.pop("CASTORDOC_JOB_MISSING_TOKEN", None)
    with pytest.raises(RuntimeError, match="CASTORDOC_JOB_MISSING_TOKEN"):
        mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_JOB_MISSING_TOKEN")


def test_push_matches_and_pushes_descriptions_tags_for_resolved_assets(mod):
    os.environ["CASTORDOC_JOB_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, {"orders": "t-orders"})

    result = mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_JOB_TEST_TOKEN", push_lineage_edges=False)
    assert result["matched"] == 1
    assert result["unmatched"] == 1
    ops_seen = [c[0] for c in calls]
    assert "updateTableDescriptions" in ops_seen
    assert "attachTags" in ops_seen


def test_push_lineage_edge_skipped_when_endpoint_unmatched(mod):
    os.environ["CASTORDOC_JOB_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, {"orders": "t-orders"})  # ml_model unresolved

    result = mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_JOB_TEST_TOKEN", push_lineage_edges=True)
    assert result["lineage_edges_pushed"] == 0
    assert result["lineage_edges_skipped"] == 1
    assert "upsertLineages" not in [c[0] for c in calls]


def test_push_lineage_edge_pushed_when_both_endpoints_matched(mod):
    os.environ["CASTORDOC_JOB_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, {"orders": "t-orders", "ml_model": "t-ml"})

    result = mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_JOB_TEST_TOKEN", push_lineage_edges=True)
    assert result["lineage_edges_pushed"] == 1
    lineage_call = next(v for op, v in calls if op == "upsertLineages")
    assert lineage_call["data"] == [{"parentTableId": "t-orders", "childTableId": "t-ml"}]


def test_push_data_quality_enabled_pushes_per_matched_table(mod):
    os.environ["CASTORDOC_JOB_TEST_TOKEN"] = "sekret"
    calls = []
    mod.requests.post = _dispatch_fake_post(calls, {"orders": "t-orders"})

    result = mod._push(_FakeLog(), mod._transform(_sample_payload()), "https://x", "CASTORDOC_JOB_TEST_TOKEN", push_data_quality=True)
    assert result["quality_records_pushed"] == 1
    quality_call = next(v for op, v in calls if op == "upsertDataQualities")
    assert quality_call["data"]["tableId"] == "t-orders"
    assert quality_call["data"]["qualityChecks"][0]["status"] == "SUCCESS"


# ── Component wiring (build_defs) ──────────────────────────────────────────
# NOTE: `_walk_and_push`'s op body calls `context.repository_def`, which
# Dagster only ever populates when a job executes as part of a *loaded
# repository* (e.g. `dagster dev`, a real code location run) -- not via
# direct invocation (`dg.build_op_context()`'s `repository_def` property
# unconditionally raises `DagsterInvalidPropertyError`) nor via
# `Definitions(...).get_job_def(...).execute_in_process()` (confirmed: it
# raises "No repository definition was set on the step context"). That's
# a real Dagster framework constraint, not something this component's test
# suite can route around -- so the op's actual walk/transform/push logic is
# covered directly above via `_build_payload_from_repo` (against a real
# `get_repository_def()`), `_transform`, `_resolve_table_id`, and `_push`.
# What's left to cover here is that `build_defs` wires the job/schedule
# correctly.

def test_build_defs_wires_job_name_and_op(mod):
    component = mod.CastordocExportJobComponent(job_name="sync_job", api_token_env="X")
    defs = component.build_defs(None)
    job_def = next(iter(defs.jobs))
    assert job_def.name == "sync_job"
    assert f"{component.job_name}_op" in [node.name for node in job_def.nodes]


def test_build_defs_attaches_schedule_when_configured(mod):
    component = mod.CastordocExportJobComponent(
        job_name="sync_job", api_token_env="X", schedule="0 3 * * *", default_status="RUNNING",
    )
    defs = component.build_defs(None)
    schedule_def = next(iter(defs.schedules))
    assert schedule_def.cron_schedule == "0 3 * * *"
    assert schedule_def.default_status == dg.DefaultScheduleStatus.RUNNING


def test_build_defs_omits_schedule_when_not_configured(mod):
    component = mod.CastordocExportJobComponent(job_name="sync_job", api_token_env="X")
    defs = component.build_defs(None)
    assert not (defs.schedules or [])
