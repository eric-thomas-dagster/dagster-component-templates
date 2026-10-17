import importlib.util
from pathlib import Path

import dagster as dg
import pandas as pd
import pytest

ROOT = Path(__file__).parents[1]


def _load():
    spec = importlib.util.spec_from_file_location("ctx_receipt_cv", ROOT / "component.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load()


def _receipt(store, cv, signal=None, asset="docs", ref="u"):
    rid = store.record_retrieval(context_name="c", items=[
        {"item_id": f"i-{cv}", "source_asset_key": asset, "source_version": "v1", "source_code_version": cv}])
    if signal:
        store.record_outcome(rid, signal=signal, outcome_ref=ref)
    return rid


def test_code_version_round_trips_through_store_and_spec(mod, tmp_path):
    a = mod.ContextReceiptStore(str(tmp_path / "a.db"))
    rid = a.record_retrieval(
        context_name="c",
        items=[{"item_id": "x", "source_asset_key": "docs", "source_code_version": "r1"}],
        excluded=[{"item_id": "y", "reason": "stale", "source_code_version": "r0"}])
    r = a.get_receipt(rid)
    assert r["items"][0]["source_code_version"] == "r1" and r["excluded"][0]["source_code_version"] == "r0"
    b = mod.ContextReceiptStore(str(tmp_path / "b.db"))
    for event in a.export_events(rid):
        b.ingest(event)
    assert b.get_receipt(rid)["items"][0]["source_code_version"] == "r1"
    with pytest.raises(TypeError):
        a.record_retrieval(context_name="c", items=[{"item_id": "z", "source_code_version": 3}])


def test_outcomes_by_code_version_groups_recipes_and_unknown(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "o.db"))
    _receipt(store, "r1", "thumbs_up")
    _receipt(store, "r1", "thumbs_up", ref="u2")
    _receipt(store, "r1")  # served, no outcome yet
    _receipt(store, "r2", "thumbs_down")
    store.record_retrieval(context_name="c", items=[{"item_id": "n", "source_asset_key": "docs"}])  # no recipe recorded
    _receipt(store, "r9", "thumbs_up", asset="other")  # different asset: excluded
    out = store.outcomes_by_code_version("docs")
    assert out["r1"] == {"receipts": 3, "outcomes": 2, "mean_outcome": 1.0}
    assert out["r2"] == {"receipts": 1, "outcomes": 1, "mean_outcome": -1.0}
    assert out["unknown"] == {"receipts": 1, "outcomes": 0, "mean_outcome": None}
    assert "r9" not in out


def test_sensor_reports_outcomes_per_code_version(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "s.db"))
    instance = dg.DagsterInstance.ephemeral()

    @dg.asset(name="docs")
    def docs():
        return dg.MaterializeResult(data_version=dg.DataVersion("v1"))

    dg.materialize([docs], instance=instance)
    _receipt(store, "r1", "thumbs_up")
    _receipt(store, "r2", "thumbs_down")
    sensor = mod.build_usage_sensor(store, name="s", interval_seconds=60, rebuild_asset_keys=[],
                                    stale_reads_threshold=99, prior_strength=1.0)
    md = sensor(dg.build_sensor_context(instance=instance)).asset_events[0].metadata
    assert md["context_receipts/outcomes_by_code_version"].value == {
        "r1": {"outcomes": 1, "mean_outcome": 1.0}, "r2": {"outcomes": 1, "mean_outcome": -1.0}}


def test_decorator_code_version_param_and_passthrough(mod):
    @mod.context_asset(code_version="recipe-7")
    @dg.asset(name="ctx_a")
    def ctx_a():
        return pd.DataFrame({"chunk_id": ["a"], "chunk_text": ["t"]})

    @mod.context_asset()
    @dg.asset(name="ctx_b", code_version="from-asset")
    def ctx_b():
        return pd.DataFrame({"chunk_id": ["a"], "chunk_text": ["t"]})

    assert ctx_a.get_asset_spec(dg.AssetKey("ctx_a")).code_version == "recipe-7"
    assert ctx_b.get_asset_spec(dg.AssetKey("ctx_b")).code_version == "from-asset"
