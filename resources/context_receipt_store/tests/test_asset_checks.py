import importlib.util
from pathlib import Path

import dagster as dg
import pytest

ROOT = Path(__file__).parents[1]


def _load():
    spec = importlib.util.spec_from_file_location("ctx_receipt_checks", ROOT / "component.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load()


@dg.asset(name="docs")
def docs():
    return dg.MaterializeResult(data_version=dg.DataVersion("v2"))


def _run_checks(mod, store, **kw):
    """Execute the real checks (materialize() alone does not run them)."""
    checks = mod.build_usage_checks(
        store, "docs",
        window_hours=24, max_stale_share=kw.get("max_stale_share", 0.1),
        min_mean_outcome=-0.2, min_verified_share=0.5, min_checked_receipts=kw.get("min_checked", 3),
    )
    defs = dg.Definitions(assets=[docs], asset_checks=checks)
    result = defs.get_implicit_global_asset_job_def().execute_in_process(instance=dg.DagsterInstance.ephemeral())
    assert result.success
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


def _receipt(store, version="v2"):
    return store.record_retrieval(
        context_name="c", items=[{"item_id": f"i{version}", "source_asset_key": "docs", "source_version": version}]
    )


def test_staleness_check_fails_error_when_stale_share_high_and_passes_when_fresh(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "a.db"))
    for _ in range(3):
        _receipt(store, "v1")  # stale: current is v2
    _receipt(store, "v2")
    ev = _run_checks(mod, store)["context_staleness"]
    assert not ev.passed and ev.severity == dg.AssetCheckSeverity.ERROR
    assert ev.metadata["stale_share"].value == 0.75 and ev.metadata["judged_items"].value == 4
    fresh = mod.ContextReceiptStore(str(tmp_path / "b.db"))
    _receipt(fresh, "v2")
    assert _run_checks(mod, fresh)["context_staleness"].passed
    empty = mod.ContextReceiptStore(str(tmp_path / "c.db"))
    assert _run_checks(mod, empty)["context_staleness"].passed  # nothing served, nothing stale


def test_outcome_quality_warns_on_bad_outcomes_and_needs_enough_samples(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "q.db"))
    for _ in range(2):
        store.record_outcome(_receipt(store), signal="thumbs_down")
    ev = _run_checks(mod, store, min_checked=3)["context_outcome_quality"]
    assert ev.passed and "insufficient" in ev.metadata["note"].value  # too few to judge
    store.record_outcome(_receipt(store), signal="thumbs_down")
    ev = _run_checks(mod, store, min_checked=3)["context_outcome_quality"]
    assert not ev.passed and ev.severity == dg.AssetCheckSeverity.WARN
    assert ev.metadata["mean_outcome"].value == -1.0


def test_verified_share_counts_outputs_that_reached_verified_evidence(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "v.db"))
    assert _run_checks(mod, store)["context_verified_share"].passed  # no outputs yet: nothing to judge
    ids = [_receipt(store) for _ in range(2)]
    for n, rid in enumerate(ids):
        store.attach_output(rid, output_id=f"o{n}", model="m", output_hash="h")
    ev = _run_checks(mod, store)["context_verified_share"]
    assert not ev.passed and ev.metadata["verified_share"].value == 0.0
    store.add_evidence(ids[0], level="verified", details={})
    ev = _run_checks(mod, store)["context_verified_share"]
    assert ev.passed and ev.metadata["verified_share"].value == 0.5
    # "cited" is the model's own claim, so it does not count as verified
    store.add_evidence(ids[1], level="cited", details={})
    assert _run_checks(mod, store)["context_verified_share"].metadata["verified_share"].value == 0.5


def test_component_attaches_four_checks_per_asset(mod, tmp_path):
    comp = mod.ContextReceiptStoreComponent(
        database_path=str(tmp_path / "c.db"), check_asset_keys=["docs", "other"]
    )
    defs = comp.build_defs(None)
    names = sorted((k.asset_key.to_user_string(), k.name) for c in defs.asset_checks for k in c.check_keys)
    assert names == sorted(
        (a, n) for a in ("docs", "other") for n in ("context_staleness", "context_outcome_quality", "context_verified_share", "context_grounding")
    )
    assert not list(mod.ContextReceiptStoreComponent(database_path=str(tmp_path / "d.db")).build_defs(None).asset_checks or [])
