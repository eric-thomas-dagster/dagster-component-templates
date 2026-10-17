"""store.selftest(): one call verifies a backend end to end, reports per step, and always cleans up its own synthetic rows."""
import sys
from pathlib import Path

import dagster as dg
import pytest

sys.path.insert(0, str(Path(__file__).parent))
from backends import KINDS, all_cells, load_module, make_store, row_counts  # noqa: E402
from test_bigquery_backend import item, scenario  # noqa: E402


@pytest.fixture()
def mod():
    return load_module()


@pytest.fixture(params=KINDS)
def store(request, mod, tmp_path, monkeypatch):
    return make_store(request.param, mod, tmp_path, monkeypatch)


def failed(results):
    return [r for r in results if not r["ok"]]


def test_selftest_passes_on_every_backend_and_leaves_nothing(store):
    results = store.selftest()
    assert [r["step"] for r in results][-1] == "cleanup"
    assert not failed(results), failed(results)
    assert {r["step"] for r in results} >= {"create_tables", "record_retrieval", "duplicate_detection", "outcome_evidence_output_events",
                                            "get_receipt", "read_since", "item_feedback", "refresh_and_get_item_scores",
                                            "outcomes_by_code_version", "asset_usage_since", "find_by", "maintenance_report", "cleanup"}
    assert all(set(r) == {"step", "ok", "detail"} for r in results)
    assert row_counts(store) == {"context_receipts": 0, "context_receipt_items": 0, "context_receipt_excluded": 0, "context_receipt_events": 0}
    assert not [c for c in all_cells(store) if "selftest" in c]


def test_selftest_does_not_touch_real_rows_and_runs_concurrently_safe_ids(store):
    scenario(store)
    store.refresh_item_scores(3.0)
    before_counts, before_scores = row_counts(store), store.get_item_scores(["a", "b", "c"])
    first, second = store.selftest(prior_strength=3.0), store.selftest(prior_strength=3.0)
    assert not failed(first) and not failed(second)
    assert row_counts(store) == before_counts
    after = store.get_item_scores(["a", "b", "c"])
    for d in (before_scores, after):
        for v in d.values():
            v.pop("updated_at")        # the store-wide refresh rewrites rows; values must not change (same prior_strength)
    assert after == before_scores
    assert store.get_receipt("r1") is not None


def test_selftest_with_pseudonymization_checks_and_still_cleans_up(store):
    store.enable_pseudonymization("s3cret")
    results = store.selftest()
    assert not failed(results), failed(results)
    assert "pseudonymized" in next(r for r in results if r["step"] == "get_receipt")["detail"]
    assert row_counts(store)["context_receipts"] == 0


BREAKS = {  # step that must report failure -> (method to break, replacement)
    "get_receipt": ("get_receipt", lambda self, rid: None),
    "item_feedback": ("item_feedback", lambda self, *a, **k: {}),
    "refresh_and_get_item_scores": ("get_item_scores", lambda self, ids: {}),
    "outcomes_by_code_version": ("outcomes_by_code_version", lambda self, *a, **k: {}),
    "asset_usage_since": ("asset_usage_since", lambda self, *a, **k: {"receipts": 0, "items": [], "with_output": 0, "verified_outputs": 0, "outcome_means": []}),
    "find_by": ("find_by_item", lambda self, item_id: []),
    "duplicate_detection": ("record_retrieval", None),   # filled below
}


@pytest.mark.parametrize("step", ["get_receipt", "item_feedback", "refresh_and_get_item_scores", "outcomes_by_code_version",
                                  "asset_usage_since", "find_by", "duplicate_detection"])
def test_broken_backend_method_fails_that_step_not_the_run_and_still_cleans_up(store, step):
    method, replacement = BREAKS[step]
    if step == "duplicate_detection":
        original = store.record_retrieval

        def swallow(**kwargs):            # a backend that silently accepts duplicates
            try:
                return original(**kwargs)
            except Exception as exc:
                if type(exc).__name__ == "DuplicateReceiptError":
                    return kwargs["receipt_id"]
                raise
        setattr(store, method, swallow)
    else:
        setattr(store, method, replacement.__get__(store))
    results = store.selftest()
    bad = failed(results)
    assert step in {r["step"] for r in bad}, results
    assert len(results) >= 13                                    # the other steps still ran
    assert next(r for r in results if r["step"] == "cleanup")["ok"], results
    assert row_counts(store)["context_receipts"] == 0
    assert not [c for c in all_cells(store) if "selftest" in c]


def test_a_write_that_raises_midway_is_reported_and_partial_rows_are_removed(store):
    original = store.record_outcome

    def boom(*a, **k):
        raise RuntimeError("warehouse said no")
    store.record_outcome = boom
    results = store.selftest()
    assert not next(r for r in results if r["step"] == "outcome_evidence_output_events")["ok"]
    assert "warehouse said no" in next(r for r in results if r["step"] == "outcome_evidence_output_events")["detail"]
    store.record_outcome = original
    assert next(r for r in results if r["step"] == "cleanup")["ok"]
    assert row_counts(store)["context_receipts"] == 0 and row_counts(store)["context_receipt_events"] == 0


def test_broken_cleanup_is_reported(store):
    store.delete_receipts = lambda ids: 0                          # a delete that silently does nothing
    results = store.selftest()
    cleanup = next(r for r in results if r["step"] == "cleanup")
    assert not cleanup["ok"] and "still present" in cleanup["detail"]
    type(store).delete_receipts(store, _synthetic_ids(store))
    assert row_counts(store)["context_receipts"] == 0


def _synthetic_ids(store):
    if hasattr(store, "_query"):
        return [r["receipt_id"] for r in store._query(f"SELECT receipt_id FROM {store._t('context_receipts')}")]
    import sqlite3
    return [r[0] for r in sqlite3.connect(store.database_path).execute("SELECT receipt_id FROM context_receipts")]


@pytest.mark.parametrize("kind", ["sql-sqlite", "bigquery-fake"])
def test_selftest_on_an_async_store_flushes_and_passes(kind, mod, tmp_path, monkeypatch):
    store = make_store(kind, mod, tmp_path, monkeypatch, async_writes=True)
    results = store.selftest()
    assert not failed(results), failed(results)
    assert row_counts(store)["context_receipts"] == 0


def test_selftest_asset_materializes_and_fails_loudly(mod, tmp_path):
    comp = mod.ContextReceiptStoreComponent(database_path=str(tmp_path / "c.sqlite3"), resource_key="rcpt", selftest_asset=True)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), resources=defs.resources)
    assert result.success
    mat = result.get_asset_materialization_events()[0].materialization
    assert mat.asset_key == dg.AssetKey("rcpt_selftest")
    assert mat.metadata["failed"].value == 0 and mat.metadata["steps"].value.records

    assert not list(mod.ContextReceiptStoreComponent(database_path=str(tmp_path / "d.sqlite3")).build_defs(None).assets or [])


def test_selftest_asset_raises_failure_with_step_table_when_a_step_fails(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "f.sqlite3"))
    store.get_item_scores = lambda ids: {}
    asset = mod.build_selftest_asset(store, "rcpt")
    result = dg.materialize([asset], raise_on_error=False)
    assert not result.success
    failure = [e for e in result.all_events if e.event_type_value == "STEP_FAILURE"][0]
    assert "refresh_and_get_item_scores" in str(failure.event_specific_data.error.message) or "refresh_and_get_item_scores" in str(failure.event_specific_data.error)
