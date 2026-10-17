"""maintenance_report: one pure fold over item_feedback, identical on every backend, plus the WARN-severity asset check."""
import sys
from pathlib import Path

import dagster as dg
import pytest

sys.path.insert(0, str(Path(__file__).parent))
from backends import KINDS, load_module, make_store  # noqa: E402
from test_bigquery_backend import item  # noqa: E402


@pytest.fixture()
def mod():
    return load_module()


@pytest.fixture(params=KINDS)
def store(request, mod, tmp_path, monkeypatch):
    return make_store(request.param, mod, tmp_path, monkeypatch)


def populate(store):
    """bad: 6 receipts rated down; good: 6 rated up; quiet: 6 never rated (and 'noisy' excluded from them 6x); meh: 2 retrievals."""
    for i in range(6):
        store.record_retrieval(context_name="kb", receipt_id=f"b{i}", items=[item("bad")])
        store.record_outcome(f"b{i}", signal="thumbs_down")
        store.record_retrieval(context_name="kb", receipt_id=f"g{i}", items=[item("good")])
        store.record_outcome(f"g{i}", signal="thumbs_up")
        store.record_retrieval(context_name="kb", receipt_id=f"q{i}", items=[item("quiet")],
                               excluded=[{"item_id": "noisy", "reason": "policy", "source_asset_key": "kb", "source_code_version": "cv1"}])
    store.record_retrieval(context_name="kb", receipt_id="mm", items=[item("meh"), item("meh2", rank=1)])
    store.record_retrieval(context_name="kb", receipt_id="mm2", items=[item("meh")])
    store.record_outcome("mm", signal="thumbs_down")
    store.record_outcome("mm2", signal="thumbs_down")


def test_report_sections_on_every_backend(store):
    populate(store)
    rep = store.maintenance_report("kb", known_item_ids=["bad", "good", "quiet", "meh", "never_served"], dead_after_receipts=10)
    assert [r["item_id"] for r in rep["rewrite_candidates"]] == ["bad"]                 # meh is just as bad but retrieved only twice
    bad = rep["rewrite_candidates"][0]
    assert (bad["retrievals"], bad["rated"], bad["mean_outcome"], bad["cited"], bad["excluded"]) == (6, 6, -1.0, 0, {})
    assert [r["item_id"] for r in rep["top_performers"]] == ["good"]
    assert [r["item_id"] for r in rep["noisy_exclusions"]] == ["noisy"] and rep["noisy_exclusions"][0]["excluded"] == {"policy": 6}
    assert rep["dead_weight"] == ["never_served"] and rep["dead_weight_reason"] is None
    assert rep["summary"] == {"items_seen": 6, "receipts_in_window": 20, "rewrite_candidates": 1, "dead_weight": 1,
                              "top_performers": 1, "noisy_exclusions": 1}
    relaxed = store.maintenance_report("kb", min_retrievals=2, min_outcomes=2)
    assert [r["item_id"] for r in relaxed["rewrite_candidates"]] == ["bad", "meh"]       # same mean: more retrievals first


def test_dead_weight_is_not_judged_without_enough_traffic_or_without_known_ids(store):
    for i in range(3):
        store.record_retrieval(context_name="kb", receipt_id=f"t{i}", items=[item("a")])
    few = store.maintenance_report("kb", known_item_ids=["a", "ghost"], dead_after_receipts=50)
    assert few["dead_weight"] is None and "3 receipt" in few["dead_weight_reason"] and few["summary"]["dead_weight"] is None
    enough = store.maintenance_report("kb", known_item_ids=["a", "ghost"], dead_after_receipts=3)
    assert enough["dead_weight"] == ["ghost"]
    unknown = store.maintenance_report("kb")
    assert unknown["dead_weight"] is None and "known_item_ids" in unknown["dead_weight_reason"]


def test_all_backends_agree(mod, tmp_path, monkeypatch):
    reports = {}
    for kind in KINDS:
        s = make_store(kind, mod, tmp_path, monkeypatch)
        populate(s)
        s.add_evidence("b0", level="cited", details={"item_ids": ["bad"]})
        reports[kind] = s.maintenance_report("kb", known_item_ids=["bad", "good", "z"], min_retrievals=2, min_outcomes=2, dead_after_receipts=5)
    assert reports["sqlite"]["rewrite_candidates"][0]["cited"] == 1
    for kind, rep in reports.items():
        assert rep == reports["sqlite"], kind


def test_pure_fold_thresholds_ordering_and_edges(mod):
    fb = {
        "w1": {"retrievals": 9, "rated": 5, "mean_outcome": -0.9, "cited": 0, "excluded": {}, "last_retrieved_at": None},
        "w2": {"retrievals": 20, "rated": 8, "mean_outcome": -0.9, "cited": 2, "excluded": {"pii": 1}, "last_retrieved_at": None},
        "w3": {"retrievals": 7, "rated": 3, "mean_outcome": -0.3, "cited": 0, "excluded": {}, "last_retrieved_at": None},   # boundary: <= bad_below
        "few_ratings": {"retrievals": 50, "rated": 2, "mean_outcome": -1.0, "cited": 0, "excluded": {}, "last_retrieved_at": None},
        "few_retrievals": {"retrievals": 4, "rated": 4, "mean_outcome": -1.0, "cited": 0, "excluded": {}, "last_retrieved_at": None},
        "unrated": {"retrievals": 30, "rated": 0, "mean_outcome": None, "cited": 0, "excluded": {}, "last_retrieved_at": None},
        "star": {"retrievals": 10, "rated": 10, "mean_outcome": 0.9, "cited": 9, "excluded": {}, "last_retrieved_at": None},
        "only_excluded": {"retrievals": 0, "rated": 0, "mean_outcome": None, "cited": 0, "excluded": {"pii": 3, "stale": 3}, "last_retrieved_at": None},
    }
    rep = mod.fold_maintenance_report(fb, receipts_in_window=100, known_item_ids=["w1", "only_excluded", "absent"])
    assert [r["item_id"] for r in rep["rewrite_candidates"]] == ["w2", "w1", "w3"]       # worst first, ties by more retrievals
    assert [r["item_id"] for r in rep["top_performers"]] == ["star"]
    noisy = rep["noisy_exclusions"]
    assert [r["item_id"] for r in noisy] == ["only_excluded"] and noisy[0]["excluded_total"] == 6
    assert rep["dead_weight"] == ["absent", "only_excluded"]                              # excluded-only counts as never retrieved
    assert rep["summary"] == {"items_seen": 8, "receipts_in_window": 100, "rewrite_candidates": 3, "dead_weight": 2,
                              "top_performers": 1, "noisy_exclusions": 1}
    empty = mod.fold_maintenance_report({})
    assert empty["rewrite_candidates"] == [] and empty["dead_weight"] is None and empty["summary"]["items_seen"] == 0


def test_check_is_warn_severity_reports_counts_and_never_fails_the_run(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "c.db"))
    for i in range(6):
        store.record_retrieval(context_name="kb", receipt_id=f"c{i}", items=[item("bad", asset="docs")])
        store.record_outcome(f"c{i}", signal="thumbs_down")

    @dg.asset(name="docs")
    def docs():
        ...

    check = mod.build_maintenance_check(store, "docs", window_days=1, min_retrievals=5, min_outcomes=3)
    result = dg.Definitions(assets=[docs], asset_checks=[check]).get_implicit_global_asset_job_def().execute_in_process(
        instance=dg.DagsterInstance.ephemeral())
    assert result.success                                                                  # WARN never fails the run
    (ev,) = result.get_asset_check_evaluations()
    assert ev.check_name == "context_maintenance" and ev.severity == dg.AssetCheckSeverity.WARN and not ev.passed
    assert ev.metadata["rewrite_candidates"].value == 1 and ev.metadata["worst_chunks"].value == ["bad"]
    healthy = mod.ContextReceiptStore(str(tmp_path / "h.db"))
    ok = mod.build_maintenance_check(healthy, "docs")
    res = dg.Definitions(assets=[docs], asset_checks=[ok]).get_implicit_global_asset_job_def().execute_in_process(instance=dg.DagsterInstance.ephemeral())
    (ev2,) = res.get_asset_check_evaluations()
    assert ev2.passed and ev2.metadata["rewrite_candidates"].value == 0


def test_component_adds_the_check_per_asset(mod, tmp_path):
    comp = mod.ContextReceiptStoreComponent(database_path=str(tmp_path / "k.db"), maintenance_check_asset_keys=["docs", "other"])
    defs = comp.build_defs(None)
    names = sorted((k.asset_key.to_user_string(), k.name) for c in defs.asset_checks for k in c.check_specs)
    assert names == [("docs", "context_maintenance"), ("other", "context_maintenance")]
    assert not list(mod.ContextReceiptStoreComponent(database_path=str(tmp_path / "n.db")).build_defs(None).asset_checks or [])
