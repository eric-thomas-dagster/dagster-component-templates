"""outcome_samples_by_code_version: raw per-receipt effective outcomes, identical on every backend and consistent with
outcomes_by_code_version (both come from one fold)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from .backends import KINDS, load_module, make_store  # noqa: E402


@pytest.fixture()
def mod():
    return load_module()


@pytest.fixture(params=KINDS)
def store(request, mod, tmp_path, monkeypatch):
    return make_store(request.param, mod, tmp_path, monkeypatch)


def _it(cv, asset="docs"):
    return [{"item_id": f"i-{cv}", "source_asset_key": asset, "source_version": "v1", "source_code_version": cv}]


def populate(store):
    # r1: a:thumbs_up, b: mean of two refs (up, down) = 0.0, c: superseded ref (down then up -> 1.0), d: unanswered
    store.record_retrieval(context_name="c", receipt_id="a", items=_it("r1"))
    store.record_outcome("a", signal="thumbs_up", outcome_ref="u")
    store.record_retrieval(context_name="c", receipt_id="b", items=_it("r1"))
    store.record_outcome("b", signal="thumbs_up", outcome_ref="u1")
    store.record_outcome("b", signal="thumbs_down", outcome_ref="u2")
    store.record_retrieval(context_name="c", receipt_id="c", items=_it("r1"))
    store.record_outcome("c", signal="thumbs_down", outcome_ref="u")
    store.record_outcome("c", signal="thumbs_up", outcome_ref="u")
    store.record_retrieval(context_name="c", receipt_id="d", items=_it("r1"))
    store.record_retrieval(context_name="c", receipt_id="e", items=_it("r2"))
    store.record_outcome("e", signal="thumbs_down")
    store.record_retrieval(context_name="c", receipt_id="f", items=[{"item_id": "n", "source_asset_key": "docs"}])
    store.record_retrieval(context_name="c", receipt_id="g", items=_it("r9", asset="other"))
    store.record_outcome("g", signal="thumbs_up")


def test_samples_are_effective_per_receipt_outcomes(store):
    populate(store)
    got = store.outcome_samples_by_code_version("docs")
    assert {k: sorted(v) for k, v in got.items()} == {"r1": [0.0, 1.0, 1.0], "r2": [-1.0], "unknown": []}


def test_summary_and_samples_cannot_disagree(store):
    populate(store)
    samples = store.outcome_samples_by_code_version("docs")
    summary = store.outcomes_by_code_version("docs")
    assert set(samples) == set(summary)
    for cv, s in samples.items():
        assert summary[cv]["outcomes"] == len(s)
        assert summary[cv]["mean_outcome"] == (round(sum(s) / len(s), 4) if s else None)


def test_since_window_applies(store):
    populate(store)
    assert store.outcome_samples_by_code_version("docs", "2999-01-01T00:00:00+00:00") == {}


def test_parity_with_sqlite(mod, store, tmp_path):
    populate(store)
    lite = mod.ContextReceiptStore(str(tmp_path / "ref.sqlite3"))
    populate(lite)
    assert store.outcome_samples_by_code_version("docs") == lite.outcome_samples_by_code_version("docs")
    assert store.outcomes_by_code_version("docs") == lite.outcomes_by_code_version("docs")


def test_pure_fold_summary_derives_from_samples(mod):
    rows = [{"receipt_id": "x", "cv": "a"}, {"receipt_id": "y", "cv": "a"}, {"receipt_id": "z", "cv": "b"}]
    events = [{"receipt_id": "x", "payload_json": '{"value": 1.0, "outcome_ref": "o"}'},
              {"receipt_id": "x", "payload_json": '{"value": -1.0, "outcome_ref": "o"}'}]
    assert mod._fold_outcome_samples_by_code_version(rows, events) == {"a": [-1.0], "b": []}
    assert mod._fold_outcomes_by_code_version(rows, events) == {
        "a": {"receipts": 2, "outcomes": 1, "mean_outcome": -1.0}, "b": {"receipts": 1, "outcomes": 0, "mean_outcome": None}}
