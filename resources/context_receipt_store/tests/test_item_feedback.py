"""item_feedback(): what a build needs to learn from -- per-chunk retrievals, outcomes, citations and exclusions."""
import importlib.util
from pathlib import Path

import pytest


def _load():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("context_receipt_store_feedback_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def store(tmp_path):
    return _load().ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))


def item(chunk, cv="cv1", asset="kb", rank=1):
    return {"item_id": chunk, "source_asset_key": asset, "source_version": "v1", "source_code_version": cv, "rank": rank}


def retrieve(store, *chunks, cv="cv1", asset="kb", excluded=()):
    return store.record_retrieval(context_name="kb_ctx", items=[item(c, cv, asset, i) for i, c in enumerate(chunks)], excluded=list(excluded))


def test_counts_retrievals_outcomes_and_citations(store):
    r1 = retrieve(store, "a", "b")
    r2 = retrieve(store, "a")
    r3 = retrieve(store, "a", "c")
    store.record_outcome(r1, signal="thumbs_down")                        # -1 for a and b
    store.record_outcome(r2, signal="thumbs_up")                          # +1 for a
    store.add_evidence(r3, level="cited", details={"item_ids": ["c"]})    # only c was actually cited
    fb = store.item_feedback("kb")
    assert fb["a"]["retrievals"] == 3 and fb["a"]["rated"] == 2 and fb["a"]["mean_outcome"] == 0.0 and fb["a"]["cited"] == 0
    assert fb["b"]["retrievals"] == 1 and fb["b"]["mean_outcome"] == -1.0
    assert fb["c"]["retrievals"] == 1 and fb["c"]["rated"] == 0 and fb["c"]["mean_outcome"] is None and fb["c"]["cited"] == 1


def test_unscoped_evidence_credits_every_item_of_the_receipt(store):
    r = retrieve(store, "a", "b")
    store.add_evidence(r, level="verified", details={})
    fb = store.item_feedback("kb")
    assert fb["a"]["cited"] == 1 and fb["b"]["cited"] == 1


def test_a_later_outcome_with_the_same_ref_supersedes_the_earlier(store):
    r = retrieve(store, "a")
    store.record_outcome(r, signal="thumbs_down", outcome_ref="user-7")
    store.record_outcome(r, signal="thumbs_up", outcome_ref="user-7")     # the user changed their mind
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 1.0
    store.record_outcome(r, signal="thumbs_down", outcome_ref="user-8")   # a different person: averaged in
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 0.0


def test_the_store_itself_forbids_a_duplicate_item_in_one_receipt(store):
    with pytest.raises(ValueError, match="duplicate item_id"):      # so retrievals really are one-per-receipt
        store.record_retrieval(context_name="c", items=[item("a", rank=1), item("a", rank=2)])


def test_exclusion_reasons_are_aggregated_and_exclusion_only_items_appear(store):
    def excl(reason):
        return {"item_id": "gone", "reason": reason, "source_asset_key": "kb", "source_code_version": "cv1"}

    retrieve(store, "a", excluded=[excl("stale")])
    retrieve(store, "a", excluded=[excl("pii")])
    gone = store.item_feedback("kb")["gone"]
    assert gone["retrievals"] == 0 and gone["excluded"] == {"stale": 1, "pii": 1} and gone["mean_outcome"] is None


def test_scoped_to_one_asset_and_one_recipe(store):
    r_old = retrieve(store, "a", cv="old")
    r_new = retrieve(store, "a", cv="new")
    retrieve(store, "z", asset="other_asset")
    store.record_outcome(r_old, signal="thumbs_down")
    store.record_outcome(r_new, signal="thumbs_up")
    assert "z" not in store.item_feedback("kb")                                         # another asset's chunks never leak in
    assert store.item_feedback("kb")["a"]["retrievals"] == 2
    assert store.item_feedback("kb", code_version="old")["a"]["mean_outcome"] == -1.0   # old feedback describes OLD chunks
    assert store.item_feedback("kb", code_version="new")["a"]["mean_outcome"] == 1.0
    assert store.item_feedback("kb", code_version="never-built") == {}


def test_window_filters_by_time(store):
    retrieve(store, "a")
    assert store.item_feedback("kb", since="2999-01-01T00:00:00+00:00") == {}
    assert store.item_feedback("kb", since="2000-01-01T00:00:00+00:00")["a"]["retrievals"] == 1
    assert store.item_feedback("kb")["a"]["last_retrieved_at"]


def test_empty_store_and_many_receipts(store):
    assert store.item_feedback("kb") == {}
    for _ in range(620):                                   # more receipts than SQLite's bound-variable batch
        r = retrieve(store, "hot")
        store.record_outcome(r, signal="thumbs_up")
    fb = store.item_feedback("kb")["hot"]
    assert fb["retrievals"] == 620 and fb["rated"] == 620 and fb["mean_outcome"] == 1.0


def test_agrees_with_the_stores_own_priors(store):
    for sig in ("thumbs_down", "thumbs_down", "thumbs_up"):
        store.record_outcome(retrieve(store, "a"), signal=sig)
    store.refresh_item_scores()
    score = store.get_item_scores(["a"])["a"]
    fb = store.item_feedback("kb")["a"]
    assert score["retrievals"] == fb["retrievals"] == 3
    assert fb["mean_outcome"] == round((-1 - 1 + 1) / 3, 4)             # unweighted mean of the three receipts' outcomes
    assert score["prior"] < 0 and fb["mean_outcome"] < 0                # the store's prior and the feedback agree on direction
