"""retract_outcome() / record_outcome(pending=True) + approve_outcome(): undoing or holding back a
rating. A pending outcome does not count toward any aggregation until approved; a retraction cancels
a rating outright, active or still pending, with no un-retract."""
import importlib.util
from pathlib import Path

import pytest


def _load():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("context_receipt_store_retract_pending_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return _load()


@pytest.fixture()
def store(mod, tmp_path):
    return mod.ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))


def item(chunk, asset="kb", cv="cv1"):
    return {"item_id": chunk, "source_asset_key": asset, "source_version": "v1", "source_code_version": cv}


def retrieve(store, receipt_id, *chunks):
    return store.record_retrieval(context_name="kb_ctx", receipt_id=receipt_id, items=[item(c) for c in chunks])


def test_pending_requires_outcome_ref(store):
    retrieve(store, "r1", "a")
    with pytest.raises(ValueError, match="outcome_ref"):
        store.record_outcome("r1", signal="thumbs_up", pending=True)


def test_pending_outcome_does_not_count_until_approved(store):
    retrieve(store, "r1", "a")
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1", pending=True)
    feedback = store.item_feedback("kb")
    assert feedback["a"]["rated"] == 0 and feedback["a"]["mean_outcome"] is None
    usage = store.asset_usage_since("kb", "")
    assert usage["outcome_means"] == []
    store.refresh_item_scores(prior_strength=0)
    assert store.get_item_scores(["a"])["a"]["prior"] == 0.0
    store.approve_outcome("r1", "u1")
    feedback = store.item_feedback("kb")
    assert feedback["a"]["rated"] == 1 and feedback["a"]["mean_outcome"] == 1.0
    usage = store.asset_usage_since("kb", "")
    assert usage["outcome_means"] == [1.0]
    store.refresh_item_scores(prior_strength=0)
    assert store.get_item_scores(["a"])["a"]["prior"] == 1.0


def test_approve_is_idempotent_and_a_noop_when_nothing_is_pending(store):
    retrieve(store, "r1", "a")
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1", pending=True)
    store.approve_outcome("r1", "u1")
    store.approve_outcome("r1", "u1")  # already active -- harmless
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 1.0
    store.approve_outcome("r1", "never-rated")  # unknown ref -- harmless
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 1.0


def test_retract_cancels_an_active_outcome(store):
    retrieve(store, "r1", "a")
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1")
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 1.0
    store.retract_outcome("r1", "u1")
    feedback = store.item_feedback("kb")
    assert feedback["a"]["rated"] == 0 and feedback["a"]["mean_outcome"] is None


def test_retract_cancels_a_still_pending_outcome(store):
    retrieve(store, "r1", "a")
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1", pending=True)
    store.retract_outcome("r1", "u1")
    store.approve_outcome("r1", "u1")  # too late -- retraction wins, approval is a no-op
    assert store.item_feedback("kb")["a"]["mean_outcome"] is None


def test_retract_is_idempotent_and_irreversible(store):
    retrieve(store, "r1", "a")
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1")
    store.retract_outcome("r1", "u1")
    store.retract_outcome("r1", "u1")  # harmless repeat
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1")  # un-retract by re-rating, not by un-retracting
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 1.0


def test_retract_of_a_never_rated_ref_is_a_noop(store):
    retrieve(store, "r1", "a")
    store.retract_outcome("r1", "nobody-rated-this")
    assert store.item_feedback("kb")["a"]["mean_outcome"] is None


def test_unknown_receipt_raises_key_error(store):
    with pytest.raises(KeyError):
        store.approve_outcome("ghost", "u1")
    with pytest.raises(KeyError):
        store.retract_outcome("ghost", "u1")


def test_outcome_ref_must_be_a_non_empty_string(store):
    retrieve(store, "r1", "a")
    with pytest.raises(ValueError):
        store.approve_outcome("r1", "")
    with pytest.raises(ValueError):
        store.retract_outcome("r1", "")


def test_retracting_one_of_two_refs_leaves_the_other_counted(store):
    retrieve(store, "r1", "a")
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1")
    store.record_outcome("r1", signal="thumbs_down", outcome_ref="u2")
    store.retract_outcome("r1", "u1")
    # only u2's -1.0 remains for r1's one effective outcome
    assert store.item_feedback("kb")["a"]["mean_outcome"] == -1.0


def test_retracting_every_ref_on_a_receipt_leaves_it_unrated_not_a_crash(store):
    retrieve(store, "r1", "a", "b")
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1")
    store.retract_outcome("r1", "u1")
    usage = store.asset_usage_since("kb", "")  # previously risked ZeroDivisionError on an emptied-out outcome dict
    assert usage["outcome_means"] == []
    feedback = store.item_feedback("kb")
    assert feedback["a"]["mean_outcome"] is None and feedback["b"]["mean_outcome"] is None


def test_anonymous_outcomes_without_outcome_ref_are_unaffected(store):
    retrieve(store, "r1", "a")
    store.record_outcome("r1", signal="thumbs_up")  # no outcome_ref -- can't be retracted/approved, always counts
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 1.0


def test_outcomes_by_code_version_respects_pending_and_retraction(store):
    retrieve(store, "r1", "a")
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1", pending=True)
    assert store.outcomes_by_code_version("kb")["cv1"]["outcomes"] == 0
    store.approve_outcome("r1", "u1")
    assert store.outcomes_by_code_version("kb")["cv1"]["outcomes"] == 1
    store.retract_outcome("r1", "u1")
    assert store.outcomes_by_code_version("kb")["cv1"]["outcomes"] == 0


def test_a_later_outcome_for_the_same_ref_resets_its_own_pending_status(store):
    retrieve(store, "r1", "a")
    store.record_outcome("r1", signal="thumbs_down", outcome_ref="u1", pending=True)
    store.approve_outcome("r1", "u1")
    assert store.item_feedback("kb")["a"]["mean_outcome"] == -1.0
    # the user changes their mind; the new rating starts fresh as pending again, not auto-approved
    store.record_outcome("r1", signal="thumbs_up", outcome_ref="u1", pending=True)
    assert store.item_feedback("kb")["a"]["mean_outcome"] is None
    store.approve_outcome("r1", "u1")
    assert store.item_feedback("kb")["a"]["mean_outcome"] == 1.0
