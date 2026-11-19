"""recent_outcomes(): the latest individual rating EVENTS for an asset -- receipt id, time, signal,
rater (outcome_ref), and the item ids that receipt returned for this asset. Unlike item_feedback()/
asset_usage_since() (both pure aggregations), this is the raw event log: a later outcome_ref that
supersedes an earlier one still appears as two separate rows here."""
import importlib.util
from pathlib import Path

import pytest


def _load():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("context_receipt_store_recent_outcomes_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def store(tmp_path):
    return _load().ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))


def item(chunk, cv="cv1", asset="kb", rank=1):
    return {"item_id": chunk, "source_asset_key": asset, "source_version": "v1", "source_code_version": cv, "rank": rank}


def retrieve(store, *chunks, cv="cv1", asset="kb"):
    return store.record_retrieval(context_name="kb_ctx", items=[item(c, cv, asset, i) for i, c in enumerate(chunks)])


def test_returns_individual_rating_rows_with_rater_and_item_ids(store):
    r = retrieve(store, "a", "b")
    store.record_outcome(r, signal="thumbs_up", outcome_ref="jane@co.com")
    rows = store.recent_outcomes("kb")
    assert len(rows) == 1
    row = rows[0]
    assert row["receipt_id"] == r
    assert row["signal"] == "thumbs_up"
    assert row["value"] == 1.0
    assert row["outcome_ref"] == "jane@co.com"
    assert row["item_ids"] == ["a", "b"]
    assert row["occurred_at"]


def test_never_includes_query_text_or_chunk_text(store):
    r = retrieve(store, "a")
    store.record_outcome(r, signal="thumbs_up", outcome_ref="jane@co.com")
    row = store.recent_outcomes("kb")[0]
    assert set(row) == {"receipt_id", "occurred_at", "signal", "value", "outcome_ref", "source_partition", "item_ids"}


def test_a_later_outcome_with_the_same_ref_appears_as_two_rows_not_folded(store):
    """Unlike item_feedback()'s own mean_outcome, this is the raw event log -- a later outcome_ref
    that supersedes an earlier one for scoring purposes still shows up as its own event here, so a
    UI can show someone changed their mind, not just the final effective value."""
    r = retrieve(store, "a")
    store.record_outcome(r, signal="thumbs_down", outcome_ref="user-7")
    store.record_outcome(r, signal="thumbs_up", outcome_ref="user-7")
    rows = store.recent_outcomes("kb")
    assert len(rows) == 2
    assert [row["signal"] for row in rows] == ["thumbs_up", "thumbs_down"]  # newest first


def test_ordered_newest_first(store):
    r1 = retrieve(store, "a")
    r2 = retrieve(store, "b")
    store.record_outcome(r1, signal="thumbs_down")
    store.record_outcome(r2, signal="thumbs_up")
    rows = store.recent_outcomes("kb")
    assert [row["receipt_id"] for row in rows] == [r2, r1]


def test_limit_bounds_the_result(store):
    for i in range(5):
        store.record_outcome(retrieve(store, f"item{i}"), signal="thumbs_up")
    assert len(store.recent_outcomes("kb", limit=2)) == 2
    assert len(store.recent_outcomes("kb", limit=100)) == 5


def test_scoped_to_one_asset_only(store):
    r_kb = retrieve(store, "a", asset="kb")
    r_other = retrieve(store, "z", asset="other_asset")
    store.record_outcome(r_kb, signal="thumbs_up")
    store.record_outcome(r_other, signal="thumbs_down")
    rows = store.recent_outcomes("kb")
    assert len(rows) == 1 and rows[0]["receipt_id"] == r_kb


def test_a_mixed_asset_receipt_only_lists_this_assets_own_item_ids(store):
    r = store.record_retrieval(context_name="kb_ctx", items=[item("a", asset="kb", rank=0), item("z", asset="other_asset", rank=1)])
    store.record_outcome(r, signal="thumbs_up")
    row = store.recent_outcomes("kb")[0]
    assert row["item_ids"] == ["a"]


def test_window_filters_by_time(store):
    r = retrieve(store, "a")
    store.record_outcome(r, signal="thumbs_up")
    assert store.recent_outcomes("kb", since="2999-01-01T00:00:00+00:00") == []
    assert len(store.recent_outcomes("kb", since="2000-01-01T00:00:00+00:00")) == 1


def test_no_outcomes_yet_is_an_empty_list_not_an_error(store):
    retrieve(store, "a")  # retrieved but never rated
    assert store.recent_outcomes("kb") == []


def test_empty_store(store):
    assert store.recent_outcomes("kb") == []


def test_many_receipts_past_sqlites_bound_variable_batch(store):
    for i in range(620):  # more receipts than SQLite's 500-variable batch
        store.record_outcome(retrieve(store, "hot"), signal="thumbs_up", outcome_ref=f"user-{i}")
    rows = store.recent_outcomes("kb", limit=10)
    assert len(rows) == 10
    refs = [row["outcome_ref"] for row in rows]
    assert refs == [f"user-{i}" for i in range(619, 609, -1)]  # newest first, across the batch boundary
