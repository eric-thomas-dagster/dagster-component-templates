"""Partition isolation + backfill_policy for rag_pipeline: real chromadb, real ContextReceiptStore, real Dagster
materializations. Only embedding / completion calls are faked."""
import hashlib
from unittest.mock import patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module
from .test_receipts import _fake_completion, _store, _vec, index, _out, _meta  # noqa: F401

chromadb = pytest.importorskip("chromadb")

DAY1, DAY2 = "2024-01-01", "2024-01-02"
ROWS = pd.DataFrame({
    "query": ["day1 q1 charged twice", "day1 q2 export report", "day2 q1 shipping time"],
    "day": ["2024-01-01T09:00:00", "2024-01-01T17:30:00", "2024-01-02T08:00:00"],
    "customer": ["acme", "globex", "acme"],
})
STEPS = [{"id": "found", "op": "retrieve", "retriever": "kb", "top_k": 2},
         {"id": "ans", "op": "generate", "source": "found", "model": "gpt-4o-mini"}]


@pytest.fixture()
def mod():
    return load_component_module()


def comp(mod, index, receipts=True, **kw):
    kw.setdefault("partition_type", "daily")
    kw.setdefault("partition_start", DAY1)
    return mod.RAGPipelineComponent(
        asset_name="answers", upstream_asset_key="questions", steps=STEPS, retrievers={
            "kb": {"kind": "chromadb", "path": index, "collection_name": "support_kb"}},
        receipts={"resource_key": "context_receipts"} if receipts else None, **kw)


def questions_asset(rows=ROWS, partitions_def=None):
    @dg.asset(name="questions", partitions_def=partitions_def)
    def questions():
        return rows
    return questions


def run(mod, c, key=None, store=None, upstream=None, io=None, raise_on_error=True, **kw):
    resources = {"io_manager": io or dg.InMemoryIOManager()}
    if store is not None:
        resources["context_receipts"] = dg.ResourceDefinition.hardcoded_resource(store)
    with patch.object(mod, "_embed_query", lambda cfg, q: _vec(q)), patch("litellm.completion", side_effect=_fake_completion), \
            patch("litellm.completion_cost", return_value=0.0):
        return dg.materialize([upstream or questions_asset(), c.build_defs(None).assets[0]], partition_key=key,
                              resources=resources, raise_on_error=raise_on_error, **kw)


def qhash(q):
    return hashlib.sha256(q.encode()).hexdigest()


# --- partition_date_column / partition_static_column were dead config ---------------------------

def test_date_column_restricts_each_partition_to_its_own_rows(mod, index, tmp_path):
    _, store = _store(tmp_path)
    c = comp(mod, index, partition_date_column="day")
    o1 = _out(run(mod, c, DAY1, store))
    o2 = _out(run(mod, c, DAY2, store))
    assert o1["query"].tolist() == ["day1 q1 charged twice", "day1 q2 export report"]
    assert o2["query"].tolist() == ["day2 q1 shipping time"]
    assert _meta(run(mod, c, DAY1, store))["dagster/row_count"].value == 2
    hashes = lambda o: {store.get_receipt(r)["query_hash"] for r in o["receipt_id"]}   # noqa: E731
    assert hashes(o1) == {qhash(q) for q in o1["query"]} and hashes(o2) == {qhash("day2 q1 shipping time")}


def test_static_column_restricts_each_partition_to_its_own_rows(mod, index):
    c = comp(mod, index, receipts=False, partition_type="static", partition_start=None,
             partition_values="acme,globex", partition_static_column="customer")
    assert run(mod, c, "acme").output_for_node("answers")["query"].tolist() == ["day1 q1 charged twice", "day2 q1 shipping time"]
    assert run(mod, c, "globex").output_for_node("answers")["query"].tolist() == ["day1 q2 export report"]


def test_multi_dimension_applies_both_filters(mod, index):
    dims = [{"name": "date", "type": "daily", "start": DAY1}, {"name": "customer", "type": "static", "values": ["acme", "globex"]}]
    c = comp(mod, index, receipts=False, partition_type=None, partition_start=None, partition_dimensions=dims,
             partition_date_column="day", partition_static_column="customer", partition_static_dim="customer")
    key = lambda d, cu: dg.MultiPartitionKey({"date": d, "customer": cu})   # noqa: E731
    assert run(mod, c, key(DAY1, "acme")).output_for_node("answers")["query"].tolist() == ["day1 q1 charged twice"]
    assert run(mod, c, key(DAY1, "globex")).output_for_node("answers")["query"].tolist() == ["day1 q2 export report"]
    assert run(mod, c, key(DAY2, "acme")).output_for_node("answers")["query"].tolist() == ["day2 q1 shipping time"]
    assert len(run(mod, c, key(DAY2, "globex")).output_for_node("answers")) == 0    # an empty partition is a normal success


def test_without_filter_columns_the_upstream_is_used_as_is(mod, index):
    c = comp(mod, index, receipts=False)
    assert len(run(mod, c, DAY1).output_for_node("answers")) == 3


def test_a_partitioned_upstream_is_read_one_partition_at_a_time(mod, index):
    parts = dg.DailyPartitionsDefinition(DAY1)

    def upstream():
        @dg.asset(name="questions", partitions_def=parts)
        def questions(context: dg.AssetExecutionContext):
            return pd.DataFrame({"query": [f"question for {context.partition_key}"]})
        return questions
    c = comp(mod, index, receipts=False)
    assert run(mod, c, DAY1, upstream=upstream()).output_for_node("answers")["query"].tolist() == [f"question for {DAY1}"]
    assert run(mod, c, DAY2, upstream=upstream()).output_for_node("answers")["query"].tolist() == [f"question for {DAY2}"]


# --- no state leaks between partitions ---------------------------------------------------------

def test_partition_key_templating_and_receipts_are_per_partition(mod, index, tmp_path):
    _, store = _store(tmp_path)
    c = comp(mod, index, partition_date_column="day", query_prompt_template="[{partition_key}] {query}")
    r1, r2 = run(mod, c, DAY1, store), run(mod, c, DAY2, store)
    o1, o2 = _out(r1), _out(r2)
    for o, day in ((o1, DAY1), (o2, DAY2)):
        got = {store.get_receipt(r)["query_hash"] for r in o["receipt_id"]}
        assert got == {qhash(f"[{day}] {q}") for q in o["query"]}           # each partition templated with ITS key
    assert set(o1["receipt_id"]).isdisjoint(o2["receipt_id"])
    assert {store.get_receipt(r)["dagster_run_id"] for r in o1["receipt_id"]} == {r1.run_id}
    assert {store.get_receipt(r)["dagster_run_id"] for r in o2["receipt_id"]} == {r2.run_id}


def test_retrieved_chunks_source_partition_reaches_the_receipt(mod, tmp_path):
    path = str(tmp_path / "chroma")
    col = chromadb.PersistentClient(path=path).create_collection("parts", metadata={"hnsw:space": "cosine"})
    texts = ["duplicate charge refund", "export a report"]
    col.add(ids=["c1", "c2"], documents=texts, embeddings=[_vec(t) for t in texts], metadatas=[
        {"chunk_id": "c1", "source_asset_key": "kb", "source_version": "v1", "source_partition": "2024-01-01", "source_code_version": "r1"},
        {"chunk_id": "c2", "source_asset_key": "kb", "source_version": "v1", "source_partition": "2024-01-02", "source_code_version": "r1"}])
    _, store = _store(tmp_path)
    c = mod.RAGPipelineComponent(asset_name="answers", upstream_asset_key="questions", steps=STEPS, partition_type="daily",
                                 partition_start=DAY1, receipts={"resource_key": "context_receipts"},
                                 retrievers={"kb": {"kind": "chromadb", "path": path, "collection_name": "parts"}})
    rid = _out(run(mod, c, DAY2, store))["receipt_id"][0]
    assert {i["item_id"]: i["source_partition"] for i in store.get_receipt(rid)["items"]} == {"c1": "2024-01-01", "c2": "2024-01-02"}


# --- backfill_policy ---------------------------------------------------------------------------

def _policy(mod, index, **kw):
    return comp(mod, index, receipts=False, **kw).build_defs(None).assets[0].backfill_policy


def test_policies_are_wired(mod, index):
    assert _policy(mod, index) is None
    p = _policy(mod, index, backfill_policy="multi_run")
    assert p.max_partitions_per_run == 1
    assert _policy(mod, index, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=4).max_partitions_per_run == 4
    assert _policy(mod, index, backfill_policy="single_run").max_partitions_per_run is None
    assert _policy(mod, index, backfill_policy="single_run", partition_date_column="day").max_partitions_per_run is None


def test_policy_is_ignored_when_unpartitioned(mod, index):
    assert _policy(mod, index, backfill_policy="single_run", partition_type=None, partition_start=None) is None


@pytest.mark.parametrize("kw", [
    {"backfill_policy": "single_run"},
    {"backfill_policy": "multi_run", "backfill_policy_max_partitions_per_run": 3},
])
@pytest.mark.parametrize("tmpl", ["[{partition_key}] {query}", "{partition.customer}: {query}"])
def test_multi_partition_runs_are_rejected_with_a_partition_template(mod, index, kw, tmpl):
    with pytest.raises(ValueError, match="one partition per\\s+run"):
        comp(mod, index, query_prompt_template=tmpl, **kw).build_defs(None)


def test_one_partition_per_run_is_fine_with_a_partition_template(mod, index):
    for kw in ({}, {"backfill_policy": "multi_run"}, {"backfill_policy": "multi_run", "backfill_policy_max_partitions_per_run": 1}):
        comp(mod, index, query_prompt_template="[{partition_key}] {query}", **kw).build_defs(None)
    comp(mod, index, query_prompt_template="Q: {query}", backfill_policy="single_run").build_defs(None)   # no partition placeholder


@pytest.mark.parametrize("kw, msg", [
    ({"backfill_policy_max_partitions_per_run": 2}, "only applies"),
    ({"backfill_policy": "single_run", "backfill_policy_max_partitions_per_run": 2}, "only applies"),
    ({"backfill_policy": "multi_run", "backfill_policy_max_partitions_per_run": 0}, ">= 1"),
])
def test_bad_policy_combinations(mod, index, kw, msg):
    with pytest.raises(ValueError, match=msg):
        comp(mod, index, **kw).build_defs(None)
    with pytest.raises(Exception):
        comp(mod, index, backfill_policy="sometimes")


class _RangeIO(dg.IOManager):
    """Stores one value per asset key, whatever partition range the run covered."""
    def __init__(self):
        self.vals = {}

    def handle_output(self, context, obj):
        self.vals[context.asset_key.to_user_string()] = obj

    def load_input(self, context):
        return self.vals[context.asset_key.to_user_string()]


def test_single_run_answers_a_whole_range_in_one_run_with_every_row_in_its_own_partition(mod, index, tmp_path):
    from dagster._core.storage.tags import ASSET_PARTITION_RANGE_END_TAG, ASSET_PARTITION_RANGE_START_TAG
    _, store = _store(tmp_path)
    c = comp(mod, index, backfill_policy="single_run", partition_date_column="day")
    io = _RangeIO()
    res = run(mod, c, None, store, io=io, tags={ASSET_PARTITION_RANGE_START_TAG: DAY1, ASSET_PARTITION_RANGE_END_TAG: DAY2})
    assert res.success
    out = io.vals["answers"]
    assert out["query"].tolist() == ROWS["query"].tolist()                  # the window covers both days
    assert len(set(out["receipt_id"])) == 3 and all(store.get_receipt(r) for r in out["receipt_id"])
    one_run = {store.get_receipt(r)["dagster_run_id"] for r in out["receipt_id"]}
    assert one_run == {res.run_id}


def test_a_multi_partition_run_with_a_partition_template_fails_loudly(mod, index, monkeypatch):
    from dagster._core.storage.tags import ASSET_PARTITION_RANGE_END_TAG, ASSET_PARTITION_RANGE_START_TAG
    monkeypatch.setattr(mod, "_build_backfill_policy", lambda *a, **k: dg.BackfillPolicy.single_run())   # bypass the load-time check
    c = comp(mod, index, receipts=False, query_prompt_template="[{partition_key}] {query}")
    res = run(mod, c, None, io=_RangeIO(), raise_on_error=False,
              tags={ASSET_PARTITION_RANGE_START_TAG: DAY1, ASSET_PARTITION_RANGE_END_TAG: DAY2})
    assert not res.success
    msgs = " ".join(e.event_specific_data.error.message + (e.event_specific_data.error.cause.message if e.event_specific_data.error.cause else "")
                    for e in res.get_step_failure_events())
    assert "covers several partitions" in msgs, msgs


def test_a_row_exactly_at_midnight_belongs_to_the_later_partition_only(mod, index):
    rows = pd.DataFrame({"query": ["last second of day1", "midnight"], "day": ["2024-01-01T23:59:59", "2024-01-02T00:00:00"]})
    c = comp(mod, index, receipts=False, partition_date_column="day")
    assert run(mod, c, DAY1, upstream=questions_asset(rows)).output_for_node("answers")["query"].tolist() == ["last second of day1"]
    assert run(mod, c, DAY2, upstream=questions_asset(rows)).output_for_node("answers")["query"].tolist() == ["midnight"]
