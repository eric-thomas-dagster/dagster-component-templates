"""Committed tests for MongodbWriterComponent: resource_key retrofit + backfill_policy.

Only the external service is faked; the component runs for real under dg.materialize().
Backfill tests use the `dagster/asset_partition_range_*` run tags a real single_run
backfill sets, plus PartitionedMemoryIOManager (the default filesystem IO manager can't
persist/load a multi-partition range) and a discriminating partition-agnostic-upstream
test so the component's own range filter is what narrows the rows.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import (
    FakeMongoClient,
    FakeMongoResource,
    FakeCollection,
    install_fake_pymongo,

    PartitionedMemoryIOManager,
    daily_upstream,
    load_component_module,
    materialize_upstream_days,
)

RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-02",
    "dagster/asset_partition_range_end": "2024-01-04",
}


@pytest.fixture()
def mod():
    return load_component_module()


def _unpartitioned_upstream():
    @dg.asset(name="orders_flat")
    def orders_flat() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3], "total": [1.5, 2.5, 3.5]})
    return orders_flat


@pytest.fixture()
def fake(monkeypatch):
    monkeypatch.setenv("MONGODB_URI", "mongodb://inline.example:27017")
    return install_fake_pymongo(monkeypatch)


def _component(mod, **kw):
    return mod.MongodbWriterComponent(
        asset_name="to_mongo", upstream_asset_key=kw.pop("upstream_asset_key", "orders_flat"), database="shop",
        collection="orders", **kw)


def _written_ids(fake):
    (client,) = fake.instances
    return sorted(d["id"] for d in client.colls[("shop", "orders")].docs)


def test_resource_key_default_none_and_inline_append(mod, fake):
    comp = _component(mod, if_exists="append")
    assert comp.resource_key is None
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([_unpartitioned_upstream(), sink]).success
    (client,) = fake.instances
    assert client.args == ("mongodb://inline.example:27017",)
    assert _written_ids(fake) == [1, 2, 3]


def test_inline_replace_drops_existing_docs(mod, fake):
    fake.preseed = {("shop", "orders"): [{"id": 99, "total": 0}]}
    assert dg.materialize([_unpartitioned_upstream(), _component(mod).build_defs(None).assets[0]]).success
    assert _written_ids(fake) == [1, 2, 3]


def test_inline_upsert_updates_matching_and_inserts_new(mod, fake):
    fake.preseed = {("shop", "orders"): [{"id": 2, "total": 0, "keep": "me"}]}
    sink = _component(mod, if_exists="upsert", upsert_key="id").build_defs(None).assets[0]
    assert dg.materialize([_unpartitioned_upstream(), sink]).success
    (client,) = fake.instances
    docs = {d["id"]: d for d in client.colls[("shop", "orders")].docs}
    assert sorted(docs) == [1, 2, 3]
    assert docs[2]["total"] == 2.5 and docs[2]["keep"] == "me"  # $set merges, no duplicate


def test_resource_path_append_and_upsert_ignore_inline_config(mod, fake, monkeypatch):
    monkeypatch.delenv("MONGODB_URI")  # inline config absent: resource must be sufficient
    for kw, expect in (({"if_exists": "append"}, [1, 2, 3]),
                       ({"if_exists": "upsert", "upsert_key": "id"}, [1, 2, 3])):
        shared = FakeMongoClient()
        shared.colls[("shop", "orders")] = FakeCollection()
        shared.colls[("shop", "orders")].docs = [{"id": 2, "total": 0}] if "upsert_key" in kw else []
        fake.instances.clear()
        sink = _component(mod, resource_key="mongo_shared", **kw).build_defs(None).assets[0]
        assert sink.required_resource_keys - {"io_manager"} == {"mongo_shared"}
        assert dg.materialize([_unpartitioned_upstream(), sink],
                              resources={"mongo_shared": FakeMongoResource(shared)}).success
        assert fake.instances == []  # the inline MongoClient was never constructed
        docs = shared.colls[("shop", "orders")].docs
        assert sorted(d["id"] for d in docs) == expect
        if "upsert_key" in kw:
            assert [d["total"] for d in docs if d["id"] == 2] == [2.5]


def test_resource_key_set_but_not_registered_fails_loudly(mod, fake):
    sink = _component(mod, resource_key="missing_key", if_exists="append").build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_unpartitioned_upstream(), sink])


def _partitioned_component(mod, **kw):
    kw.setdefault("if_exists", "append")
    return _component(mod, upstream_asset_key="orders_src", partition_type="daily", partition_start="2024-01-01",
                      partition_date_column="order_date", **kw)


# ------------------------------------------------------------- backfill_policy

def test_backfill_policy_wired_and_off_by_default(mod, fake):
    assert _partitioned_component(mod).build_defs(None).assets[0].backfill_policy is None
    single = _partitioned_component(mod, backfill_policy="single_run").build_defs(None).assets[0]
    assert single.backfill_policy == dg.BackfillPolicy.single_run()
    multi = _partitioned_component(
        mod, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=2).build_defs(None).assets[0]
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_rows(mod, fake):
    up = daily_upstream()
    sink = _partitioned_component(mod).build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03"])
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    assert _written_ids(fake) == [201, 202]


def test_single_run_backfill_range_writes_every_key_in_range_once(mod, fake):
    up = daily_upstream()
    sink = _partitioned_component(mod, backfill_policy="single_run").build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03", "2024-01-04"])
    r = dg.materialize([up, sink], selection=[sink.key], instance=instance,
                       resources={"io_manager": io}, tags=RANGE_TAGS)
    assert r.success
    # All 3 days' rows, each exactly once (no duplicates in the recorded writes).
    assert _written_ids(fake) == [101, 102, 201, 202, 301, 302]
    md = r.asset_materializations_for_node("to_mongo")[0].metadata
    assert md["row_count"].value == 6


def test_range_filter_actually_runs_on_a_partition_agnostic_upstream(mod, fake):
    """The upstream here is NOT partitioned, so it always hands over all 3 days of
    rows; only the component's own range filter can narrow them to the 2-day range."""
    @dg.asset(name="orders_src")
    def all_days() -> pd.DataFrame:
        return pd.concat([
            pd.DataFrame({"id": [d * 10 + 1, d * 10 + 2],
                          "order_date": [f"2024-01-0{d} 09:00:00", f"2024-01-0{d} 18:00:00"],
                          "total": [1.5, 2.5]})
            for d in (2, 3, 4)
        ], ignore_index=True)

    sink = _partitioned_component(mod, backfill_policy="single_run").build_defs(None).assets[0]
    r = dg.materialize([all_days, sink], tags={
        "dagster/asset_partition_range_start": "2024-01-03",
        "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    assert _written_ids(fake) == [31, 32, 41, 42]
