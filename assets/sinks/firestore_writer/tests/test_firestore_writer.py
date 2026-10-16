"""Committed tests for FirestoreWriterComponent: backfill_policy + write path.

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
    install_fake_firestore,

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
    return install_fake_firestore(monkeypatch)


def _component(mod, **kw):
    return mod.FirestoreWriterComponent(
        asset_name="to_firestore", upstream_asset_key=kw.pop("upstream_asset_key", "orders_flat"), collection="orders", **kw)


def _written_ids(fake):
    (client,) = fake.instances
    return sorted(d["id"] for d in client.docs["orders"].values())


def test_unpartitioned_write_with_id_column_and_creds(mod, fake, monkeypatch):
    monkeypatch.setenv("FS_PROJECT", "my-proj")
    monkeypatch.setenv("FS_CREDS", "/tmp/key.json")
    sink = _component(mod, id_column="id", project_id_env_var="FS_PROJECT",
                      credentials_env_var="FS_CREDS").build_defs(None).assets[0]
    assert sink.backfill_policy is None
    assert dg.materialize([_unpartitioned_upstream(), sink]).success
    (client,) = fake.instances
    assert client.project == "my-proj" and client.credentials == "creds-from:/tmp/key.json"
    assert sorted(client.docs["orders"]) == ["1", "2", "3"]  # doc ids come from id_column
    assert client.docs["orders"]["2"] == {"id": 2, "total": 2.5}


def test_merge_true_preserves_existing_fields_and_no_id_column_autogenerates(mod, fake):
    sink = _component(mod, id_column="id", merge=True).build_defs(None).assets[0]
    # pre-existing document in the (single) client that will be created: seed via a subclass hook
    orig_init = fake.__init__

    def seeded(self, *a, **k):
        orig_init(self, *a, **k)
        self.docs["orders"] = {"2": {"legacy": "keep"}}

    fake.__init__ = seeded
    try:
        assert dg.materialize([_unpartitioned_upstream(), sink]).success
    finally:
        fake.__init__ = orig_init
    (client,) = fake.instances
    assert client.docs["orders"]["2"] == {"legacy": "keep", "id": 2, "total": 2.5}

    fake.instances.clear()
    sink2 = _component(mod).build_defs(None).assets[0]
    assert dg.materialize([_unpartitioned_upstream(), sink2]).success
    assert sorted(fake.instances[0].docs["orders"]) == ["auto-1", "auto-2", "auto-3"]


def _partitioned_component(mod, **kw):
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
    md = r.asset_materializations_for_node("to_firestore")[0].metadata
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
