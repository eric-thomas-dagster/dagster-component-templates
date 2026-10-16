"""Committed tests for CosmosdbWriterComponent: resource_key retrofit + backfill_policy.

Only the external Cosmos client is faked (recording in-memory container). The
component runs for real under dg.materialize(). Backfill tests use the
`dagster/asset_partition_range_*` run tags a real single_run backfill sets plus
PartitionedMemoryIOManager() (the default filesystem IO manager can't persist/load a
multi-partition range) and assert the RANGE code path really ran.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import (
    FakeCosmosClient,
    FakeCosmosResource,
    PartitionedMemoryIOManager,
    daily_upstream,
    install_fake_azure_cosmos,
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


@pytest.fixture()
def fake_cosmos(monkeypatch):
    monkeypatch.setenv("COSMOS_ENDPOINT", "https://inline.example:443/")
    monkeypatch.setenv("COSMOS_KEY", "inline-key")
    return install_fake_azure_cosmos(monkeypatch)


def _written_ids(client):
    (container,) = client.containers.values()
    return sorted(item["id"] for _, item in container.items)


# ---------------------------------------------------------------- resource_key

def _unpartitioned_upstream():
    @dg.asset(name="orders_flat")
    def _up() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3], "total": [1.0, 2.0, 3.0]})
    return _up


def test_resource_key_defaults_to_none_and_uses_inline_env_vars(mod, fake_cosmos):
    comp = mod.CosmosdbWriterComponent(
        asset_name="to_cosmos", upstream_asset_key="orders_flat", database="shop", container="orders")
    assert comp.resource_key is None
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == set()
    assert dg.materialize([_unpartitioned_upstream(), sink]).success
    (client,) = fake_cosmos.instances
    assert client.endpoint == "https://inline.example:443/" and client.credential == "inline-key"
    assert _written_ids(client) == [1, 2, 3]


def test_resource_key_uses_registered_resource_and_ignores_inline_env(mod, fake_cosmos, monkeypatch):
    # Inline env vars deliberately absent: the resource path must not need them.
    monkeypatch.delenv("COSMOS_ENDPOINT")
    monkeypatch.delenv("COSMOS_KEY")
    shared_client = FakeCosmosClient(endpoint="https://resource.example:443/", credential="resource-key")
    fake_cosmos.instances = []
    comp = mod.CosmosdbWriterComponent(
        asset_name="to_cosmos", upstream_asset_key="orders_flat", database="shop",
        container="orders", resource_key="cosmos_shared")
    sink = comp.build_defs(None).assets[0]
    assert sink.required_resource_keys - {"io_manager"} == {"cosmos_shared"}
    result = dg.materialize([_unpartitioned_upstream(), sink],
                            resources={"cosmos_shared": FakeCosmosResource(shared_client)})
    assert result.success
    assert fake_cosmos.instances == []  # the inline CosmosClient was never constructed
    assert _written_ids(shared_client) == [1, 2, 3]


def test_resource_key_set_but_not_registered_fails_loudly(mod, fake_cosmos):
    comp = mod.CosmosdbWriterComponent(
        asset_name="to_cosmos", upstream_asset_key="orders_flat", database="shop",
        container="orders", resource_key="missing_key")
    sink = comp.build_defs(None).assets[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([_unpartitioned_upstream(), sink])


# ------------------------------------------------------------- backfill_policy

def _partitioned_component(mod, **kw):
    return mod.CosmosdbWriterComponent(
        asset_name="to_cosmos", upstream_asset_key="orders_src", database="shop", container="orders",
        partition_type="daily", partition_start="2024-01-01", partition_date_column="order_date", **kw)


def test_backfill_policy_wired_and_off_by_default(mod, fake_cosmos):
    assert _partitioned_component(mod).build_defs(None).assets[0].backfill_policy is None
    single = _partitioned_component(mod, backfill_policy="single_run").build_defs(None).assets[0]
    assert single.backfill_policy == dg.BackfillPolicy.single_run()
    multi = _partitioned_component(
        mod, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=2).build_defs(None).assets[0]
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_rows(mod, fake_cosmos):
    up = daily_upstream()
    sink = _partitioned_component(mod).build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03"])
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    (client,) = fake_cosmos.instances
    assert _written_ids(client) == [201, 202]


def test_single_run_backfill_range_writes_every_key_in_range_once(mod, fake_cosmos):
    up = daily_upstream()
    sink = _partitioned_component(mod, backfill_policy="single_run").build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03", "2024-01-04"])
    r = dg.materialize([up, sink], selection=[sink.key], instance=instance,
                       resources={"io_manager": io}, tags=RANGE_TAGS)
    assert r.success
    # One run, one client, all 3 days' rows exactly once (101,102,201,202,301,302).
    (client,) = fake_cosmos.instances
    assert _written_ids(client) == [101, 102, 201, 202, 301, 302]
    md = r.asset_materializations_for_node("to_cosmos")[0].metadata
    assert md["row_count"].value == 6


def test_range_filter_actually_runs_on_a_partition_agnostic_upstream(mod, fake_cosmos):
    """The upstream here is NOT partitioned, so it always hands over all 3 days of
    rows; only the component's own range filter can narrow them to the 2-day range.
    (Before the fix, a range had no single partition_key, the filter was skipped and
    all 6 rows were written.)"""
    @dg.asset(name="orders_src")
    def all_days() -> pd.DataFrame:
        return pd.concat([
            pd.DataFrame({"id": [d * 10 + 1, d * 10 + 2],
                          "order_date": [f"2024-01-0{d} 09:00:00", f"2024-01-0{d} 18:00:00"]})
            for d in (2, 3, 4)
        ], ignore_index=True)

    sink = _partitioned_component(mod, backfill_policy="single_run").build_defs(None).assets[0]
    r = dg.materialize([all_days, sink], tags={
        "dagster/asset_partition_range_start": "2024-01-03",
        "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    (client,) = fake_cosmos.instances
    assert _written_ids(client) == [31, 32, 41, 42]


def _all_rows_upstream():
    @dg.asset(name="orders_src")
    def all_rows() -> pd.DataFrame:
        rows = []
        for d in (2, 3):
            for seg in ("a", "b"):
                rows.append({"id": d * 10 + (1 if seg == "a" else 2), "order_date": f"2024-01-0{d} 10:00:00", "segment": seg})
        return pd.DataFrame(rows)
    return all_rows


def _multi_component(mod, **kw):
    return mod.CosmosdbWriterComponent(
        asset_name="to_cosmos", upstream_asset_key="orders_src", database="shop", container="orders",
        partition_type="multi", partition_start="2024-01-01", partition_values="a,b",
        partition_date_column="order_date", partition_static_column="segment", **kw)


def test_multi_dimension_range_filters_on_date_and_static_axes(mod, fake_cosmos):
    """MultiPartitionKey objects survive in context.partition_keys for a range; the
    filter is date AND static per key, OR'd across keys."""
    sink = _multi_component(mod, partition_static_dim="static_dim", backfill_policy="single_run").build_defs(None).assets[0]
    r = dg.materialize([_all_rows_upstream(), sink], tags={
        "dagster/asset_partition_range_start": "2024-01-02|a",
        "dagster/asset_partition_range_end": "2024-01-03|a"})
    assert r.success
    (client,) = fake_cosmos.instances
    assert _written_ids(client) == [21, 31]  # segment 'a' on 01-02 and 01-03 only


def test_multi_dimension_default_static_dim_name_matches_the_built_axis(mod, fake_cosmos):
    """With partition_type='multi' the built partitions_def names its static axis 'static_dim'.
    The filter used to look the static key up under `partition_static_dim or 'segment'`, which could
    never match, so the static filter was silently skipped (every segment written). It now tries
    partition_static_dim, then 'static_dim', then 'segment' -- so the default config filters."""
    sink = _multi_component(mod).build_defs(None).assets[0]
    r = dg.materialize([_all_rows_upstream(), sink], partition_key=dg.MultiPartitionKey({"date": "2024-01-02", "static_dim": "a"}))
    assert r.success
    (client,) = fake_cosmos.instances
    assert _written_ids(client) == [21]  # segment 'a' only on 01-02: the static filter ran
