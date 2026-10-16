"""Committed tests for DataframeToAdlsComponent: backfill_policy + range-safe partition filter.

One write per run. The external boundary (adlfs is not installed / there is no real ADLS)
is faked in the test module: a `adlfs` stub (the component only imports it) and pandas'
DataFrame.to_csv, which records the abfs:// URL and the exact frame it receives. The
component (partition filter, backfill wiring, metadata) runs for real under dg.materialize().
"""
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import (
    PartitionedMemoryIOManager,
    all_days_upstream,
    daily_upstream,
    load_component_module,
    materialize_upstream_days,
)

RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-02",
    "dagster/asset_partition_range_end": "2024-01-04",
}
DAYS = ["2024-01-02", "2024-01-03", "2024-01-04"]


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def cloud(monkeypatch):
    """Records [(url, frame, storage_options)] for every write."""
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "acct")
    monkeypatch.setenv("AZURE_STORAGE_KEY", "k")
    monkeypatch.setitem(sys.modules, "adlfs", types.ModuleType("adlfs"))
    writes = []

    def _to_csv(self, path, *a, **k):
        writes.append((str(path), self.reset_index(drop=True).copy(), k.get("storage_options")))

    monkeypatch.setattr(pd.DataFrame, "to_csv", _to_csv)
    return writes


def make_sink(mod, **kw):
    kw.setdefault("partition_type", "daily")
    kw.setdefault("partition_start", "2024-01-01")
    return mod.DataframeToAdlsComponent(
        asset_name="to_cloud", upstream_asset_key="orders_src", container="cont", blob_path="out/orders.csv",
        format="csv", partition_date_column="order_date", **kw
    ).build_defs(None).assets[0]


def run_range(sink, multi, tags=RANGE_TAGS):
    up = daily_upstream()
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager(multi=multi)
    materialize_upstream_days(up, instance, io, DAYS)
    return dg.materialize([up, sink], selection=[sink.key], instance=instance,
                          resources={"io_manager": io}, tags=tags)


def test_backfill_policy_wired_and_off_by_default(mod):
    assert make_sink(mod).backfill_policy is None
    assert make_sink(mod, backfill_policy="single_run").backfill_policy == dg.BackfillPolicy.single_run()
    multi = make_sink(mod, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=2)
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_rows(mod, cloud):
    up = daily_upstream()
    sink = make_sink(mod, backfill_policy="single_run")
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, DAYS)
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    ((url, df, _),) = cloud
    assert url.startswith("abfs://") and url.endswith("out/orders.csv")
    assert sorted(df["id"]) == [201, 202]


def test_single_run_range_writes_all_keys_in_range_once(mod, cloud):
    r = run_range(make_sink(mod, backfill_policy="single_run"), "concat")
    assert r.success
    ((url, df, _),) = cloud
    assert sorted(df["id"]) == [101, 102, 201, 202, 301, 302]
    md = r.asset_materializations_for_node("to_cloud")[0].metadata
    assert md["row_count"].value == 6 and md["dagster/row_count"].value == 6


def test_range_with_dict_upstream_is_bridged_and_written_once(mod, cloud):
    r = run_range(make_sink(mod, backfill_policy="single_run"), "dict")
    assert r.success
    ((url, df, _),) = cloud
    assert sorted(df["id"]) == [101, 102, 201, 202, 301, 302]


def test_range_filter_actually_runs_on_a_partition_agnostic_upstream(mod, cloud):
    """Upstream is NOT partitioned: it hands over 3 days of rows, so only the component's
    own range filter can narrow them to the 2-day range 03..04."""
    tags = {"dagster/asset_partition_range_start": "2024-01-03",
            "dagster/asset_partition_range_end": "2024-01-04"}
    sink = make_sink(mod, backfill_policy="single_run")
    r = dg.materialize([all_days_upstream(), sink], tags=tags)
    assert r.success
    ((url, df, _),) = cloud
    assert sorted(df["id"]) == [201, 202, 301, 302]
    assert r.asset_materializations_for_node("to_cloud")[0].metadata["row_count"].value == 4


def test_range_filter_on_static_partitions_keeps_any_key_in_range(mod, cloud):
    @dg.asset(name="orders_src")
    def all_regions() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3, 4], "region": ["a", "b", "c", "a"]})

    sink = mod.DataframeToAdlsComponent(
        asset_name="to_cloud", upstream_asset_key="orders_src", container="cont", blob_path="out/orders.csv", format="csv",
        partition_type="static", partition_values="a,b,c", partition_static_column="region",
        backfill_policy="single_run").build_defs(None).assets[0]
    r = dg.materialize([all_regions, sink], tags={
        "dagster/asset_partition_range_start": "a", "dagster/asset_partition_range_end": "b"})
    assert r.success
    ((url, df, _),) = cloud
    assert sorted(df["id"]) == [1, 2, 4]  # regions a and b, not c

