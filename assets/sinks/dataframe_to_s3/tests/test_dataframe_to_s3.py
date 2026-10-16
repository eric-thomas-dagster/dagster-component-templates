"""Committed tests for DataframeToS3Component: backfill_policy + per-key writes on a range run.

boto3 is installed but s3fs is not, and there is no real S3, so the external boundary is
faked in the test module: an `s3fs` stub (the component only imports it) and pandas'
DataFrame.to_csv / to_json, which record the s3:// URL and the exact frame they receive.
The component itself (partition filtering, key templating, per-key dispatch, metadata)
runs for real under dg.materialize().
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
IDS = {"2024-01-02": [101, 102], "2024-01-03": [201, 202], "2024-01-04": [301, 302]}


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def s3(monkeypatch):
    """Records {s3_url: frame} for every write."""
    monkeypatch.setenv("S3_BUCKET", "my-bucket")
    monkeypatch.setitem(sys.modules, "s3fs", types.ModuleType("s3fs"))
    written = {}

    def _rec(self, path, *a, **k):
        assert str(path).startswith("s3://"), path
        assert str(path) not in written, f"{path} written twice"
        written[str(path)] = self.reset_index(drop=True).copy()

    monkeypatch.setattr(pd.DataFrame, "to_csv", _rec)
    monkeypatch.setattr(pd.DataFrame, "to_json", _rec)
    return written


def make_sink(mod, key, **kw):
    kw.setdefault("partition_type", "daily")
    kw.setdefault("partition_start", "2024-01-01")
    kw.setdefault("format", "csv")
    return mod.DataframeToS3Component(
        asset_name="to_s3", upstream_asset_key="orders_src", key=key, **kw
    ).build_defs(None).assets[0]


def run_range(sink, multi, up=None, tags=RANGE_TAGS, raise_on_error=True):
    if up is None:
        up = daily_upstream()
        instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager(multi=multi)
        materialize_upstream_days(up, instance, io, DAYS)
        return dg.materialize([up, sink], selection=[sink.key], instance=instance,
                              resources={"io_manager": io}, tags=tags, raise_on_error=raise_on_error)
    return dg.materialize([up, sink], tags=tags, raise_on_error=raise_on_error)


def test_backfill_policy_wired_and_off_by_default(mod):
    assert make_sink(mod, "k.csv").backfill_policy is None
    assert make_sink(mod, "k.csv", backfill_policy="single_run").backfill_policy == dg.BackfillPolicy.single_run()
    multi = make_sink(mod, "k.csv", backfill_policy="multi_run", backfill_policy_max_partitions_per_run=2)
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_key(mod, s3):
    up = daily_upstream()
    sink = make_sink(mod, "orders/{partition_key}.csv", partition_date_column="order_date",
                     backfill_policy="single_run")
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager(multi="dict")
    materialize_upstream_days(up, instance, io, DAYS)
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    assert list(s3) == ["s3://my-bucket/orders/2024-01-03.csv"]
    assert sorted(s3["s3://my-bucket/orders/2024-01-03.csv"]["id"]) == IDS["2024-01-03"]
    md = r.asset_materializations_for_node("to_s3")[0].metadata
    assert md["row_count"].value == 2 and "partition_keys_written" not in md


@pytest.mark.parametrize("multi", ["dict", "concat"])
def test_range_templated_key_writes_one_object_per_partition_key(mod, s3, multi):
    sink = make_sink(mod, "orders/{partition_key}.csv", partition_date_column="order_date",
                     backfill_policy="single_run")
    r = run_range(sink, multi)
    assert r.success
    assert sorted(s3) == [f"s3://my-bucket/orders/{d}.csv" for d in DAYS]
    for d in DAYS:
        df = s3[f"s3://my-bucket/orders/{d}.csv"]
        assert sorted(df["id"]) == IDS[d]
        assert all(str(v).startswith(d) for v in df["order_date"])
    md = r.asset_materializations_for_node("to_s3")[0].metadata
    assert md["row_count"].value == 6 and md["dagster/row_count"].value == 6
    assert md["partition_keys_written"].value == 3


def test_range_templated_json_format_also_splits_per_key(mod, s3):
    sink = make_sink(mod, "orders/{partition_key}.jsonl", format="json",
                     partition_date_column="order_date", backfill_policy="single_run")
    assert run_range(sink, "dict").success
    assert sorted(s3) == [f"s3://my-bucket/orders/{d}.jsonl" for d in DAYS]


def test_range_split_runs_on_a_partition_agnostic_upstream(mod, s3):
    """Upstream is NOT partitioned: it hands over all 3 days, so only the component's own
    logic can split per key and drop the 2024-01-02 rows outside the 03..04 range."""
    sink = make_sink(mod, "orders/{partition_key}.csv", partition_date_column="order_date",
                     backfill_policy="single_run")
    tags = {"dagster/asset_partition_range_start": "2024-01-03",
            "dagster/asset_partition_range_end": "2024-01-04"}
    r = run_range(sink, "concat", up=all_days_upstream(), tags=tags)
    assert r.success
    assert sorted(s3) == ["s3://my-bucket/orders/2024-01-03.csv", "s3://my-bucket/orders/2024-01-04.csv"]
    assert sorted(s3["s3://my-bucket/orders/2024-01-03.csv"]["id"]) == [201, 202]
    assert sorted(s3["s3://my-bucket/orders/2024-01-04.csv"]["id"]) == [301, 302]


def test_range_templated_frame_without_split_column_fails_loudly(mod, s3):
    sink = make_sink(mod, "orders/{partition_key}.csv", backfill_policy="single_run")
    r = run_range(sink, "concat", raise_on_error=False)
    assert not r.success
    failure = [e for e in r.all_events if e.event_type_value == "STEP_FAILURE"][0]
    assert "dict of DataFrames" in failure.event_specific_data.error.cause.message
    assert s3 == {}


@pytest.mark.parametrize("multi", ["dict", "concat"])
def test_range_plain_key_writes_one_object_with_whole_range(mod, s3, multi):
    sink = make_sink(mod, "orders/all.csv", partition_date_column="order_date", backfill_policy="single_run")
    r = run_range(sink, multi)
    assert r.success
    assert list(s3) == ["s3://my-bucket/orders/all.csv"]
    assert sorted(s3["s3://my-bucket/orders/all.csv"]["id"]) == [101, 102, 201, 202, 301, 302]
    md = r.asset_materializations_for_node("to_s3")[0].metadata
    assert md["row_count"].value == 6 and "partition_keys_written" not in md


def test_range_run_id_key_is_one_write(mod, s3):
    sink = make_sink(mod, "orders/{run_id}.csv", partition_date_column="order_date", backfill_policy="single_run")
    r = run_range(sink, "dict")
    assert r.success
    assert list(s3) == [f"s3://my-bucket/orders/{r.run_id}.csv"]
    assert len(s3[f"s3://my-bucket/orders/{r.run_id}.csv"]) == 6
