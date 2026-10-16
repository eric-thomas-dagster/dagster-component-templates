"""Committed tests for DataframeToJsonComponent: backfill_policy + per-partition-key writes on a range run.

The component runs for real under dg.materialize() and writes REAL JSON files into
tmp_path, which the tests read back. Nothing is mocked. Range runs are driven the way a
real single_run backfill drives them: the `dagster/asset_partition_range_*` run tags plus a
partition-range-aware IO manager (PartitionedMemoryIOManager).
"""
import pathlib

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


def read(path):
    return pd.read_json(path, orient='records', convert_dates=False)


def make_sink(mod, path, **kw):
    kw.setdefault("partition_type", "daily")
    kw.setdefault("partition_start", "2024-01-01")
    return mod.DataframeToJsonComponent(
        asset_name="to_file", upstream_asset_key="orders_src", file_path=str(path), **kw
    ).build_defs(None).assets[0]


def run_range(sink, up, multi, days=DAYS, tags=RANGE_TAGS, materialize_days=True, raise_on_error=True):
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager(multi=multi)
    if materialize_days:
        materialize_upstream_days(up, instance, io, days)
    return dg.materialize([up, sink], selection=[sink.key], instance=instance,
                          resources={"io_manager": io}, tags=tags, raise_on_error=raise_on_error)


# ------------------------------------------------------------- policy wiring

def test_backfill_policy_wired_and_off_by_default(mod, tmp_path):
    p = tmp_path / "o.json"
    assert make_sink(mod, p).backfill_policy is None
    assert make_sink(mod, p, backfill_policy="single_run").backfill_policy == dg.BackfillPolicy.single_run()
    multi = make_sink(mod, p, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=2)
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)
    # unpartitioned asset: policy is ignored (Dagster would reject it otherwise)
    flat = mod.DataframeToJsonComponent(asset_name="to_file", upstream_asset_key="orders_src", file_path=str(p),
                     backfill_policy="single_run").build_defs(None).assets[0]
    assert flat.backfill_policy is None


# ------------------------------------------------------------ single key run

def test_single_partition_run_writes_only_that_days_file(mod, tmp_path):
    up = daily_upstream()
    sink = make_sink(mod, tmp_path / "out_{partition_key}.json", partition_date_column="order_date",
                     backfill_policy="single_run")
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager(multi="dict")
    materialize_upstream_days(up, instance, io, DAYS)
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out_2024-01-03.json"]
    assert sorted(read(tmp_path / "out_2024-01-03.json")["id"]) == IDS["2024-01-03"]
    md = r.asset_materializations_for_node("to_file")[0].metadata
    assert md["row_count"].value == 2
    assert "partition_keys_written" not in md


# -------------------------------------------------- range: templated -> per key

@pytest.mark.parametrize("multi", ["dict", "concat"])
def test_range_with_templated_path_writes_one_file_per_key(mod, tmp_path, multi):
    """dict upstream (filesystem-IO-manager style) is sliced per key; a single frame
    spanning the range is split per key via partition_date_column."""
    sink = make_sink(mod, tmp_path / "out_{partition_key}.json", partition_date_column="order_date",
                     backfill_policy="single_run")
    r = run_range(sink, daily_upstream(), multi)
    assert r.success
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"out_{d}.json" for d in DAYS]
    for d in DAYS:
        df = read(tmp_path / f"out_{d}.json")
        assert sorted(df["id"]) == IDS[d]
        assert all(str(v).startswith(d) for v in df["order_date"])
    md = r.asset_materializations_for_node("to_file")[0].metadata
    assert md["row_count"].value == 6
    assert md["dagster/row_count"].value == 6
    assert md["partition_keys_written"].value == 3


def test_range_templated_dict_upstream_needs_no_date_column(mod, tmp_path):
    sink = make_sink(mod, tmp_path / "out_{partition_key}.json", backfill_policy="single_run")
    r = run_range(sink, daily_upstream(), "dict")
    assert r.success
    for d in DAYS:
        assert sorted(read(tmp_path / f"out_{d}.json")["id"]) == IDS[d]


def test_range_split_runs_on_a_partition_agnostic_upstream(mod, tmp_path):
    """The upstream is NOT partitioned, so it always hands over all 3 days. Only the
    component's own range logic can produce per-key files with exactly each key's rows
    and skip the 2024-01-02 rows outside the 03..04 range."""
    sink = make_sink(mod, tmp_path / "out_{partition_key}.json", partition_date_column="order_date",
                     backfill_policy="single_run")
    tags = {"dagster/asset_partition_range_start": "2024-01-03",
            "dagster/asset_partition_range_end": "2024-01-04"}
    r = dg.materialize([all_days_upstream(), sink], tags=tags)
    assert r.success
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out_2024-01-03.json", "out_2024-01-04.json"]
    assert sorted(read(tmp_path / "out_2024-01-03.json")["id"]) == [201, 202]
    assert sorted(read(tmp_path / "out_2024-01-04.json")["id"]) == [301, 302]
    assert r.asset_materializations_for_node("to_file")[0].metadata["row_count"].value == 4


def test_range_templated_frame_without_split_column_fails_loudly(mod, tmp_path):
    sink = make_sink(mod, tmp_path / "out_{partition_key}.json", backfill_policy="single_run")
    r = run_range(sink, daily_upstream(), "concat", raise_on_error=False)
    assert not r.success
    failure = [e for e in r.all_events if e.event_type_value == "STEP_FAILURE"][0]
    assert "dict of DataFrames" in failure.event_specific_data.error.cause.message
    assert list(tmp_path.iterdir()) == []  # nothing half-written


# ----------------------------------------- range: NOT templated -> one write

@pytest.mark.parametrize("multi", ["dict", "concat"])
def test_range_with_plain_path_writes_one_file_with_whole_range(mod, tmp_path, multi):
    sink = make_sink(mod, tmp_path / "all.json", partition_date_column="order_date",
                     backfill_policy="single_run")
    r = run_range(sink, daily_upstream(), multi)
    assert r.success
    assert [p.name for p in tmp_path.iterdir()] == ["all.json"]
    assert sorted(read(tmp_path / "all.json")["id"]) == [101, 102, 201, 202, 301, 302]
    md = r.asset_materializations_for_node("to_file")[0].metadata
    assert md["row_count"].value == 6
    assert "partition_keys_written" not in md


def test_range_run_id_placeholder_is_not_a_partition_template(mod, tmp_path):
    sink = make_sink(mod, tmp_path / "all_{run_id}.json", partition_date_column="order_date",
                     backfill_policy="single_run")
    r = run_range(sink, daily_upstream(), "dict")
    assert r.success
    (f,) = list(tmp_path.iterdir())
    # this format does not expand {run_id} itself (pre-existing); the point here is ONE write, not per-key
    assert len(read(f)) == 6


def test_plain_path_range_filter_narrows_partition_agnostic_upstream(mod, tmp_path):
    sink = make_sink(mod, tmp_path / "all.json", partition_date_column="order_date",
                     backfill_policy="single_run")
    tags = {"dagster/asset_partition_range_start": "2024-01-03",
            "dagster/asset_partition_range_end": "2024-01-04"}
    r = dg.materialize([all_days_upstream(), sink], tags=tags)
    assert r.success
    assert sorted(read(tmp_path / "all.json")["id"]) == [201, 202, 301, 302]

