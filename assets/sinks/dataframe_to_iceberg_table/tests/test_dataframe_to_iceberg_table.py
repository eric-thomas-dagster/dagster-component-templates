"""Committed tests for DataframeToIcebergTableComponent: backfill_policy + per-key overwrite.

Nothing is mocked: a REAL pyiceberg SQL catalog (sqlite file) with a local warehouse in
tmp_path backs every test, and tables are read back through pyiceberg. The dangerous case is
mode='overwrite' with overwrite_filter on a backfill range: each key must replace exactly its
own rows and leave other days untouched, and a re-run must be idempotent.
"""
import dagster as dg
import pandas as pd
import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog

from .conftest import (
    PartitionedMemoryIOManager,
    load_component_module,
    materialize_upstream_days,
)

RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-02",
    "dagster/asset_partition_range_end": "2024-01-04",
}
DAYS = ["2024-01-02", "2024-01-03", "2024-01-04"]
FILTER = "event_date = '{partition_key}'"


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def cat_props(tmp_path):
    (tmp_path / "wh").mkdir()
    return {"uri": f"sqlite:///{tmp_path / 'cat.db'}", "warehouse": f"file://{tmp_path / 'wh'}"}


def catalog(props):
    return load_catalog("default", **{**props, "type": "sql"})


def day_frame(day, v):
    base = {"2024-01-02": 100, "2024-01-03": 200, "2024-01-04": 300}[day]
    return pd.DataFrame({"id": [base + 1, base + 2], "event_date": [day, day], "v": [v, v]})


def events_upstream():
    @dg.asset(name="events_src", partitions_def=dg.DailyPartitionsDefinition(start_date="2024-01-01"))
    def _up(context: dg.AssetExecutionContext) -> pd.DataFrame:
        return day_frame(context.partition_key, "new")
    return _up


def seed_table(props):
    """Existing table: an untouched 2024-01-01 day plus STALE ('old') rows for days 02..04."""
    old = pd.concat(
        [pd.DataFrame({"id": [1, 2], "event_date": ["2024-01-01"] * 2, "v": ["keep"] * 2})]
        + [day_frame(d, "old") for d in DAYS], ignore_index=True)
    cat = catalog(props)
    cat.create_namespace("lake")
    tbl = cat.create_table(("lake", "events"), schema=pa.Table.from_pandas(old).schema)
    tbl.append(pa.Table.from_pandas(old))


def table(props):
    return catalog(props).load_table(("lake", "events")).scan().to_pandas().sort_values("id").reset_index(drop=True)


def make_sink(mod, props, **kw):
    kw.setdefault("partition_type", "daily")
    kw.setdefault("partition_start", "2024-01-01")
    kw.setdefault("mode", "overwrite")
    kw.setdefault("overwrite_filter", FILTER)
    return mod.DataframeToIcebergTableComponent(
        asset_name="to_iceberg", upstream_asset_key="events_src", catalog_type="sql",
        catalog_properties=props, namespace="lake", table="events", **kw
    ).build_defs(None).assets[0]


def run(sink, multi, partition_key=None, tags=None, raise_on_error=True, instance=None, io=None):
    up = events_upstream()
    instance = instance or dg.DagsterInstance.ephemeral()
    io = io or PartitionedMemoryIOManager(multi=multi)
    if not io.store:
        materialize_upstream_days(up, instance, io, DAYS)
    kw = {"partition_key": partition_key} if partition_key else {"tags": tags or RANGE_TAGS}
    return dg.materialize([up, sink], selection=[sink.key], instance=instance,
                          resources={"io_manager": io}, raise_on_error=raise_on_error, **kw), instance, io


def by_day(df):
    return df.groupby("event_date")["v"].agg(lambda s: sorted(set(s))).to_dict()


def test_backfill_policy_wired_and_off_by_default(mod, cat_props):
    assert make_sink(mod, cat_props).backfill_policy is None
    assert make_sink(mod, cat_props, backfill_policy="single_run").backfill_policy == dg.BackfillPolicy.single_run()
    multi = make_sink(mod, cat_props, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=2)
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_overwrite_replaces_only_that_days_rows(mod, cat_props):
    seed_table(cat_props)
    r, _, _ = run(make_sink(mod, cat_props, backfill_policy="single_run"), "dict", partition_key="2024-01-03")
    assert r.success
    df = table(cat_props)
    assert by_day(df) == {"2024-01-01": ["keep"], "2024-01-02": ["old"], "2024-01-03": ["new"], "2024-01-04": ["old"]}
    assert len(df) == 8
    md = r.asset_materializations_for_node("to_iceberg")[0].metadata
    assert md["rows_written"].value == 2 and "partition_keys_written" not in md


def test_range_overwrite_replaces_each_keys_rows_and_keeps_the_rest(mod, cat_props):
    seed_table(cat_props)
    r, _, _ = run(make_sink(mod, cat_props, backfill_policy="single_run"), "dict")
    assert r.success
    df = table(cat_props)
    assert df["id"].tolist() == [1, 2, 101, 102, 201, 202, 301, 302]
    assert by_day(df) == {"2024-01-01": ["keep"], "2024-01-02": ["new"], "2024-01-03": ["new"], "2024-01-04": ["new"]}
    md = r.asset_materializations_for_node("to_iceberg")[0].metadata
    assert md["rows_written"].value == 6
    assert md["partition_keys_written"].value == 3
    assert md["mode"].value == "overwrite"


def test_range_overwrite_is_idempotent_on_rerun(mod, cat_props):
    seed_table(cat_props)
    sink = make_sink(mod, cat_props, backfill_policy="single_run")
    r1, instance, io = run(sink, "dict")
    first = table(cat_props)
    r2, _, _ = run(sink, "dict", instance=instance, io=io)
    assert r1.success and r2.success
    pd.testing.assert_frame_equal(first, table(cat_props))
    assert len(first) == 8


def test_range_overwrite_into_a_missing_table_creates_it(mod, cat_props):
    r, _, _ = run(make_sink(mod, cat_props, backfill_policy="single_run"), "dict")
    assert r.success
    assert table(cat_props)["id"].tolist() == [101, 102, 201, 202, 301, 302]


def test_range_only_writes_keys_in_the_range_from_a_wider_dict_upstream(mod, cat_props):
    """Discriminating: UNPARTITIONED upstream returning a dict spanning 02..05; only the
    component's own per-key slicing can restrict writes to the 03..04 range."""
    seed_table(cat_props)

    @dg.asset(name="events_src")
    def wide() -> dict:
        d = {k: day_frame(k, "new") for k in DAYS}
        d["2024-01-05"] = pd.DataFrame({"id": [901], "event_date": ["2024-01-05"], "v": ["new"]})
        return d

    tags = {"dagster/asset_partition_range_start": "2024-01-03",
            "dagster/asset_partition_range_end": "2024-01-04"}
    r = dg.materialize([wide, make_sink(mod, cat_props, backfill_policy="single_run")], tags=tags)
    assert r.success
    df = table(cat_props)
    assert by_day(df) == {"2024-01-01": ["keep"], "2024-01-02": ["old"], "2024-01-03": ["new"], "2024-01-04": ["new"]}
    assert 901 not in df["id"].tolist()


def test_range_with_a_single_frame_upstream_fails_loudly_and_leaves_table_intact(mod, cat_props):
    seed_table(cat_props)
    before = table(cat_props)
    r, _, _ = run(make_sink(mod, cat_props, backfill_policy="single_run"), "concat", raise_on_error=False)
    assert not r.success
    failure = [e for e in r.all_events if e.event_type_value == "STEP_FAILURE"][0]
    assert "dict of DataFrames" in failure.event_specific_data.error.cause.message
    pd.testing.assert_frame_equal(before, table(cat_props))


@pytest.mark.parametrize("multi", ["dict", "concat"])
def test_range_append_is_a_single_write_of_the_whole_range(mod, cat_props, multi):
    sink = make_sink(mod, cat_props, mode="append", overwrite_filter=None, backfill_policy="single_run")
    r, _, _ = run(sink, multi)
    assert r.success
    assert table(cat_props)["id"].tolist() == [101, 102, 201, 202, 301, 302]
    md = r.asset_materializations_for_node("to_iceberg")[0].metadata
    assert md["rows_written"].value == 6 and "partition_keys_written" not in md


def test_range_overwrite_without_filter_is_a_single_whole_table_overwrite(mod, cat_props):
    seed_table(cat_props)
    r, _, _ = run(make_sink(mod, cat_props, overwrite_filter=None, backfill_policy="single_run"), "dict")
    assert r.success
    assert table(cat_props)["id"].tolist() == [101, 102, 201, 202, 301, 302]
    assert "partition_keys_written" not in r.asset_materializations_for_node("to_iceberg")[0].metadata


def test_range_run_works_with_the_real_default_io_manager(mod, cat_props):
    """Regression: the sink used to return Output(value=None), which made the default
    filesystem IO manager fail in handle_output on a single_run range."""
    @dg.asset(name="events_src")
    def flat() -> pd.DataFrame:
        return pd.concat([day_frame(d, "new") for d in DAYS], ignore_index=True)

    sink = make_sink(mod, cat_props, mode="append", overwrite_filter=None, backfill_policy="single_run")
    assert dg.materialize([flat, sink], tags=RANGE_TAGS).success
    assert len(table(cat_props)) == 6
