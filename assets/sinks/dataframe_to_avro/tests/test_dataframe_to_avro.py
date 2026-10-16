"""Committed tests for DataframeToAvroComponent: backfill_policy + per-key writes on a range run.

fastavro is not installed here, so ONLY `fastavro.writer` is faked (it records the records
it was handed per target file and writes them as JSON lines). The component, fsspec's real
local filesystem, path resolution, partition handling and metadata all run for real under
dg.materialize(). This sink has no row filter, so a frame spanning a range cannot be split.
"""
import json
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import (
    PartitionedMemoryIOManager,
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


@pytest.fixture(autouse=True)
def fake_fastavro(monkeypatch):
    fa = types.ModuleType("fastavro")
    fa.calls = []

    def writer(f, schema, records, codec="null"):
        fa.calls.append({"name": f.path if hasattr(f, "path") else str(f), "n": len(records)})
        for r in records:
            f.write((json.dumps(r, default=str) + "\n").encode())

    fa.writer = writer
    monkeypatch.setitem(sys.modules, "fastavro", fa)
    return fa


def read(path):
    return pd.DataFrame([json.loads(line) for line in path.read_text().splitlines()])


def make_sink(mod, path, **kw):
    kw.setdefault("partition_type", "daily")
    kw.setdefault("partition_start", "2024-01-01")
    return mod.DataframeToAvroComponent(
        asset_name="to_avro", upstream_asset_key="orders_src", file_path=str(path), **kw
    ).build_defs(None).assets[0]


def run_range(sink, multi, raise_on_error=True):
    up = daily_upstream()
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager(multi=multi)
    materialize_upstream_days(up, instance, io, DAYS)
    return dg.materialize([up, sink], selection=[sink.key], instance=instance,
                          resources={"io_manager": io}, tags=RANGE_TAGS, raise_on_error=raise_on_error)


def test_backfill_policy_wired_and_off_by_default(mod, tmp_path):
    p = tmp_path / "o.avro"
    assert make_sink(mod, p).backfill_policy is None
    assert make_sink(mod, p, backfill_policy="single_run").backfill_policy == dg.BackfillPolicy.single_run()
    multi = make_sink(mod, p, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=2)
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_file(mod, tmp_path, fake_fastavro):
    up = daily_upstream()
    sink = make_sink(mod, tmp_path / "out_{partition_key}.avro", backfill_policy="single_run")
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager(multi="dict")
    materialize_upstream_days(up, instance, io, DAYS)
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    assert [p.name for p in tmp_path.iterdir()] == ["out_2024-01-03.avro"]
    assert sorted(read(tmp_path / "out_2024-01-03.avro")["id"]) == IDS["2024-01-03"]
    md = r.asset_materializations_for_node("to_avro")[0].metadata
    assert md["row_count"].value == 2 and "partition_keys_written" not in md
    assert md["destination"].value == str(tmp_path / "out_2024-01-03.avro")


def test_range_dict_upstream_templated_path_writes_one_file_per_key(mod, tmp_path, fake_fastavro):
    sink = make_sink(mod, tmp_path / "out_{partition_key}.avro", backfill_policy="single_run")
    r = run_range(sink, "dict")
    assert r.success
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"out_{d}.avro" for d in DAYS]
    for d in DAYS:
        assert sorted(read(tmp_path / f"out_{d}.avro")["id"]) == IDS[d]
    assert len(fake_fastavro.calls) == 3
    md = r.asset_materializations_for_node("to_avro")[0].metadata
    assert md["row_count"].value == 6
    assert md["partition_keys_written"].value == 3
    assert md["codec"].value == "null"


def test_range_frame_upstream_templated_path_raises_clear_error(mod, tmp_path):
    """Avro has no row filter, so a single frame spanning the range cannot be split per key:
    it must fail loudly rather than write the whole range into every key's file."""
    sink = make_sink(mod, tmp_path / "out_{partition_key}.avro", backfill_policy="single_run")
    r = run_range(sink, "concat", raise_on_error=False)
    assert not r.success
    failure = [e for e in r.all_events if e.event_type_value == "STEP_FAILURE"][0]
    assert "dict of DataFrames" in failure.event_specific_data.error.cause.message
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("multi", ["dict", "concat"])
def test_range_plain_path_writes_one_file_with_whole_range(mod, tmp_path, multi, fake_fastavro):
    sink = make_sink(mod, tmp_path / "all.avro", backfill_policy="single_run")
    r = run_range(sink, multi)
    assert r.success
    assert [p.name for p in tmp_path.iterdir()] == ["all.avro"]
    assert sorted(read(tmp_path / "all.avro")["id"]) == [101, 102, 201, 202, 301, 302]
    assert len(fake_fastavro.calls) == 1
    md = r.asset_materializations_for_node("to_avro")[0].metadata
    assert md["row_count"].value == 6 and "partition_keys_written" not in md


def test_range_env_var_path_is_not_a_partition_template(mod, tmp_path, monkeypatch, fake_fastavro):
    monkeypatch.setenv("AVRO_OUT_DIR", str(tmp_path))
    sink = make_sink(mod, "${AVRO_OUT_DIR}/all.avro", backfill_policy="single_run")
    r = run_range(sink, "dict")
    assert r.success
    assert [p.name for p in tmp_path.iterdir()] == ["all.avro"]
    assert len(fake_fastavro.calls) == 1


def test_range_run_works_with_the_real_default_io_manager(mod, tmp_path, fake_fastavro):
    """Regression: the sink used to return Output(value=None), so on a single_run range the
    default filesystem IO manager failed in handle_output ('does not support persisting an
    output associated with multiple partitions'). A sink returns MaterializeResult instead."""
    @dg.asset(name="orders_src")
    def flat() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2]})

    sink = make_sink(mod, tmp_path / "all.avro", backfill_policy="single_run")
    r = dg.materialize([flat, sink], tags=RANGE_TAGS)  # default IO manager, no override
    assert r.success
    assert read(tmp_path / "all.avro")["id"].tolist() == [1, 2]
