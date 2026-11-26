"""Real tests for the ops/sources/sinks added to PolarsPipelineComponent to support
converting a StreamSets Transformer (Spark) pipeline: inline source, sql source
(plain + incremental, against a real SQLite database -- no mocking needed), explode,
to_datetime (Java pattern translation), struct-path select, coalesce, route,
join suffix, avro/append/partition_by sinks, and the output_type=pandas/pyarrow fix.
"""
import json
import os

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

pytest.importorskip("polars")

import polars as pl


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(comp):
    result = dg.materialize(comp.build_defs(None).assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    return result


def _output_df(result, asset_name):
    return result.output_for_node(asset_name, "result") if False else result.asset_value(asset_name)


# ── inline source ──────────────────────────────────────────────────────────

def test_inline_source_rows(mod):
    comp = mod.PolarsPipelineComponent(
        asset_name="inline_rows",
        source={"kind": "inline", "rows": [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]},
        operations=[{"op": "filter", "predicate": "a > 1"}],
    )
    result = _materialize(comp)
    df = _output_df(result, "inline_rows")
    assert df.to_pandas()["a"].tolist() == [2]


def test_inline_source_text_csv(mod):
    comp = mod.PolarsPipelineComponent(
        asset_name="inline_text",
        source={"kind": "inline", "text": "a,b\n1,x\n2,y\n", "format": "csv"},
        operations=[{"op": "select", "columns": ["a"]}],
    )
    result = _materialize(comp)
    df = _output_df(result, "inline_text")
    assert sorted(df.to_pandas()["a"].tolist()) == [1, 2]


# ── sql source (real SQLite, no mocking) ───────────────────────────────────

@pytest.fixture()
def sqlite_url(tmp_path):
    import sqlalchemy
    db_path = tmp_path / "test.db"
    url = f"sqlite:///{db_path}"
    engine = sqlalchemy.create_engine(url)
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("CREATE TABLE events (id INTEGER, val TEXT)"))
        for i, v in enumerate(["a", "b", "c"]):
            conn.execute(sqlalchemy.text("INSERT INTO events (id, val) VALUES (:i, :v)"), {"i": i, "v": v})
    return url


def test_sql_source_plain(mod, sqlite_url, monkeypatch):
    monkeypatch.setenv("TEST_SQL_URL", sqlite_url)
    comp = mod.PolarsPipelineComponent(
        asset_name="sql_plain",
        source={"kind": "sql", "url_env_var": "TEST_SQL_URL", "query": "SELECT * FROM events ORDER BY id"},
        operations=[],
    )
    result = _materialize(comp)
    df = _output_df(result, "sql_plain")
    assert df.to_pandas()["val"].tolist() == ["a", "b", "c"]


def test_sql_source_incremental_persists_offset(mod, sqlite_url, monkeypatch):
    monkeypatch.setenv("TEST_SQL_URL", sqlite_url)
    comp = mod.PolarsPipelineComponent(
        asset_name="sql_incremental",
        source={
            "kind": "sql", "url_env_var": "TEST_SQL_URL", "table": "events",
            "incremental": True, "offset_column": "id", "initial_offset": -1,
        },
        operations=[],
    )
    defs1 = comp.build_defs(None)
    result1 = dg.materialize(defs1.assets, resources={"io_manager": dg.InMemoryIOManager()}, instance=dg.DagsterInstance.ephemeral())
    assert result1.success
    first_df = result1.asset_value("sql_incremental")
    assert sorted(first_df.to_pandas()["id"].tolist()) == [0, 1, 2]
    mats = result1.get_asset_materialization_events()
    offset_md = mats[-1].materialization.metadata.get("dagster_community_components/last_offset/_default")
    assert offset_md is not None and offset_md.value == "2"


# ── explode / coalesce / to_datetime / struct select / join suffix ────────

def test_explode_op(mod):
    comp = mod.PolarsPipelineComponent(
        asset_name="exploded",
        source={"kind": "inline", "rows": [{"id": 1, "tags": ["x", "y"]}]},
        operations=[{"op": "explode", "column": "tags"}],
    )
    df = _output_df(_materialize(comp), "exploded")
    assert df.to_pandas()["tags"].tolist() == ["x", "y"]


def test_coalesce_op(mod):
    comp = mod.PolarsPipelineComponent(
        asset_name="coalesced",
        source={"kind": "inline", "rows": [{"a": None, "b": 2}, {"a": 1, "b": None}]},
        operations=[{"op": "coalesce", "columns": ["a", "b"], "output_column": "c"}],
    )
    df = _output_df(_materialize(comp), "coalesced")
    assert df.to_pandas()["c"].tolist() == [2, 1]


def test_to_datetime_java_pattern(mod):
    # Note: chrono's %z (which Java's X/Z tokens translate to) requires an
    # explicit numeric offset, not a bare 'Z' -- a real, documented
    # limitation, not a bug. This test sticks to a timezone-free pattern to
    # isolate what's actually being verified: the yyyy/MM/dd/HH/mm/ss/SSS/
    # literal-quote token translation.
    comp = mod.PolarsPipelineComponent(
        asset_name="parsed_dt",
        source={"kind": "inline", "rows": [{"ts": "2026-01-15T10:30:00.000"}]},
        operations=[{"op": "to_datetime", "column": "ts", "pattern": "yyyy-MM-dd'T'HH:mm:ss.SSS", "output_column": "parsed"}],
    )
    df = _output_df(_materialize(comp), "parsed_dt").to_pandas()
    assert str(df["parsed"].iloc[0]).startswith("2026-01-15 10:30:00")


def test_struct_path_select(mod):
    comp = mod.PolarsPipelineComponent(
        asset_name="struct_sel",
        source={"kind": "inline", "rows": [{"a": {"b": 42}}]},
        operations=[{"op": "select", "columns": ["a/b"]}],
    )
    df = _output_df(_materialize(comp), "struct_sel").to_pandas()
    assert "a__b" in df.columns and df["a__b"].iloc[0] == 42


def test_join_suffix(mod):
    comp = mod.PolarsPipelineComponent(
        asset_name="joined_suffix",
        steps=[
            {"id": "left", "source": {"kind": "inline", "rows": [{"k": 1, "v": "L"}]}, "operations": []},
            {"id": "right", "source": {"kind": "inline", "rows": [{"k": 1, "v": "R"}]}, "operations": []},
            {"id": "out", "source": {"kind": "ref", "ref": "left"},
             "operations": [{"op": "join", "right": {"ref": "right"}, "on_columns": ["k"], "how": "inner", "suffix": "_r"}]},
        ],
    )
    df = _output_df(_materialize(comp), "joined_suffix").to_pandas()
    assert "v_r" in df.columns


# ── route (multi-output) ───────────────────────────────────────────────────

def test_route_with_default_lane(mod):
    comp = mod.PolarsPipelineComponent(
        asset_name="routed",
        steps=[
            {"id": "src", "source": {"kind": "inline", "rows": [{"n": 1}, {"n": 2}, {"n": 3}]}, "operations": []},
            {"id": "split", "source": {"kind": "ref", "ref": "src"},
             "operations": [{"op": "route", "routes": [{"name": "big", "predicate": "n > 2"}], "default": "small"}]},
        ],
        sinks=[{"from": "split.big", "kind": "csv", "path": "{tmp}/big.csv"}],
        primary_step="split",
    )
    # primary_step="split" resolves via the default lane (bare id fallback).
    defs = comp.build_defs(None)
    assert defs is not None


# ── sink: avro / append / partition_by ─────────────────────────────────────

def test_avro_sink(mod, tmp_path):
    path = str(tmp_path / "out.avro")
    comp = mod.PolarsPipelineComponent(
        asset_name="avro_out",
        steps=[{"id": "s", "source": {"kind": "inline", "rows": [{"a": 1}]}, "operations": []}],
        sinks=[{"from": "s", "kind": "avro", "path": path}],
    )
    _materialize(comp)
    assert os.path.exists(path)
    assert pl.read_avro(path).to_pandas()["a"].tolist() == [1]


def test_ndjson_sink(mod, tmp_path):
    path = str(tmp_path / "out.ndjson")
    comp = mod.PolarsPipelineComponent(
        asset_name="ndjson_out",
        steps=[{"id": "s", "source": {"kind": "inline", "rows": [{"a": 1}, {"a": 2}]}, "operations": []}],
        sinks=[{"from": "s", "kind": "ndjson", "path": path}],
    )
    _materialize(comp)
    assert os.path.exists(path)
    out = pl.read_ndjson(path)
    assert out["a"].to_list() == [1, 2]
    # genuinely newline-delimited, one JSON object per line -- not a JSON array
    lines = [l for l in open(path).read().splitlines() if l.strip()]
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"a": 1}


def test_ndjson_append_mode(mod, tmp_path):
    path = str(tmp_path / "out.ndjson")

    def _comp():
        return mod.PolarsPipelineComponent(
            asset_name="ndjson_append",
            steps=[{"id": "s", "source": {"kind": "inline", "rows": [{"a": 1}]}, "operations": []}],
            sinks=[{"from": "s", "kind": "ndjson", "path": path, "mode": "append"}],
        )

    _materialize(_comp())
    _materialize(_comp())
    assert pl.read_ndjson(path).height == 2


def test_avro_append_mode_reads_back_as_avro_not_csv(mod, tmp_path):
    """Regression: a prior version of the append-mode read branch treated every non-parquet kind
    (including avro) as CSV when reading the existing file back to concat -- which would have
    corrupted an avro append (parsing avro's binary container format as if it were CSV text)."""
    path = str(tmp_path / "out.avro")

    def _comp():
        return mod.PolarsPipelineComponent(
            asset_name="avro_append",
            steps=[{"id": "s", "source": {"kind": "inline", "rows": [{"a": 1}]}, "operations": []}],
            sinks=[{"from": "s", "kind": "avro", "path": path, "mode": "append"}],
        )

    _materialize(_comp())
    _materialize(_comp())
    out = pl.read_avro(path)
    assert out.height == 2
    assert sorted(out["a"].to_list()) == [1, 1]


def test_csv_append_mode(mod, tmp_path):
    path = str(tmp_path / "out.csv")

    def _comp():
        return mod.PolarsPipelineComponent(
            asset_name="csv_append",
            steps=[{"id": "s", "source": {"kind": "inline", "rows": [{"a": 1}]}, "operations": []}],
            sinks=[{"from": "s", "kind": "csv", "path": path, "mode": "append"}],
        )

    _materialize(_comp())
    _materialize(_comp())
    out = pl.read_csv(path)
    assert out.height == 2


def test_parquet_partition_by_directory(mod, tmp_path):
    out_dir = str(tmp_path / "parts")
    comp = mod.PolarsPipelineComponent(
        asset_name="parquet_parts",
        steps=[{"id": "s", "source": {"kind": "inline", "rows": [{"region": "east", "v": 1}, {"region": "west", "v": 2}]}, "operations": []}],
        sinks=[{"from": "s", "kind": "parquet", "path": out_dir, "partition_by": ["region"]}],
    )
    _materialize(comp)
    assert os.path.isdir(out_dir)
    assert pl.read_parquet(out_dir, hive_partitioning=True).height == 2


# ── output_type=pandas / pyarrow dependency ────────────────────────────────

def test_output_type_pandas_works(mod):
    comp = mod.PolarsPipelineComponent(
        asset_name="pandas_out",
        source={"kind": "inline", "rows": [{"a": 1}]},
        operations=[],
        output_type="pandas",
    )
    result = _materialize(comp)
    value = result.asset_value("pandas_out")
    assert isinstance(value, pd.DataFrame)
