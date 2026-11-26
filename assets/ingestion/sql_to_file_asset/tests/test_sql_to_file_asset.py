"""Tests for SqlToFileAssetComponent -- a Copy activity whose sink is a file, not a table.
Real materialize() calls: a real temp-file SQLite database (via database_url_env_var) and a real
DuckDB resource (via resource_key, same discipline as data_masking's own
test_source_warehouse_query.py) for the SQL-read side, real local-file writes for the sink side.
No mocking on either side.
"""
import os

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        import dagster_duckdb  # noqa: F401
        return True
    except ImportError:
        return False


requires_duckdb = pytest.mark.skipif(not _duckdb_available(), reason="requires duckdb + dagster-duckdb")


def _materialize(comp, resources=None):
    defs = comp.build_defs(context=None)
    return dg.materialize(defs.assets, resources=resources or {})


def test_database_url_env_var_writes_csv(mod, tmp_path, monkeypatch):
    import sqlite3
    db_path = tmp_path / "src.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
    conn.commit()
    conn.close()

    monkeypatch.setenv("SQL_TO_FILE_TEST_DB_URL", f"sqlite:///{db_path}")
    sink_path = str(tmp_path / "out.csv")
    comp = mod.SqlToFileAssetComponent(
        asset_key="orders_export",
        database_url_env_var="SQL_TO_FILE_TEST_DB_URL",
        query="SELECT * FROM t ORDER BY id",
        sink_path=sink_path,
        sink_format="csv",
    )
    result = _materialize(comp)
    assert result.success
    out = pd.read_csv(sink_path)
    assert out["name"].tolist() == ["a", "b"]


@requires_duckdb
def test_resource_key_dispatch_against_real_duckdb_writes_parquet(mod, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "t.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 10.5), (2, 20.0)) AS t(id, amount)")
    conn.close()

    sink_path = str(tmp_path / "out.parquet")
    comp = mod.SqlToFileAssetComponent(
        asset_key="amounts_export",
        resource_key="duckdb_resource",
        query="SELECT * FROM t ORDER BY id",
        sink_path=sink_path,
        sink_format="parquet",
    )
    result = _materialize(comp, resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    out = pd.read_parquet(sink_path)
    assert out["amount"].tolist() == [10.5, 20.0]


def test_dry_run_writes_nothing_and_needs_no_connection(mod, tmp_path):
    sink_path = str(tmp_path / "out.csv")
    comp = mod.SqlToFileAssetComponent(
        asset_key="x",
        query="SELECT 1",
        sink_path=sink_path,
        sink_format="csv",
        dry_run=True,
    )
    result = _materialize(comp)
    assert result.success
    assert not os.path.exists(sink_path)


def test_dry_run_needs_neither_resource_key_nor_database_url_env_var(mod):
    # Would raise at model-validation time if dry_run didn't exempt this requirement.
    mod.SqlToFileAssetComponent(asset_key="x", query="SELECT 1", sink_path="/tmp/x.csv", sink_format="csv", dry_run=True)


def test_requires_exactly_one_connection_source(mod):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.SqlToFileAssetComponent(asset_key="x", query="SELECT 1", sink_path="/tmp/x.csv", sink_format="csv")
    with pytest.raises(ValueError, match="set exactly one"):
        mod.SqlToFileAssetComponent(
            asset_key="x", query="SELECT 1", sink_path="/tmp/x.csv", sink_format="csv",
            resource_key="r", database_url_env_var="E",
        )


def test_unknown_sink_format_is_a_clear_error(mod):
    with pytest.raises(ValueError, match="sink_format must be one of"):
        mod.SqlToFileAssetComponent(
            asset_key="x", query="SELECT 1", sink_path="/tmp/x.csv", sink_format="xml",
            database_url_env_var="E",
        )


def test_run_window_outside_window_is_detected_correctly(mod):
    import datetime
    # 2026-01-05 is a Monday; 03:00 UTC is outside the 09:00-17:00 window.
    ts = datetime.datetime(2026, 1, 5, 3, 0, tzinfo=datetime.timezone.utc).timestamp()
    assert mod._is_in_run_window(ts, "09:00-17:00 UTC mon,tue,wed,thu,fri") is False
    # 12:00 UTC the same day is inside it.
    ts_in = datetime.datetime(2026, 1, 5, 12, 0, tzinfo=datetime.timezone.utc).timestamp()
    assert mod._is_in_run_window(ts_in, "09:00-17:00 UTC mon,tue,wed,thu,fri") is True


def test_run_window_skips_materialization_without_writing(mod, tmp_path, monkeypatch):
    import datetime
    import sqlite3
    db_path = tmp_path / "src.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.execute("INSERT INTO t VALUES (1)")
    conn.commit()
    conn.close()
    monkeypatch.setenv("SQL_TO_FILE_TEST_DB_URL2", f"sqlite:///{db_path}")

    # A single-minute window at a UTC time guaranteed not to be "now" during a real test run.
    far_past_hour = (datetime.datetime.now(datetime.timezone.utc).hour + 12) % 24
    sink_path = str(tmp_path / "out.csv")
    comp = mod.SqlToFileAssetComponent(
        asset_key="x",
        database_url_env_var="SQL_TO_FILE_TEST_DB_URL2",
        query="SELECT * FROM t",
        sink_path=sink_path,
        sink_format="csv",
        run_window=f"{far_past_hour:02d}:00-{far_past_hour:02d}:01 UTC",
    )
    result = _materialize(comp)
    assert result.success
    assert not os.path.exists(sink_path)


def test_run_window_malformed_spec_fails_open(mod):
    assert mod._is_in_run_window(0, "not a valid spec") is True


def test_partition_key_templated_into_sink_path(mod, tmp_path, monkeypatch):
    import sqlite3
    db_path = tmp_path / "src.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.execute("INSERT INTO t VALUES (1)")
    conn.commit()
    conn.close()
    monkeypatch.setenv("SQL_TO_FILE_TEST_DB_URL3", f"sqlite:///{db_path}")

    sink_dir = tmp_path / "parts"
    sink_dir.mkdir()
    comp = mod.SqlToFileAssetComponent(
        asset_key="x",
        database_url_env_var="SQL_TO_FILE_TEST_DB_URL3",
        query="SELECT * FROM t",
        sink_path=str(sink_dir / "{partition_key}.csv"),
        sink_format="csv",
        partition_type="static",
        partition_values="us,eu",
    )
    defs = comp.build_defs(context=None)
    result = dg.materialize(defs.assets, partition_key="us")
    assert result.success
    assert os.path.exists(sink_dir / "us.csv")
