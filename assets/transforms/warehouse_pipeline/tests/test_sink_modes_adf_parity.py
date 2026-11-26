"""Tests for WarehousePipelineComponent's ADF-parity sink modes (ask 12): a true
UPDATE-by-key `merge` (beyond today's insert-only `incremental_merge_key` dedup) and
`truncate_insert` (keep the existing table, replace its rows). `merge`/`truncate_insert`
run for real against a real file-backed DuckDB database (not mocked); the other dialects'
SQL shape (postgres/sqlite/mysql MERGE-vs-ON-CONFLICT-vs-ON-DUPLICATE-KEY branching) is
covered as pure SQL-text generation, since this suite has no fixture for a live
Postgres/MySQL/MSSQL/Snowflake/Redshift/BigQuery/Databricks warehouse.
"""
import pytest

from .conftest import load_component_module


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        return True
    except ImportError:
        return False


requires_duckdb = pytest.mark.skipif(not _duckdb_available(), reason="requires duckdb")


@pytest.fixture()
def mod():
    return load_component_module()


# ---------------------------------------------------------------------------
# Real DuckDB execution: merge + truncate_insert
# ---------------------------------------------------------------------------


def _engine(db_path):
    import sqlalchemy
    return sqlalchemy.create_engine(f"duckdb:///{db_path}")


def _exec(db_path, *statements):
    """Run statements via a SQLAlchemy engine matching the component's own connection
    mechanism, disposed immediately after -- duckdb refuses a second connection to the
    same file "with a different configuration than existing connections" if a raw
    duckdb.connect() and the component's own sqlalchemy engine are both left open."""
    eng = _engine(db_path)
    try:
        with eng.begin() as conn:
            for stmt in statements:
                conn.exec_driver_sql(stmt)
    finally:
        eng.dispose()


def _fetch(db_path, query):
    eng = _engine(db_path)
    try:
        with eng.connect() as conn:
            return conn.exec_driver_sql(query).fetchall()
    finally:
        eng.dispose()


@requires_duckdb
def test_merge_updates_matched_rows_and_inserts_new(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(db_path, "CREATE TABLE orders (id BIGINT, total DOUBLE)", "INSERT INTO orders VALUES (1, 10.0), (2, 20.0)")

    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        steps=[{"id": "s1", "source": {"upstream_table": "orders"}, "operations": []}],
        sinks=[{"from": "s1", "table": "orders_merged", "mode": "merge", "merge_keys": ["id"]}],
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success

    _exec(db_path, "UPDATE orders SET total = 999.0 WHERE id = 1", "INSERT INTO orders VALUES (3, 30.0)")

    assert dg.materialize(defs.assets).success

    rows = dict(_fetch(db_path, "SELECT id, total FROM orders_merged ORDER BY id"))
    assert rows == {1: 999.0, 2: 20.0, 3: 30.0}


@requires_duckdb
def test_truncate_insert_replaces_rows_not_appends(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(db_path, "CREATE TABLE orders (id BIGINT, total DOUBLE)", "INSERT INTO orders VALUES (1, 10.0), (2, 20.0)")

    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        steps=[{"id": "s1", "source": {"upstream_table": "orders"}, "operations": []}],
        sinks=[{"from": "s1", "table": "orders_ti", "mode": "truncate_insert"}],
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success

    _exec(db_path, "DELETE FROM orders", "INSERT INTO orders VALUES (9, 90.0)")

    assert dg.materialize(defs.assets).success

    rows = _fetch(db_path, "SELECT id, total FROM orders_ti ORDER BY id")
    assert rows == [(9, 90.0)]  # old rows (1, 2) are gone, not appended alongside


@requires_duckdb
def test_append_adds_rows_without_truncating(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(db_path, "CREATE TABLE orders (id BIGINT, total DOUBLE)", "INSERT INTO orders VALUES (1, 10.0), (2, 20.0)")

    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        steps=[{"id": "s1", "source": {"upstream_table": "orders"}, "operations": []}],
        sinks=[{"from": "s1", "table": "orders_append", "mode": "append"}],
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success  # bootstraps orders_append with 2 rows

    _exec(db_path, "DELETE FROM orders", "INSERT INTO orders VALUES (9, 90.0)")

    assert dg.materialize(defs.assets).success  # appends 1 more row on top

    rows = _fetch(db_path, "SELECT id, total FROM orders_append ORDER BY id")
    assert rows == [(1, 10.0), (2, 20.0), (9, 90.0)]  # old rows kept, new row added


@requires_duckdb
def test_merge_requires_merge_keys(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(db_path, "CREATE TABLE orders (id BIGINT, total DOUBLE)", "INSERT INTO orders VALUES (1, 10.0)")

    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        steps=[{"id": "s1", "source": {"upstream_table": "orders"}, "operations": []}],
        sinks=[{"from": "s1", "table": "orders_merged", "mode": "merge"}],
    )
    defs = comp.build_defs(None)
    with pytest.raises(ValueError, match="merge_keys"):
        dg.materialize(defs.assets)


# ---------------------------------------------------------------------------
# _emit_sink_sql -- pure SQL-text shape, other dialects
# ---------------------------------------------------------------------------


def _ctes_and_refs():
    step_refs = {"s1": "s1__output"}
    all_ctes = [("s1__output", "SELECT * FROM \"orders\"")]
    return step_refs, all_ctes


def test_merge_postgres_uses_on_conflict(mod):
    step_refs, all_ctes = _ctes_and_refs()
    sink = {"from": "s1", "table": "t", "mode": "merge", "merge_keys": ["id"]}
    sql = mod._emit_sink_sql(sink, step_refs, all_ctes, "postgresql", columns=["id", "total"])
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in sql
    assert "ON CONFLICT (\"id\") DO UPDATE SET \"total\" = EXCLUDED.\"total\"" in sql


def test_merge_mysql_uses_on_duplicate_key(mod):
    step_refs, all_ctes = _ctes_and_refs()
    sink = {"from": "s1", "table": "t", "mode": "merge", "merge_keys": ["id"]}
    sql = mod._emit_sink_sql(sink, step_refs, all_ctes, "mysql", columns=["id", "total"])
    assert "ON DUPLICATE KEY UPDATE `total` = VALUES(`total`)" in sql


def test_merge_snowflake_uses_merge_into(mod):
    step_refs, all_ctes = _ctes_and_refs()
    sink = {"from": "s1", "table": "t", "mode": "merge", "merge_keys": ["id"]}
    sql = mod._emit_sink_sql(sink, step_refs, all_ctes, "snowflake", columns=["id", "total"])
    assert "MERGE INTO" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_merge_unknown_dialect_raises(mod):
    step_refs, all_ctes = _ctes_and_refs()
    sink = {"from": "s1", "table": "t", "mode": "merge", "merge_keys": ["id"]}
    with pytest.raises(ValueError, match="no known syntax"):
        mod._emit_sink_sql(sink, step_refs, all_ctes, "cockroachdb", columns=["id", "total"])


def test_truncate_insert_sqlite_uses_delete_not_truncate(mod):
    step_refs, all_ctes = _ctes_and_refs()
    sink = {"from": "s1", "table": "t", "mode": "truncate_insert"}
    sql = mod._emit_sink_sql(sink, step_refs, all_ctes, "sqlite")
    assert "DELETE FROM" in sql
    assert "TRUNCATE" not in sql


def test_truncate_insert_postgres_uses_truncate(mod):
    step_refs, all_ctes = _ctes_and_refs()
    sink = {"from": "s1", "table": "t", "mode": "truncate_insert"}
    sql = mod._emit_sink_sql(sink, step_refs, all_ctes, "postgresql")
    assert "TRUNCATE TABLE" in sql


def test_append_emits_insert_with_no_truncate_or_delete(mod):
    step_refs, all_ctes = _ctes_and_refs()
    sink = {"from": "s1", "table": "t", "mode": "append"}
    sql = mod._emit_sink_sql(sink, step_refs, all_ctes, "postgresql")
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "INSERT INTO" in sql
    assert "TRUNCATE" not in sql
    assert "DELETE FROM" not in sql
