"""Tests for the `unpivot` and `rank` ops added to WarehousePipelineComponent for ADF Data
Flow transform parity -- same gap closed in pandas_pipeline/polars_pipeline this session.

  - `unpivot` compiles to a portable UNION ALL (one branch per value column) rather than a
    native UNPIVOT keyword, since UNPIVOT support/syntax is inconsistent across this
    component's target dialects (postgres/redshift/mysql have no native UNPIVOT at all).
  - `rank` compiles to real SQL window functions (RANK/DENSE_RANK/ROW_NUMBER/NTILE/
    CUME_DIST) -- standard across every dialect this component targets -- rather than
    pandas_pipeline/polars_pipeline's own pandas-style tie-breaking vocabulary
    (average/min/max/first), which has no portable SQL window-function equivalent.

Real-execution coverage runs against a real file-backed DuckDB database, matching this
suite's existing convention in test_adf_gap_ops.py. Cross-dialect SQL-text shape (quoting,
error messages) is covered via `_build_op_sql` directly.
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


def _engine(db_path):
    import sqlalchemy
    return sqlalchemy.create_engine(f"duckdb:///{db_path}")


def _exec(db_path, *statements):
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


def test_valid_ops_includes_unpivot_and_rank(mod):
    assert {"unpivot", "rank"}.issubset(mod._VALID_OPS)


# ---------------------------------------------------------------------------
# unpivot
# ---------------------------------------------------------------------------


@requires_duckdb
def test_unpivot_real_duckdb_execution(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE wide (id BIGINT, q1 DOUBLE, q2 DOUBLE)",
        "INSERT INTO wide VALUES (1, 10, 1), (2, 20, 2)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "wide"},
        operations=[{
            "op": "unpivot", "id_columns": ["id"], "value_columns": ["q1", "q2"],
            "var_column": "quarter", "value_column": "amount",
        }],
        output_table="long",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = sorted(_fetch(db_path, "SELECT id, quarter, amount FROM long"))
    assert rows == [(1, "q1", 10.0), (1, "q2", 1.0), (2, "q1", 20.0), (2, "q2", 2.0)]


@requires_duckdb
def test_unpivot_drop_null_values_real_duckdb_execution(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE wide (id BIGINT, a DOUBLE, b DOUBLE)",
        "INSERT INTO wide VALUES (1, 10, NULL), (2, NULL, 20)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "wide"},
        operations=[{
            "op": "unpivot", "id_columns": ["id"], "value_columns": ["a", "b"],
            "drop_null_values": True,
        }],
        output_table="long_nonull",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = _fetch(db_path, "SELECT * FROM long_nonull")
    assert len(rows) == 2


def test_unpivot_requires_id_and_value_columns(mod):
    with pytest.raises(ValueError, match="id_columns"):
        mod._build_op_sql("prev", {"op": "unpivot", "value_columns": ["a"]}, "duckdb", {})
    with pytest.raises(ValueError, match="value_columns"):
        mod._build_op_sql("prev", {"op": "unpivot", "id_columns": ["id"]}, "duckdb", {})


def test_unpivot_compiles_to_union_all_not_native_unpivot_keyword(mod):
    sql = mod._build_op_sql(
        "prev", {"op": "unpivot", "id_columns": ["id"], "value_columns": ["a", "b"]},
        "postgres", {},
    )
    assert "UNION ALL" in sql
    assert "UNPIVOT" not in sql.upper()


# ---------------------------------------------------------------------------
# rank
# ---------------------------------------------------------------------------


@requires_duckdb
def test_rank_dense_rank_real_duckdb_execution(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE scores (g VARCHAR, v DOUBLE)",
        "INSERT INTO scores VALUES ('a', 10), ('a', 20), ('a', 20), ('b', 5)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "scores"},
        operations=[{
            "op": "rank", "order_by": ["v"], "method": "dense_rank", "group_by": ["g"],
            "output_column": "r",
        }],
        output_table="ranked",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = sorted(_fetch(db_path, "SELECT g, v, r FROM ranked ORDER BY g, v"))
    assert rows == [("a", 10.0, 1), ("a", 20.0, 2), ("a", 20.0, 2), ("b", 5.0, 1)]


@requires_duckdb
def test_rank_ntile_real_duckdb_execution(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE scores (v DOUBLE)",
        "INSERT INTO scores VALUES (5), (5), (10), (20), (20)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "scores"},
        operations=[{
            "op": "rank", "order_by": ["v"], "method": "ntile", "ntile_buckets": 2,
            "output_column": "bucket",
        }],
        output_table="ntiled",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = sorted(_fetch(db_path, "SELECT v, bucket FROM ntiled ORDER BY v"))
    assert rows == [(5.0, 1), (5.0, 1), (10.0, 1), (20.0, 2), (20.0, 2)]


def test_rank_rejects_pandas_style_method(mod):
    with pytest.raises(ValueError, match="average"):
        mod._build_op_sql("prev", {"op": "rank", "order_by": ["v"], "method": "average"}, "duckdb", {})


def test_rank_ntile_requires_bucket_count(mod):
    with pytest.raises(ValueError, match="ntile_buckets"):
        mod._build_op_sql("prev", {"op": "rank", "order_by": ["v"], "method": "ntile"}, "duckdb", {})


def test_rank_compiles_standard_window_function_across_dialects(mod):
    for dialect in ("postgres", "mysql", "mssql", "snowflake", "bigquery", "redshift", "databricks"):
        sql = mod._build_op_sql("prev", {"op": "rank", "order_by": ["v"], "method": "row_number"}, dialect, {})
        assert "ROW_NUMBER()" in sql
        assert "OVER (" in sql
