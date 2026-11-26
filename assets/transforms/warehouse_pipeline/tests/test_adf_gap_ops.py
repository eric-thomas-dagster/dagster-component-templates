"""Tests closing 3 ADF Data Flow transform gaps for WarehousePipelineComponent, plus 2
smaller fixes found while auditing this file's own `join`/`sort` ops for the same gaps:

  - `join` how='semi'/'anti' (ADF's Exists transform) -- was entirely missing; compiles to
    a portable correlated `WHERE [NOT] EXISTS (...)` subquery, not `SEMI JOIN`/`ANTI JOIN`
    keywords (not standard across this component's target dialects).
  - `sort` gained `nulls_last` (ADF's Sort transform defaults to nulls FIRST, matched here
    by nulls_last defaulting to False) -- native `NULLS FIRST/LAST` on dialects that
    support it, a portable `CASE WHEN ... IS NULL` tiebreaker on mssql/mysql which don't.
  - `pivot` (ADF's Pivot transform) -- portable conditional-aggregation SQL
    (`SUM(CASE WHEN pivot_column = 'v' THEN value_column END)`), since no unbounded PIVOT
    keyword is universal across dialects. `pivot_values` is REQUIRED (unlike
    pandas_pipeline/polars_pipeline, which discover values at runtime).
  - `lookup` (ADF's Lookup transform) -- a LEFT JOIN, with a ROW_NUMBER()-over-PARTITION
    pre-filter (same pattern as `top_n_per_group`) when `match_sort_by` is given, to pick
    exactly one right-side row per key. `output_mode: is_match` emits a single boolean
    column via `WHERE EXISTS` instead of pulling right-side columns in.
  - `window_calculation` (ADF's Rank/Window transforms, narrowed to `ntile`/`cume_dist`
    only -- rank-equivalent concepts are already covered by `top_n`/`top_n_per_group`) --
    near-literal `NTILE(n) OVER (...)` / `CUME_DIST() OVER (...)`, standard across every
    dialect this component targets.

Real-execution coverage runs against a real file-backed DuckDB database (not mocked),
matching this suite's existing convention in test_sink_modes_adf_parity.py. Cross-dialect
SQL shape (mssql/mysql's sort workaround; the EXISTS/NOT EXISTS shape itself) is covered
as pure SQL-text generation via `_build_op_sql` directly, since this suite has no fixture
for a live Postgres/MySQL/MSSQL/Snowflake/Redshift/BigQuery/Databricks warehouse.
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


def test_valid_ops_includes_adf_gap_ops(mod):
    assert {"pivot", "lookup", "window_calculation"}.issubset(mod._VALID_OPS)


# ---------------------------------------------------------------------------
# Pre-existing bug found while building `lookup` (reuses the same star-exclusion
# pattern as `drop`/`top_n_per_group`): DuckDB and Snowflake don't support
# `SELECT * EXCEPT (col)` at all -- EXCEPT is reserved for the set operator on both;
# they need `SELECT * EXCLUDE (col)`. Only BigQuery/Databricks actually use EXCEPT.
# ---------------------------------------------------------------------------


@requires_duckdb
def test_top_n_per_group_real_duckdb_execution_does_not_crash_on_except_keyword(mod, tmp_path):
    """Regression: before this fix, op=top_n_per_group's own generated SQL used
    `SELECT * EXCEPT (...)` for dialect='duckdb', which DuckDB's parser rejects outright
    (EXCEPT is its set-operator keyword) -- every real top_n_per_group run on duckdb
    raised _duckdb.ParserException, never a Python-level error a user could trace back to
    config."""
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE scores (grp VARCHAR, id BIGINT, score DOUBLE)",
        "INSERT INTO scores VALUES ('a', 1, 10), ('a', 2, 20), ('b', 3, 5)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "scores"},
        operations=[{"op": "top_n_per_group", "group_by": ["grp"], "sort_by": "score", "n": 1, "ascending": False}],
        output_table="scores_top1",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = dict(_fetch(db_path, "SELECT grp, id FROM scores_top1 ORDER BY grp"))
    assert rows == {"a": 2, "b": 3}


@requires_duckdb
def test_drop_real_duckdb_execution_does_not_crash_on_except_keyword(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(db_path, "CREATE TABLE t (a INT, b INT, c INT)", "INSERT INTO t VALUES (1, 2, 3)")
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "t"},
        operations=[{"op": "drop", "columns": ["b"]}],
        output_table="t_dropped",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = _fetch(db_path, "SELECT * FROM t_dropped")
    assert rows == [(1, 3)]


def test_drop_uses_exclude_keyword_on_duckdb_and_snowflake(mod):
    assert "EXCLUDE" in mod._build_op_sql("prev", {"op": "drop", "columns": ["x"]}, "duckdb", {})
    assert "EXCLUDE" in mod._build_op_sql("prev", {"op": "drop", "columns": ["x"]}, "snowflake", {})


def test_drop_uses_except_keyword_on_bigquery_and_databricks(mod):
    assert "EXCEPT" in mod._build_op_sql("prev", {"op": "drop", "columns": ["x"]}, "bigquery", {})
    assert "EXCEPT" in mod._build_op_sql("prev", {"op": "drop", "columns": ["x"]}, "databricks", {})


# ---------------------------------------------------------------------------
# join: how='semi'/'anti'
# ---------------------------------------------------------------------------


@requires_duckdb
def test_join_semi_filters_to_matching_left_rows(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE orders (id BIGINT, customer_id BIGINT)",
        "INSERT INTO orders VALUES (1, 100), (2, 200), (3, 300)",
        "CREATE TABLE gold_customers (customer_id BIGINT)",
        "INSERT INTO gold_customers VALUES (100), (300)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "orders"},
        operations=[{"op": "join", "right": "gold_customers", "how": "semi", "on_columns": ["customer_id"]}],
        output_table="orders_semi",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = sorted(r[0] for r in _fetch(db_path, "SELECT id FROM orders_semi"))
    assert rows == [1, 3]


@requires_duckdb
def test_join_anti_filters_to_non_matching_left_rows(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE orders (id BIGINT, customer_id BIGINT)",
        "INSERT INTO orders VALUES (1, 100), (2, 200), (3, 300)",
        "CREATE TABLE gold_customers (customer_id BIGINT)",
        "INSERT INTO gold_customers VALUES (100), (300)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "orders"},
        operations=[{"op": "join", "right": "gold_customers", "how": "anti", "on_columns": ["customer_id"]}],
        output_table="orders_anti",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = sorted(r[0] for r in _fetch(db_path, "SELECT id FROM orders_anti"))
    assert rows == [2]


def test_join_semi_requires_on_columns(mod):
    with pytest.raises(ValueError, match="semi.*anti"):
        mod._build_op_sql("prev", {"op": "join", "right": "t", "how": "semi"}, "duckdb", {})


def test_join_semi_compiles_to_exists_not_semi_join_keyword(mod):
    sql = mod._build_op_sql("prev", {"op": "join", "right": "t", "how": "semi", "on_columns": ["id"]}, "duckdb", {})
    assert "EXISTS" in sql
    assert "SEMI JOIN" not in sql
    assert "_l.*" in sql  # left columns only -- no right-side columns pulled in


def test_join_anti_compiles_to_not_exists(mod):
    sql = mod._build_op_sql("prev", {"op": "join", "right": "t", "how": "anti", "on_columns": ["id"]}, "duckdb", {})
    assert "NOT EXISTS" in sql
    assert "ANTI JOIN" not in sql


# ---------------------------------------------------------------------------
# sort: nulls_last
# ---------------------------------------------------------------------------


@requires_duckdb
def test_sort_nulls_last_true_places_nulls_after_non_null_real_duckdb(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(db_path, "CREATE TABLE t (id BIGINT, v BIGINT)", "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 5)")
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "t"},
        operations=[{"op": "sort", "by": ["v"], "nulls_last": True}],
        output_table="t_sorted_last",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = _fetch(db_path, "SELECT id FROM t_sorted_last")
    assert [r[0] for r in rows] == [3, 1, 2]  # v=5, v=10, v=NULL -- null sorts last


@requires_duckdb
def test_sort_nulls_first_is_the_default_real_duckdb(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(db_path, "CREATE TABLE t (id BIGINT, v BIGINT)", "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 5)")
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "t"},
        operations=[{"op": "sort", "by": ["v"]}],  # nulls_last unset -- ADF Sort's own default
        output_table="t_sorted_first",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = _fetch(db_path, "SELECT id FROM t_sorted_first")
    assert [r[0] for r in rows] == [2, 3, 1]  # v=NULL, v=5, v=10 -- null sorts first


def test_sort_nulls_duckdb_uses_native_nulls_clause(mod):
    sql = mod._build_op_sql("prev", {"op": "sort", "by": ["v"], "nulls_last": True}, "duckdb", {})
    assert "NULLS LAST" in sql


def test_sort_nulls_default_uses_native_nulls_first(mod):
    sql = mod._build_op_sql("prev", {"op": "sort", "by": ["v"]}, "duckdb", {})
    assert "NULLS FIRST" in sql


def test_sort_nulls_mssql_uses_case_workaround_not_native_syntax(mod):
    sql = mod._build_op_sql("prev", {"op": "sort", "by": ["v"], "nulls_last": True}, "mssql", {})
    assert "NULLS LAST" not in sql
    assert "NULLS FIRST" not in sql
    assert "CASE WHEN [v] IS NULL THEN 1 ELSE 0 END) ASC" in sql


def test_sort_nulls_mysql_uses_case_workaround_not_native_syntax(mod):
    sql = mod._build_op_sql("prev", {"op": "sort", "by": ["v"]}, "mysql", {})
    assert "NULLS" not in sql
    assert "CASE WHEN `v` IS NULL THEN 0 ELSE 1 END) ASC" in sql


# ---------------------------------------------------------------------------
# pivot
# ---------------------------------------------------------------------------


@requires_duckdb
def test_pivot_conditional_aggregation_real_duckdb(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE sales (region VARCHAR, quarter VARCHAR, amount DOUBLE)",
        "INSERT INTO sales VALUES "
        "('west', 'Q1', 100.0), ('west', 'Q2', 150.0), ('east', 'Q1', 200.0), ('east', 'Q2', 50.0)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "sales"},
        operations=[{
            "op": "pivot",
            "index": ["region"],
            "pivot_column": "quarter",
            "value_columns": ["amount"],
            "agg_func": "sum",
            "pivot_values": ["Q1", "Q2"],
        }],
        output_table="sales_pivoted",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = {r[0]: (r[1], r[2]) for r in _fetch(db_path, 'SELECT region, "Q1", "Q2" FROM sales_pivoted ORDER BY region')}
    assert rows == {"east": (200.0, 50.0), "west": (100.0, 150.0)}


@requires_duckdb
def test_pivot_fill_value_replaces_missing_combinations_real_duckdb(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE sales (region VARCHAR, quarter VARCHAR, amount DOUBLE)",
        "INSERT INTO sales VALUES ('west', 'Q1', 100.0)",  # no 'east' row, no Q2 at all
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "sales"},
        operations=[{
            "op": "pivot",
            "index": ["region"],
            "pivot_column": "quarter",
            "value_columns": ["amount"],
            "pivot_values": ["Q1", "Q2"],
            "fill_value": 0,
        }],
        output_table="sales_pivoted_filled",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = {r[0]: (r[1], r[2]) for r in _fetch(db_path, 'SELECT region, "Q1", "Q2" FROM sales_pivoted_filled')}
    assert rows == {"west": (100.0, 0.0)}


def test_pivot_requires_pivot_values(mod):
    with pytest.raises(ValueError, match="pivot_values"):
        mod._build_op_sql(
            "prev",
            {"op": "pivot", "index": ["region"], "pivot_column": "quarter", "value_columns": ["amount"]},
            "duckdb", {},
        )


def test_pivot_multi_value_columns_prefixes_alias_to_avoid_collisions(mod):
    sql = mod._build_op_sql(
        "prev",
        {
            "op": "pivot", "index": ["region"], "pivot_column": "quarter",
            "value_columns": ["amount", "count"], "pivot_values": ["Q1"],
        },
        "duckdb", {},
    )
    assert '"amount_Q1"' in sql
    assert '"count_Q1"' in sql


def test_pivot_single_value_column_uses_bare_pivot_value_as_alias(mod):
    sql = mod._build_op_sql(
        "prev",
        {
            "op": "pivot", "index": ["region"], "pivot_column": "quarter",
            "value_columns": ["amount"], "pivot_values": ["Q1"],
        },
        "duckdb", {},
    )
    assert 'AS "Q1"' in sql


def test_pivot_escapes_single_quote_in_pivot_value_no_sql_injection(mod):
    sql = mod._build_op_sql(
        "prev",
        {
            "op": "pivot", "index": ["region"], "pivot_column": "quarter",
            "value_columns": ["amount"], "pivot_values": ["Q1'; DROP TABLE x; --"],
        },
        "duckdb", {},
    )
    # The embedded single quote is doubled (escaped), not left to break out of the
    # string literal -- the "DROP TABLE" text stays inert data inside one CASE WHEN literal.
    assert "= 'Q1''; DROP TABLE x; --'" in sql


# ---------------------------------------------------------------------------
# lookup
# ---------------------------------------------------------------------------


@requires_duckdb
def test_lookup_columns_mode_real_duckdb(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE orders (id BIGINT, customer_id BIGINT)",
        "INSERT INTO orders VALUES (1, 100), (2, 200)",
        "CREATE TABLE customers (customer_id BIGINT, name VARCHAR)",
        "INSERT INTO customers VALUES (100, 'Alice'), (200, 'Bob')",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "orders"},
        operations=[{
            "op": "lookup", "right": "customers", "on_columns": ["customer_id"],
            "select_cols": ["_l.id", "_r.name"],
        }],
        output_table="orders_enriched",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = dict(_fetch(db_path, "SELECT id, name FROM orders_enriched ORDER BY id"))
    assert rows == {1: "Alice", 2: "Bob"}


@requires_duckdb
def test_lookup_match_sort_by_picks_exactly_one_row_per_key_real_duckdb(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE orders (id BIGINT, customer_id BIGINT)",
        "INSERT INTO orders VALUES (1, 100)",
        "CREATE TABLE price_history (customer_id BIGINT, price DOUBLE, updated_at BIGINT)",
        "INSERT INTO price_history VALUES (100, 10.0, 1), (100, 20.0, 2), (100, 5.0, 0)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "orders"},
        operations=[{
            "op": "lookup", "right": "price_history", "on_columns": ["customer_id"],
            "match_sort_by": ["updated_at"], "match_sort_ascending": False,
            "select_cols": ["_l.id", "_r.price"],
        }],
        output_table="orders_priced",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = _fetch(db_path, "SELECT id, price FROM orders_priced")
    assert rows == [(1, 20.0)]  # updated_at=2 (most recent) wins, not just the first match -- and no row duplication


@requires_duckdb
def test_lookup_is_match_mode_produces_boolean_column_real_duckdb(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE orders (id BIGINT, customer_id BIGINT)",
        "INSERT INTO orders VALUES (1, 100), (2, 999)",
        "CREATE TABLE gold_customers (customer_id BIGINT)",
        "INSERT INTO gold_customers VALUES (100)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "orders"},
        operations=[{
            "op": "lookup", "right": "gold_customers", "on_columns": ["customer_id"],
            "output_mode": "is_match", "output_column": "is_gold",
        }],
        output_table="orders_flagged",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = dict(_fetch(db_path, "SELECT id, is_gold FROM orders_flagged ORDER BY id"))
    assert rows == {1: True, 2: False}


def test_lookup_requires_on_columns(mod):
    with pytest.raises(ValueError, match="on_columns"):
        mod._build_op_sql("prev", {"op": "lookup", "right": "t"}, "duckdb", {})


def test_lookup_is_match_rejects_unknown_output_mode(mod):
    with pytest.raises(ValueError, match="output_mode"):
        mod._build_op_sql(
            "prev", {"op": "lookup", "right": "t", "on_columns": ["id"], "output_mode": "bogus"}, "duckdb", {},
        )


# ---------------------------------------------------------------------------
# window_calculation: ntile + cume_dist
# ---------------------------------------------------------------------------


@requires_duckdb
def test_window_calculation_ntile_real_duckdb(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE scores (id BIGINT, score DOUBLE)",
        "INSERT INTO scores VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "scores"},
        operations=[{
            "op": "window_calculation", "function": "ntile", "n": 2,
            "order_by": ["score"], "output_column": "bucket",
        }],
        output_table="scores_ntile",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = dict(_fetch(db_path, "SELECT id, bucket FROM scores_ntile ORDER BY id"))
    assert rows == {1: 1, 2: 1, 3: 2, 4: 2}


@requires_duckdb
def test_window_calculation_cume_dist_real_duckdb(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE scores (id BIGINT, score DOUBLE)",
        "INSERT INTO scores VALUES (1, 10), (2, 20), (3, 20), (4, 40)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "scores"},
        operations=[{
            "op": "window_calculation", "function": "cume_dist",
            "order_by": ["score"], "output_column": "cd",
        }],
        output_table="scores_cd",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = dict(_fetch(db_path, "SELECT id, cd FROM scores_cd ORDER BY id"))
    assert rows[1] == pytest.approx(0.25)
    assert rows[2] == pytest.approx(0.75)
    assert rows[3] == pytest.approx(0.75)
    assert rows[4] == pytest.approx(1.0)


@requires_duckdb
def test_window_calculation_group_by_partitions_independently_real_duckdb(mod, tmp_path):
    import dagster as dg

    db_path = tmp_path / "wp.duckdb"
    _exec(
        db_path,
        "CREATE TABLE scores (id BIGINT, grp VARCHAR, score DOUBLE)",
        "INSERT INTO scores VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 100), (4, 'b', 200)",
    )
    comp = mod.WarehousePipelineComponent(
        asset_name="out_asset",
        dialect="duckdb",
        database_url=f"duckdb:///{db_path}",
        source={"upstream_table": "scores"},
        operations=[{
            "op": "window_calculation", "function": "ntile", "n": 2,
            "order_by": ["score"], "group_by": ["grp"], "output_column": "bucket",
        }],
        output_table="scores_ntile_grouped",
        mode="replace",
    )
    defs = comp.build_defs(None)
    assert dg.materialize(defs.assets).success
    rows = dict(_fetch(db_path, "SELECT id, bucket FROM scores_ntile_grouped ORDER BY id"))
    # Each group gets its own 1/2 split, independent of the other group's values.
    assert rows == {1: 1, 2: 2, 3: 1, 4: 2}


def test_window_calculation_ntile_requires_n(mod):
    with pytest.raises(ValueError, match="requires 'n'"):
        mod._build_op_sql("prev", {"op": "window_calculation", "function": "ntile", "order_by": ["x"]}, "duckdb", {})


def test_window_calculation_rejects_unsupported_function(mod):
    with pytest.raises(ValueError, match="ntile"):
        mod._build_op_sql("prev", {"op": "window_calculation", "function": "rank", "order_by": ["x"]}, "duckdb", {})


def test_window_calculation_requires_order_by(mod):
    with pytest.raises(ValueError, match="order_by"):
        mod._build_op_sql("prev", {"op": "window_calculation", "function": "cume_dist"}, "duckdb", {})
