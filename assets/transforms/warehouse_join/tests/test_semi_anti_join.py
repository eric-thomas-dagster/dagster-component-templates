"""Real tests for ADF-"Exists"-style semi/anti join support.

Gap: `_VALID_HOWS` previously had no `semi`/`anti` at all -- not even
accepted as valid input. Standard SQL has no SEMI JOIN/ANTI JOIN keyword in
most dialects, so these compile to a portable `WHERE EXISTS (...)` /
`WHERE NOT EXISTS (...)` subquery instead (DuckDB and a few others support
SEMI/ANTI JOIN as an extension, but this component's siblings need to work
across every dialect in `_SUPPORTED_DIALECTS`, so the extension keyword is
deliberately not relied on).

Real DuckDB execution throughout (matching this component's existing
test_resource_key.py convention) -- a real CTAS, read back independently
after materialize to confirm the actual rows, not mocked SQL assertions
alone. The generated SQL text is also asserted on directly, since that's
this component's existing style (see _ctas_join's "warehouse/sql" metadata).
"""
import dagster as dg
import pytest

from .conftest import load_component_module, requires_duckdb, duckdb_reconnect_readonly


@pytest.fixture()
def mod():
    return load_component_module()


def test_ctas_join_semi_sql_is_exists_subquery(mod):
    sql = mod._ctas_join(
        "out", "customers", "orders", "semi", ["customer_id"], None, None,
        None, "replace", "duckdb",
    )
    assert "EXISTS (SELECT 1 FROM" in sql
    assert "NOT EXISTS" not in sql
    assert "JOIN" not in sql.upper()  # no SEMI JOIN keyword relied on -- portable EXISTS form
    assert "_l.*" in sql
    assert "_r.*" not in sql  # no right-side columns


def test_ctas_join_anti_sql_is_not_exists_subquery(mod):
    sql = mod._ctas_join(
        "out", "customers", "orders", "anti", ["customer_id"], None, None,
        None, "replace", "duckdb",
    )
    assert "WHERE NOT EXISTS (SELECT 1 FROM" in sql
    assert "_l.*" in sql
    assert "_r.*" not in sql


def test_ctas_join_semi_anti_require_on_or_left_right_on(mod):
    with pytest.raises(ValueError, match="on"):
        mod._ctas_join(
            "out", "customers", "orders", "semi", None, None, None,
            None, "replace", "duckdb",
        )


@requires_duckdb
def test_semi_join_keeps_matched_rows_no_right_columns(mod, tmp_path):
    import duckdb

    db_path = str(tmp_path / "wh.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE customers AS SELECT * FROM "
        "(VALUES (1, 'Ada'), (2, 'Grace'), (3, 'Hopper')) AS t(customer_id, name)"
    )
    conn.execute(
        "CREATE TABLE orders AS SELECT * FROM "
        "(VALUES (1, 100), (1, 101), (2, 102)) AS t(customer_id, order_id)"
    )
    conn.close()

    from dagster_duckdb import DuckDBResource

    component = mod.WarehouseJoinComponent(
        asset_name="customers_with_orders",
        resource_key="duckdb_resource",
        dialect="duckdb",
        left_table="customers",
        right_table="orders",
        output_table="customers_with_orders",
        how="semi",
        on_columns=["customer_id"],
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success

    conn = duckdb_reconnect_readonly(db_path)
    rows = conn.execute(
        "SELECT customer_id, name FROM customers_with_orders ORDER BY customer_id"
    ).fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM customers_with_orders LIMIT 0").description]
    conn.close()
    # Only customers 1 and 2 placed orders; duplicate order rows for
    # customer 1 must not duplicate the output row. No right-side columns.
    assert rows == [(1, "Ada"), (2, "Grace")]
    assert cols == ["customer_id", "name"]


@requires_duckdb
def test_anti_join_keeps_unmatched_rows_only(mod, tmp_path):
    import duckdb

    db_path = str(tmp_path / "wh2.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE customers AS SELECT * FROM "
        "(VALUES (1, 'Ada'), (2, 'Grace'), (3, 'Hopper')) AS t(customer_id, name)"
    )
    conn.execute(
        "CREATE TABLE orders AS SELECT * FROM "
        "(VALUES (1, 100), (1, 101), (2, 102)) AS t(customer_id, order_id)"
    )
    conn.close()

    from dagster_duckdb import DuckDBResource

    component = mod.WarehouseJoinComponent(
        asset_name="customers_without_orders",
        resource_key="duckdb_resource",
        dialect="duckdb",
        left_table="customers",
        right_table="orders",
        output_table="customers_without_orders",
        how="anti",
        on_columns=["customer_id"],
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success

    conn = duckdb_reconnect_readonly(db_path)
    rows = conn.execute(
        "SELECT customer_id, name FROM customers_without_orders ORDER BY customer_id"
    ).fetchall()
    conn.close()
    assert rows == [(3, "Hopper")]
