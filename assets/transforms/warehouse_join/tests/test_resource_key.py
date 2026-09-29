"""Regression tests for WarehouseJoinComponent's resource_key support --
previously this component only accepted database_url/database_url_env_var,
with no way to reuse a project's already-registered warehouse resource
(DuckDBResource, SnowflakeResource, ...), unlike every other warehouse-
facing component in this repo. Real DuckDB execution throughout, not
mocked -- a real CTAS join against real tables, read back independently
after materialize to confirm the actual result, not just "didn't crash".
"""
import dagster as dg
import pytest

from .conftest import load_component_module, requires_duckdb, duckdb_reconnect_readonly


@pytest.fixture()
def mod():
    return load_component_module()


@requires_duckdb
def test_resource_key_runs_a_real_ctas_join(mod, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "wh.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE customers AS SELECT * FROM (VALUES (1, 'Ada'), (2, 'Grace')) AS t(customer_id, name)")
    conn.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1, 100, 50.0), (1, 101, 25.0)) AS t(customer_id, order_id, amount)")
    conn.close()

    component = mod.WarehouseJoinComponent(
        asset_name="customers_with_orders",
        resource_key="duckdb_resource",
        dialect="duckdb",
        left_table="customers",
        right_table="orders",
        output_table="customers_with_orders",
        how="inner",
        on_columns=["customer_id"],
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success

    # Read back independently -- confirms a REAL table was written with
    # the REAL joined rows, not just that materialize() didn't raise.
    conn = duckdb_reconnect_readonly(db_path)
    rows = conn.execute("SELECT customer_id, name, order_id, amount FROM customers_with_orders ORDER BY order_id").fetchall()
    conn.close()
    assert rows == [(1, "Ada", 100, 50.0), (1, "Ada", 101, 25.0)]


@requires_duckdb
def test_resource_key_requires_get_engine(mod):
    class _FakeResourceWithoutGetEngine:
        pass

    component = mod.WarehouseJoinComponent(
        asset_name="x",
        resource_key="bad_resource",
        dialect="duckdb",
        left_table="a",
        right_table="b",
        output_table="c",
        on_columns=["id"],
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize(
        [asset_def],
        resources={"bad_resource": _FakeResourceWithoutGetEngine()},
        raise_on_error=False,
    )
    assert not result.success

def test_database_url_env_var_fallback_still_works(mod, tmp_path, monkeypatch):
    """The pre-existing (non-resource) path must keep working unchanged."""
    import duckdb
    db_path = str(tmp_path / "wh2.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE a AS SELECT * FROM (VALUES (1, 'x')) AS t(id, val)")
    conn.execute("CREATE TABLE b AS SELECT * FROM (VALUES (1, 'y')) AS t(id, val2)")
    conn.close()

    monkeypatch.setenv("TEST_WAREHOUSE_URL", f"duckdb:///{db_path}")
    component = mod.WarehouseJoinComponent(
        asset_name="joined",
        database_url_env_var="TEST_WAREHOUSE_URL",
        dialect="duckdb",
        left_table="a",
        right_table="b",
        output_table="joined",
        on_columns=["id"],
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def])
    assert result.success

    conn = duckdb_reconnect_readonly(db_path)
    rows = conn.execute("SELECT id, val, val2 FROM joined").fetchall()
    conn.close()
    assert rows == [(1, "x", "y")]
