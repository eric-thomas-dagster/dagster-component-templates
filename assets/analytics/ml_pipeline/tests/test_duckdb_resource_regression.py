"""Regression test for a real, live-confirmed bug in MLPipelineComponent's
`source: {kind: warehouse_query}` and `table_sinks` handling:

`resource.get_connection()` (the DB-API branch, used by dagster_duckdb's
DuckDBResource and any resource without a SQLAlchemy .get_engine()) is a
`@contextmanager` -- calling it without `with` hands back a
`_GeneratorContextManager`, not a connection, and every use of it downstream
(`pd.read_sql`, `.to_sql`, `.begin()`) fails. Confirmed live against a real
DuckDB database before this fix existed.

NOT covered here: `table_sinks` `mode: upsert_on_match` against a resource
whose `.get_connection()` returns a raw DB-API connection (as opposed to a
SQLAlchemy one) -- that mode issues a SQLAlchemy `text()` DELETE with named
params, which a raw DuckDB connection's `.execute()` doesn't understand
(`_duckdb.InvalidInputException`). This is a separate, pre-existing gap
(the component's own comment already scopes upsert_on_match to
"duckdb-via-sqlalchemy", not the bare DuckDBResource) -- not something this
fix introduced or resolves.
"""
import duckdb
import dagster as dg
import pytest

from .conftest import load_component_module, requires_duckdb

pytestmark = requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "readings.duckdb")
    conn = duckdb.connect(path)
    conn.execute(
        "CREATE TABLE readings AS SELECT * FROM (VALUES "
        "(1, 10.0), (2, 20.0), (3, 30.0)) AS t(id, value)"
    )
    conn.close()
    return path


def test_warehouse_query_source_and_table_sink_against_real_duckdb(mod, db_path):
    from dagster_duckdb import DuckDBResource

    component = mod.MLPipelineComponent(
        asset_name_prefix="regress",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM readings"},
        target_column="value",
        feature_columns=["id"],
        steps=[{"id": "raw", "op": "select", "columns": ["id", "value"]}],
        outputs={
            "assets": ["raw"],
            "table_sinks": [
                {"from": "raw", "resource_key": "duckdb_resource", "table": "readings_out", "if_exists": "replace"},
            ],
        },
    )
    defs = component.build_defs(context=None)
    result = dg.materialize(list(defs.assets), resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success

    conn = duckdb.connect(db_path)
    out = conn.execute("SELECT * FROM readings_out ORDER BY id").fetchall()
    conn.close()
    assert out == [(1, 10.0), (2, 20.0), (3, 30.0)]
