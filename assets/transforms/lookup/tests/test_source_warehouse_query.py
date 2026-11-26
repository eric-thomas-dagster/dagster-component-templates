"""Tests for LookupComponent's dual dataframe-or-SQL-source input
(source: {kind: warehouse_query, ...}), added alongside the same
_ingest_warehouse_query helper used across the repo.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        import dagster_duckdb  # noqa: F401
        return True
    except ImportError:
        return False


requires_duckdb = pytest.mark.skipif(
    not _duckdb_available(),
    reason="requires duckdb + dagster-duckdb (pip install duckdb dagster-duckdb)",
)


@pytest.fixture
def mod():
    return load_component_module()


@requires_duckdb
@pytest.mark.parametrize("legacy_field_names", [False, True])
def test_source_warehouse_query_against_real_duckdb(mod, legacy_field_names):
    import duckdb
    import tempfile, os
    import pandas as pd
    from dagster_duckdb import DuckDBResource

    tmp_dir = tempfile.mkdtemp()
    db_path = os.path.join(tmp_dir, "t.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a@x.com', 'cat1', 10), (2, 'b@x.com', 'cat2', 20)) AS t(id, name, category, value)")
    conn.close()

    kwargs = dict({"on": ["id"]})
    kwargs["source"] = {"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM t"}
    kwargs["asset_name"] = "out_asset"

    @dg.asset(key="lookup_ref")
    def _second():
        return pd.DataFrame({"id": [1, 2], "extra": ["A", "B"]})

    kwargs["upstream_lookup_key" if legacy_field_names else "lookup_asset_key"] = "lookup_ref"

    comp = mod.LookupComponent(**kwargs)
    defs = comp.build_defs(None)
    resources = {"duckdb_resource": DuckDBResource(database=db_path)}
    extra_assets = [_second]

    result = dg.materialize([*extra_assets, *defs.assets], resources=resources)
    assert result.success
    out = result.asset_value("out_asset")
    assert sorted(out["extra"].tolist()) == ["A", "B"]
    assert sorted(out["name"].tolist()) == ["a@x.com", "b@x.com"]


@pytest.mark.parametrize("legacy_field_names", [False, True])
def test_upstream_asset_key_and_source_mutually_exclusive(mod, legacy_field_names):
    kwargs = dict({"on": ["id"]})
    kwargs["asset_name"] = "out_asset"
    kwargs["upstream_lookup_key" if legacy_field_names else "lookup_asset_key"] = "some_other_asset"

    with pytest.raises(ValueError, match="set exactly one"):
        mod.LookupComponent(**kwargs).build_defs(None)

    kwargs["upstream_asset_key"] = "some_asset"
    kwargs["source"] = {"kind": "warehouse_query", "resource_key": "r", "sql": "SELECT 1"}
    with pytest.raises(ValueError, match="set exactly one"):
        mod.LookupComponent(**kwargs).build_defs(None)
