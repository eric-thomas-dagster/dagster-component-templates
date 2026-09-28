"""Regression tests for VectorStoreWriterComponent's dual-ingestion backport
(`source: {kind: warehouse_query}` alongside the original `upstream_asset_key`),
added for parity with context_engineering_pipeline -- e.g. embeddings already
computed and stored in a warehouse table, indexed directly without an
intermediate Dagster asset.
"""
import dagster as dg
import pytest

from .conftest import load_component_module, requires_chromadb, requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


@requires_duckdb
@requires_chromadb
def test_source_warehouse_query_against_real_duckdb_and_chromadb(mod, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "embeds.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE embeds AS SELECT * FROM (VALUES "
        "(1, 'hello world', [0.1::DOUBLE, 0.2::DOUBLE, 0.3::DOUBLE]), "
        "(2, 'another doc', [0.4::DOUBLE, 0.5::DOUBLE, 0.6::DOUBLE])) AS t(id, text, embedding)"
    )
    conn.close()

    component = mod.VectorStoreWriterComponent(
        asset_name="vs_out",
        provider="chromadb",
        collection_name="test_collection",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM embeds"},
        connection_string=str(tmp_path / "chroma"),
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success


def test_mutual_exclusivity_guard(mod):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.VectorStoreWriterComponent(asset_name="x", provider="chromadb", collection_name="c").build_defs(context=None)
