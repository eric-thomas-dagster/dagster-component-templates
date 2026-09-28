"""Regression tests for DocumentChunkerComponent's dual-ingestion backport
(`source: {kind: warehouse_query}` alongside the original `upstream_asset_key`),
added for parity with context_engineering_pipeline. See that component's
README for the broader design rationale.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


@requires_duckdb
def test_source_warehouse_query_against_real_duckdb(mod, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "docs.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE docs AS SELECT * FROM (VALUES "
        "(1, 'This is a short document. It has two sentences.'), "
        "(2, 'Another document here. Also two sentences.')) AS t(doc_id, text)"
    )
    conn.close()

    component = mod.DocumentChunkerComponent(
        asset_name="chunks_out",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM docs"},
        strategy="fixed",
        chunk_size=1000,
        source_column="text",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("chunks_out")
    assert len(df_out) == 2


def test_upstream_asset_key_still_works(mod):
    df = pd.DataFrame({"doc_id": [1, 2], "text": ["Hello world. This is a test.", "Another doc. With two sentences."]})
    component = mod.DocumentChunkerComponent(
        asset_name="chunks_out2",
        upstream_asset_key="raw_docs",
        strategy="fixed",
        chunk_size=1000,
        source_column="text",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw_docs")
    def raw_docs():
        return df

    result = dg.materialize([asset_def, raw_docs])
    assert result.success


def test_mutual_exclusivity_guard(mod):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.DocumentChunkerComponent(asset_name="x", strategy="fixed", source_column="text").build_defs(context=None)
