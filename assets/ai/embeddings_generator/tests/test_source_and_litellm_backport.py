"""Regression tests for EmbeddingsGeneratorComponent's two backported
capabilities (added for parity with context_engineering_pipeline):

- `source: {kind: warehouse_query}` dual ingestion alongside `upstream_asset_key`
- `provider: litellm` -- a universal embedding gateway across every provider
  litellm supports, alongside the existing openai/cohere/sentence_transformers/
  huggingface providers.
"""
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb, requires_sentence_transformers


@pytest.fixture()
def mod():
    return load_component_module()


@requires_duckdb
@requires_sentence_transformers
def test_source_warehouse_query_against_real_duckdb(mod, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "docs.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE docs AS SELECT * FROM (VALUES (1, 'hello world'), (2, 'another document')) AS t(id, text)")
    conn.close()

    component = mod.EmbeddingsGeneratorComponent(
        asset_name="embeds_out",
        provider="sentence_transformers",
        model="all-MiniLM-L6-v2",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM docs"},
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("embeds_out")
    assert len(df_out) == 2 and "embedding" in df_out.columns


def test_provider_litellm_calls_litellm_embedding(mod):
    df = pd.DataFrame({"text": ["a", "b"]})
    component = mod.EmbeddingsGeneratorComponent(
        asset_name="embeds_out2",
        provider="litellm",
        model="text-embedding-3-small",
        upstream_asset_key="raw",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return df

    fake_resp = MagicMock()
    fake_resp.data = [{"embedding": [0.1, 0.2]}, {"embedding": [0.3, 0.4]}]
    fake_resp.usage = MagicMock(total_tokens=10)
    with patch("litellm.embedding", return_value=fake_resp) as mock_embed:
        result = dg.materialize([asset_def, raw])
    assert result.success
    mock_embed.assert_called_once()
    df_out = result.output_for_node("embeds_out2")
    assert len(df_out) == 2


def test_mutual_exclusivity_guard(mod):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.EmbeddingsGeneratorComponent(asset_name="x", provider="openai", model="m").build_defs(context=None)
