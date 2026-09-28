"""Regression tests for ZeroShotClassifierComponent's two backported
capabilities (added for parity with context_engineering_pipeline):

- `source: {kind: warehouse_query}` dual ingestion alongside `upstream_asset_key`
- `mode: llm` -- a litellm-backed judge alongside the original zero_shot-only
  (HuggingFace) classification.
"""
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_duckdb, requires_transformers


@pytest.fixture()
def mod():
    return load_component_module()


@requires_duckdb
@requires_transformers
def test_source_warehouse_query_against_real_duckdb(mod, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "tix.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE tix AS SELECT * FROM (VALUES "
        "(1, 'I was charged twice, please refund'), "
        "(2, 'The app crashes on upload')) AS t(id, body)"
    )
    conn.close()

    component = mod.ZeroShotClassifierComponent(
        asset_name="classified_out",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT * FROM tix"},
        text_column="body",
        candidate_labels=["billing", "technical"],
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("classified_out")
    assert len(df_out) == 2


def test_mode_llm_calls_litellm_completion(mod):
    df = pd.DataFrame({"body": ["I was overcharged this month"]})
    component = mod.ZeroShotClassifierComponent(
        asset_name="classified_llm",
        upstream_asset_key="raw",
        text_column="body",
        candidate_labels=["billing", "technical"],
        mode="llm",
        llm_model="gpt-4o-mini",
    )
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return df

    fake_message = MagicMock()
    fake_message.content = '{"category": "billing"}'
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=fake_message)]
    with patch("litellm.completion", return_value=fake_resp) as mock_completion:
        result = dg.materialize([asset_def, raw])
    assert result.success
    mock_completion.assert_called_once()
    df_out = result.output_for_node("classified_llm")
    assert df_out["predicted_label"].tolist() == ["billing"]


def test_mutual_exclusivity_guard(mod):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.ZeroShotClassifierComponent(
            asset_name="x", text_column="t", candidate_labels=["a", "b"],
        ).build_defs(load_context=None)


def test_invalid_mode_raises(mod):
    with pytest.raises(ValueError, match="mode must be"):
        mod.ZeroShotClassifierComponent(
            asset_name="x", upstream_asset_key="up", text_column="t",
            candidate_labels=["a", "b"], mode="bogus",
        ).build_defs(load_context=None)
