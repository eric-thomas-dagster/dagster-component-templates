"""Committed regression tests for RagGroundingCheckComponent.

- mode='overlap' (the default) is deterministic and free -- tested for real,
  nothing mocked, including the fabricated-number detection and the
  cross-materialization regression check (mirrors rag_eval's own pattern).
- mode='llm_judge' has litellm.completion mocked (no free/local backend
  exists for an LLM judge) -- request/response handling and score mapping
  verified for real.
- `source: {kind: warehouse_query}` tested for real against DuckDB.
"""
import tempfile

import dagster as dg
import pandas as pd
import pytest

from .conftest import execute_with_checks, load_component_module, make_upstream_asset, requires_duckdb


@pytest.fixture()
def mod():
    return load_component_module()


def test_overlap_mode_scores_grounded_and_ungrounded_answers(mod):
    df = pd.DataFrame({
        "query": ["What was Q3 revenue?", "What is the refund policy?", "How many employees?"],
        "answer": [
            "Q3 revenue grew 12% year over year to $45 million.",
            "Customers can request a refund within 30 days of purchase.",
            "The company has over 10,000 employees worldwide.",
        ],
        "sources": [
            [{"text": "In Q3, revenue grew 12% year over year, reaching $45 million in total."}],
            [{"text": "Our refund policy allows customers to request a refund within 30 days of purchase."}],
            [{"text": "The company was founded in 2010 and is headquartered in Austin."}],
        ],
    })
    component = mod.RagGroundingCheckComponent(asset_name="grounding_out", upstream_asset_key="rag_answers", mode="overlap")
    defs = component.build_defs(context=None)
    upstream = make_upstream_asset("rag_answers", df)
    result = execute_with_checks(defs, upstream)
    assert result.success
    df_out = result.output_for_node("grounding_out")

    assert df_out.loc[0, "grounded"] and df_out.loc[0, "fabricated_numbers"] == []
    assert df_out.loc[1, "grounded"] and df_out.loc[1, "fabricated_numbers"] == []
    assert not df_out.loc[2, "grounded"]
    assert "10,000" in df_out.loc[2, "fabricated_numbers"]


def test_quarter_style_numbers_are_not_false_positive_fabrications(mod):
    """Regression test: "Q3" must not be misread as a fabricated number '3'."""
    df = pd.DataFrame({
        "answer": ["Q3 results were strong."],
        "sources": [[{"text": "Q3 results exceeded expectations."}]],
    })
    component = mod.RagGroundingCheckComponent(asset_name="grounding_out", upstream_asset_key="rag_answers", mode="overlap")
    defs = component.build_defs(context=None)
    upstream = make_upstream_asset("rag_answers", df)
    result = execute_with_checks(defs, upstream)
    assert result.success
    df_out = result.output_for_node("grounding_out")
    assert df_out.loc[0, "fabricated_numbers"] == []


def test_cross_materialization_regression_check(mod):
    good_df = pd.DataFrame({
        "answer": ["Refunds within 30 days.", "Shipping takes 5 business days."],
        "sources": [
            [{"text": "Refunds within 30 days of purchase are accepted."}],
            [{"text": "Standard shipping takes 5 business days to arrive."}],
        ],
    })
    bad_df = pd.DataFrame({
        "answer": ["Refunds within 90 days and a $50 restocking fee applies.", "Shipping is free and instant."],
        "sources": [
            [{"text": "Refunds within 30 days of purchase are accepted."}],
            [{"text": "Standard shipping takes 5 business days to arrive."}],
        ],
    })
    component = mod.RagGroundingCheckComponent(
        asset_name="grounding_regress", upstream_asset_key="rag_answers", mode="overlap",
        min_grounding_score_threshold=0.1, regression_pct_threshold=10.0,
    )
    defs = component.build_defs(context=None)

    with tempfile.TemporaryDirectory() as tmp:
        instance = dg.DagsterInstance.ephemeral(tempdir=tmp)

        result1 = execute_with_checks(defs, make_upstream_asset("rag_answers", good_df), instance=instance)
        checks1 = result1.get_asset_check_evaluations()
        assert len(checks1) == 1 and checks1[0].passed

        result2 = execute_with_checks(defs, make_upstream_asset("rag_answers", bad_df), instance=instance)
        checks2 = result2.get_asset_check_evaluations()
        assert len(checks2) == 1 and not checks2[0].passed
        meta = {k: getattr(v, "value", v) for k, v in (checks2[0].metadata or {}).items()}
        assert meta["prior_score"] == 1.0
        assert meta["current_score"] < meta["prior_score"]


def test_mode_llm_judge_calls_litellm_and_maps_score(mod):
    from unittest.mock import MagicMock, patch

    df = pd.DataFrame({
        "answer": ["The company was founded in 1999 by three engineers."],
        "sources": [[{"text": "The company was founded in 2005."}]],
    })
    component = mod.RagGroundingCheckComponent(asset_name="grounding_llm", upstream_asset_key="rag_answers", mode="llm_judge")
    defs = component.build_defs(context=None)
    upstream = make_upstream_asset("rag_answers", df)

    fake_message = MagicMock()
    fake_message.content = '{"grounded": false, "confidence": 0.9, "reasoning": "not supported"}'
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=fake_message)]
    with patch("litellm.completion", return_value=fake_resp) as mock_completion:
        result = execute_with_checks(defs, upstream)
    assert result.success
    mock_completion.assert_called_once()
    df_out = result.output_for_node("grounding_llm")
    assert df_out.loc[0, "grounding_score"] == pytest.approx(0.1)
    assert not df_out.loc[0, "grounded"]
    assert df_out.loc[0, "judge_reasoning"] == "not supported"


@requires_duckdb
def test_source_warehouse_query_against_real_duckdb(mod, tmp_path):
    import duckdb
    from dagster_duckdb import DuckDBResource

    db_path = str(tmp_path / "rag.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE rag_answers AS SELECT * FROM (VALUES "
        "('refund policy?', 'Refunds within 30 days.', 'Refunds within 30 days of purchase are accepted.')"
        ") AS t(query, answer, sources_text)"
    )
    conn.close()

    component = mod.RagGroundingCheckComponent(
        asset_name="grounding_sql_source",
        source={"kind": "warehouse_query", "resource_key": "duckdb_resource", "sql": "SELECT query, answer, sources_text AS sources FROM rag_answers"},
        mode="overlap",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"duckdb_resource": DuckDBResource(database=db_path)})
    assert result.success
    df_out = result.output_for_node("grounding_sql_source")
    assert len(df_out) == 1 and df_out.loc[0, "grounded"]


def test_mutual_exclusivity_guard(mod):
    with pytest.raises(ValueError, match="set exactly one"):
        mod.RagGroundingCheckComponent(asset_name="x").build_defs(context=None)


def test_invalid_mode_raises(mod):
    with pytest.raises(ValueError, match="mode must be"):
        mod.RagGroundingCheckComponent(asset_name="x", upstream_asset_key="up", mode="bogus").build_defs(context=None)


def test_max_fabrication_rate_fails_check(mod):
    df = pd.DataFrame({
        "answer": ["Revenue was $999 million.", "We shipped 500 units."],
        "sources": [[{"text": "Revenue grew significantly."}], [{"text": "We shipped many units."}]],
    })
    component = mod.RagGroundingCheckComponent(
        asset_name="grounding_fab", upstream_asset_key="rag_answers", mode="overlap",
        min_grounding_score_threshold=0.0, max_fabrication_rate=0.1,
    )
    defs = component.build_defs(context=None)
    result = execute_with_checks(defs, make_upstream_asset("rag_answers", df))
    checks = result.get_asset_check_evaluations()
    assert len(checks) == 1 and not checks[0].passed
    meta = {k: getattr(v, "value", v) for k, v in (checks[0].metadata or {}).items()}
    assert "fabrication_rate" in meta["reason"].lower() or "fabrication" in meta["reason"].lower()
