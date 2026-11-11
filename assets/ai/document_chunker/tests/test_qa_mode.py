"""strategy='qa': not a text-splitting strategy -- produces question/answer pairs indexed by
question. qa_source='column' reshapes existing question/answer columns 1:1; qa_source='llm'
generates qa_num_pairs pairs per document by calling an LLM (litellm, mocked here -- no free/local
backend reports structured JSON reliably, matching context_engineering_pipeline's own
classify mode='llm' test convention).
"""
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, df):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw_docs")
    def raw_docs():
        return df

    return dg.materialize([asset_def, raw_docs])


def test_qa_source_column_reshapes_1_to_1(mod):
    df = pd.DataFrame({
        "q": ["What is the refund policy?", "How do I reset my password?"],
        "a": ["Refunds are issued within 5 business days.", "Click 'forgot password' on the login page."],
    })
    component = mod.DocumentChunkerComponent(
        asset_name="qa_out",
        upstream_asset_key="raw_docs",
        strategy="qa",
        qa_source="column",
        qa_question_column="q",
        qa_answer_column="a",
        source_column="q",
    )
    result = _materialize(component, df)
    assert result.success
    out = result.output_for_node("qa_out")
    assert len(out) == 2
    assert list(out["chunk"]) == list(df["q"])
    assert list(out["answer"]) == list(df["a"])


def test_qa_source_column_requires_both_columns(mod):
    with pytest.raises(ValueError, match="qa_question_column and qa_answer_column"):
        mod.DocumentChunkerComponent(
            asset_name="x", upstream_asset_key="raw_docs", strategy="qa",
            qa_source="column", qa_question_column="q", source_column="q",
        ).build_defs(context=None)


def test_qa_source_llm_requires_qa_model(mod):
    with pytest.raises(ValueError, match="qa_source='llm' requires qa_model"):
        mod.DocumentChunkerComponent(
            asset_name="x", upstream_asset_key="raw_docs", strategy="qa",
            qa_source="llm", source_column="text",
        ).build_defs(context=None)


def test_qa_source_llm_generates_pairs_from_document_text(mod):
    df = pd.DataFrame({"text": ["Our return window is 30 days from the ship date."]})
    component = mod.DocumentChunkerComponent(
        asset_name="qa_out",
        upstream_asset_key="raw_docs",
        strategy="qa",
        qa_source="llm",
        qa_model="gpt-4o-mini",
        qa_num_pairs=2,
        source_column="text",
    )
    fake_resp = MagicMock()
    fake_resp.choices[0].message.content = (
        '[{"question": "What is the return window?", "answer": "30 days from ship date."}, '
        '{"question": "When does the window start?", "answer": "From the ship date."}]'
    )
    with patch("litellm.completion", return_value=fake_resp) as mock_completion:
        result = _materialize(component, df)
    assert result.success
    mock_completion.assert_called_once()
    assert mock_completion.call_args.kwargs["model"] == "gpt-4o-mini"
    out = result.output_for_node("qa_out")
    assert len(out) == 2
    assert out["chunk"].iloc[0] == "What is the return window?"
    assert out["answer"].iloc[0] == "30 days from ship date."


def test_qa_source_llm_strips_markdown_code_fence(mod):
    df = pd.DataFrame({"text": ["Short doc."]})
    component = mod.DocumentChunkerComponent(
        asset_name="qa_out", upstream_asset_key="raw_docs", strategy="qa",
        qa_source="llm", qa_model="gpt-4o-mini", qa_num_pairs=1, source_column="text",
    )
    fake_resp = MagicMock()
    fake_resp.choices[0].message.content = '```json\n[{"question": "Q?", "answer": "A."}]\n```'
    with patch("litellm.completion", return_value=fake_resp):
        result = _materialize(component, df)
    out = result.output_for_node("qa_out")
    assert out["chunk"].iloc[0] == "Q?"


def test_qa_source_llm_failure_for_one_document_does_not_crash_the_run(mod):
    df = pd.DataFrame({"text": ["Doc one.", "Doc two."]})
    component = mod.DocumentChunkerComponent(
        asset_name="qa_out", upstream_asset_key="raw_docs", strategy="qa",
        qa_source="llm", qa_model="gpt-4o-mini", source_column="text",
    )
    with patch("litellm.completion", side_effect=RuntimeError("API down")):
        result = _materialize(component, df)
    assert result.success
    out = result.output_for_node("qa_out")
    assert len(out) == 0  # both documents failed generation, but the run itself didn't crash


def test_parent_chunk_size_rejected_with_qa_strategy(mod):
    with pytest.raises(ValueError, match="parent_chunk_size is not supported with strategy='qa'"):
        mod.DocumentChunkerComponent(
            asset_name="x", upstream_asset_key="raw_docs", strategy="qa",
            qa_source="column", qa_question_column="q", qa_answer_column="a",
            parent_chunk_size=500, source_column="q",
        ).build_defs(context=None)
