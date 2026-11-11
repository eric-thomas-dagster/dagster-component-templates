"""chunk op strategy='qa': not a text-splitting strategy -- produces question/answer pairs indexed
by question, in the same output shape _do_chunk itself produces (chunk_id/chunk_index/
total_chunks/content_hash/output_column) so write_vector_store and every other downstream op works
identically. qa_source='column' reshapes existing columns 1:1; qa_source='llm' generates
qa_num_pairs pairs per row via litellm (mocked -- no free/local backend reports structured JSON
reliably, matching this component's own classify mode='llm' test convention).
"""
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, df=None, upstream_name="raw_docs", resources=None):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    assets = [asset_def]
    if df is not None:
        @dg.asset(name=upstream_name)
        def _upstream():
            return df
        assets.append(_upstream)
    return dg.materialize(assets, resources=resources or {}, raise_on_error=False)


def test_qa_source_column_reshapes_1_to_1(mod):
    df = pd.DataFrame({
        "doc_id": ["d1", "d2"],
        "q": ["What is the refund policy?", "How do I reset my password?"],
        "a": ["Refunds within 5 business days.", "Click 'forgot password'."],
    })
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="qa_kb",
        upstream_asset_key="raw_docs",
        id_column="doc_id",
        text_column="q",
        steps=[{"id": "chunks", "op": "chunk", "strategy": "qa", "qa_source": "column",
                "qa_question_column": "q", "qa_answer_column": "a"}],
    )
    result = _materialize(component, df)
    assert result.success
    out = result.output_for_node("qa_kb")
    assert len(out) == 2
    assert list(out["chunk_text"]) == list(df["q"])
    assert list(out["answer"]) == list(df["a"])
    assert out["chunk_id"].nunique() == 2


def test_qa_source_column_requires_both_columns(mod):
    df = pd.DataFrame({"doc_id": ["d1"], "q": ["Q?"], "a": ["A."]})
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="qa_kb", upstream_asset_key="raw_docs", id_column="doc_id", text_column="q",
        steps=[{"id": "chunks", "op": "chunk", "strategy": "qa", "qa_source": "column", "qa_question_column": "q"}],
    )
    result = _materialize(component, df)
    assert not result.success


def test_qa_source_llm_generates_pairs_from_text_column(mod):
    df = pd.DataFrame({"doc_id": ["d1"], "body": ["Our return window is 30 days from ship date."]})
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="qa_kb",
        upstream_asset_key="raw_docs",
        id_column="doc_id",
        text_column="body",
        steps=[{"id": "chunks", "op": "chunk", "strategy": "qa", "qa_source": "llm",
                "qa_model": "gpt-4o-mini", "qa_num_pairs": 2}],
    )
    fake_resp = MagicMock()
    fake_resp.choices[0].message.content = (
        '[{"question": "What is the return window?", "answer": "30 days."}, '
        '{"question": "From when?", "answer": "Ship date."}]'
    )
    with patch("litellm.completion", return_value=fake_resp) as mock_completion:
        result = _materialize(component, df)
    assert result.success
    mock_completion.assert_called_once()
    assert mock_completion.call_args.kwargs["model"] == "gpt-4o-mini"
    out = result.output_for_node("qa_kb")
    assert len(out) == 2
    assert out["chunk_text"].iloc[0] == "What is the return window?"
    assert out["answer"].iloc[0] == "30 days."


def test_qa_source_llm_failure_does_not_crash_the_run(mod):
    df = pd.DataFrame({"doc_id": ["d1"], "body": ["Some doc text."]})
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="qa_kb", upstream_asset_key="raw_docs", id_column="doc_id", text_column="body",
        steps=[{"id": "chunks", "op": "chunk", "strategy": "qa", "qa_source": "llm", "qa_model": "gpt-4o-mini"}],
    )
    with patch("litellm.completion", side_effect=RuntimeError("API down")):
        result = _materialize(component, df)
    assert result.success
    out = result.output_for_node("qa_kb")
    assert len(out) == 0


def test_run_metadata_reports_qa_source_not_misleading_chunk_defaults(mod):
    df = pd.DataFrame({"doc_id": ["d1"], "q": ["Q?"], "a": ["A."]})
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="qa_kb", upstream_asset_key="raw_docs", id_column="doc_id", text_column="q",
        steps=[{"id": "chunks", "op": "chunk", "strategy": "qa", "qa_source": "column",
                "qa_question_column": "q", "qa_answer_column": "a"}],
    )
    result = _materialize(component, df)
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    settings = meta["chunking_settings"].data
    assert settings == {"strategy": "qa", "qa_source": "column"}
    assert "chunk_size" not in settings


def test_qa_source_must_be_column_or_llm(mod):
    df = pd.DataFrame({"doc_id": ["d1"], "q": ["Q?"]})
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="qa_kb", upstream_asset_key="raw_docs", id_column="doc_id", text_column="q",
        steps=[{"id": "chunks", "op": "chunk", "strategy": "qa", "qa_source": "bogus"}],
    )
    result = _materialize(component, df)
    assert not result.success
