"""search_mode (full_text/hybrid, client-side BM25/RRF re-rank over an oversampled vector-fetched
candidate pool), rerank_model (litellm.rerank, mocked -- no free/local reranker), and
score_threshold (auto-detects distance- vs score-shaped results). The post-processing itself
(_rerank_and_filter_results, _bm25_scores, _reciprocal_rank_fusion, _apply_rerank_model) is tested
directly as a unit wherever that's clearer than a full materialize() round-trip; search_mode and
score_threshold's distance-based path are also exercised end-to-end against the real s3vectors
demo-mode seam (no AWS calls), matching this directory's sibling test_s3vectors_provider.py.
"""
import json
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
VectorStoreQueryComponent = mod.VectorStoreQueryComponent


def _write_demo_file(path, rows):
    data = {"indexes": {"idx": {"dimension": len(rows[0][1]), "distanceMetric": "cosine", "vectors": {}}}}
    for key, vector, metadata in rows:
        data["indexes"]["idx"]["vectors"][key] = {"data": {"float32": vector}, "metadata": metadata}
    path.write_text(json.dumps(data))


def _component(**kw):
    attrs = dict(
        asset_name="results", provider="s3vectors", collection_name="idx",
        vector_bucket_name="buck", aws_region="us-east-2", upstream_asset_key="queries",
    )
    attrs.update(kw)
    return VectorStoreQueryComponent(**attrs)


def _materialize(comp, df, **kwargs):
    @dg.asset(name="queries")
    def queries():
        return df

    defs = comp.build_defs(None)
    return dg.materialize([queries, *defs.assets], **kwargs)


# --- unit-level: the pure post-processing logic ------------------------------------------


def test_bm25_scores_favor_real_term_overlap():
    # BM25's IDF needs more than 2 documents to discriminate at all -- with only 2, a term in
    # exactly one of them has IDF log((2-1+0.5)/(1+0.5)) = log(1) = 0 (verified directly against
    # rank_bm25.BM25Okapi before writing this test), so every score comes out exactly 0.0. A
    # realistic-sized corpus is required, not a hand-picked-to-be-tiny one.
    scores = mod._bm25_scores("return policy", [
        "Our return policy is 30 days.",
        "Completely unrelated text about shipping.",
        "The weather today is sunny and warm.",
        "We sell shoes and clothing online.",
    ])
    assert scores[0] > scores[1]


def test_bm25_scores_missing_text_scores_zero_not_crash():
    scores = mod._bm25_scores("return policy", [
        None,
        "Our return policy is 30 days.",
        "The weather today is sunny and warm.",
        "We sell shoes and clothing online.",
    ])
    assert scores[0] == 0.0
    assert scores[1] > 0.0


def test_reciprocal_rank_fusion_rewards_candidates_ranked_well_in_both_lists():
    # candidate 0 is #1 in both rankings; candidate 1 is #1 vector but #2 text; candidate 2 is reversed.
    fused = mod._reciprocal_rank_fusion([[0, 1, 2], [0, 2, 1]])
    assert fused[0] > fused[1]
    assert fused[0] > fused[2]


def test_extract_result_text_chromadb_vs_metadata_providers():
    assert mod._extract_result_text({"document": "hello"}, "chromadb", "text") == "hello"
    assert mod._extract_result_text({"metadata": {"text": "hi"}}, "pinecone", "text") == "hi"
    assert mod._extract_result_text({"payload": {"body": "hi"}}, "qdrant", "body") == "hi"
    assert mod._extract_result_text({"metadata": {}}, "pinecone", "text") is None


def test_rerank_and_filter_full_text_reorders_by_bm25_not_vector_distance():
    results = [
        {"id": "a", "distance": 0.1, "document": "completely unrelated shipping text"},
        {"id": "b", "distance": 0.5, "document": "our refund policy allows returns"},
        {"id": "c", "distance": 0.6, "document": "the weather today is sunny and warm"},
        {"id": "d", "distance": 0.7, "document": "we sell shoes and clothing online"},
    ]
    out = mod._rerank_and_filter_results(
        results, provider="chromadb", query_text="refund policy", search_mode="full_text",
        text_metadata_key="text", top_k=4, rerank_model=None, rerank_api_key_env_var="X",
        score_threshold=None,
    )
    assert out[0]["id"] == "b"  # worse vector distance, but the real text match


def test_rerank_and_filter_hybrid_truncates_to_top_k():
    results = [
        {"id": f"d{i}", "distance": float(i), "document": f"doc number {i} about refunds"}
        for i in range(10)
    ]
    out = mod._rerank_and_filter_results(
        results, provider="chromadb", query_text="refunds", search_mode="hybrid",
        text_metadata_key="text", top_k=3, rerank_model=None, rerank_api_key_env_var="X",
        score_threshold=None,
    )
    assert len(out) == 3


def test_rerank_and_filter_vector_mode_is_a_pure_passthrough_truncation():
    results = [{"id": "a", "distance": 0.1}, {"id": "b", "distance": 0.2}, {"id": "c", "distance": 0.3}]
    out = mod._rerank_and_filter_results(
        results, provider="chromadb", query_text=None, search_mode="vector",
        text_metadata_key="text", top_k=2, rerank_model=None, rerank_api_key_env_var="X",
        score_threshold=None,
    )
    assert [r["id"] for r in out] == ["a", "b"]


def test_rerank_model_reorders_and_adds_relevance_score():
    results = [
        {"id": "a", "distance": 0.1, "document": "unrelated"},
        {"id": "b", "distance": 0.5, "document": "refund policy details"},
    ]
    fake_resp = MagicMock()
    fake_resp.results = [{"index": 0, "relevance_score": 0.1}, {"index": 1, "relevance_score": 0.9}]
    with patch("litellm.rerank", return_value=fake_resp) as mock_rerank:
        out = mod._rerank_and_filter_results(
            results, provider="chromadb", query_text="refund policy", search_mode="vector",
            text_metadata_key="text", top_k=2, rerank_model="rerank-english-v3.0",
            rerank_api_key_env_var="COHERE_API_KEY", score_threshold=None,
        )
    mock_rerank.assert_called_once()
    assert out[0]["id"] == "b"
    assert out[0]["relevance_score"] == 0.9


def test_score_threshold_with_rerank_drops_low_relevance():
    results = [{"id": "a", "distance": 0.1, "document": "x"}, {"id": "b", "distance": 0.2, "document": "y"}]
    fake_resp = MagicMock()
    fake_resp.results = [{"index": 0, "relevance_score": 0.2}, {"index": 1, "relevance_score": 0.8}]
    with patch("litellm.rerank", return_value=fake_resp):
        out = mod._rerank_and_filter_results(
            results, provider="chromadb", query_text="q", search_mode="vector",
            text_metadata_key="text", top_k=2, rerank_model="m", rerank_api_key_env_var="X",
            score_threshold=0.5,
        )
    assert [r["id"] for r in out] == ["b"]


def test_score_threshold_distance_based_drops_hits_above_threshold():
    results = [{"id": "a", "distance": 0.1}, {"id": "b", "distance": 0.9}]
    out = mod._rerank_and_filter_results(
        results, provider="chromadb", query_text=None, search_mode="vector",
        text_metadata_key="text", top_k=2, rerank_model=None, rerank_api_key_env_var="X",
        score_threshold=0.5,
    )
    assert [r["id"] for r in out] == ["a"]


def test_score_threshold_score_based_drops_hits_below_threshold():
    """pinecone/qdrant return a similarity 'score' (higher is better) instead of 'distance' --
    the auto-detection must flip the comparison direction for this shape."""
    results = [{"id": "a", "score": 0.9}, {"id": "b", "score": 0.1}]
    out = mod._rerank_and_filter_results(
        results, provider="pinecone", query_text=None, search_mode="vector",
        text_metadata_key="text", top_k=2, rerank_model=None, rerank_api_key_env_var="X",
        score_threshold=0.5,
    )
    assert [r["id"] for r in out] == ["a"]


# --- end-to-end: real s3vectors demo mode, no AWS calls ----------------------------------


def test_search_mode_requires_query_text_or_column(tmp_path):
    with pytest.raises(ValueError, match="query text to score against"):
        _component(search_mode="full_text", connection_string=str(tmp_path / "demo.json")).build_defs(None)


def test_rerank_model_requires_query_text_or_column(tmp_path):
    with pytest.raises(ValueError, match="needs query_text"):
        _component(rerank_model="m", connection_string=str(tmp_path / "demo.json")).build_defs(None)


def test_oversample_factor_fetches_more_candidates_before_truncating(tmp_path):
    demo_path = tmp_path / "demo.json"
    _write_demo_file(demo_path, [
        (f"doc_{i}", [1.0, 0.0], {"text": f"document number {i} about refunds and returns"})
        for i in range(5)
    ])
    comp = _component(
        connection_string=str(demo_path), top_k=2, search_mode="full_text",
        query_text="refunds", oversample_factor=3,
    )
    result = _materialize(comp, pd.DataFrame({"embedding": [[1.0, 0.0]]}), raise_on_error=True)
    assert result.success
    out = result.asset_value("results")
    assert len(out) == 2  # still truncated to top_k despite fetching top_k * oversample_factor
