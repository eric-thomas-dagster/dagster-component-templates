"""api_base + a free-form litellm_params mapping (api_version, aws_region_name, vertex_project, ...)
let enrich, classify mode='llm', embed provider='litellm', and chunk strategy='qa' qa_source='llm'
reach non-OpenAI litellm endpoints (Azure, Bedrock, Vertex, Ollama, vLLM). The enrich op also gets
a new api_key_env_var (it previously had no way to name which env var holds its API key at all).
Mocks only litellm itself -- real chunk/classify/embed/enrich op code runs.
"""
import hashlib
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class Ctx:
    class log:
        @staticmethod
        def info(*a, **k): ...
        warning = info


def _frame(texts, **cols):
    return pd.DataFrame({"chunk_id": [f"c{i}" for i in range(len(texts))], "chunk_text": texts,
                         "content_hash": [hashlib.sha256(t.encode()).hexdigest() for t in texts], **cols})


def _fake_completion_resp(content="ok"):
    resp = MagicMock()
    resp.choices = [MagicMock(message=MagicMock(content=content))]
    return resp


# ── enrich: api_key_env_var + api_base + litellm_params ─────────────────────

def test_enrich_forwards_api_key_env_var_api_base_and_litellm_params(mod, monkeypatch):
    monkeypatch.setenv("MY_AZURE_KEY", "sekret")
    df = _frame(["alpha text"])
    with patch("litellm.completion", return_value=_fake_completion_resp("summary")) as mock_completion:
        mod._do_enrich(df, {
            "op": "enrich", "model": "azure/my-deployment", "mode": "summary",
            "api_key_env_var": "MY_AZURE_KEY",
            "api_base": "https://my-resource.openai.azure.com",
            "litellm_params": {"api_version": "2024-05-01-preview"},
        }, Ctx)
    kwargs = mock_completion.call_args.kwargs
    assert kwargs["api_key"] == "sekret"
    assert kwargs["api_base"] == "https://my-resource.openai.azure.com"
    assert kwargs["api_version"] == "2024-05-01-preview"


def test_enrich_without_api_key_env_var_passes_none_not_a_crash(mod):
    df = _frame(["alpha text"])
    with patch("litellm.completion", return_value=_fake_completion_resp("summary")) as mock_completion:
        mod._do_enrich(df, {"op": "enrich", "model": "m", "mode": "summary"}, Ctx)
    assert mock_completion.call_args.kwargs["api_key"] is None


# ── classify mode='llm': api_base + litellm_params ──────────────────────────

def test_classify_llm_forwards_api_base_and_litellm_params(mod, monkeypatch):
    monkeypatch.setenv("BEDROCK_KEY", "sekret")
    df = _frame(["a support ticket about billing"])
    fake_resp = _fake_completion_resp('{"category": "billing"}')
    with patch("litellm.completion", return_value=fake_resp) as mock_completion:
        mod._do_classify(df, {
            "op": "classify", "mode": "llm", "candidate_labels": ["billing", "technical"],
            "model": "bedrock/anthropic.claude-3-haiku", "api_key_env_var": "BEDROCK_KEY",
            "api_base": "https://bedrock.example.com",
            "litellm_params": {"aws_region_name": "us-east-1"},
        }, Ctx)
    kwargs = mock_completion.call_args.kwargs
    assert kwargs["api_base"] == "https://bedrock.example.com"
    assert kwargs["aws_region_name"] == "us-east-1"


# ── embed provider='litellm': api_base + litellm_params ─────────────────────

def test_embed_litellm_forwards_api_base_and_litellm_params(mod):
    df = _frame(["alpha text", "beta text"])

    def _fake_embedding(model, input, api_key=None, **kwargs):
        resp = MagicMock()
        resp.data = [{"embedding": [0.1, 0.2]} for _ in input]
        resp.usage = None
        assert kwargs.get("api_base") == "http://localhost:11434"
        assert kwargs.get("vertex_project") == "my-gcp-project"
        return resp

    with patch("litellm.embedding", side_effect=_fake_embedding):
        out = mod._do_embed_raw(df, {
            "op": "embed", "provider": "litellm", "model": "ollama/nomic-embed-text",
            "api_base": "http://localhost:11434",
            "litellm_params": {"vertex_project": "my-gcp-project"},
        }, Ctx)
    assert "embedding" in out.columns


# ── chunk strategy='qa' qa_source='llm': qa_api_base + qa_litellm_params ────

def test_qa_llm_forwards_qa_api_base_and_qa_litellm_params(mod):
    df = pd.DataFrame({"doc_id": ["d1"], "body": ["Our return window is 30 days."]})
    fake_resp = _fake_completion_resp(
        '[{"question": "What is the return window?", "answer": "30 days."}]'
    )
    with patch("litellm.completion", return_value=fake_resp) as mock_completion:
        mod._do_chunk(df, {
            "op": "chunk", "strategy": "qa", "qa_source": "llm", "qa_model": "ollama/llama3",
            "qa_api_base": "http://localhost:11434",
            "qa_litellm_params": {"api_version": "v1"},
        }, "doc_id", "body", [], Ctx)
    kwargs = mock_completion.call_args.kwargs
    assert kwargs["api_base"] == "http://localhost:11434"
    assert kwargs["api_version"] == "v1"
