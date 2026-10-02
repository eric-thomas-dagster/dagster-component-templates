"""Tests for PerplexityLLMComponent. Mocks the single isolated
`_call_chat_completion` function -- the real (paid) Perplexity API is never
called."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, make_upstream_asset

mod = load_component_module()
PerplexityLLMComponent = mod.PerplexityLLMComponent


class FakeMessage:
    def __init__(self, content, citations=None):
        self.content = content
        if citations is not None:
            self.citations = citations


class FakeChoice:
    def __init__(self, content, message_citations=None):
        self.message = FakeMessage(content, citations=message_citations)


class FakeSearchResult:
    """Stands in for a pydantic-model-like search_results entry that isn't
    a plain dict -- exercises _extract_search_results' model_dump fallback."""
    def __init__(self, **kw):
        self._data = kw

    def model_dump(self):
        return dict(self._data)


class FakeResponse:
    """Mirrors the real Perplexity chat completion shape: `citations` and
    `search_results` as TOP-LEVEL fields on the response (confirmed against
    docs.perplexity.ai/api-reference/chat-completions-post), alongside the
    standard OpenAI-shaped `choices`."""
    def __init__(self, content, citations=None, search_results=None,
                 related_questions=None, message_citations=None):
        self.choices = [FakeChoice(content, message_citations=message_citations)]
        if citations is not None:
            self.citations = citations
        if search_results is not None:
            self.search_results = search_results
        if related_questions is not None:
            self.related_questions = related_questions


def _component(**overrides):
    attrs = dict(
        asset_name="briefs",
        upstream_asset_key="companies",
        input_column="company_name",
        output_column="brief",
    )
    attrs.update(overrides)
    return PerplexityLLMComponent(**attrs)


def _materialize(component, df, monkeypatch):
    monkeypatch.setenv("PERPLEXITY_API_KEY", "test-key-123")
    defs = component.build_defs(None)
    upstream = make_upstream_asset("companies", df)
    full_defs = dg.Definitions(assets=[*defs.assets, upstream])
    return full_defs.get_implicit_global_asset_job_def().execute_in_process()


def test_basic_per_row_inference_with_top_level_citations(monkeypatch):
    def fake_call(client, **kwargs):
        prompt = kwargs["messages"][-1]["content"]
        return FakeResponse(
            f"info about {prompt}",
            citations=["https://example.com/a", "https://example.com/b"],
        )

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"company_name": ["Acme", "Globex"]})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("briefs"))
    assert list(out["brief"]) == ["info about Acme", "info about Globex"]
    assert out["perplexity_citations"].iloc[0] == [
        "https://example.com/a", "https://example.com/b",
    ]
    assert out["perplexity_citations"].iloc[1] == [
        "https://example.com/a", "https://example.com/b",
    ]


def test_citations_fallback_to_message_when_top_level_absent(monkeypatch):
    """Some write-ups show citations nested under choice.message.citations
    instead of the response's top level; the component must not crash and
    must still recover them from there."""
    def fake_call(client, **kwargs):
        return FakeResponse(
            "answer",
            citations=None,
            message_citations=["https://nested.example.com/x"],
        )

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"company_name": ["Acme"]})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("briefs"))
    assert out["perplexity_citations"].iloc[0] == ["https://nested.example.com/x"]


def test_no_citations_returns_empty_list_not_error(monkeypatch):
    def fake_call(client, **kwargs):
        return FakeResponse("answer with no sources")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"company_name": ["Acme"]})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("briefs"))
    assert out["perplexity_citations"].iloc[0] == []


def test_include_search_results_adds_detail_column(monkeypatch):
    def fake_call(client, **kwargs):
        return FakeResponse(
            "answer",
            citations=["https://example.com/a"],
            search_results=[
                {"title": "A", "url": "https://example.com/a", "date": "2026-01-01"},
                FakeSearchResult(title="B", url="https://example.com/b", date="2026-02-02"),
            ],
        )

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"company_name": ["Acme"]})
    comp = _component(include_search_results=True)
    result = _materialize(comp, df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("briefs"))
    detail = out["perplexity_citations_detail"].iloc[0]
    assert detail[0] == {"title": "A", "url": "https://example.com/a", "date": "2026-01-01"}
    assert detail[1] == {"title": "B", "url": "https://example.com/b", "date": "2026-02-02"}


def test_search_domain_and_recency_filters_passed_through(monkeypatch):
    seen = {}

    def fake_call(client, **kwargs):
        seen.update(kwargs)
        return FakeResponse("answer", citations=[])

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"company_name": ["Acme"]})
    comp = _component(
        search_domain_filter=["nytimes.com", "-reddit.com"],
        search_recency_filter="week",
        return_related_questions=True,
    )
    _materialize(comp, df, monkeypatch)

    assert seen["search_domain_filter"] == ["nytimes.com", "-reddit.com"]
    assert seen["search_recency_filter"] == "week"
    assert seen["return_related_questions"] is True


def test_related_questions_surfaced_in_metadata(monkeypatch):
    def fake_call(client, **kwargs):
        return FakeResponse(
            "answer", citations=[],
            related_questions=["What about Globex?", "Who founded Acme?"],
        )

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"company_name": ["Acme"]})
    result = _materialize(_component(), df, monkeypatch)

    mats = result.asset_materializations_for_node("briefs")
    md = mats[-1].metadata
    assert "sample_related_questions" in md
    assert md["total_citations"].value == 0


def test_per_row_error_captured_without_failing_whole_asset(monkeypatch):
    def fake_call(client, **kwargs):
        prompt = kwargs["messages"][-1]["content"]
        if "bad" in prompt:
            raise RuntimeError("401 invalid_api_key")
        return FakeResponse("fine", citations=["https://example.com/ok"])

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"company_name": ["bad row", "good row"]})
    comp = _component(max_retries=0)
    result = _materialize(comp, df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("briefs"))
    assert pd.isna(out["brief"].iloc[0])
    assert out["brief"].iloc[1] == "fine"
    assert out["perplexity_citations"].iloc[0] == []
    assert out["perplexity_citations"].iloc[1] == ["https://example.com/ok"]
    assert "401" in out["brief_error"].iloc[0]


def test_empty_upstream_short_circuits(monkeypatch):
    calls = []
    monkeypatch.setattr(mod, "_call_chat_completion", lambda client, **kw: calls.append(kw))

    df = pd.DataFrame({"company_name": []})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("briefs"))
    assert len(out) == 0
    assert "brief" in out.columns
    assert "perplexity_citations" in out.columns
    assert calls == []


def test_mutually_exclusive_input_fields_raise():
    comp = _component(user_prompt_template="hi {company_name}")
    with pytest.raises(ValueError, match="OR user_prompt_template, not both"):
        comp.build_defs(None)


def test_missing_input_fields_raise():
    comp = PerplexityLLMComponent(asset_name="x", upstream_asset_key="y")
    with pytest.raises(ValueError, match="set input_column or user_prompt_template"):
        comp.build_defs(None)


def test_missing_api_key_raises(monkeypatch):
    monkeypatch.delenv("PERPLEXITY_API_KEY", raising=False)
    comp = _component()
    defs = comp.build_defs(None)
    upstream = make_upstream_asset("companies", pd.DataFrame({"company_name": ["x"]}))
    full_defs = dg.Definitions(assets=[*defs.assets, upstream])
    result = full_defs.get_implicit_global_asset_job_def().execute_in_process(raise_on_error=False)
    assert not result.success


def test_partition_bridge_concats_dict_of_frames(monkeypatch):
    def fake_call(client, **kwargs):
        return FakeResponse("ok", citations=["https://example.com/x"])

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)
    monkeypatch.setenv("PERPLEXITY_API_KEY", "test-key-123")

    comp = _component()
    defs = comp.build_defs(None)
    asset_fn = list(defs.assets)[0]

    upstream_dict = {
        "p1": pd.DataFrame({"company_name": ["a"]}),
        "p2": pd.DataFrame({"company_name": ["b"]}),
    }
    out_df = asset_fn(dg.build_asset_context(), upstream_dict)
    assert len(out_df) == 2
    assert list(out_df["brief"]) == ["ok", "ok"]
    assert out_df["perplexity_citations"].iloc[0] == ["https://example.com/x"]
