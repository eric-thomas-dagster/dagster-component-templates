"""Tests for MammouthLLMComponent. Mocks the single isolated
`_call_chat_completion` function -- the real (paid) Mammouth API is never
called."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, make_upstream_asset

mod = load_component_module()
MammouthLLMComponent = mod.MammouthLLMComponent


class FakeMessage:
    def __init__(self, content):
        self.content = content


class FakeChoice:
    def __init__(self, content):
        self.message = FakeMessage(content)


class FakeUsage:
    """Mirrors the real Mammouth usage shape (standard OpenAI wire):
    prompt_tokens / completion_tokens / total_tokens (confirmed against
    https://info.mammouth.ai/docs/api-quick-start/)."""
    def __init__(self, prompt_tokens=0, completion_tokens=0):
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = prompt_tokens + completion_tokens


class FakeResponse:
    """Mirrors the real Mammouth chat completion shape: standard OpenAI
    `choices[0].message.content` + top-level `usage`."""
    def __init__(self, content, prompt_tokens=10, completion_tokens=5):
        self.choices = [FakeChoice(content)]
        self.usage = FakeUsage(prompt_tokens, completion_tokens)


def _component(**overrides):
    attrs = dict(
        asset_name="summaries",
        upstream_asset_key="tickets",
        input_column="ticket_body",
        output_column="summary",
    )
    attrs.update(overrides)
    return MammouthLLMComponent(**attrs)


def _materialize(component, df, monkeypatch):
    monkeypatch.setenv("MAMMOUTH_API_KEY", "test-key-123")
    defs = component.build_defs(None)
    upstream = make_upstream_asset("tickets", df)
    full_defs = dg.Definitions(assets=[*defs.assets, upstream])
    return full_defs.get_implicit_global_asset_job_def().execute_in_process()


def test_basic_per_row_inference_default_recommended_alias(monkeypatch):
    seen_models = []

    def fake_call(client, **kwargs):
        seen_models.append(kwargs["model"])
        prompt = kwargs["messages"][-1]["content"]
        return FakeResponse(f"summary of {prompt}")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"ticket_body": ["login broken", "billing issue"]})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("summaries"))
    assert list(out["summary"]) == ["summary of login broken", "summary of billing issue"]
    # default text_model is the 'mammouth-recommended' alias
    assert seen_models == ["mammouth-recommended", "mammouth-recommended"]


def test_explicit_underlying_model_name_passed_through(monkeypatch):
    seen_models = []

    def fake_call(client, **kwargs):
        seen_models.append(kwargs["model"])
        return FakeResponse("ok")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"ticket_body": ["x"]})
    comp = _component(text_model="claude-opus-5-5")
    _materialize(comp, df, monkeypatch)

    assert seen_models == ["claude-opus-5-5"]


def test_usage_tokens_tallied_into_metadata(monkeypatch):
    def fake_call(client, **kwargs):
        return FakeResponse("ok", prompt_tokens=20, completion_tokens=7)

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"ticket_body": ["a", "b"]})
    result = _materialize(_component(), df, monkeypatch)

    mats = result.asset_materializations_for_node("summaries")
    md = mats[-1].metadata
    assert md["prompt_tokens"].value == 40
    assert md["completion_tokens"].value == 14
    assert md["provider"].value == "Mammouth AI"


def test_response_format_and_top_p_passed_through(monkeypatch):
    seen = {}

    def fake_call(client, **kwargs):
        seen.update(kwargs)
        return FakeResponse("ok")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"ticket_body": ["x"]})
    comp = _component(response_format="json_object", top_p=0.9)
    _materialize(comp, df, monkeypatch)

    assert seen["response_format"] == {"type": "json_object"}
    assert seen["top_p"] == 0.9


def test_per_row_error_captured_without_failing_whole_asset(monkeypatch):
    def fake_call(client, **kwargs):
        prompt = kwargs["messages"][-1]["content"]
        if "bad" in prompt:
            raise RuntimeError("401 invalid_api_key")
        return FakeResponse("fine")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"ticket_body": ["bad row", "good row"]})
    comp = _component(max_retries=0)
    result = _materialize(comp, df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("summaries"))
    assert pd.isna(out["summary"].iloc[0])
    assert out["summary"].iloc[1] == "fine"
    assert "401" in out["summary_error"].iloc[0]


def test_429_rate_limit_retries_then_succeeds(monkeypatch):
    attempts = {"count": 0}

    def fake_call(client, **kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("Error code: 429 - rate limit exceeded")
        return FakeResponse("recovered")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)

    df = pd.DataFrame({"ticket_body": ["x"]})
    comp = _component(max_retries=5)
    result = _materialize(comp, df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("summaries"))
    assert out["summary"].iloc[0] == "recovered"
    assert attempts["count"] == 3


def test_429_rate_limit_exhausts_retries_captures_error(monkeypatch):
    def fake_call(client, **kwargs):
        raise RuntimeError("Error code: 429 - rate limit exceeded")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)

    df = pd.DataFrame({"ticket_body": ["x"]})
    comp = _component(max_retries=2)
    result = _materialize(comp, df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("summaries"))
    assert pd.isna(out["summary"].iloc[0])
    assert "429" in out["summary_error"].iloc[0]


def test_empty_upstream_short_circuits(monkeypatch):
    calls = []
    monkeypatch.setattr(mod, "_call_chat_completion", lambda client, **kw: calls.append(kw))

    df = pd.DataFrame({"ticket_body": []})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("summaries"))
    assert len(out) == 0
    assert "summary" in out.columns
    assert calls == []


def test_mutually_exclusive_input_fields_raise():
    comp = _component(user_prompt_template="hi {ticket_body}")
    with pytest.raises(ValueError, match="OR user_prompt_template, not both"):
        comp.build_defs(None)


def test_missing_input_fields_raise():
    comp = MammouthLLMComponent(asset_name="x", upstream_asset_key="y")
    with pytest.raises(ValueError, match="set input_column or user_prompt_template"):
        comp.build_defs(None)


def test_missing_api_key_raises(monkeypatch):
    monkeypatch.delenv("MAMMOUTH_API_KEY", raising=False)
    comp = _component()
    defs = comp.build_defs(None)
    upstream = make_upstream_asset("tickets", pd.DataFrame({"ticket_body": ["x"]}))
    full_defs = dg.Definitions(assets=[*defs.assets, upstream])
    result = full_defs.get_implicit_global_asset_job_def().execute_in_process(raise_on_error=False)
    assert not result.success


def test_partition_bridge_concats_dict_of_frames(monkeypatch):
    def fake_call(client, **kwargs):
        return FakeResponse("ok")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)
    monkeypatch.setenv("MAMMOUTH_API_KEY", "test-key-123")

    comp = _component()
    defs = comp.build_defs(None)
    asset_fn = list(defs.assets)[0]

    upstream_dict = {
        "p1": pd.DataFrame({"ticket_body": ["a"]}),
        "p2": pd.DataFrame({"ticket_body": ["b"]}),
    }
    out_df = asset_fn(dg.build_asset_context(), upstream_dict)
    assert len(out_df) == 2
    assert list(out_df["summary"]) == ["ok", "ok"]


def test_user_prompt_template_formats_row(monkeypatch):
    seen_prompts = []

    def fake_call(client, **kwargs):
        seen_prompts.append(kwargs["messages"][-1]["content"])
        return FakeResponse("ok")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"ticket_body": ["broken login"], "priority": ["high"]})
    comp = _component(
        input_column=None,
        user_prompt_template="[{priority}] {ticket_body}",
    )
    _materialize(comp, df, monkeypatch)

    assert seen_prompts == ["[high] broken login"]
