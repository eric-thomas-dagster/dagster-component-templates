"""Tests for MistralLLMComponent. Mocks the single isolated
`_call_chat_completion` function -- the real (paid) Mistral API is never
called."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, make_upstream_asset

mod = load_component_module()
MistralLLMComponent = mod.MistralLLMComponent


class FakeMessage:
    def __init__(self, content):
        self.content = content


class FakeChoice:
    def __init__(self, content):
        self.message = FakeMessage(content)


class FakeResponse:
    def __init__(self, content):
        self.choices = [FakeChoice(content)]


def _component(**overrides):
    attrs = dict(
        asset_name="summaries",
        upstream_asset_key="tickets",
        input_column="ticket_text",
        output_column="summary",
    )
    attrs.update(overrides)
    return MistralLLMComponent(**attrs)


def _materialize(component, df, monkeypatch, instance=None):
    monkeypatch.setenv("MISTRAL_API_KEY", "test-key-123")
    ctx = None
    defs = component.build_defs(ctx)
    upstream = make_upstream_asset("tickets", df)
    full_defs = dg.Definitions(assets=[*defs.assets, upstream])
    return full_defs.get_implicit_global_asset_job_def().execute_in_process(instance=instance)


def test_basic_per_row_inference(monkeypatch):
    calls = []

    def fake_call(client, **kwargs):
        calls.append(kwargs)
        prompt = kwargs["messages"][-1]["content"]
        return FakeResponse(f"summary of: {prompt}")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"ticket_text": ["printer is broken", "cannot log in"]})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("summaries"))
    assert list(out["summary"]) == [
        "summary of: printer is broken",
        "summary of: cannot log in",
    ]
    assert len(calls) == 2
    assert calls[0]["model"] == "mistral-large-latest"


def test_system_prompt_included(monkeypatch):
    seen = {}

    def fake_call(client, **kwargs):
        seen["messages"] = kwargs["messages"]
        return FakeResponse("ok")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"ticket_text": ["hello"]})
    comp = _component(system_prompt="You are terse.")
    _materialize(comp, df, monkeypatch)

    assert seen["messages"][0] == {"role": "system", "content": "You are terse."}
    assert seen["messages"][1] == {"role": "user", "content": "hello"}


def test_user_prompt_template(monkeypatch):
    seen = {}

    def fake_call(client, **kwargs):
        seen["messages"] = kwargs["messages"]
        return FakeResponse("ok")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"name": ["Ada"], "issue": ["password reset"]})
    comp = _component(
        input_column=None,
        user_prompt_template="User {name} needs help with: {issue}",
    )
    _materialize(comp, df, monkeypatch)

    assert seen["messages"][-1]["content"] == "User Ada needs help with: password reset"


def test_per_row_error_captured_without_failing_whole_asset(monkeypatch):
    def fake_call(client, **kwargs):
        prompt = kwargs["messages"][-1]["content"]
        if "bad" in prompt:
            raise RuntimeError("401 invalid_api_key")
        return FakeResponse("fine")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)

    df = pd.DataFrame({"ticket_text": ["bad row", "good row"]})
    comp = _component(max_retries=0)
    result = _materialize(comp, df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("summaries"))
    assert pd.isna(out["summary"].iloc[0])
    assert out["summary"].iloc[1] == "fine"
    assert "401" in out["summary_error"].iloc[0]
    assert pd.isna(out["summary_error"].iloc[1])


def test_empty_upstream_short_circuits(monkeypatch):
    calls = []
    monkeypatch.setattr(mod, "_call_chat_completion", lambda client, **kw: calls.append(kw))

    df = pd.DataFrame({"ticket_text": []})
    result = _materialize(_component(), df, monkeypatch)

    assert result.success
    out = result.asset_value(dg.AssetKey("summaries"))
    assert len(out) == 0
    assert "summary" in out.columns
    assert calls == []


def test_mutually_exclusive_input_fields_raise():
    comp = _component(user_prompt_template="hi {ticket_text}")
    ctx = None
    with pytest.raises(ValueError, match="OR user_prompt_template, not both"):
        comp.build_defs(ctx)


def test_missing_input_fields_raise():
    comp = MistralLLMComponent(asset_name="x", upstream_asset_key="y")
    ctx = None
    with pytest.raises(ValueError, match="set input_column or user_prompt_template"):
        comp.build_defs(ctx)


def test_missing_api_key_raises(monkeypatch):
    monkeypatch.delenv("MISTRAL_API_KEY", raising=False)
    ctx = None
    comp = _component()
    defs = comp.build_defs(ctx)
    upstream = make_upstream_asset("tickets", pd.DataFrame({"ticket_text": ["x"]}))
    full_defs = dg.Definitions(assets=[*defs.assets, upstream])
    result = full_defs.get_implicit_global_asset_job_def().execute_in_process(raise_on_error=False)
    assert not result.success


def test_partition_bridge_concats_dict_of_frames(monkeypatch):
    def fake_call(client, **kwargs):
        return FakeResponse("ok")

    monkeypatch.setattr(mod, "_call_chat_completion", fake_call)
    monkeypatch.setenv("MISTRAL_API_KEY", "test-key-123")

    ctx = None
    comp = _component()
    defs = comp.build_defs(ctx)
    asset_fn = list(defs.assets)[0]

    upstream_dict = {
        "p1": pd.DataFrame({"ticket_text": ["a"]}),
        "p2": pd.DataFrame({"ticket_text": ["b"]}),
    }
    out_df = asset_fn(dg.build_asset_context(), upstream_dict)
    assert len(out_df) == 2
    assert list(out_df["summary"]) == ["ok", "ok"]
