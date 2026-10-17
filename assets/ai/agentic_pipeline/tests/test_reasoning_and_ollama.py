"""<think> handling, Ollama routing and text tool-call recovery (Qwen3 / DeepSeek-R1 style local models)."""
import dagster as dg
import pytest

from .conftest import new_state, tool_call

TOOLS = [{"type": "function", "function": {"name": "f", "description": "", "parameters": {"type": "object", "properties": {}}}}]


@pytest.mark.parametrize("raw,answer,reasoning", [
    ("<think>plan it</think>Paris", "Paris", "plan it"),
    ("<think>a</think>X<think>b</think>Y", "XY", "a\n\nb"),
    ("<think>cut off mid thought", "", "cut off mid thought"),           # truncated by max_tokens: no answer yet
    ("template put the open tag in the prompt</think>Final", "Final", "template put the open tag in the prompt"),
    ("plain answer", "plain answer", None),
    ("", "", None),
])
def test_split_reasoning(mod, raw, answer, reasoning):
    assert mod._split_reasoning(raw) == (answer, reasoning)


def test_llm_call_strips_think_and_surfaces_reasoning(mod, fake_litellm, ctx):
    fake_litellm.reply("<think>France's capital is Paris</think>Paris")
    state = new_state()
    mod._run_step({"id": "a", "op": "llm_call", "model": "ollama/qwen3", "prompt_template": "{text}"}, state, ctx)
    out = state["a"]
    assert out["text"] == "Paris"                       # downstream JSON parsing / routing never sees the thinking
    assert out["reasoning"] == [{"model": "ollama/qwen3", "text": "France's capital is Paris"}]
    assert out["reasoning_chars"] == len("France's capital is Paris")


def test_provider_supplied_reasoning_content_is_kept(mod, fake_litellm, ctx):
    fake_litellm.reply("Answer", reasoning_content="provider side trace")
    state = new_state()
    mod._run_step({"id": "a", "op": "llm_call", "model": "deepseek/deepseek-reasoner", "prompt_template": "{text}"}, state, ctx)
    assert state["a"]["reasoning"][0]["text"] == "provider side trace"


def test_no_reasoning_key_when_model_does_not_think(mod, fake_litellm, ctx):
    fake_litellm.reply("Paris")
    state = new_state()
    mod._run_step({"id": "a", "op": "llm_call", "model": "gpt-4o-mini", "prompt_template": "{text}"}, state, ctx)
    assert "reasoning" not in state["a"] and "policy_usage" not in state["a"]


def test_ollama_with_tools_uses_chat_route_and_auto_tool_choice(mod, fake_litellm):
    fake_litellm.reply("done")
    mod._completion(model="ollama/qwen3:8b", system_prompt=None, user_prompt="hi", api_key_env_var=None, api_base_env_var=None,
                    temperature=0.0, max_tokens=64, tools=TOOLS, tool_choice="required")
    kw = fake_litellm.calls[-1]
    assert kw["model"] == "ollama_chat/qwen3:8b"      # /api/chat supports tools; /api/generate does not
    assert kw["tool_choice"] == "auto"                 # Ollama's chat API has no 'required'


def test_ollama_without_tools_is_unchanged(mod, fake_litellm):
    fake_litellm.reply("done")
    mod._completion(model="ollama/qwen3:8b", system_prompt=None, user_prompt="hi", api_key_env_var=None, api_base_env_var=None,
                    temperature=0.0, max_tokens=64)
    assert fake_litellm.calls[-1]["model"] == "ollama/qwen3:8b"


def test_non_ollama_keeps_required_tool_choice(mod, fake_litellm):
    fake_litellm.reply("done")
    mod._completion(model="gpt-4o-mini", system_prompt=None, user_prompt="hi", api_key_env_var=None, api_base_env_var=None,
                    temperature=0.0, max_tokens=64, tools=TOOLS, tool_choice="required")
    assert fake_litellm.calls[-1]["tool_choice"] == "required"


def test_text_tool_call_is_recovered(mod, fake_litellm):
    fake_litellm.reply('Let me check.<tool_call>{"name": "pick_1", "arguments": {"reasoning": "best"}}</tool_call>')
    out = mod._completion(model="ollama/qwen3", system_prompt=None, user_prompt="x", api_key_env_var=None,
                          api_base_env_var=None, temperature=0.0, max_tokens=64, tools=TOOLS)
    assert out["tool_calls"] == [{"name": "pick_1", "arguments": '{"reasoning": "best"}'}]
    assert "<tool_call>" not in out["content"]


def test_structured_tool_calls_win_over_text(mod, fake_litellm):
    fake_litellm.reply("", tool_calls=[tool_call("real", {"a": 1})])
    out = mod._completion(model="gpt-4o-mini", system_prompt=None, user_prompt="x", api_key_env_var=None,
                          api_base_env_var=None, temperature=0.0, max_tokens=64, tools=TOOLS)
    assert [c["name"] for c in out["tool_calls"]] == ["real"]


def test_reasoning_reaches_dagster_metadata_end_to_end(mod, fake_litellm):
    fake_litellm.reply("<think>step by step</think>42")
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="qwen",
        source={"kind": "literal", "text": "meaning of life?"},
        steps=[{"id": "answer", "op": "llm_call", "model": "ollama/qwen3", "prompt_template": "{text}"}],
        outputs={"assets": ["answer"]},
    )
    result = dg.materialize(comp.build_defs(None).assets, raise_on_error=False)
    assert result.success
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["answer__text"].value == "42"
    assert "step by step" in md["answer__reasoning"].value
    assert "answer__policy_usage" not in md                # no policy configured -> no policy noise
