"""`{input}`: a first-class per-run value for a pipeline's prompts, read from a run tag
(`run_input_tag`, default 'agentic/input') -- the alternative to the dynamic-partition-per-
question workaround (sanitized/truncated partition keys, id-suffix collision avoidance).
Real `dg.materialize(..., tags=...)`; only litellm.completion is faked."""
import dagster as dg
import pytest


def _user_prompt(call):
    return next(m["content"] for m in call["messages"] if m["role"] == "user")


def _llm_step(id, tmpl, source=None):
    st = {"id": id, "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY", "prompt_template": tmpl}
    if source:
        st["source"] = source
    return st


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")


def test_input_tag_substitutes_into_the_prompt(mod, fake_litellm):
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "hi"},
        steps=[_llm_step("ask", "Answer this question: {input}")],
        outputs={"assets": ["ask"]},
    )
    fake_litellm.reply("42")
    result = dg.materialize(comp.build_defs(None).assets, tags={"agentic/input": "What is 6*7?"})
    assert result.success
    assert _user_prompt(fake_litellm.calls[0]) == "Answer this question: What is 6*7?"


def test_input_placeholder_left_unsubstituted_when_tag_is_not_set(mod, fake_litellm):
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "hi"},
        steps=[_llm_step("ask", "Answer this question: {input}")],
        outputs={"assets": ["ask"]},
    )
    fake_litellm.reply("ok")
    result = dg.materialize(comp.build_defs(None).assets)
    assert result.success
    assert _user_prompt(fake_litellm.calls[0]) == "Answer this question: {input}"


def test_custom_run_input_tag_name(mod, fake_litellm):
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "hi"},
        steps=[_llm_step("ask", "{input}")],
        outputs={"assets": ["ask"]},
        run_input_tag="my/custom_tag",
    )
    fake_litellm.reply("ok")
    result = dg.materialize(comp.build_defs(None).assets, tags={"my/custom_tag": "custom value"})
    assert result.success
    assert _user_prompt(fake_litellm.calls[0]) == "custom value"


def test_run_input_tag_disabled_with_none_leaves_input_placeholder_alone(mod, fake_litellm):
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "hi"},
        steps=[_llm_step("ask", "{input}")],
        outputs={"assets": ["ask"]},
        run_input_tag=None,
    )
    fake_litellm.reply("ok")
    result = dg.materialize(comp.build_defs(None).assets, tags={"agentic/input": "should not be read"})
    assert result.success
    assert _user_prompt(fake_litellm.calls[0]) == "{input}"


def test_input_and_partition_key_both_substitute_independently(mod, fake_litellm):
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "hi"},
        steps=[_llm_step("ask", "partition={partition_key} input={input}")],
        outputs={"assets": ["ask"]},
        partition_type="static", partition_values="a,b",
    )
    fake_litellm.reply("ok")
    result = dg.materialize(comp.build_defs(None).assets, tags={"agentic/input": "q1"}, partition_key="a")
    assert result.success
    assert _user_prompt(fake_litellm.calls[0]) == "partition=a input=q1"


def test_input_flows_downstream_through_a_second_step(mod, fake_litellm):
    """Confirms the per-step-dict substitution choke point (_run_step) applies {input}
    consistently to every step, not just the first one reading the raw run tags."""
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "hi"},
        steps=[_llm_step("a", "{input}"), _llm_step("b", "follow-up to: {text} re: {input}", source="a")],
        outputs={"assets": ["a", "b"]},
    )
    fake_litellm.reply("first answer")
    fake_litellm.reply("second answer")
    result = dg.materialize(comp.build_defs(None).assets, tags={"agentic/input": "the question"})
    assert result.success
    assert _user_prompt(fake_litellm.calls[1]) == "follow-up to: first answer re: the question"


def test_input_works_with_per_step_ops_too(mod, fake_litellm):
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "hi"},
        steps=[_llm_step("ask", "{input}")],
        outputs={"assets": ["ask"]},
        per_step_ops=True,
    )
    fake_litellm.reply("ok")
    result = dg.materialize(comp.build_defs(None).assets, tags={"agentic/input": "per-step-ops question"})
    assert result.success
    assert _user_prompt(fake_litellm.calls[0]) == "per-step-ops question"
