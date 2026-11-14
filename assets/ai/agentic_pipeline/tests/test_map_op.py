"""`op: map` strict mode -- a single item missing a required field used to fail the WHOLE step
(RuntimeError naming that item); `on_missing_field` adds retry_once/default/skip options so one
bad item doesn't have to take down an otherwise-good batch. `strict`'s old True/False meaning is
unchanged when `on_missing_field` is left unset."""
import json

import pytest

from .conftest import new_state, tool_call


def run(mod, ctx, step, state=None):
    state = state if state is not None else new_state()
    mod._run_step(step, state, ctx)
    return state[step["id"]], state


SCHEMA = {"type": "object", "properties": {"urgent": {"type": "boolean"}}, "required": ["urgent"]}


def map_step(**over):
    base = {"id": "extracted", "op": "map", "model": "m", "prompt_template": "{item}", "output_schema": SCHEMA}
    base.update(over)
    return base


def test_strict_default_fails_the_whole_step_naming_the_item(mod, fake_litellm, ctx):
    state = new_state(text=json.dumps(["a", "b"]))
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {"urgent": True})])
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {})])  # missing 'urgent'
    with pytest.raises(RuntimeError, match=r"map \(fail\): missing required fields \['urgent'\] .* item 'b'"):
        run(mod, ctx, map_step(), state)


def test_strict_false_without_on_missing_field_keeps_the_old_passthrough(mod, fake_litellm, ctx):
    state = new_state(text=json.dumps(["a"]))
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {})])  # missing 'urgent'
    out, _ = run(mod, ctx, map_step(strict=False), state)
    assert out["items"][0]["extracted"] == {}  # kept as-is, no fill, no raise
    assert out["n_skipped"] == 0


def test_on_missing_field_default_fills_a_type_based_default(mod, fake_litellm, ctx):
    state = new_state(text=json.dumps(["a", "b"]))
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {"urgent": True})])
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {})])  # missing 'urgent'
    out, _ = run(mod, ctx, map_step(on_missing_field="default"), state)
    assert [r["extracted"]["urgent"] for r in out["items"]] == [True, False]
    assert out["n_skipped"] == 0
    assert out["n_llm_calls"] == 2  # no retry in this mode


def test_on_missing_field_default_prefers_the_schemas_own_default(mod, fake_litellm, ctx):
    schema = {"type": "object", "properties": {"priority": {"type": "string", "default": "normal"}}, "required": ["priority"]}
    state = new_state(text=json.dumps(["a"]))
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {})])
    out, _ = run(mod, ctx, map_step(output_schema=schema, on_missing_field="default"), state)
    assert out["items"][0]["extracted"]["priority"] == "normal"


def test_on_missing_field_retry_once_succeeds_on_the_retry(mod, fake_litellm, ctx):
    state = new_state(text=json.dumps(["a"]))
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {})])          # first try: missing
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {"urgent": True})])  # retry: ok
    out, _ = run(mod, ctx, map_step(on_missing_field="retry_once"), state)
    assert out["items"][0]["extracted"] == {"urgent": True}
    assert out["n_llm_calls"] == 2  # the original call + one retry, for this one item


def test_on_missing_field_retry_once_still_raises_if_the_retry_is_also_missing(mod, fake_litellm, ctx):
    state = new_state(text=json.dumps(["a"]))
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {})])
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {})])
    with pytest.raises(RuntimeError, match=r"map \(retry_once\): missing required fields \['urgent'\] .* item 'a' even after one retry"):
        run(mod, ctx, map_step(on_missing_field="retry_once"), state)


def test_on_missing_field_skip_drops_the_item_but_keeps_the_rest(mod, fake_litellm, ctx):
    state = new_state(text=json.dumps(["a", "b", "c"]))
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {"urgent": True})])
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {})])  # 'b': missing -> skipped
    fake_litellm.reply("", tool_calls=[tool_call("extract_data", {"urgent": False})])
    out, _ = run(mod, ctx, map_step(on_missing_field="skip", output_join="records"), state)
    assert out["n_items"] == 3
    assert out["n_skipped"] == 1
    assert out["items"][1] is None
    records = json.loads(out["text"])
    assert [r["item"] for r in records] == ["a", "c"]  # 'b' absent from the joined output too


def test_on_missing_field_rejects_an_unknown_value(mod, ctx):
    state = new_state(text=json.dumps(["a"]))
    with pytest.raises(ValueError, match="on_missing_field must be"):
        run(mod, ctx, map_step(on_missing_field="bogus"), state)
