"""`op: group` -- deterministic grouping (no LLM, no invoke_component overhead) so a per-group label
step sees a group's FULL records together ({key, records: [...]}), not the parallel same-length
lists a groupby+list-aggregation (invoke_component + SummarizeComponent) produces. Unit coverage of
`_do_group` directly, plus one real end-to-end materialization through build_defs()."""
import json

import dagster as dg
import pytest

from .conftest import new_state


def test_group_groups_records_by_key_in_first_seen_order(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([
        {"account": "A1", "amount": 10}, {"account": "A2", "amount": 20},
        {"account": "A1", "amount": 30}, {"account": "A1", "amount": 40},
    ])
    result = mod._do_group({"id": "g", "op": "group", "key": "account"}, state, ctx)
    assert result["n_items"] == 2
    assert result["n_input_rows"] == 4
    groups = [r["item"] for r in result["items"]]
    assert [g["key"] for g in groups] == ["A1", "A2"]
    assert [r["amount"] for r in groups[0]["records"]] == [10, 30, 40]
    assert [r["amount"] for r in groups[1]["records"]] == [20]
    json.loads(result["text"])  # real JSON, no leftovers


def test_group_sort_key_orders_groups_ascending(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([{"account": "Z"}, {"account": "A"}, {"account": "M"}])
    result = mod._do_group({"id": "g", "op": "group", "key": "account", "sort": "key"}, state, ctx)
    assert [r["item"]["key"] for r in result["items"]] == ["A", "M", "Z"]


def test_group_non_string_key_value_still_groups_correctly(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([{"id": 1, "x": "a"}, {"id": 1, "x": "b"}, {"id": 2, "x": "c"}])
    result = mod._do_group({"id": "g", "op": "group", "key": "id"}, state, ctx)
    groups = [r["item"] for r in result["items"]]
    assert groups[0]["key"] == 1 and len(groups[0]["records"]) == 2
    assert groups[1]["key"] == 2 and len(groups[1]["records"]) == 1


def test_group_drops_records_missing_the_key_field_and_counts_them(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([{"account": "A1", "amount": 1}, {"amount": 2}])
    result = mod._do_group({"id": "g", "op": "group", "key": "account"}, state, ctx)
    assert result["n_items"] == 1
    assert result["n_missing_key"] == 1


def test_group_requires_key(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([{"id": 1}])
    with pytest.raises(ValueError, match="requires `key:`"):
        mod._do_group({"id": "g", "op": "group"}, state, ctx)


def test_group_rejects_a_bad_sort_value(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([{"id": 1}])
    with pytest.raises(ValueError, match="sort must be"):
        mod._do_group({"id": "g", "op": "group", "key": "id", "sort": "bogus"}, state, ctx)


def test_group_on_empty_input_returns_no_groups_without_raising(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = "[]"
    result = mod._do_group({"id": "g", "op": "group", "key": "account"}, state, ctx)
    assert result["n_items"] == 0 and result["items"] == []


def test_group_end_to_end_through_a_real_materialization(mod, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p",
        source={"kind": "literal", "text": '[{"account": "A1", "amount": 10}, {"account": "A2", "amount": 20}, {"account": "A1", "amount": 30}]'},
        steps=[{"id": "grouped", "op": "group", "key": "account"}],
        outputs={"assets": ["grouped"]},
    )
    defs = comp.build_defs(None)
    result = dg.materialize(defs.assets, instance=dg.DagsterInstance.ephemeral())
    assert result.success
    md = {k: v.value for e in result.get_asset_materialization_events() for k, v in e.materialization.metadata.items()}
    groups = json.loads(md["grouped__text"])
    assert groups == [
        {"key": "A1", "records": [{"account": "A1", "amount": 10}, {"account": "A1", "amount": 30}]},
        {"key": "A2", "records": [{"account": "A2", "amount": 20}]},
    ]


def test_group_output_composes_with_a_downstream_map_over_each_group(mod, ctx, fake_litellm, monkeypatch):
    """The headline use case: a group's records list reads directly via map's {item}."""
    from .conftest import tool_call
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([
        {"account": "A1", "amount": 10}, {"account": "A1", "amount": 30}, {"account": "A2", "amount": 20},
    ])
    group_out = mod._do_group({"id": "g", "op": "group", "key": "account"}, state, ctx)
    state["g"] = group_out
    fake_litellm.reply("A1 looks fine")
    fake_litellm.reply("A2 looks fine")
    map_step = {
        "id": "labels", "op": "map", "source": "g", "model": "gpt-4o-mini",
        "api_key_env_var": "OPENAI_API_KEY", "prompt_template": "Review this account's transactions: {item}",
    }
    map_out = mod._do_map(map_step, state, ctx)
    assert map_out["n_items"] == 2
    sent_prompt = fake_litellm.calls[0]["messages"][-1]["content"]
    assert '"key": "A1"' in sent_prompt and '"records"' in sent_prompt
