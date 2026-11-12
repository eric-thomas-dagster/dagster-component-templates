"""`op: filter` -- a deterministic row filter (no LLM, no invoke_component overhead) so "label, keep the matching
ones, ask a person" doesn't need to reach for invoke_component just to filter rows. Same `condition` syntax
(pandas `.query()`) as the standalone FilterComponent. Unit coverage of `_do_filter` directly, plus one real
end-to-end materialization through build_defs()."""
import json

import dagster as dg
import pytest

from .conftest import new_state


def test_filter_keeps_matching_rows_and_output_is_json_safe(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([{"id": 1, "amount": 100}, {"id": 2, "amount": 5}, {"id": 3, "amount": 200}])
    result = mod._do_filter({"id": "f", "op": "filter", "condition": "amount > 10"}, state, ctx)
    assert result["n_items"] == 2
    assert result["n_input_rows"] == 3
    kept_ids = {r["item"]["id"] for r in result["items"]}
    assert kept_ids == {1, 3}
    # round-trips as real JSON (no numpy/pandas-typed leftovers)
    json.loads(result["text"])


def test_filter_negate_inverts_the_match(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([{"id": 1, "amount": 100}, {"id": 2, "amount": 5}])
    result = mod._do_filter({"id": "f", "op": "filter", "condition": "amount > 10", "negate": True}, state, ctx)
    assert [r["item"]["id"] for r in result["items"]] == [2]


def test_filter_requires_condition(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([{"id": 1}])
    with pytest.raises(ValueError, match="requires `condition:`"):
        mod._do_filter({"id": "f", "op": "filter"}, state, ctx)


def test_filter_on_empty_input_returns_no_rows_without_raising(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = "[]"
    result = mod._do_filter({"id": "f", "op": "filter", "condition": "amount > 10"}, state, ctx)
    assert result["n_items"] == 0 and result["items"] == []


def test_filter_bad_condition_raises_a_clear_error(mod, ctx):
    state = new_state(text="hello")
    state["source"]["text"] = json.dumps([{"id": 1, "amount": 100}])
    with pytest.raises(ValueError, match="filter: condition"):
        mod._do_filter({"id": "f", "op": "filter", "condition": "not valid python &&"}, state, ctx)


def test_filter_end_to_end_through_a_real_materialization(mod, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p",
        source={"kind": "literal", "text": '[{"id": 1, "amount": 100}, {"id": 2, "amount": 5}]'},
        steps=[{"id": "kept", "op": "filter", "condition": "amount > 10"}],
        outputs={"assets": ["kept"]},
    )
    defs = comp.build_defs(None)
    result = dg.materialize(defs.assets, instance=dg.DagsterInstance.ephemeral())
    assert result.success
    md = {k: v.value for e in result.get_asset_materialization_events() for k, v in e.materialization.metadata.items()}
    assert json.loads(md["kept__text"]) == [{"id": 1, "amount": 100}]
