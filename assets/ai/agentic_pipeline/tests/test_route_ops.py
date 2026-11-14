"""`op: route` / `op: conditional_route` -- batch-level by default (one router/condition decision
for the WHOLE source text); `per_item: true` fans out a fresh decision + specialist answer for
each item of a list source instead, reusing the same list-parsing rule as `map`/`invoke_component`.
"""
import json

import pytest

from .conftest import new_state, tool_call


def run(mod, ctx, step, state=None):
    state = state if state is not None else new_state()
    mod._run_step(step, state, ctx)
    return state[step["id"]], state


SPECIALISTS = [
    {"name": "shipping", "description": "shipping questions", "model": "m"},
    {"name": "returns", "description": "return/refund questions", "model": "m"},
    {"name": "account", "description": "login/account questions", "model": "m"},
]


def route_step(**over):
    base = {
        "id": "triage", "op": "route",
        "router": {"model": "router-model"},
        "specialists": SPECIALISTS,
        "per_item": True,
    }
    base.update(over)
    return base


# ── route: per_item ───────────────────────────────────────────────────────────

def test_per_item_routes_three_items_to_three_specialists_in_order(mod, fake_litellm, ctx):
    items = ["when will my order ship", "my order arrived damaged", "I can't log in"]
    state = new_state(text=json.dumps(items))
    # router then specialist, per item, in order (sequential default: max_concurrent=1)
    fake_litellm.reply("", tool_calls=[tool_call("shipping", {"reasoning": "shipping question"})])
    fake_litellm.reply("ETA is Friday.")
    fake_litellm.reply("", tool_calls=[tool_call("returns", {"reasoning": "damaged order"})])
    fake_litellm.reply("Starting a return for you.")
    fake_litellm.reply("", tool_calls=[tool_call("account", {"reasoning": "login issue"})])
    fake_litellm.reply("Let's reset your password.")

    out, _ = run(mod, ctx, route_step(), state)
    assert out["n_items"] == 3
    assert [r["selected_specialist"] for r in out["items"]] == ["shipping", "returns", "account"]
    assert [r["item"] for r in out["items"]] == items
    assert out["routing_counts"] == {"shipping": 1, "returns": 1, "account": 1}
    assert out["n_fallbacks"] == 0
    assert out["n_llm_calls"] == 6  # router + specialist, per item


def test_per_item_omitted_or_false_behaves_exactly_as_before(mod, fake_litellm, ctx):
    fake_litellm.reply("", tool_calls=[tool_call("shipping", {"reasoning": "shipping question"})])
    fake_litellm.reply("ETA is Friday.")
    step = {"id": "triage", "op": "route", "router": {"model": "router-model"}, "specialists": SPECIALISTS}
    out, _ = run(mod, ctx, step, new_state(text="when will my order ship"))
    assert out["selected_specialist"] == "shipping"
    assert out["router_reasoning"] == "shipping question"
    assert out["routing_source"] == "router_tool_call"
    assert out["router_model"] == "router-model"
    assert out["specialist_model"] == "m"
    assert out["n_llm_calls"] == 2
    assert "items" not in out and "routing_counts" not in out and "n_items" not in out and "n_fallbacks" not in out

    fake_litellm.reply("", tool_calls=[tool_call("shipping", {"reasoning": "shipping question"})])
    fake_litellm.reply("ETA is Friday.")
    out2, _ = run(mod, ctx, route_step(per_item=False), new_state(text="when will my order ship"))
    assert out2["selected_specialist"] == "shipping" and out2["n_llm_calls"] == 2
    assert "items" not in out2 and "routing_counts" not in out2


def test_per_item_falls_back_when_the_router_picks_an_invalid_specialist(mod, fake_litellm, ctx):
    items = ["a", "b"]
    state = new_state(text=json.dumps(items))
    fake_litellm.reply("", tool_calls=[tool_call("shipping", {"reasoning": "ok"})])
    fake_litellm.reply("handled a")
    fake_litellm.reply("", tool_calls=[tool_call("not_a_real_specialist", {"reasoning": "??"})])
    fake_litellm.reply("handled b via fallback")
    out, _ = run(mod, ctx, route_step(fallback="account"), state)
    assert [r["selected_specialist"] for r in out["items"]] == ["shipping", "account"]
    assert [r["routing_source"] for r in out["items"]] == ["router_tool_call", "fallback"]
    assert out["n_fallbacks"] == 1
    assert out["routing_counts"] == {"shipping": 1, "account": 1}


def test_per_item_error_names_the_item_when_the_router_fails_with_no_fallback(mod, fake_litellm, ctx):
    items = ["a", "b"]
    state = new_state(text=json.dumps(items))
    fake_litellm.reply("", tool_calls=[tool_call("shipping", {"reasoning": "ok"})])
    fake_litellm.reply("handled a")
    fake_litellm.reply("", tool_calls=[tool_call("not_a_real_specialist", {"reasoning": "??"})])
    with pytest.raises(RuntimeError, match=r"item 'b'"):
        run(mod, ctx, route_step(fallback=None), state)


def test_per_item_max_concurrent_processes_every_item_in_order(mod, fake_litellm, ctx):
    items = ["a", "b", "c"]
    state = new_state(text=json.dumps(items))
    for _ in items:
        fake_litellm.reply("", tool_calls=[tool_call("shipping", {"reasoning": "x"})])
        fake_litellm.reply("handled")
    out, _ = run(mod, ctx, route_step(max_concurrent=3), state)
    # result slots are written positionally by the main thread (ThreadPoolExecutor.map preserves
    # input order), so this holds regardless of which worker thread actually finished first.
    assert [r["item"] for r in out["items"]] == items
    assert out["routing_counts"] == {"shipping": 3}
    assert out["n_llm_calls"] == 6


def test_per_item_output_join_records_round_trips_through_a_downstream_synthesize(mod, fake_litellm, ctx):
    """route(per_item) after a filter, then a synthesize reading the records -- the shape a real
    'triage a batch of tickets' pipeline uses."""
    tickets = [
        {"id": 1, "text": "when will my order ship", "urgent": True},
        {"id": 2, "text": "fyi loved the packaging", "urgent": False},
        {"id": 3, "text": "my order arrived damaged", "urgent": True},
    ]
    state = new_state(text=json.dumps(tickets))
    mod._run_step({"id": "urgent_only", "op": "filter", "source": "source", "condition": "urgent == True"}, state, ctx)
    assert state["urgent_only"]["n_items"] == 2

    fake_litellm.reply("", tool_calls=[tool_call("shipping", {"reasoning": "shipping question"})])
    fake_litellm.reply("ETA is Friday.")
    fake_litellm.reply("", tool_calls=[tool_call("returns", {"reasoning": "damaged order"})])
    fake_litellm.reply("Starting a return for you.")
    mod._run_step(route_step(source="urgent_only", output_join="records"), state, ctx)
    records = json.loads(state["triage"]["text"])
    assert [r["id"] for r in records] == [1, 3]
    assert [r["selected_specialist"] for r in records] == ["shipping", "returns"]
    assert all("text" in r and "router_reasoning" in r for r in records)

    fake_litellm.reply("Both urgent tickets are being handled: shipping ETA and a return.")
    mod._run_step({"id": "digest", "op": "synthesize", "sources": ["triage"], "model": "m"}, state, ctx)
    assert "handled" in state["digest"]["text"].lower()
    sent_prompt = fake_litellm.calls[-1]["messages"][-1]["content"]
    assert "shipping" in sent_prompt and "returns" in sent_prompt  # the records reached the model


# ── conditional_route: per_item ────────────────────────────────────────────────

def test_conditional_route_per_item_picks_by_each_items_own_field_value(mod, fake_litellm, ctx):
    items = [{"id": 1, "topic": "billing"}, {"id": 2, "topic": "shipping"}, {"id": 3, "topic": "other"}]
    state = new_state(text=json.dumps(items))
    step = {
        "id": "triage", "op": "conditional_route", "per_item": True,
        "conditions": [
            {"when": {"jsonpath": "$.topic", "value_equals": "billing"}, "then": "billing_team"},
            {"when": {"jsonpath": "$.topic", "value_equals": "shipping"}, "then": "shipping_team"},
        ],
        "default": "general_team",
        "specialists": [
            {"name": "billing_team", "description": "billing", "model": "m"},
            {"name": "shipping_team", "description": "shipping", "model": "m"},
            {"name": "general_team", "description": "everything else", "model": "m"},
        ],
    }
    fake_litellm.reply("billing reply")
    fake_litellm.reply("shipping reply")
    fake_litellm.reply("general reply")
    out, _ = run(mod, ctx, step, state)
    assert [r["selected_specialist"] for r in out["items"]] == ["billing_team", "shipping_team", "general_team"]
    assert [r["routing_source"] for r in out["items"]] == ["condition_match", "condition_match", "default"]
    assert out["n_defaulted"] == 1
    assert out["routing_counts"] == {"billing_team": 1, "shipping_team": 1, "general_team": 1}
    assert out["n_llm_calls"] == 3  # no router call for conditional_route -- one specialist call per item
