"""`per_item` on `mcp_call`: calls the tool once per item in a list source instead of once for
the whole batch -- the "file one thing per finding" pattern (a pipeline that decides on 0-N
findings upstream, then must call a filing tool once per finding, not once for the whole batch
as a single blob). Only the low-level MCP call (`_call_mcp_tool_async`) is faked, matching this
test suite's existing convention (see test_mcp_server_resource_key.py's `mcp_call_capture`)."""
import json

import pytest

from .conftest import new_state

SERVER = {"name": "mini", "type": "stdio", "command": ["x"]}


@pytest.fixture
def mcp_call_capture(mod, monkeypatch):
    """Scripted _call_mcp_tool_async: records every call's args; raises for any item whose
    resolved args contain "boom" (simulating one failing call among others)."""
    seen = []

    async def call(**kw):
        seen.append(kw["tool_args"])
        if "boom" in json.dumps(kw["tool_args"]):
            raise RuntimeError("tool crashed")
        return {"raw": json.dumps({"ok": kw["tool_args"].get("item")}), "value": {"ok": kw["tool_args"].get("item")},
                "kind": "json", "is_error": False, "content_parts": []}

    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    return seen


def test_per_item_calls_tool_once_per_item_in_order(mod, ctx, mcp_call_capture):
    state = new_state(text=json.dumps(["a", "b", "c"]))
    step = {
        "id": "filed", "op": "mcp_call", "server": SERVER, "mcp_tool_name": "open_issue",
        "tool_args": {"item": "{item}"}, "per_item": True,
    }
    out = mod._do_mcp_call(step, state, ctx)

    assert out["n_items"] == 3
    assert out["n_errors"] == 0
    assert [c["item"] for c in mcp_call_capture] == ["a", "b", "c"]
    assert [i["item"] for i in out["items"]] == ["a", "b", "c"]
    assert all(i["is_error"] is False for i in out["items"])


def test_per_item_empty_list_makes_zero_calls(mod, ctx, mcp_call_capture):
    state = new_state(text="")
    step = {
        "id": "filed", "op": "mcp_call", "server": SERVER, "mcp_tool_name": "open_issue",
        "tool_args": {"item": "{item}"}, "per_item": True,
    }
    out = mod._do_mcp_call(step, state, ctx)

    assert out["n_items"] == 0
    assert out["n_errors"] == 0
    assert out["items"] == []
    assert out["text"] == ""
    assert mcp_call_capture == []  # zero calls, not an error


def test_per_item_one_failure_does_not_abort_the_others(mod, ctx, mcp_call_capture):
    state = new_state(text=json.dumps(["a", "boom", "c"]))
    step = {
        "id": "filed", "op": "mcp_call", "server": SERVER, "mcp_tool_name": "open_issue",
        "tool_args": {"item": "{item}"}, "per_item": True,
    }
    out = mod._do_mcp_call(step, state, ctx)

    assert out["n_items"] == 3
    assert out["n_errors"] == 1
    # all three items were attempted, in order, despite the middle one raising
    assert [c["item"] for c in mcp_call_capture] == ["a", "boom", "c"]
    assert out["items"][0]["is_error"] is False
    assert out["items"][1]["is_error"] is True
    assert "tool crashed" in out["items"][1]["error"]
    assert out["items"][2]["is_error"] is False


def test_per_item_object_field_access(mod, ctx, mcp_call_capture):
    """{item.field} looks up a key when the item is a JSON object -- the Hooli-reported gap:
    findings are objects ({"title": ..., "body": ...}) and the filing tool wants title/body as
    separate args, not the whole item as one blob."""
    state = new_state(text=json.dumps([{"title": "a", "body": "b"}]))
    step = {
        "id": "filed", "op": "mcp_call", "server": SERVER, "mcp_tool_name": "open_issue",
        "tool_args": {"title": "{item.title}", "body": "{item.body}"}, "per_item": True,
    }
    out = mod._do_mcp_call(step, state, ctx)

    assert out["n_items"] == 1
    assert out["n_errors"] == 0
    assert mcp_call_capture == [{"title": "a", "body": "b"}]


def test_per_item_object_field_missing_is_isolated_per_item(mod, ctx, mcp_call_capture):
    """A missing field on one item is that item's own error, not a step-wide failure --
    matches the existing per-item failure-isolation behavior for a raising tool call."""
    state = new_state(text=json.dumps([{"title": "a", "body": "b"}, {"title": "c"}]))
    step = {
        "id": "filed", "op": "mcp_call", "server": SERVER, "mcp_tool_name": "open_issue",
        "tool_args": {"title": "{item.title}", "body": "{item.body}"}, "per_item": True,
    }
    out = mod._do_mcp_call(step, state, ctx)

    assert out["n_items"] == 2
    assert out["n_errors"] == 1
    assert out["items"][0]["is_error"] is False
    assert out["items"][1]["is_error"] is True
    assert "body" in out["items"][1]["error"]
    # the first item was still filed despite the second one's missing field
    assert mcp_call_capture == [{"title": "a", "body": "b"}]


def test_per_item_whole_item_still_works_alongside_field_access(mod, ctx, mcp_call_capture):
    """{item} (whole item, JSON-stringified) keeps working when used alongside {item.field}."""
    state = new_state(text=json.dumps([{"title": "a", "body": "b"}]))
    step = {
        "id": "filed", "op": "mcp_call", "server": SERVER, "mcp_tool_name": "open_issue",
        "tool_args": {"title": "{item.title}", "raw": "{item}"}, "per_item": True,
    }
    out = mod._do_mcp_call(step, state, ctx)

    assert out["n_errors"] == 0
    assert mcp_call_capture[0]["title"] == "a"
    assert json.loads(mcp_call_capture[0]["raw"]) == {"title": "a", "body": "b"}


def test_per_item_source_from_fenced_llm_json(mod, ctx, mcp_call_capture):
    """A free-text llm_call step commonly wraps its JSON findings list in a ```json fence
    (no output_schema/forced tool-calling on llm_call, unlike map/extract) -- per_item must
    still parse it as a list, not fall back to a one-item newline split."""
    llm_text = (
        "Here are the findings I found:\n\n```json\n"
        '[{"title": "a", "body": "b"}, {"title": "c", "body": "d"}]\n'
        "```\n\nLet me know if you need more."
    )
    state = new_state(text=llm_text)
    step = {
        "id": "filed", "op": "mcp_call", "server": SERVER, "mcp_tool_name": "open_issue",
        "tool_args": {"title": "{item.title}", "body": "{item.body}"}, "per_item": True,
    }
    out = mod._do_mcp_call(step, state, ctx)

    assert out["n_items"] == 2
    assert out["n_errors"] == 0
    assert mcp_call_capture == [{"title": "a", "body": "b"}, {"title": "c", "body": "d"}]


def test_non_per_item_mcp_call_is_unchanged(mod, ctx, mcp_call_capture):
    """per_item defaults to False -- the original single-call shape still works."""
    state = new_state(text="hello")
    step = {
        "id": "a", "op": "mcp_call", "server": SERVER, "mcp_tool_name": "open_issue",
        "tool_args": {"note": "{text}"},
    }
    out = mod._do_mcp_call(step, state, ctx)

    assert len(mcp_call_capture) == 1
    assert mcp_call_capture[0] == {"note": "hello"}
    assert "n_items" not in out
    assert out["is_error"] is False
