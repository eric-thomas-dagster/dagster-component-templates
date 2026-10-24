"""<think> stripping, ollama routing, text <tool_call> recovery and opt-in include_reasoning for litellm_agent.

A REAL stdio MCP server (FastMCP) provides the tool and the REAL component is materialized; only the model call
(litellm.acompletion) is faked."""
import inspect
import sys
import textwrap
import types

import dagster as dg
import pytest

from .conftest import load_component_module

pytest.importorskip("mcp")

SERVER = textwrap.dedent('''
    try:
        from mcp.server.mcpserver import MCPServer as _Server  # mcp 2.x renamed FastMCP
    except ImportError:
        from mcp.server.fastmcp import FastMCP as _Server  # mcp 1.x
    mcp = _Server("tools")

    @mcp.tool()
    def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    mcp.run(transport="stdio")
''')


class _Msg:
    def __init__(self, content, tool_calls=None):
        self.content, self.tool_calls = content, tool_calls

    def model_dump(self, exclude_none=False):
        d = {"role": "assistant", "content": self.content, "tool_calls": self.tool_calls}
        return {k: v for k, v in d.items() if v is not None} if exclude_none else d


def _run(monkeypatch, tmp_path, replies, model="ollama/qwen3", **extra):
    script = tmp_path / "server.py"
    script.write_text(SERVER)
    calls = []
    queue = list(replies)

    async def acompletion(messages, **kw):
        calls.append({"messages": [dict(m) for m in messages], **kw})
        return types.SimpleNamespace(choices=[types.SimpleNamespace(message=_Msg(queue.pop(0)))])

    fake = types.ModuleType("litellm")
    fake.acompletion = acompletion
    monkeypatch.setitem(sys.modules, "litellm", fake)
    mod = load_component_module()
    cls = next(c for c in vars(mod).values() if inspect.isclass(c) and issubclass(c, dg.Component)
               and c is not dg.Component and c.__module__ == mod.__name__)
    comp = cls(asset_name="agent_out", prompt="what is 2+3?", model=model,
               mcp_servers=[{"name": "tools", "type": "stdio", "command": [sys.executable, str(script)]}], **extra)
    res = dg.materialize(comp.build_defs(None).assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    mats = res.get_asset_materialization_events()
    return res.output_for_node("agent_out"), calls, mats[0].materialization.metadata


CALL = '<tool_call>{"name": "tools__add", "arguments": {"a": 2, "b": 3}}</tool_call>'
REPLIES = [f"<think>need the add tool</think>{CALL}", "<think>sum is 5</think>The sum is 5."]


def test_text_tool_call_is_executed_and_thinking_never_reaches_the_transcript(monkeypatch, tmp_path):
    out, calls, md = _run(monkeypatch, tmp_path, REPLIES)
    assert out["final_answer"] == "The sum is 5."
    assert out["tool_calls_made"] == 1 and out["stopped_reason"] == "final_answer"
    assert out["tool_call_details"][0]["result_preview"].strip() == "5"
    assert "<think>" not in str(out["transcript"]) and "<tool_call>" not in str(out["transcript"])
    assert "reasoning" not in out and "reasoning" not in md            # default output unchanged
    assert calls[0]["model"] == "ollama_chat/qwen3"                    # /api/generate has no tool support
    # the second turn carries a well-formed assistant tool_calls entry + the tool result
    second = calls[1]["messages"]
    assert second[-2]["tool_calls"][0]["function"]["name"] == "tools__add"
    assert second[-1]["role"] == "tool"


def test_non_ollama_model_name_is_untouched_and_plain_reply_works(monkeypatch, tmp_path):
    out, calls, _ = _run(monkeypatch, tmp_path, ["no tools needed"], model="gpt-4o-mini")
    assert out["final_answer"] == "no tools needed" and out["tool_calls_made"] == 0
    assert calls[0]["model"] == "gpt-4o-mini"


def test_include_reasoning(monkeypatch, tmp_path):
    out, _, md = _run(monkeypatch, tmp_path, REPLIES, include_reasoning=True)
    assert out["reasoning"] == "need the add tool\n\nsum is 5"
    assert "need the add tool" in md["reasoning"].value
