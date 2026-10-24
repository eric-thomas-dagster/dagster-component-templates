"""The MCP 1.x/2.x compat helpers are inlined (components are self-contained -- no shared module) into every
component that talks to an MCP server. This keeps the copies byte-identical so a fix lands everywhere."""
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent / "assets"
COMPONENTS = [
    "ai/mcp_tool_picker",
    "sources/mcp_tool_call",
    "ai/litellm_agent",
    "ai/openai_agent",
    "ai/anthropic_agent",
    "ai/gemini_agent",
    "ai/vercel_ai_gateway_agent",
    "ai/snowflake_cortex_agent",
]
BEGIN = "# --- mcp-compat-helpers:begin"
END = "# --- mcp-compat-helpers:end ---"


def _block(rel):
    src = (ROOT / rel / "component.py").read_text()
    assert BEGIN in src and END in src, f"{rel}: mcp-compat-helpers block missing"
    return src[src.index(BEGIN): src.index(END) + len(END)]


def test_mcp_compat_helper_copies_are_identical():
    blocks = {rel: _block(rel) for rel in COMPONENTS}
    ref_rel = COMPONENTS[0]
    for rel, blk in blocks.items():
        assert blk == blocks[ref_rel], f"{rel} drifted from {ref_rel}'s mcp-compat-helpers block"


@pytest.mark.parametrize("rel", COMPONENTS)
def test_no_raw_mcp_1x_only_calls_left(rel):
    src = (ROOT / rel / "component.py").read_text()
    outside = src[: src.index(BEGIN)]
    assert "inputSchema" not in outside.replace('_mcp_field(tool, "inputSchema", None)', "")
    assert 'getattr(call_result, "isError"' not in src
    assert "streamablehttp_client(url, headers" not in outside
