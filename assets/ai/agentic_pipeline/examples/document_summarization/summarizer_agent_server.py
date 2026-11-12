#!/usr/bin/env python3
"""Minimal local MCP stdio server exposing one tool: summarize_document.

This IS the "summarizer agent" -- an LLM call wrapped behind an MCP tool
with its own persona/system prompt, same pattern as the triage agent in
../support_ticket_triage/triage_mcp_server.py. Forced structured output
(summary + action_items) rather than free text, so downstream steps or
asset metadata get a real list to act on, not prose to re-parse.

Works on mcp 1.x and 2.x (same compat shim this repo's own MCP server
components use -- FastMCP was renamed MCPServer in 2.x).
"""
import json
import os

try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
    from mcp.server.mcpserver import MCPServer as _Server
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server

SUMMARIZER_SYSTEM_PROMPT = """You are an executive assistant. Given a \
document, produce a short executive summary (2-4 sentences, no jargon, \
written for someone who has not read the document) and a list of concrete \
action items extracted from it.

Call the `summarize` tool with your result."""

SUMMARIZE_TOOL = {
    "type": "function",
    "function": {
        "name": "summarize",
        "description": "Record the executive summary and action items.",
        "parameters": {
            "type": "object",
            "properties": {
                "summary": {"type": "string"},
                "action_items": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["summary", "action_items"],
        },
    },
}

mcp = _Server("summarizer-mcp")


@mcp.tool()
def summarize_document(document_text: str) -> dict:
    """Produce an executive summary + action items for a document."""
    import litellm

    resp = litellm.completion(
        model="gpt-4o-mini",
        api_key=os.environ["OPENAI_API_KEY"],
        messages=[
            {"role": "system", "content": SUMMARIZER_SYSTEM_PROMPT},
            {"role": "user", "content": document_text},
        ],
        tools=[SUMMARIZE_TOOL],
        tool_choice="required",
        temperature=0.0,
    )
    tool_call = resp.choices[0].message.tool_calls[0]
    return json.loads(tool_call.function.arguments)


if __name__ == "__main__":
    mcp.run(transport="stdio")
