#!/usr/bin/env python3
"""Minimal local MCP stdio server exposing one tool: triage_ticket.

This IS the "agent" -- not a separate company's hosted infrastructure, just
an LLM call wrapped behind an MCP tool with its own system prompt/persona
("context given to an LLM to make it act like a specialist"). This is the
realistic way to stand up your own agent for `delegate` to call: any team
can wrap a prompt like this behind MCP, register it as an AgentCardComponent,
and it becomes callable exactly like a hosted third-party agent would be --
no external service required, just an LLM API key.

Works on mcp 1.x and 2.x (same compat shim this repo's own MCP server
components use -- FastMCP was renamed MCPServer in 2.x).
"""
import json
import os

try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
    from mcp.server.mcpserver import MCPServer as _Server
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server

TRIAGE_SYSTEM_PROMPT = """You are a senior support triage specialist. Given a \
severe support ticket's context, decide which engineering team should own it \
and how urgent it is.

Call the `triage_decision` tool with your decision. Teams: infra, payments, \
data. Urgency: P0 (drop everything), P1 (today), P2 (this week)."""

TRIAGE_TOOL = {
    "type": "function",
    "function": {
        "name": "triage_decision",
        "description": "Record the triage decision for this ticket.",
        "parameters": {
            "type": "object",
            "properties": {
                "team": {"type": "string", "enum": ["infra", "payments", "data"]},
                "urgency": {"type": "string", "enum": ["P0", "P1", "P2"]},
                "reasoning": {"type": "string"},
            },
            "required": ["team", "urgency", "reasoning"],
        },
    },
}

mcp = _Server("triage-mcp")


@mcp.tool()
def triage_ticket(ticket_context: str) -> dict:
    """Assign an owning team and urgency to a severe support ticket."""
    import litellm

    resp = litellm.completion(
        model="gpt-4o-mini",
        api_key=os.environ["OPENAI_API_KEY"],
        messages=[
            {"role": "system", "content": TRIAGE_SYSTEM_PROMPT},
            {"role": "user", "content": ticket_context},
        ],
        tools=[TRIAGE_TOOL],
        tool_choice="required",
        temperature=0.0,
    )
    tool_call = resp.choices[0].message.tool_calls[0]
    return json.loads(tool_call.function.arguments)


if __name__ == "__main__":
    mcp.run(transport="stdio")
