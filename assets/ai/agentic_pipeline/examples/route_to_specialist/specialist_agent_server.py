#!/usr/bin/env python3
"""Minimal local MCP stdio server exposing one tool: draft_response.

One script, three registered "agents" (billing/technical/general) -- same
pattern as support_ticket_triage/triage_mcp_server.py and
document_summarization/summarizer_agent_server.py, just parameterized by
which persona to run (argv[1]), since all three specialists share the same
shape (take a question, draft a response) and differ only in their system
prompt. Each persona is registered as its OWN AgentCardComponent (see
billing_specialist_agent.yaml etc.) pointing at this same script with a
different argv.

Works on mcp 1.x and 2.x (same compat shim this repo's own MCP server
components use -- FastMCP was renamed MCPServer in 2.x).
"""
import json
import os
import sys

try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
    from mcp.server.mcpserver import MCPServer as _Server
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server

PERSONAS = {
    "billing": (
        "You are a billing support specialist. Answer questions about charges, "
        "invoices, refunds, and subscriptions clearly and helpfully, in 2-4 "
        "sentences."
    ),
    "technical": (
        "You are a technical support specialist. Answer questions about product "
        "bugs, integrations, APIs, and technical errors clearly and helpfully, "
        "in 2-4 sentences."
    ),
    "general": (
        "You are a general support specialist. Answer questions that don't fit "
        "billing or technical support -- account basics, how-to questions, "
        "general product info -- clearly and helpfully, in 2-4 sentences."
    ),
}

persona_name = sys.argv[1] if len(sys.argv) > 1 else "general"
if persona_name not in PERSONAS:
    raise ValueError(f"unknown persona {persona_name!r}; choices: {list(PERSONAS)}")
SYSTEM_PROMPT = PERSONAS[persona_name]

mcp = _Server(f"specialist-{persona_name}-mcp")


@mcp.tool()
def draft_response(question: str) -> dict:
    f"""Draft a {persona_name} support response to a customer question."""
    import litellm

    resp = litellm.completion(
        model="gpt-4o-mini",
        api_key=os.environ["OPENAI_API_KEY"],
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
        temperature=0.2,
    )
    return {"response": resp.choices[0].message.content, "specialist": persona_name}


if __name__ == "__main__":
    mcp.run(transport="stdio")
