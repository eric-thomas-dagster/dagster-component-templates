#!/usr/bin/env python3
"""Minimal local MCP stdio server exposing one tool: respond.

One script, three personas (advocate/skeptic/arbitrator) -- selected by
argv[1], same pattern as the other examples in this folder. The two
debaters share a capability tag (`debate`) and are told apart by the
picker LLM's semantic reading of the task's FOR/AGAINST phrasing against
each card's own skill description; the arbitrator has its own distinct
capability tag (`arbitrate`) since there's no ambiguity about which agent
should render the final verdict.

Works on mcp 1.x and 2.x (same compat shim this repo's own MCP server
components use -- FastMCP was renamed MCPServer in 2.x).
"""
import os
import sys

try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
    from mcp.server.mcpserver import MCPServer as _Server
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server

PERSONAS = {
    "advocate": (
        "You are a debate agent arguing IN FAVOR of a proposal. Given a "
        "proposal, construct the strongest, most persuasive case FOR it, "
        "in 3-5 sentences."
    ),
    "skeptic": (
        "You are a debate agent arguing AGAINST a proposal. Given a "
        "proposal, construct the strongest, most critical case AGAINST it "
        "-- risks, flaws, what could go wrong -- in 3-5 sentences."
    ),
    "arbitrator": (
        "You are an impartial judge. Given a proposal and two arguments "
        "(one for, one against), weigh them honestly and render a clear, "
        "reasoned final verdict in 3-5 sentences -- don't just summarize "
        "both sides, actually decide."
    ),
}

persona_name = sys.argv[1] if len(sys.argv) > 1 else "arbitrator"
if persona_name not in PERSONAS:
    raise ValueError(f"unknown persona {persona_name!r}; choices: {list(PERSONAS)}")
SYSTEM_PROMPT = PERSONAS[persona_name]

mcp = _Server(f"debate-{persona_name}-mcp")


@mcp.tool()
def respond(context: str) -> dict:
    f"""Produce this agent's ({persona_name}) response given context."""
    import litellm

    resp = litellm.completion(
        model="gpt-4o-mini",
        api_key=os.environ["OPENAI_API_KEY"],
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context},
        ],
        temperature=0.3,
    )
    return {"response": resp.choices[0].message.content, "persona": persona_name}


if __name__ == "__main__":
    mcp.run(transport="stdio")
