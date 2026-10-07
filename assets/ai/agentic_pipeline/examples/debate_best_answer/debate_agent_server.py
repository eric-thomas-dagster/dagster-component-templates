#!/usr/bin/env python3
"""Minimal local MCP stdio server exposing one tool: respond.

One script, three personas (advocate/skeptic/arbitrator) -- selected by
argv[1], same pattern as the other examples in this folder. The two
debaters share a capability tag (`debate`) and are told apart by the
picker LLM's semantic reading of the task's FOR/AGAINST phrasing against
each card's own skill description; the arbitrator has its own distinct
capability tag (`arbitrate`) since there's no ambiguity about which agent
should render the final verdict.
"""
import asyncio
import json
import os
import sys

import mcp.server.stdio
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.types import Tool, TextContent

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

server = Server(f"debate-{persona_name}-mcp")


def _run_llm(context_text: str) -> str:
    import litellm

    resp = litellm.completion(
        model="gpt-4o-mini",
        api_key=os.environ["OPENAI_API_KEY"],
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context_text},
        ],
        temperature=0.3,
    )
    return resp.choices[0].message.content


@server.list_tools()
async def list_tools():
    return [
        Tool(
            name="respond",
            description=f"Produce this agent's ({persona_name}) response given context.",
            inputSchema={
                "type": "object",
                "properties": {"context": {"type": "string"}},
                "required": ["context"],
            },
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    if name != "respond":
        raise ValueError(f"unknown tool {name!r}")
    response = _run_llm(arguments.get("context", ""))
    return [TextContent(type="text", text=json.dumps({"response": response, "persona": persona_name}))]


async def main():
    async with mcp.server.stdio.stdio_server() as (read, write):
        await server.run(
            read, write,
            InitializationOptions(
                server_name=f"debate-{persona_name}-mcp",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(), experimental_capabilities={}
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
