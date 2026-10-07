#!/usr/bin/env python3
"""Minimal local MCP stdio server exposing one tool: triage_ticket.

This IS the "agent" -- not a separate company's hosted infrastructure, just
an LLM call wrapped behind an MCP tool with its own system prompt/persona
("context given to an LLM to make it act like a specialist"). This is the
realistic way to stand up your own agent for `delegate` to call: any team
can wrap a prompt like this behind MCP, register it as an AgentCardComponent,
and it becomes callable exactly like a hosted third-party agent would be --
no external service required, just an LLM API key.
"""
import asyncio
import json
import os

import mcp.server.stdio
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.types import Tool, TextContent

server = Server("triage-mcp")

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


def _run_triage_llm(ticket_context: str) -> dict:
    """The agent's actual "brain" -- a real LLM call with a specialist
    persona, using the same litellm wrapper agentic_pipeline itself uses."""
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


@server.list_tools()
async def list_tools():
    return [
        Tool(
            name="triage_ticket",
            description="Assign an owning team and urgency to a severe support ticket.",
            inputSchema={
                "type": "object",
                "properties": {"ticket_context": {"type": "string"}},
                "required": ["ticket_context"],
            },
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    if name != "triage_ticket":
        raise ValueError(f"unknown tool {name!r}")
    decision = _run_triage_llm(arguments.get("ticket_context", ""))
    return [TextContent(type="text", text=json.dumps(decision))]


async def main():
    async with mcp.server.stdio.stdio_server() as (read, write):
        await server.run(
            read, write,
            InitializationOptions(
                server_name="triage-mcp",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(), experimental_capabilities={}
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
