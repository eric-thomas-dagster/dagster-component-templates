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

server = Server(f"specialist-{persona_name}-mcp")


def _run_specialist_llm(question: str) -> str:
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
    return resp.choices[0].message.content


@server.list_tools()
async def list_tools():
    return [
        Tool(
            name="draft_response",
            description=f"Draft a {persona_name} support response to a customer question.",
            inputSchema={
                "type": "object",
                "properties": {"question": {"type": "string"}},
                "required": ["question"],
            },
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    if name != "draft_response":
        raise ValueError(f"unknown tool {name!r}")
    response = _run_specialist_llm(arguments.get("question", ""))
    return [TextContent(type="text", text=json.dumps({"response": response, "specialist": persona_name}))]


async def main():
    async with mcp.server.stdio.stdio_server() as (read, write):
        await server.run(
            read, write,
            InitializationOptions(
                server_name=f"specialist-{persona_name}-mcp",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(), experimental_capabilities={}
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
