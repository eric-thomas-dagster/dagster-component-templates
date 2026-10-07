#!/usr/bin/env python3
"""Minimal local MCP stdio server exposing one tool: summarize_document.

This IS the "summarizer agent" -- an LLM call wrapped behind an MCP tool
with its own persona/system prompt, same pattern as the triage agent in
../support_ticket_triage/triage_mcp_server.py. Forced structured output
(summary + action_items) rather than free text, so downstream steps or
asset metadata get a real list to act on, not prose to re-parse.
"""
import asyncio
import json
import os

import mcp.server.stdio
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.types import Tool, TextContent

server = Server("summarizer-mcp")

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


def _run_summarizer_llm(document_text: str) -> dict:
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


@server.list_tools()
async def list_tools():
    return [
        Tool(
            name="summarize_document",
            description="Produce an executive summary + action items for a document.",
            inputSchema={
                "type": "object",
                "properties": {"document_text": {"type": "string"}},
                "required": ["document_text"],
            },
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    if name != "summarize_document":
        raise ValueError(f"unknown tool {name!r}")
    result = _run_summarizer_llm(arguments.get("document_text", ""))
    return [TextContent(type="text", text=json.dumps(result))]


async def main():
    async with mcp.server.stdio.stdio_server() as (read, write):
        await server.run(
            read, write,
            InitializationOptions(
                server_name="summarizer-mcp",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(), experimental_capabilities={}
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
