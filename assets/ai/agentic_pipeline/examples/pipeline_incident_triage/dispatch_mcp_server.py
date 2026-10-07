#!/usr/bin/env python3
"""Minimal local MCP stdio server with three REAL tools for pipeline
incident triage -- inspired by a real "Dispatch" incident-triage tool
(cross-stack pipeline triage: vendor status + runbooks + git context,
synthesized into a root-cause diagnosis + wait-vs-debug decision).

All three tools do real work, not simulated:
  - check_vendor_status: live HTTP call to the vendor's real, public
    Statuspage.io-backed status API (same endpoints real status badges
    and uptime-monitoring integrations use).
  - search_runbooks: real keyword search over runbooks.json (this is
    "institutional knowledge" -- inherently local, not an external API,
    but the search itself is real, not canned).
  - get_recent_commits: real `git log` against a real repo path (defaults
    to this very repo), not fabricated commit history.
"""
import asyncio
import json
import os
import subprocess

import requests

import mcp.server.stdio
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.types import Tool, TextContent

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
RUNBOOKS_PATH = os.path.join(THIS_DIR, "runbooks.json")
DEFAULT_REPO_PATH = os.path.abspath(os.path.join(THIS_DIR, "..", "..", "..", "..", ".."))

VENDOR_STATUS_URLS = {
    "github": "https://www.githubstatus.com/api/v2/status.json",
    "fivetran": "https://status.fivetran.com/api/v2/status.json",
    "dbt": "https://status.getdbt.com/api/v2/status.json",
    "snowflake": "https://status.snowflake.com/api/v2/status.json",
}

server = Server("dispatch-mcp")


def _check_vendor_status(vendor: str) -> dict:
    url = VENDOR_STATUS_URLS.get(vendor.lower())
    if not url:
        return {
            "vendor": vendor,
            "status": "unknown",
            "description": f"No status endpoint configured for {vendor!r}. Known vendors: {list(VENDOR_STATUS_URLS)}",
        }
    try:
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        return {
            "vendor": vendor,
            "status": data["status"]["indicator"],
            "description": data["status"]["description"],
            "source_url": url,
        }
    except Exception as e:
        return {"vendor": vendor, "status": "error", "description": f"Could not reach status page: {e}"}


def _search_runbooks(query: str) -> dict:
    with open(RUNBOOKS_PATH) as f:
        runbooks = json.load(f)
    q_words = set(query.lower().split())
    scored = []
    for rb in runbooks:
        score = sum(1 for kw in rb["keywords"] if kw in query.lower() or kw in q_words)
        if score > 0:
            scored.append((score, rb))
    scored.sort(key=lambda x: -x[0])
    matches = [rb for _, rb in scored[:2]]
    return {"query": query, "matches": matches if matches else "no matching runbook found"}


def _get_recent_commits(n: int, repo_path: str = None) -> dict:
    path = repo_path or DEFAULT_REPO_PATH
    try:
        result = subprocess.run(
            ["git", "log", f"-{n}", "--oneline", "--no-decorate"],
            cwd=path, capture_output=True, text=True, timeout=10, check=True,
        )
        commits = [ln for ln in result.stdout.splitlines() if ln.strip()]
        return {"repo_path": path, "commits": commits}
    except Exception as e:
        return {"repo_path": path, "error": str(e)}


@server.list_tools()
async def list_tools():
    return [
        Tool(
            name="check_vendor_status",
            description="Check a vendor's REAL live public status page (github, fivetran, dbt, snowflake).",
            inputSchema={
                "type": "object",
                "properties": {"vendor": {"type": "string", "enum": list(VENDOR_STATUS_URLS)}},
                "required": ["vendor"],
            },
        ),
        Tool(
            name="search_runbooks",
            description="Search internal runbooks/institutional knowledge for guidance matching an error description.",
            inputSchema={
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
        ),
        Tool(
            name="get_recent_commits",
            description="Get the N most recent git commits from the pipeline's repo, to check for a recent deploy that might explain the failure.",
            inputSchema={
                "type": "object",
                "properties": {
                    "n": {"type": "integer", "description": "Number of commits to fetch."},
                    "repo_path": {"type": "string", "description": "Optional repo path; defaults to this project's repo."},
                },
                "required": ["n"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    if name == "check_vendor_status":
        result = _check_vendor_status(arguments["vendor"])
    elif name == "search_runbooks":
        result = _search_runbooks(arguments["query"])
    elif name == "get_recent_commits":
        result = _get_recent_commits(arguments.get("n", 10), arguments.get("repo_path"))
    else:
        raise ValueError(f"unknown tool {name!r}")
    return [TextContent(type="text", text=json.dumps(result, default=str))]


async def main():
    async with mcp.server.stdio.stdio_server() as (read, write):
        await server.run(
            read, write,
            InitializationOptions(
                server_name="dispatch-mcp",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(), experimental_capabilities={}
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
