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

Works on mcp 1.x and 2.x (same compat shim this repo's own MCP server
components use -- FastMCP was renamed MCPServer in 2.x).
"""
import json
import os
import subprocess
from typing import Optional

import requests

try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
    from mcp.server.mcpserver import MCPServer as _Server
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
RUNBOOKS_PATH = os.path.join(THIS_DIR, "runbooks.json")
DEFAULT_REPO_PATH = os.path.abspath(os.path.join(THIS_DIR, "..", "..", "..", "..", ".."))

VENDOR_STATUS_URLS = {
    "github": "https://www.githubstatus.com/api/v2/status.json",
    "fivetran": "https://status.fivetran.com/api/v2/status.json",
    "dbt": "https://status.getdbt.com/api/v2/status.json",
    "snowflake": "https://status.snowflake.com/api/v2/status.json",
}

mcp = _Server("dispatch-mcp")


@mcp.tool()
def check_vendor_status(vendor: str) -> dict:
    """Check a vendor's REAL live public status page (github, fivetran, dbt, snowflake)."""
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


@mcp.tool()
def search_runbooks(query: str) -> dict:
    """Search internal runbooks/institutional knowledge for guidance matching an error description."""
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


@mcp.tool()
def get_recent_commits(n: int, repo_path: Optional[str] = None) -> dict:
    """Get the N most recent git commits from the pipeline's repo, to check for a recent deploy that might explain the failure."""
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


if __name__ == "__main__":
    mcp.run(transport="stdio")
