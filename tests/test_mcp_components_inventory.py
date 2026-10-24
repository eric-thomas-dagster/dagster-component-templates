"""Every component that imports the MCP SDK (`mcp`) or `fastmcp` must be registered here.

Why: MCP SDK majors break APIs (mcp 2.x renamed FastMCP -> MCPServer, Tool objects stopped being dicts,
inputSchema -> input_schema, ...). The `mcp-canary` workflow (.github/workflows/mcp-canary.yml) runs the tests of the
components listed below against min / latest 1.x / latest 2.x / pre-release `mcp` and latest `fastmcp`. A component that
talks MCP but is not listed would be invisible to that canary, so this test fails until it is.
"""
import ast
import functools
import os
import pathlib
import warnings

ROOT = pathlib.Path(__file__).resolve().parent.parent

# component directory (relative to repo root) -> role. Keep the mcp-canary.yml TEST list in sync with this.
MCP_COMPONENTS = {
    "assets/ai/agentic_pipeline": "client+server",   # MCP client steps; example servers
    "assets/ai/dagster_instance_tools": "server",   # serves read-only Dagster tools
    "assets/ai/mcp_tool_picker": "client",
    "assets/sources/mcp_tool_call": "client",
    "assets/ai/litellm_agent": "client",
    "assets/ai/openai_agent": "client",
    "assets/ai/anthropic_agent": "client",
    "assets/ai/gemini_agent": "client",
    "assets/ai/vercel_ai_gateway_agent": "client",
    "assets/ai/snowflake_cortex_agent": "client",
    "resources/context_receipt_store": "server (fastmcp gateway examples)",
}
SKIP_DIRS = {".git", "node_modules", ".venv", "venv", "site-packages", "__pycache__"}


def _imports_mcp(path: pathlib.Path) -> bool:
    text = path.read_text(encoding="utf-8", errors="ignore")
    if "mcp" not in text:  # cheap pre-filter: thousands of files, almost none mention mcp
        return False
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            tree = ast.parse(text)
    except SyntaxError:
        return False
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names = [node.module]
        if any(n.split(".")[0] in ("mcp", "fastmcp") for n in names):
            return True
    return False


def _component_dir(rel: pathlib.Path) -> str:
    parts = rel.parts
    # assets/<category>/<name>/..., resources/<name>/..., quickstarts/<name>/..., otherwise the file's directory
    depth = 3 if parts[0] == "assets" else 2
    return "/".join(parts[:depth])


@functools.lru_cache(maxsize=1)
def _found():
    found = {}
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and d != "tests"]
        for name in filenames:
            if not name.endswith(".py") or name.startswith("test_") or name == "conftest.py":
                continue
            p = pathlib.Path(dirpath) / name
            rel = p.relative_to(ROOT)
            if len(rel.parts) > 1 and _imports_mcp(p):
                found.setdefault(_component_dir(rel), []).append(str(rel))
    return found


def test_every_mcp_importing_component_is_in_the_inventory():
    unregistered = {d: f for d, f in _found().items() if d not in MCP_COMPONENTS}
    assert not unregistered, (
        f"These components import `mcp`/`fastmcp` but are not in MCP_COMPONENTS in {pathlib.Path(__file__).name}: "
        f"{unregistered}.\nTo fix: (1) use the inlined `# --- mcp-compat-helpers:begin/end ---` block (see "
        "assets/ai/mcp_tool_picker/component.py) for client code, or a try-new-import/fall-back-to-old shim for servers "
        "(mcp.server.mcpserver.MCPServer vs mcp.server.fastmcp.FastMCP); (2) add a test that talks to a REAL MCP server, "
        "not a mock; (3) add the directory to MCP_COMPONENTS here, to the client list in "
        "tests/test_mcp_compat_helpers_drift.py if it uses the helper block, and to the TEST list in "
        ".github/workflows/mcp-canary.yml."
    )


def test_inventory_entries_still_exist_and_use_mcp():
    stale = [d for d in MCP_COMPONENTS if d not in _found()]
    assert not stale, f"MCP_COMPONENTS lists directories that no longer import mcp/fastmcp (remove them or fix): {stale}"


def test_inventory_components_have_tests_in_canary_workflow():
    wf = (ROOT / ".github" / "workflows" / "mcp-canary.yml").read_text()
    missing = [d for d in MCP_COMPONENTS if d not in wf]
    assert not missing, f"mcp-canary.yml does not run the tests of: {missing}"
