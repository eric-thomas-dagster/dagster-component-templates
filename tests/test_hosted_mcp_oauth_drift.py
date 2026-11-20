"""General hosted-MCP OAuth (token read/refresh/rotate under a file lock, the shared rate
limiter, and the streamable-HTTP tool-call wrapper that detects a real HTTP 429 or a configurable
soft text-based rate-limit signal) is inlined (this repo's self-contained-component convention --
no shared module) into every file that needs it: hosted_mcp_server's component.py and
mcp_oauth_login's __main__.py (not a component.py -- hence the broader glob below). This test
guards against drift between the two copies, the same way test_notion_mcp_oauth_drift.py guards
its own (Notion-specific, intentionally NOT shared with these generalized ones) inlined block.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_BEGIN = "# BEGIN hosted mcp oauth"
_END = "# END hosted mcp oauth"


def _candidate_files():
    for p in sorted(ROOT.glob("**/*.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] in ("dist", "quickstarts", "tests"):
            continue
        yield p


def _block(text: str) -> str:
    return text[text.index(_BEGIN): text.index(_END) + len(_END)]


def test_inlined_blocks_are_identical():
    blocks = {}
    for p in _candidate_files():
        text = p.read_text()
        if _BEGIN in text:
            blocks.setdefault(_block(text), []).append(str(p.relative_to(ROOT)))
    assert len(blocks) == 1, {k[:60]: v for k, v in blocks.items()}
    all_paths = [v for vs in blocks.values() for v in vs]
    assert len(all_paths) == 2, all_paths
