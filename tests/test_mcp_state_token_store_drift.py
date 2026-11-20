"""The state-backed MCP token store (reads the current {access_token, ...} blob for a state_key
from Dagster's own defs_state_storage, and the single writer's upload helper) is inlined (this
repo's self-contained-component convention -- no shared module) into every file that offers
`token_store='state'`: hosted_mcp_server's component.py, mcp_oauth_login's __main__.py (the
sign-in CLI's `--state-key` path), and mcp_token_refresher's component.py (the one writer). This
test guards against drift between the copies, the same way test_hosted_mcp_oauth_drift.py guards
its own (different) inlined block.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_BEGIN = "# BEGIN mcp state token store"
_END = "# END mcp state token store"


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
    assert len(all_paths) >= 2, all_paths
