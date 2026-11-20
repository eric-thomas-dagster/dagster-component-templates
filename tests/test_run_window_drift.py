"""`run_window` lets an asset skip cleanly (not fail) outside a configured time window.

The parser/checker is ported verbatim from `sensors/event_automation`'s own
`business_hours_only` logic (same 'HH:MM-HH:MM tz [days]' format, same fail-open-on-malformed-
input behavior) and inlined into each component that offers `run_window` (no shared helper
module -- this repo's self-contained-component convention). This test guards against drift
between the copies, the same way test_freshness_policy_compat.py and
test_generic_timeout_drift.py guard their own inlined blocks.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_BEGIN = "# BEGIN run_window"
_END = "# END run_window"


def _component_files():
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] in ("dist", "quickstarts"):
            continue
        yield p


def _block(text: str) -> str:
    return text[text.index(_BEGIN): text.index(_END) + len(_END)]


def test_inlined_blocks_are_identical():
    blocks = {}
    for p in _component_files():
        text = p.read_text()
        if _BEGIN in text:
            blocks.setdefault(_block(text), []).append(str(p.relative_to(ROOT)))
    assert len(blocks) == 1, {k[:60]: len(v) for k, v in blocks.items()}
    assert sum(len(v) for v in blocks.values()) >= 3  # task_chain_asset, shell_command_asset, sql_asset
