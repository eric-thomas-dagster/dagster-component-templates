"""The "github PR scope" block (repo-scoped GitHub pull-request operations: read, propose a targeted-edit fix,
assess risk, review under a second identity, merge when ready) is inlined, byte-identical, in every component that
needs it (no shared helper files, per this repo's self-contained-component convention). This drift test compares
the copies.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_BEGIN, _END = "# BEGIN github PR scope", "# END github PR scope"

COMPONENTS = [
    "assets/ai/github_pr_tools/component.py",
    "sensors/github_pull_request_agent_sensor/component.py",
    "sensors/slack_pr_approval_watcher/component.py",
    "sensors/teams_pr_approval_watcher/component.py",
]
REF = ROOT / COMPONENTS[0]


def _block(p: Path) -> str:
    t = p.read_text()
    return t[t.index(_BEGIN): t.index(_END) + len(_END)]


def test_every_tracked_component_actually_has_the_block():
    missing = [c for c in COMPONENTS if _BEGIN not in (ROOT / c).read_text()]
    assert not missing, f"tracked but missing the block: {missing}"


def test_inlined_copies_are_identical():
    ref = _block(REF)
    drift = [c for c in COMPONENTS if _block(ROOT / c) != ref]
    assert not drift, "github PR scope block differs from the github_pr_tools copy:\n  " + "\n  ".join(drift)
