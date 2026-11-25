"""The "teams PR review metadata" block (posting a PR-review Adaptive Card to Teams, scanning the channel for one,
fetching its votes) is inlined, byte-identical, in every component that needs it (no shared helper files, per this
repo's self-contained-component convention). This drift test compares the copies.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_BEGIN, _END = "# BEGIN teams PR review metadata", "# END teams PR review metadata"

COMPONENTS = [
    "sensors/github_pull_request_agent_sensor/component.py",
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
    assert not drift, "teams PR review metadata block differs from the github_pull_request_agent_sensor copy:\n  " + "\n  ".join(drift)
