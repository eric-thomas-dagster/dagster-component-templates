"""The "slack PR review metadata" block (posting a PR-review Slack message with its correlation config embedded
as hidden message metadata, and discovering still-open PR-review threads by scanning the channel) is inlined,
byte-identical, in both components that need it -- the poster (`github_pull_request_agent_sensor`) and the watcher
(`slack_pr_approval_watcher`). No shared file between the two; this drift test compares the copies instead.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_BEGIN, _END = "# BEGIN slack PR review metadata", "# END slack PR review metadata"

COMPONENTS = [
    "sensors/github_pull_request_agent_sensor/component.py",
    "sensors/slack_pr_approval_watcher/component.py",
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
    assert not drift, "slack PR review metadata block differs from the github_pull_request_agent_sensor copy:\n  " + "\n  ".join(drift)
