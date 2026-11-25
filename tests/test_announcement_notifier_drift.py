"""The "announcement notifier" block (post_announcement: posts one announcement to Slack or Microsoft Teams and
returns the thread-identifying run tags) is inlined, byte-identical, in every component that needs it (no shared
helper files, per this repo's self-contained-component convention). This drift test compares the copies.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_BEGIN, _END = "# BEGIN announcement notifier", "# END announcement notifier"

COMPONENTS = [
    "sensors/failed_run_agent_sensor/component.py",
    "sensors/ticket_announcer/component.py",
    "sensors/scheduled_slack_agent_sensor/component.py",
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
    assert not drift, "announcement notifier block differs from the failed_run_agent_sensor copy:\n  " + "\n  ".join(drift)
