"""correlation_mode='message_metadata' (the new default) and target='run_request'.

No sidecar file, no approval_dir file at all for the pending-approval state: the posting asset
embeds the approval's config as hidden Slack message metadata (`chat.postMessage`'s own
`metadata` param) and discovers "have I already posted this" by searching channel history; the
watcher sensor discovers "what's pending" the same way, and keeps escalation-already-sent /
already-resolved bookkeeping in the sensor's own Dagster-native cursor, not a file. Only the
Slack SDK is faked (an in-memory channel, same shape real Slack returns); everything else runs
for real through dg.materialize() / a real sensor invocation.
"""
import json

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeSlackClient:
    """An in-memory Slack channel: chat_postMessage/reactions_add/reactions_get/
    conversations_history all read and write the same `self.messages` dict, same shapes the
    real Slack Web API uses (including `metadata` round-tripping through conversations.history
    under `include_all_metadata=True`, confirmed real against Slack's own docs)."""

    def __init__(self):
        self.messages = {}
        self._n = 0

    def auth_test(self):
        return {"ok": True, "user_id": "U_BOT"}

    def chat_postMessage(self, channel, text, metadata=None, thread_ts=None):
        self._n += 1
        ts = f"{self._n}.000000"
        self.messages[ts] = {"channel": channel, "text": text, "metadata": metadata, "reactions": {}}
        return {"ok": True, "ts": ts, "channel": channel}

    def reactions_add(self, channel, timestamp, name):
        self.messages[timestamp]["reactions"].setdefault(name, [])
        return {"ok": True}

    def reactions_get(self, channel, timestamp):
        m = self.messages[timestamp]
        reactions = [{"name": k, "users": v} for k, v in m["reactions"].items() if v]
        return {"ok": True, "message": {"reactions": reactions}}

    def conversations_history(self, channel, oldest=None, include_all_metadata=None, limit=None, cursor=None):
        msgs = [
            {"ts": ts, "metadata": m["metadata"]}
            for ts, m in sorted(self.messages.items(), key=lambda kv: float(kv[0]), reverse=True)
            if m["channel"] == channel
        ]
        return {"ok": True, "messages": msgs, "response_metadata": {"next_cursor": ""}}

    def vote(self, ts, emoji, user):
        self.messages[ts]["reactions"].setdefault(emoji, []).append(user)


def _patch_client(monkeypatch, mod, client):
    monkeypatch.setattr(mod, "_get_slack_client", lambda token_env_var: client)


def _upstream():
    @dg.asset(name="upstream_x")
    def upstream_x():
        return "please approve this"
    return upstream_x


def _post(mod, monkeypatch, tmp_path, client, **overrides):
    kwargs = dict(
        asset_name="gate", upstream_asset_key="upstream_x", approval_dir=str(tmp_path),
        slack_channel="C1", approver_allowlist=["U1"], required_approvers=1,
    )
    kwargs.update(overrides)
    comp = mod.SlackApprovalGateComponent(**kwargs)
    defs = comp.build_defs(None)
    posted = next(a for a in defs.assets if "posted" in str(a.node_def.name))
    result = dg.materialize([posted, _upstream()], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    return comp, defs, result


def test_message_metadata_is_the_new_default(mod):
    comp = mod.SlackApprovalGateComponent(
        asset_name="gate", upstream_asset_key="upstream_x", approval_dir="/tmp/x",
        slack_channel="C1", approver_allowlist=["U1"], required_approvers=1,
    )
    assert comp.correlation_mode == "message_metadata"


def test_posting_writes_no_sidecar_file_at_all(mod, monkeypatch, tmp_path):
    client = _FakeSlackClient()
    _patch_client(monkeypatch, mod, client)
    _post(mod, monkeypatch, tmp_path, client)
    assert list(tmp_path.iterdir()) == [], "message_metadata mode must write no file under approval_dir"
    [msg] = client.messages.values()
    assert msg["metadata"]["event_type"] == "dagster_approval_v1"
    assert msg["metadata"]["event_payload"]["safe_key"] == "default"
    assert msg["metadata"]["event_payload"]["approver_allowlist"] == ["U1"]


def test_posting_is_idempotent_via_slack_history_scan_not_a_local_file(mod, monkeypatch, tmp_path):
    client = _FakeSlackClient()
    _patch_client(monkeypatch, mod, client)
    _post(mod, monkeypatch, tmp_path, client)
    _post(mod, monkeypatch, tmp_path, client)  # re-materialize
    assert len(client.messages) == 1, "a second materialization must discover the existing message via Slack, not re-post"


def test_watcher_discovers_via_history_scan_and_writes_token_on_quorum(mod, monkeypatch, tmp_path):
    client = _FakeSlackClient()
    _patch_client(monkeypatch, mod, client)
    comp, defs, _ = _post(mod, monkeypatch, tmp_path, client)
    watcher = next(s for s in defs.sensors if s.name == "gate_watcher")

    ctx = dg.build_sensor_context()
    out = watcher(ctx)
    assert "no quorum changes" in out.skip_reason.skip_message

    [ts] = client.messages.keys()
    client.vote(ts, "white_check_mark", "U1")
    out2 = watcher(ctx)
    assert "wrote token (approved)" in out2.skip_reason.skip_message
    token_path = tmp_path / "default.json"
    assert token_path.exists()
    token = json.loads(token_path.read_text())
    assert token["approved"] is True
    assert token["approver"] == "U1"


def test_watcher_cursor_skips_an_already_resolved_message_without_rechecking_slack(mod, monkeypatch, tmp_path):
    client = _FakeSlackClient()
    _patch_client(monkeypatch, mod, client)
    comp, defs, _ = _post(mod, monkeypatch, tmp_path, client)
    watcher = next(s for s in defs.sensors if s.name == "gate_watcher")

    [ts] = client.messages.keys()
    client.vote(ts, "white_check_mark", "U1")
    ctx = dg.build_sensor_context()
    out1 = watcher(ctx)
    assert "wrote token" in out1.skip_reason.skip_message
    cursor_after = json.loads(out1.cursor)
    assert ts in cursor_after["resolved_ts"]

    # Second tick, resumed from the persisted cursor: must not re-fetch reactions/re-write.
    def _boom(*a, **k):
        raise AssertionError("reactions_get should not be called for an already-resolved message")
    monkeypatch.setattr(mod, "_fetch_reactions", _boom)
    ctx2 = dg.build_sensor_context(cursor=out1.cursor)
    out2 = watcher(ctx2)
    assert "no open approval messages" in out2.skip_reason.skip_message


def test_target_run_request_requires_message_metadata_correlation(mod):
    with pytest.raises(ValueError, match="run_request"):
        mod.SlackApprovalGateComponent(
            asset_name="gate", upstream_asset_key="u", approval_dir="/tmp/x", target="run_request",
            slack_channel="C1", approver_allowlist=["U1"], required_approvers=1,
            correlation_mode="sidecar_file", run_request_asset_selection=["a"],
        )


def test_target_run_request_requires_a_job_name_or_asset_selection(mod):
    with pytest.raises(ValueError, match="run_request_asset_selection"):
        mod.SlackApprovalGateComponent(
            asset_name="gate", upstream_asset_key="u", target="run_request",
            slack_channel="C1", approver_allowlist=["U1"], required_approvers=1,
        )


def test_target_run_request_does_not_require_approval_dir(mod):
    comp = mod.SlackApprovalGateComponent(
        asset_name="gate", upstream_asset_key="u", target="run_request",
        slack_channel="C1", approver_allowlist=["U1"], required_approvers=1,
        run_request_asset_selection=["a"],
    )
    assert comp.approval_dir is None


def test_target_run_request_fires_a_run_request_with_decision_tags_and_writes_no_file(mod, monkeypatch, tmp_path):
    client = _FakeSlackClient()
    _patch_client(monkeypatch, mod, client)
    comp, defs, _ = _post(
        mod, monkeypatch, tmp_path, client, target="run_request",
        run_request_mode="asset_selection", run_request_asset_selection=["downstream_asset"],
        approval_dir=None,
    )
    watcher = next(s for s in defs.sensors if s.name == "gate_watcher")
    [ts] = client.messages.keys()
    client.vote(ts, "white_check_mark", "U1")

    ctx = dg.build_sensor_context()
    out = watcher(ctx)
    assert list(tmp_path.iterdir()) == [], "target: run_request must write no file at all"
    [rr] = out.run_requests
    assert rr.tags["approval/approved"] == "true"
    assert rr.tags["approval/approver"] == "U1"
    assert rr.asset_selection == [dg.AssetKey("downstream_asset")]
    assert rr.run_key == f"gate_watcher:C1:{ts}"


def test_target_run_request_job_mode_builds_a_job_run_request(mod, monkeypatch, tmp_path):
    client = _FakeSlackClient()
    _patch_client(monkeypatch, mod, client)
    comp, defs, _ = _post(
        mod, monkeypatch, tmp_path, client, target="run_request",
        run_request_mode="job", run_request_job_name="resume_job", approval_dir=None,
    )
    watcher = next(s for s in defs.sensors if s.name == "gate_watcher")
    [ts] = client.messages.keys()
    client.vote(ts, "white_check_mark", "U1")
    ctx = dg.build_sensor_context()
    out = watcher(ctx)
    [rr] = out.run_requests
    assert rr.job_name == "resume_job"


def test_escalation_fires_once_tracked_via_cursor_not_a_sidecar(mod, monkeypatch, tmp_path):
    client = _FakeSlackClient()
    _patch_client(monkeypatch, mod, client)
    comp, defs, _ = _post(
        mod, monkeypatch, tmp_path, client,
        timeout_hours=0.0, on_timeout="escalate", escalate_slack_user="U_LEAD",
    )
    watcher = next(s for s in defs.sensors if s.name == "gate_watcher")
    ctx = dg.build_sensor_context()
    out1 = watcher(ctx)
    assert "escalated" in out1.skip_reason.skip_message
    escalation_posts = [m for m in client.messages.values() if "U_LEAD" in m["text"]]
    assert len(escalation_posts) == 1

    ctx2 = dg.build_sensor_context(cursor=out1.cursor)
    out2 = watcher(ctx2)
    escalation_posts_after = [m for m in client.messages.values() if "U_LEAD" in m["text"]]
    assert len(escalation_posts_after) == 1, "escalation must fire exactly once, not every tick"
