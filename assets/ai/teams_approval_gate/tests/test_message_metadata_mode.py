"""correlation_mode='message_metadata' (the new default) and target='run_request'.

No sidecar file for pending-approval state: the posting asset embeds the approval's config as a
custom `_dagster_metadata` key inside an Adaptive Card attachment on the Teams message (Graph
stores attachment `content` as an opaque string -- confirmed against Graph's own docs, not
guessed -- so nothing is rendered to the human), and discovers "have I already posted this" /
"what's pending" by listing channel messages (confirmed `attachments` comes back inline by
default on that endpoint, no per-message fetch needed). Escalation-already-sent /
already-resolved bookkeeping lives in the sensor's own Dagster-native cursor, not a file. Only
the Graph HTTP calls (`_get_teams_bearer` / `_post_teams_message_with_metadata` /
`_iter_teams_approval_messages` / `_fetch_votes` / `_post_teams_reply`) are faked.
"""
import json

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeTeamsChannel:
    """An in-memory Teams channel: posting/listing/voting all read and write the same
    `self.messages` dict, same shapes the real Graph API uses (attachments inline on list)."""

    def __init__(self):
        self.messages = {}
        self._n = 0

    def post(self, html, event_payload=None):
        self._n += 1
        mid = f"m{self._n}"
        attachments = []
        if event_payload is not None:
            card = {"type": "AdaptiveCard", "_dagster_metadata": event_payload}
            attachments = [{"contentType": "application/vnd.microsoft.card.adaptive", "content": json.dumps(card)}]
        self.messages[mid] = {"id": mid, "body": html, "attachments": attachments, "reactions": [], "replies": []}
        return {"message_id": mid, "web_url": f"https://teams.example/{mid}"}

    def list_messages(self):
        return [m for m in self.messages.values()]

    def vote(self, mid, reaction_type, user_id):
        self.messages[mid]["reactions"].append({"reactionType": reaction_type, "user": {"user": {"id": user_id}}})


def _patch_graph(monkeypatch, mod, channel: _FakeTeamsChannel):
    monkeypatch.setattr(mod, "_get_teams_bearer", lambda fs, *a, **k: "fake-bearer")

    def fake_post_with_metadata(bearer, team_id, channel_id, html, event_payload):
        return channel.post(html, event_payload)

    def fake_iter(bearer, team_id, channel_id, lookback_hours, max_pages=10):
        for m in channel.list_messages():
            payload = mod._extract_approval_metadata(m)
            if payload is not None:
                yield {"message_id": m["id"], "payload": payload}

    def fake_get_message(bearer, team_id, channel_id, message_id):
        return channel.messages[message_id]

    monkeypatch.setattr(mod, "_post_teams_message_with_metadata", fake_post_with_metadata)
    monkeypatch.setattr(mod, "_iter_teams_approval_messages", fake_iter)
    monkeypatch.setattr(mod, "_get_teams_message", fake_get_message)

    def fake_post_reply(bearer, team_id, channel_id, message_id, html):
        channel.messages[message_id]["replies"].append({"body": {"content": html}})

    monkeypatch.setattr(mod, "_post_teams_reply", fake_post_reply)


def _upstream():
    @dg.asset(name="upstream_x")
    def upstream_x():
        return "please approve this"
    return upstream_x


def _post(mod, monkeypatch, tmp_path, channel, **overrides):
    kwargs = dict(
        asset_name="gate", upstream_asset_key="upstream_x", approval_dir=str(tmp_path),
        teams_tenant_id_env_var="TEAMS_TENANT", teams_client_id_env_var="TEAMS_CLIENT",
        teams_client_secret_env_var="TEAMS_SECRET", teams_team_id="T1", teams_channel_id="C1",
        approver_allowlist=["alice-guid"], required_approvers=1,
    )
    kwargs.update(overrides)
    comp = mod.TeamsApprovalGateComponent(**kwargs)
    defs = comp.build_defs(None)
    posted = next(a for a in defs.assets if "posted" in str(a.node_def.name))
    result = dg.materialize([posted, _upstream()], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    return comp, defs, result


def test_message_metadata_is_the_new_default(mod):
    comp = mod.TeamsApprovalGateComponent(
        asset_name="gate", upstream_asset_key="upstream_x", approval_dir="/tmp/x",
        teams_tenant_id_env_var="A", teams_client_id_env_var="B", teams_client_secret_env_var="C",
        teams_team_id="T1", teams_channel_id="C1", approver_allowlist=["u"], required_approvers=1,
    )
    assert comp.correlation_mode == "message_metadata"


def test_posting_writes_no_sidecar_file_at_all(mod, monkeypatch, tmp_path):
    channel = _FakeTeamsChannel()
    _patch_graph(monkeypatch, mod, channel)
    _post(mod, monkeypatch, tmp_path, channel)
    assert list(tmp_path.iterdir()) == [], "message_metadata mode must write no decision-state file under approval_dir"
    [msg] = channel.messages.values()
    payload = mod._extract_approval_metadata(msg)
    assert payload["safe_key"] == "default"
    assert payload["approver_allowlist"] == ["alice-guid"]


def test_posting_is_idempotent_via_teams_message_scan_not_a_local_file(mod, monkeypatch, tmp_path):
    channel = _FakeTeamsChannel()
    _patch_graph(monkeypatch, mod, channel)
    _post(mod, monkeypatch, tmp_path, channel)
    _post(mod, monkeypatch, tmp_path, channel)
    assert len(channel.messages) == 1, "a second materialization must discover the existing message via Teams, not re-post"


def test_watcher_discovers_via_message_scan_and_writes_token_on_quorum(mod, monkeypatch, tmp_path):
    channel = _FakeTeamsChannel()
    _patch_graph(monkeypatch, mod, channel)
    comp, defs, _ = _post(mod, monkeypatch, tmp_path, channel)
    watcher = next(s for s in defs.sensors if s.name == "gate_watcher")

    ctx = dg.build_sensor_context()
    out = watcher(ctx)
    assert "no quorum changes" in out.skip_reason.skip_message

    [mid] = channel.messages.keys()
    channel.vote(mid, "like", "alice-guid")
    out2 = watcher(ctx)
    assert "wrote token (approved)" in out2.skip_reason.skip_message
    token = json.loads((tmp_path / "default.json").read_text())
    assert token["approved"] is True
    assert token["approver"] == "alice-guid"


def test_watcher_cursor_skips_an_already_resolved_message(mod, monkeypatch, tmp_path):
    channel = _FakeTeamsChannel()
    _patch_graph(monkeypatch, mod, channel)
    comp, defs, _ = _post(mod, monkeypatch, tmp_path, channel)
    watcher = next(s for s in defs.sensors if s.name == "gate_watcher")

    [mid] = channel.messages.keys()
    channel.vote(mid, "like", "alice-guid")
    ctx = dg.build_sensor_context()
    out1 = watcher(ctx)
    cursor_after = json.loads(out1.cursor)
    assert mid in cursor_after["resolved_ids"]

    def _boom(*a, **k):
        raise AssertionError("_fetch_votes should not be called for an already-resolved message")
    monkeypatch.setattr(mod, "_fetch_votes", _boom)
    ctx2 = dg.build_sensor_context(cursor=out1.cursor)
    out2 = watcher(ctx2)
    assert "no open approval messages" in out2.skip_reason.skip_message


def test_target_run_request_requires_message_metadata_correlation(mod):
    with pytest.raises(ValueError, match="run_request"):
        mod.TeamsApprovalGateComponent(
            asset_name="gate", upstream_asset_key="u", approval_dir="/tmp/x", target="run_request",
            teams_tenant_id_env_var="A", teams_client_id_env_var="B", teams_client_secret_env_var="C",
            teams_team_id="T1", teams_channel_id="C1", approver_allowlist=["u"], required_approvers=1,
            correlation_mode="sidecar_file", run_request_asset_selection=["a"],
        )


def test_target_run_request_requires_a_job_name_or_asset_selection(mod):
    with pytest.raises(ValueError, match="run_request_asset_selection"):
        mod.TeamsApprovalGateComponent(
            asset_name="gate", upstream_asset_key="u", target="run_request",
            teams_tenant_id_env_var="A", teams_client_id_env_var="B", teams_client_secret_env_var="C",
            teams_team_id="T1", teams_channel_id="C1", approver_allowlist=["u"], required_approvers=1,
        )


def test_target_run_request_does_not_require_approval_dir(mod):
    comp = mod.TeamsApprovalGateComponent(
        asset_name="gate", upstream_asset_key="u", target="run_request",
        teams_tenant_id_env_var="A", teams_client_id_env_var="B", teams_client_secret_env_var="C",
        teams_team_id="T1", teams_channel_id="C1", approver_allowlist=["u"], required_approvers=1,
        run_request_asset_selection=["a"],
    )
    assert comp.approval_dir is None


def test_target_run_request_fires_a_run_request_with_decision_tags_and_writes_no_file(mod, monkeypatch, tmp_path):
    channel = _FakeTeamsChannel()
    _patch_graph(monkeypatch, mod, channel)
    comp, defs, _ = _post(
        mod, monkeypatch, tmp_path, channel, target="run_request",
        run_request_mode="asset_selection", run_request_asset_selection=["downstream_asset"],
        approval_dir=None,
    )
    watcher = next(s for s in defs.sensors if s.name == "gate_watcher")
    [mid] = channel.messages.keys()
    channel.vote(mid, "like", "alice-guid")

    ctx = dg.build_sensor_context()
    out = watcher(ctx)
    assert list(tmp_path.iterdir()) == [], "target: run_request must write no file at all"
    [rr] = out.run_requests
    assert rr.tags["approval/approved"] == "true"
    assert rr.tags["approval/approver"] == "alice-guid"
    assert rr.asset_selection == [dg.AssetKey("downstream_asset")]
    assert rr.run_key == f"gate_watcher:T1:C1:{mid}"


def test_escalation_fires_once_tracked_via_cursor_not_a_sidecar(mod, monkeypatch, tmp_path):
    channel = _FakeTeamsChannel()
    _patch_graph(monkeypatch, mod, channel)
    comp, defs, _ = _post(
        mod, monkeypatch, tmp_path, channel,
        timeout_hours=0.0, on_timeout="escalate", escalate_teams_user="lead-guid",
    )
    watcher = next(s for s in defs.sensors if s.name == "gate_watcher")
    ctx = dg.build_sensor_context()
    out1 = watcher(ctx)
    assert "escalated" in out1.skip_reason.skip_message
    [mid] = channel.messages.keys()
    assert len(channel.messages[mid]["replies"]) == 1

    ctx2 = dg.build_sensor_context(cursor=out1.cursor)
    watcher(ctx2)
    assert len(channel.messages[mid]["replies"]) == 1, "escalation must fire exactly once, not every tick"
