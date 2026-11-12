"""Two real gaps this patch closes:

1. `_get_teams_bearer` used `grant_type=client_credentials` (app-only) -- confirmed directly against Microsoft
   Graph's own docs that sending a channel message has no working application permission (only
   `Teamwork.Migrate.All`, migration-scoped); `ChannelMessage.Send` is delegated-only. Posting with the old
   client-credentials token would have failed with a real 403 against any real tenant. Fixed to a real OAuth
   refresh-token grant, with rotation handling (Azure AD may return a new refresh token on any call).
2. No `target: agentic_pipeline` mode existed at all -- agentic_pipeline's own `approve`/`mode: park` review queue
   had no Teams gate to watch it, only the separate HumanApprovalGateComponent composition path.
"""
import pytest

from .conftest import load_component_module


def test_get_teams_bearer_uses_refresh_token_grant_not_client_credentials(tmp_path, monkeypatch):
    mod = load_component_module()
    fs = mod._ApprovalFS(str(tmp_path))
    captured = {}

    class _FakeResp:
        ok = True
        def raise_for_status(self):
            pass
        def json(self):
            return {"access_token": "tok-1", "refresh_token": "seed-token"}

    class _FakeRequests:
        @staticmethod
        def post(url, data=None, timeout=None):
            captured["data"] = data
            return _FakeResp()

    monkeypatch.setitem(__import__("sys").modules, "requests", _FakeRequests)
    monkeypatch.setenv("T", "tenant"); monkeypatch.setenv("C", "client"); monkeypatch.setenv("S", "secret"); monkeypatch.setenv("R", "seed-token")
    token = mod._get_teams_bearer(fs, "T", "C", "S", "R")
    assert token == "tok-1"
    assert captured["data"]["grant_type"] == "refresh_token"
    assert captured["data"]["refresh_token"] == "seed-token"
    assert "client_credentials" not in captured["data"].values()


def test_get_teams_bearer_persists_a_rotated_refresh_token(tmp_path, monkeypatch):
    mod = load_component_module()
    fs = mod._ApprovalFS(str(tmp_path))
    calls = []

    class _FakeResp:
        ok = True
        def raise_for_status(self):
            pass
        def json(self):
            calls.append(1)
            return {"access_token": f"tok-{len(calls)}", "refresh_token": "rotated-token"}

    class _FakeRequests:
        @staticmethod
        def post(url, data=None, timeout=None):
            expected = "seed-token" if len(calls) == 0 else "rotated-token"
            assert data["refresh_token"] == expected
            return _FakeResp()

    monkeypatch.setitem(__import__("sys").modules, "requests", _FakeRequests)
    monkeypatch.setenv("T", "tenant"); monkeypatch.setenv("C", "client"); monkeypatch.setenv("S", "secret"); monkeypatch.setenv("R", "seed-token")

    mod._get_teams_bearer(fs, "T", "C", "S", "R")
    saved = fs.read_json(mod._oauth_state_path(fs))
    assert saved["refresh_token"] == "rotated-token"

    mod._get_teams_bearer(fs, "T", "C", "S", "R")  # must use the rotated token this time -- asserted inside post()


def test_get_teams_bearer_requires_refresh_token_env_var(tmp_path, monkeypatch):
    mod = load_component_module()
    fs = mod._ApprovalFS(str(tmp_path))
    monkeypatch.setenv("T", "tenant"); monkeypatch.setenv("C", "client"); monkeypatch.setenv("S", "secret")
    monkeypatch.delenv("R_UNSET", raising=False)
    with pytest.raises(RuntimeError, match="R_UNSET"):
        mod._get_teams_bearer(fs, "T", "C", "S", "R_UNSET")


def test_target_agentic_pipeline_does_not_require_upstream_asset_key(tmp_path):
    mod = load_component_module()
    comp = mod.TeamsApprovalGateComponent(
        asset_name="gate_teams", target="agentic_pipeline", approval_dir=str(tmp_path),
        teams_team_id="T1", teams_channel_id="C1", approver_allowlist=["U1"], required_approvers=1,
    )
    defs = comp.build_defs(None)
    assert [s.name for s in defs.sensors] == ["gate_teams_pending_watcher"]
    assert not defs.assets


def test_target_human_approval_gate_still_requires_upstream_asset_key(tmp_path):
    mod = load_component_module()
    comp = mod.TeamsApprovalGateComponent(
        asset_name="gate_teams", approval_dir=str(tmp_path),
        teams_team_id="T1", teams_channel_id="C1", approver_allowlist=["U1"], required_approvers=1,
    )
    with pytest.raises(ValueError, match="upstream_asset_key"):
        comp.build_defs(None)
