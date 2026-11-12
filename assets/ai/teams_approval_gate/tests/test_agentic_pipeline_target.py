"""target: agentic_pipeline -- the Teams gate watches `<approval_dir>/pending/*.json` (agentic_pipeline's own
`approve` op review queue, mode: park) instead of posting an upstream asset, and on quorum writes the decision
through this file's own `_approve_pending_record()` (mirrors agentic_pipeline's `approve_pending()` contract).

The end-to-end test below runs a REAL agentic_pipeline pipeline (loaded from its own component.py by path) until
it parks, drives the Teams gate's sensor against the real pending record on disk (only the Graph HTTP calls --
`_get_teams_bearer`/`_post_teams_message`/`_get_teams_replies` -- are faked), confirms the token it writes carries
the correct fingerprint, then runs agentic_pipeline's OWN `<prefix>_approval_resume_sensor` and confirms the
requested run actually completes."""
import importlib.util
import json
import pathlib
import sys
import types
from types import SimpleNamespace

import dagster as dg
import pytest

from .conftest import load_component_module

HERE = pathlib.Path(__file__).resolve().parent
AGENTIC_PIPELINE_PY = HERE.parents[1] / "agentic_pipeline" / "component.py"


def _load_agentic_pipeline(monkeypatch):
    if not AGENTIC_PIPELINE_PY.exists():
        pytest.skip("agentic_pipeline not present")

    fake_litellm = types.ModuleType("litellm")
    calls = []

    def completion(**kwargs):
        calls.append(kwargs)
        prompt = json.dumps(kwargs.get("messages"))
        content = "DRAFT-v1" if "Draft:" in prompt else f"PUBLISHED({prompt.split('Publish:')[-1][:30]})"
        msg = SimpleNamespace(content=content, tool_calls=[], reasoning_content=None, provider_specific_fields=None)
        usage = SimpleNamespace(total_tokens=10, prompt_tokens=5, completion_tokens=5, model_dump=lambda: {"total_tokens": 10})
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)], usage=usage)

    fake_litellm.completion = completion
    fake_litellm.completion_cost = lambda completion_response=None: 0.01
    fake_litellm.drop_params = False
    monkeypatch.setitem(sys.modules, "litellm", fake_litellm)
    monkeypatch.setenv("OPENAI_API_KEY", "k")

    spec = importlib.util.spec_from_file_location("agentic_pipeline_for_teams_gate_test", AGENTIC_PIPELINE_PY)
    ap = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(ap)
    except Exception as e:
        pytest.skip(f"agentic_pipeline not importable right now: {e}")
    return ap


def _fake_graph(monkeypatch, mod, *, replies_by_message_id):
    posted = []
    state = {"n": 0}

    def fake_bearer(fs, tenant_env, client_id_env, client_secret_env, refresh_token_env):
        return "fake-bearer"

    def fake_post(bearer, team_id, channel_id, html):
        state["n"] += 1
        mid = f"msg-{state['n']}"
        posted.append({"team_id": team_id, "channel_id": channel_id, "html": html, "message_id": mid})
        return {"message_id": mid, "web_url": f"https://teams.example/{mid}"}

    def fake_replies(bearer, team_id, channel_id, message_id):
        return replies_by_message_id.get(message_id, [])

    monkeypatch.setattr(mod, "_get_teams_bearer", fake_bearer)
    monkeypatch.setattr(mod, "_post_teams_message", fake_post)
    monkeypatch.setattr(mod, "_get_teams_replies", fake_replies)
    return posted


def test_a_real_parked_pipeline_is_approved_through_the_teams_gate_and_the_resume_run_completes(monkeypatch, tmp_path):
    mod = load_component_module()
    ap = _load_agentic_pipeline(monkeypatch)
    approval_dir = str(tmp_path / "approvals")

    pipeline = ap.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "doc"},
        steps=[
            {"id": "draft", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY", "prompt_template": "Draft: {text}"},
            {"id": "gate", "op": "approve", "source": "draft", "approval_dir": approval_dir, "mode": "park", "key": "k1"},
            {"id": "publish", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY", "prompt_template": "Publish: {text}", "source": "gate"},
        ],
        outputs={"assets": ["draft", "gate", "publish"]},
    )
    pdefs = pipeline.build_defs(None)
    instance = dg.DagsterInstance.ephemeral()

    result = dg.materialize(pdefs.assets, instance=instance)
    assert result.success
    pending_path = tmp_path / "approvals" / "pending" / "k1.json"
    assert pending_path.exists()
    pending = json.loads(pending_path.read_text())
    assert pending["summary"]

    gate = mod.TeamsApprovalGateComponent(
        asset_name="gate_teams", target="agentic_pipeline", approval_dir=approval_dir,
        teams_team_id="T1", teams_channel_id="C1",
        approver_allowlist=["U1"], required_approvers=1, poll_interval_seconds=1, mode="reply",
    )
    gdefs = gate.build_defs(None)
    watcher = gdefs.get_sensor_def("gate_teams_pending_watcher")

    replies_by_message_id = {}
    posted = _fake_graph(monkeypatch, mod, replies_by_message_id=replies_by_message_id)

    gctx = dg.build_sensor_context(instance=instance, definitions=gdefs)
    out = watcher(gctx)
    skip_msg = out.skip_reason if isinstance(out, dg.SensorResult) else out.skip_message
    assert "k1: posted" in skip_msg
    assert len(posted) == 1 and pending["summary"][:30] in posted[0]["html"]

    replies_by_message_id[posted[0]["message_id"]] = [{"from": {"user": {"id": "U1"}}, "body": {"content": "APPROVE"}}]
    out = watcher(gctx)
    skip_msg = out.skip_reason if isinstance(out, dg.SensorResult) else out.skip_message
    assert "k1: wrote decision (approved)" in skip_msg

    token_path = tmp_path / "approvals" / "k1.json"
    assert token_path.exists()
    token = json.loads(token_path.read_text())
    assert token["approved"] is True
    assert token["fingerprint"] == pending["fingerprint"]

    resume_sensor = pdefs.get_sensor_def("p_approval_resume_sensor")
    rctx = dg.build_sensor_context(instance=instance, definitions=pdefs)
    sensor_out = resume_sensor(rctx)
    run_requests = list(sensor_out.run_requests or []) if isinstance(sensor_out, dg.SensorResult) else list(sensor_out)
    assert len(run_requests) == 1
    rr = run_requests[0]

    resumed = dg.materialize(pdefs.assets, instance=instance, tags=rr.tags, raise_on_error=True)
    assert resumed.success
    published = [e for e in resumed.get_asset_materialization_events() if e.materialization.asset_key == dg.AssetKey("p_publish")]
    assert published
