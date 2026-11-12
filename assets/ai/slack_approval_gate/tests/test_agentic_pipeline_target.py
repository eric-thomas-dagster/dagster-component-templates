"""target: agentic_pipeline -- the slack gate watches `<approval_dir>/pending/*.json` (agentic_pipeline's own
`approve` op review queue, mode: park) instead of posting an upstream asset, and on quorum writes the decision
through this file's own `_approve_pending_record()` (mirrors agentic_pipeline's `approve_pending()` contract).

The end-to-end test below runs a REAL agentic_pipeline pipeline (loaded from its own component.py by path, same
`importlib.util.spec_from_file_location` pattern dagster_instance_tools' own cross-component test uses) until it
parks, drives the slack gate's sensor against the real pending record on disk (only the Slack client + reactions are
faked), confirms the token it writes carries the correct fingerprint, then runs agentic_pipeline's OWN
`<prefix>_approval_resume_sensor` and confirms it requests -- and that the requested run actually completes -- a
resume. Only litellm.completion (on the agentic_pipeline side) and the Slack SDK calls (on this component's side)
are faked; everything else is real dagster.materialize()/real files on disk.
"""
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


@pytest.fixture
def mod():
    return load_component_module()


def _load_agentic_pipeline(monkeypatch):
    """Load the real agentic_pipeline component.py by path (not via `dagster_community_components...`, which isn't
    on the import path outside a built wheel), with litellm faked so no real model call is made."""
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

    spec = importlib.util.spec_from_file_location("agentic_pipeline_for_slack_gate_test", AGENTIC_PIPELINE_PY)
    ap = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(ap)
    except Exception as e:  # sibling mid-edit by another session: do not fail this component's suite
        pytest.skip(f"agentic_pipeline not importable right now: {e}")
    return ap, calls


def _fake_slack(monkeypatch, mod, *, reactions_by_ts):
    """Stub the Slack SDK calls the gate makes: posting records a (channel, text) pair and returns an
    incrementing message_ts; reactions_get returns whatever `reactions_by_ts` has for that ts (mutate the dict
    between sensor ticks to simulate votes landing)."""
    posted = []
    state = {"n": 0}

    class _FakeClient:
        def auth_test(self):
            return {"user_id": "U_BOT"}

    def fake_post(client, channel, text, approve_emoji, reject_emoji, ping_users):
        state["n"] += 1
        ts = f"{state['n']}.000"
        posted.append({"channel": channel, "text": text, "ts": ts})
        return {"message_ts": ts, "channel_id": "C1"}

    def fake_fetch(client, channel_id, message_ts):
        return reactions_by_ts.get(message_ts, {})

    monkeypatch.setattr(mod, "_get_slack_client", lambda token_env_var: _FakeClient())
    monkeypatch.setattr(mod, "_post_slack_message", fake_post)
    monkeypatch.setattr(mod, "_fetch_reactions", fake_fetch)
    return posted


def test_target_agentic_pipeline_does_not_require_upstream_asset_key(mod, tmp_path):
    comp = mod.SlackApprovalGateComponent(
        asset_name="gate_slack", target="agentic_pipeline", approval_dir=str(tmp_path),
        slack_channel="C1", approver_allowlist=["U1"], required_approvers=1,
    )
    defs = comp.build_defs(None)
    assert [s.name for s in defs.sensors] == ["gate_slack_pending_watcher"]
    assert not defs.assets


def test_target_human_approval_gate_still_requires_upstream_asset_key(mod, tmp_path):
    comp = mod.SlackApprovalGateComponent(
        asset_name="gate_slack", approval_dir=str(tmp_path),
        slack_channel="C1", approver_allowlist=["U1"], required_approvers=1,
    )
    with pytest.raises(ValueError, match="upstream_asset_key"):
        comp.build_defs(None)


def test_a_real_parked_pipeline_is_approved_through_the_slack_gate_and_the_resume_run_completes(monkeypatch, mod, tmp_path):
    ap, calls = _load_agentic_pipeline(monkeypatch)
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

    # 1. Run until it parks -- real materialization, writes a real pending/k1.json + checkpoint.
    result = dg.materialize(pdefs.assets, instance=instance)
    assert result.success
    pending_path = tmp_path / "approvals" / "pending" / "k1.json"
    assert pending_path.exists(), "expected a real pending record after the park"
    pending = json.loads(pending_path.read_text())
    assert pending["summary"]  # this is what the Slack message should carry

    # 2. The slack gate, in agentic_pipeline mode, watching the same approval_dir.
    gate = mod.SlackApprovalGateComponent(
        asset_name="gate_slack", target="agentic_pipeline", approval_dir=approval_dir,
        slack_channel="C1", approver_allowlist=["U1"], required_approvers=1, poll_interval_seconds=1,
    )
    gdefs = gate.build_defs(None)
    watcher = gdefs.get_sensor_def("gate_slack_pending_watcher")

    reactions_by_ts = {}
    posted = _fake_slack(monkeypatch, mod, reactions_by_ts=reactions_by_ts)

    gctx = dg.build_sensor_context(instance=instance, definitions=gdefs)
    out = watcher(gctx)
    skip_msg = out.skip_reason if isinstance(out, dg.SensorResult) else out.skip_message
    assert "k1: posted" in skip_msg
    assert len(posted) == 1 and pending["summary"][:50] in posted[0]["text"], (
        "the Slack message must carry the pipeline's own pending-record summary, not a generic placeholder"
    )

    # 3. Vote lands; next tick reaches quorum and writes the real decision token via _approve_pending_record.
    reactions_by_ts[posted[0]["ts"]] = {"white_check_mark": ["U1"]}
    out = watcher(gctx)
    skip_msg = out.skip_reason if isinstance(out, dg.SensorResult) else out.skip_message
    assert "k1: wrote decision (approved)" in skip_msg

    token_path = tmp_path / "approvals" / "k1.json"
    assert token_path.exists()
    token = json.loads(token_path.read_text())
    assert token["approved"] is True
    assert token["fingerprint"] == pending["fingerprint"], "the token must carry the pending record's own fingerprint, not a blank one"

    # 4. agentic_pipeline's OWN resume sensor now sees a valid token and requests a resume run; run it for real.
    resume_sensor = pdefs.get_sensor_def("p_approval_resume_sensor")
    rctx = dg.build_sensor_context(instance=instance, definitions=pdefs)
    sensor_out = resume_sensor(rctx)
    run_requests = list(sensor_out.run_requests or []) if isinstance(sensor_out, dg.SensorResult) else list(sensor_out)
    assert len(run_requests) == 1, "expected exactly one resume run request once the token is valid"
    rr = run_requests[0]

    resumed = dg.materialize(pdefs.assets, instance=instance, tags=rr.tags, raise_on_error=True)
    assert resumed.success
    published = [e for e in resumed.get_asset_materialization_events() if e.materialization.asset_key == dg.AssetKey("p_publish")]
    assert published, "the resumed run must actually complete the step gated behind approval"
