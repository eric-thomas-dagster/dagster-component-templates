"""The shipped governed_invoice_matching example, end to end through a real materialization (only the model call is scripted)."""
import json
import pathlib

import dagster as dg
import pytest
import yaml

EXAMPLE = pathlib.Path(__file__).resolve().parent.parent / "examples" / "governed_invoice_matching" / "pipeline.yaml"
_SUMMARY = "Invoice INV-2291 ($4,180.00, Acme Industrial Supply) has no purchase order. Approve payment?"
GOOD_FACTS = json.dumps({"invoice_no": "INV-2291", "vendor": "Acme", "total": 4180, "po_number": None, "lines": []})


def component(mod, tmp_path):
    cfg = yaml.safe_load(EXAMPLE.read_text().replace("./approvals", str(tmp_path / "approvals")))["attributes"]
    return mod.AgenticPipelineComponent(**cfg)


def decision(outcome):
    return json.dumps({"outcome": outcome, "reason": "PO number missing" if outcome == "needs_review" else "all facts present",
                       "confidence": 0.9, "evidence": [{"source": "facts", "quote": "po_number: null"}]})


VERIFIED = json.dumps({"verdict": "supported", "score": 0.95, "unsupported_claims": []})


def failure_message(result):
    return " ".join(e.event_specific_data.error.message + (e.event_specific_data.error.cause.message if e.event_specific_data.error.cause else "")
                    for e in result.get_step_failure_events())


def metadata(result):
    out = {}
    for e in result.get_asset_materialization_events():
        out.update(e.materialization.metadata)
    return out


def test_example_validates_and_builds(mod, tmp_path):
    assert component(mod, tmp_path).build_defs(None).assets


def test_exception_path_is_stopped_at_the_gate_with_a_review_queue_entry(mod, fake_litellm, tmp_path, monkeypatch):
    monkeypatch.setenv("OLLAMA_API_BASE", "http://localhost:11434")
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    fake_litellm.reply("sorry, here is some prose and no json")      # cheap local model fails the json acceptance
    fake_litellm.reply(GOOD_FACTS)                                    # escalated model succeeds
    fake_litellm.reply(decision("needs_review"))
    fake_litellm.reply(VERIFIED)
    result = dg.materialize(component(mod, tmp_path).build_defs(None).assets, raise_on_error=False)
    assert not result.success                                         # no approval token => the safe default: do not pay
    assert "timeout_default_reject" in failure_message(result)         # it failed AT THE GATE, for the right reason
    pending = json.loads((tmp_path / "approvals" / "pending" / "INV-2291.json").read_text())
    assert pending["outcome"] == "timeout_default_reject"              # stays queued (marked) so a reviewer can still approve it as written
    assert [c["model"] for c in fake_litellm.calls][:2] == ["ollama/qwen3:8b", "gpt-4o-mini"]   # cascade order
    assert len(fake_litellm.calls) == 4                                # facts x2, decision, verify -- and NO audit-record call


def test_exception_path_continues_once_a_human_approves(mod, fake_litellm, tmp_path, monkeypatch):
    monkeypatch.setenv("OLLAMA_API_BASE", "http://localhost:11434")
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    (tmp_path / "approvals" / "pending").mkdir(parents=True)
    # the reviewer approves the pending record as the example ships it (token copies the record's fingerprint)
    fingerprint = mod._approval_fingerprint("gate", "", _SUMMARY, ["summary"])
    (tmp_path / "approvals" / "INV-2291.json").write_text(json.dumps({
        "approved": True, "approver": "eric", "reason": "PO confirmed by phone", "fingerprint": fingerprint, "approved_at": mod._now_iso()}))
    fake_litellm.reply(GOOD_FACTS)                                    # the cheap model is fine this time
    fake_litellm.reply(decision("needs_review"))
    fake_litellm.reply(VERIFIED)
    fake_litellm.reply("INV-2291 needs_review approved by eric")
    result = dg.materialize(component(mod, tmp_path).build_defs(None).assets, raise_on_error=False)
    assert result.success
    md = metadata(result)
    assert md["facts__model_used"].value == "ollama/qwen3:8b" and md["facts__n_escalations"].value == "0"
    assert md["decision__outcome"].value == "needs_review"
    assert md["gate__approved"].value == "True" and md["gate__approver"].value == "eric"
    # reuse: once -- the approval was spent by that run
    assert json.loads((tmp_path / "approvals" / "INV-2291.json").read_text())["consumed_at"]
    assert "INV-2291" in md["record__text"].value
    assert md["record__policy_usage"].value["llm_calls"] == 4


def test_routine_invoice_never_reaches_a_human(mod, fake_litellm, tmp_path, monkeypatch):
    monkeypatch.setenv("OLLAMA_API_BASE", "http://localhost:11434")
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    fake_litellm.reply(GOOD_FACTS)
    fake_litellm.reply(decision("auto_pay"))
    fake_litellm.reply(VERIFIED)
    fake_litellm.reply("INV-2291 auto_pay")
    result = dg.materialize(component(mod, tmp_path).build_defs(None).assets, raise_on_error=False)
    assert result.success
    md = metadata(result)
    assert md["gate__skipped"].value == "True"                        # the gate was skipped: no human, no queue entry
    assert not (tmp_path / "approvals" / "pending").exists()
    assert md["record__text"].value.startswith("INV-2291")


def test_rejected_decision_is_never_recorded_as_paid(mod, fake_litellm, tmp_path, monkeypatch):
    monkeypatch.setenv("OLLAMA_API_BASE", "http://localhost:11434")
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    fake_litellm.reply(GOOD_FACTS)
    fake_litellm.reply(decision("reject"))
    fake_litellm.reply(VERIFIED)
    result = dg.materialize(component(mod, tmp_path).build_defs(None).assets, raise_on_error=False)
    assert result.success and metadata(result)["record__skipped"].value == "True"
    assert len(fake_litellm.calls) == 3                               # no audit-record model call was made for a rejection


def test_budget_and_env_policy_are_enforced_in_the_example(mod, fake_litellm, tmp_path, monkeypatch):
    monkeypatch.setenv("OLLAMA_API_BASE", "http://localhost:11434")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)               # allowed by policy but not set => a clear error, not a leak
    fake_litellm.reply(GOOD_FACTS)
    fake_litellm.reply(decision("auto_pay"))
    result = dg.materialize(component(mod, tmp_path).build_defs(None).assets, raise_on_error=False)
    assert not result.success
    assert "OPENAI_API_KEY" in failure_message(result)                 # a clear error naming the variable
