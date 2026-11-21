"""emit_outcome_asset builds a companion '{asset_name}_outcome' asset that reads this run's own
outcome-ledger entries (written by a ledger-aware tool server like github_pr_tools/linear_ticket_tools,
keyed by run_id -- see each component's own `write_outcome_ledger_entry` calls) and collapses them into
{status, filed, would_file, refused, decisions_for_you, text}. Real `dg.materialize` round trip (the
companion asset depends on the main one via AssetIn), not a mock of the summarize logic."""
import json

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _fake_result(answer="done"):
    return {
        "final_answer": answer, "iterations": 1, "tool_calls_made": 1, "stopped_reason": "end_turn",
        "tool_call_details": [], "loaded_skill_names": [], "skill_load_count": 0,
    }


def test_outcome_asset_is_built_only_when_enabled(mod, monkeypatch):
    async def fake_run_agent(**kwargs):
        return _fake_result()

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    component = mod.AnthropicAgentComponent(asset_name="agent_out", prompt="hi")
    assets = component.build_defs(None).assets
    assert {a.key.to_user_string() for a in assets} == {"agent_out"}


def test_outcome_asset_appears_when_enabled(mod, monkeypatch):
    async def fake_run_agent(**kwargs):
        return _fake_result()

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="hi", outcome_ledger_path="/tmp/does-not-matter", emit_outcome_asset=True,
    )
    assets = component.build_defs(None).assets
    assert {a.key.to_user_string() for a in assets} == {"agent_out", "agent_out_outcome"}


def test_outcome_asset_with_no_ledger_entries_is_ok_and_empty(mod, tmp_path, monkeypatch):
    async def fake_run_agent(**kwargs):
        return _fake_result("nothing to report")

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="hi", outcome_ledger_path=str(tmp_path / "ledger"), emit_outcome_asset=True,
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    outcome = result.output_for_node("agent_out_outcome")
    assert outcome == {"status": "ok", "filed": [], "would_file": [], "refused": [], "decisions_for_you": [], "text": "nothing to report"}


def test_outcome_asset_reads_this_runs_ledger_entries(mod, tmp_path, monkeypatch):
    """The realistic case: a ledger-aware tool server writes an entry mid-run, keyed by THIS run's own
    run_id -- simulated by having the monkeypatched _run_agent write it itself, using context.run.run_id
    (anthropic_agent's own non-deprecated form), then asserting the companion asset's real output
    reflects it."""
    ledger_dir = tmp_path / "ledger"

    async def fake_run_agent(**kwargs):
        run_id = kwargs["run_id"]
        mod.write_outcome_ledger_entry(str(ledger_dir), run_id, {
            "bucket": "decision_needed", "tool": "review_pull_request", "reason": "Kind: improvement",
        })
        mod.write_outcome_ledger_entry(str(ledger_dir), run_id, {
            "bucket": "filed", "tool": "propose_fix", "url": "https://github.com/x/y/pull/1",
        })
        return _fake_result("filed the pr, one needs a human")

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    component = mod.AnthropicAgentComponent(
        asset_name="agent_out", prompt="hi", outcome_ledger_path=str(ledger_dir), emit_outcome_asset=True,
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    outcome = result.output_for_node("agent_out_outcome")
    assert outcome["status"] == "needs_human"
    assert [{"tool": e["tool"], "url": e["url"]} for e in outcome["filed"]] == [{"tool": "propose_fix", "url": "https://github.com/x/y/pull/1"}]
    assert [{"tool": e["tool"], "reason": e["reason"]} for e in outcome["decisions_for_you"]] == [{"tool": "review_pull_request", "reason": "Kind: improvement"}]
    assert outcome["text"] == "filed the pr, one needs a human"

    [ledger_file] = list(ledger_dir.glob("*.jsonl"))
    lines = [json.loads(line) for line in ledger_file.read_text().splitlines()]
    assert {e["bucket"] for e in lines} == {"decision_needed", "filed"}
