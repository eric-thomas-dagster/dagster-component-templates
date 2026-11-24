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
    return {"final_answer": answer, "iterations": 1, "tool_calls_made": 1, "stopped_reason": "end_turn", "tool_call_details": []}


def test_outcome_asset_is_built_only_when_enabled(mod, monkeypatch):
    async def fake_run_agent(**kwargs):
        return _fake_result()

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-fake")

    component = mod.OpenAIAgentComponent(asset_name="agent_out", prompt="hi")
    assets = component.build_defs(None).assets
    assert {a.key.to_user_string() for a in assets} == {"agent_out"}


def test_outcome_asset_appears_when_enabled(mod, monkeypatch):
    async def fake_run_agent(**kwargs):
        return _fake_result()

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-fake")

    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", outcome_ledger_path="/tmp/does-not-matter", emit_outcome_asset=True,
    )
    assets = component.build_defs(None).assets
    assert {a.key.to_user_string() for a in assets} == {"agent_out", "agent_out_outcome"}


def test_outcome_asset_with_no_ledger_entries_is_nothing_new_and_empty(mod, tmp_path, monkeypatch):
    """No entries at all (e.g. a tool-less agent, or every ledger_path/run_id mismatched) still
    produces a valid outcome: empty buckets, status 'nothing_new', final_answer carried through as text."""
    async def fake_run_agent(**kwargs):
        return _fake_result("nothing to report")

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-fake")

    component = mod.OpenAIAgentComponent(
        asset_name="agent_out", prompt="hi", outcome_ledger_path=str(tmp_path / "ledger"), emit_outcome_asset=True,
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    outcome = result.output_for_node("agent_out_outcome")
    assert outcome == {"status": "nothing_new", "filed": [], "would_file": [], "refused": [], "decisions_for_you": [], "summary": "nothing to report", "text": "nothing to report"}


def test_outcome_asset_reads_this_runs_ledger_entries(mod, tmp_path, monkeypatch):
    """The realistic case: a ledger-aware tool server (github_pr_tools/linear_ticket_tools) writes an
    entry mid-run, keyed by THIS run's own run_id -- simulated here by having the monkeypatched
    _run_agent itself write the ledger entry (standing in for what the real tool server's `call()`
    does during the main asset's own execution), using context.run_id, then asserting the companion
    asset's real output reflects it. Proves the two assets agree on which run_id to use, not just
    that summarize_outcome_ledger works in isolation."""
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
    monkeypatch.setenv("OPENAI_API_KEY", "sk-fake")

    component = mod.OpenAIAgentComponent(
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

    # And the file on disk is exactly the plain JSON-lines shape the ledger-writing side promises.
    [ledger_file] = list(ledger_dir.glob("*.jsonl"))
    lines = [json.loads(line) for line in ledger_file.read_text().splitlines()]
    assert {e["bucket"] for e in lines} == {"decision_needed", "filed"}
