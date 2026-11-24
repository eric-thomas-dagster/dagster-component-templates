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

    component = mod.LiteLLMAgentComponent(asset_name="agent_out", prompt="hi")
    assets = component.build_defs(None).assets
    assert {a.key.to_user_string() for a in assets} == {"agent_out"}


def test_outcome_asset_appears_when_enabled(mod, monkeypatch):
    async def fake_run_agent(**kwargs):
        return _fake_result()

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)

    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", outcome_ledger_path="/tmp/does-not-matter", emit_outcome_asset=True,
    )
    assets = component.build_defs(None).assets
    assert {a.key.to_user_string() for a in assets} == {"agent_out", "agent_out_outcome"}


def test_outcome_asset_with_no_ledger_entries_is_nothing_new_and_empty(mod, tmp_path, monkeypatch):
    async def fake_run_agent(**kwargs):
        return _fake_result("nothing to report")

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)

    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", outcome_ledger_path=str(tmp_path / "ledger"), emit_outcome_asset=True,
    )
    assets = component.build_defs(None).assets
    result = dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    outcome = result.output_for_node("agent_out_outcome")
    assert outcome == {"status": "nothing_new", "filed": [], "would_file": [], "refused": [], "decisions_for_you": [], "summary": "nothing to report", "text": "nothing to report"}


def test_outcome_asset_reads_this_runs_ledger_entries(mod, tmp_path, monkeypatch):
    """The realistic case: a ledger-aware tool server writes an entry mid-run, keyed by THIS run's own
    run_id -- simulated by having the monkeypatched _run_agent write it itself, then asserting the
    companion asset's real output reflects it."""
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

    component = mod.LiteLLMAgentComponent(
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


def test_outcome_asset_drafts_metadata_shows_the_full_body(mod, tmp_path, monkeypatch):
    """Hooli feedback: a drafts entry on the outcome asset showing each issue/PR filed, or
    drafted in practice mode, as markdown with its full body -- so a person can read what an
    agent actually filed without digging through the run's own tool-call logs."""
    ledger_dir = tmp_path / "ledger"

    async def fake_run_agent(**kwargs):
        run_id = kwargs["run_id"]
        mod.write_outcome_ledger_entry(str(ledger_dir), run_id, {
            "bucket": "filed", "tool": "open_issue", "kind": "github_issue",
            "title": "Flaky retry in orders pipeline", "body": "Full repro steps here.",
            "url": "https://github.com/x/y/issues/42",
        })
        mod.write_outcome_ledger_entry(str(ledger_dir), run_id, {
            "bucket": "would_file", "tool": "propose_fix", "component": "github_pr_tools",
            "title": "Fix the retry bug", "body": "Here's the proposed diff.",
        })
        return _fake_result("filed one, drafted another")

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)

    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", outcome_ledger_path=str(ledger_dir), emit_outcome_asset=True,
    )
    result = dg.materialize(component.build_defs(None).assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    mats = result.asset_materializations_for_node("agent_out_outcome")
    drafts = mats[0].metadata["drafts"].value
    assert "## Flaky retry in orders pipeline" in drafts
    assert "Full repro steps here." in drafts
    assert "https://github.com/x/y/issues/42" in drafts
    assert "## Fix the retry bug" in drafts
    assert "Practice mode, not filed" in drafts
    assert "Here's the proposed diff." in drafts


def test_outcome_asset_drafts_metadata_says_so_when_nothing_was_filed(mod, tmp_path, monkeypatch):
    async def fake_run_agent(**kwargs):
        return _fake_result("nothing to report")

    monkeypatch.setattr(mod, "_run_agent", fake_run_agent)

    component = mod.LiteLLMAgentComponent(
        asset_name="agent_out", prompt="hi", outcome_ledger_path=str(tmp_path / "ledger"), emit_outcome_asset=True,
    )
    result = dg.materialize(component.build_defs(None).assets, resources={"io_manager": dg.InMemoryIOManager()})
    mats = result.asset_materializations_for_node("agent_out_outcome")
    assert mats[0].metadata["drafts"].value == "Nothing was filed or drafted."
