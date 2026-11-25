"""`emit_outcome_asset`/`outcome_ledger_path`/`decisions_marker`: a companion `<prefix>_outcome`
asset that collapses a run's outcome-ledger entries (written by a ledger-aware tool server, e.g.
github_pr_tools/linear_ticket_tools) into {status, filed, would_file, refused, decisions_for_you,
text} -- same shape litellm_agent's own emit_outcome_asset already produces, ported via the
byte-identical inlined ledger block (see tests/test_agent_outcome_ledger_drift.py). Depends on
the pipeline's LAST declared output asset (outputs.assets[-1]), the same "last = natural final
step" convention `_last_step_id` already uses for sub_pipeline."""
import dagster as dg
import pytest

LLM_STEP = {"id": "a", "op": "llm_call", "model": "gpt-4o-mini"}


def _outcome_fn(mod, outcome_ledger_path, decisions_marker="decisions for you"):
    defs = mod._build_outcome_asset_defs(
        prefix="p", asset_ids=["a"], partitions_def=None, group_name="agents",
        outcome_ledger_path=outcome_ledger_path, decisions_marker=decisions_marker,
    )
    assets = defs.assets
    assert len(assets) == 1
    return assets[0]


def test_filed_entry_gives_status_filed_and_one_draft(mod, tmp_path):
    ledger = str(tmp_path / "ledger")
    mod.write_outcome_ledger_entry(ledger, "EPHEMERAL", {
        "bucket": "filed", "title": "Fix the thing", "body": "diff here", "kind": "issue", "url": "http://x/1",
    })
    outcome_asset = _outcome_fn(mod, ledger)
    ctx = dg.build_asset_context()
    out = outcome_asset(ctx, pipeline_result={"text": "All done."})

    assert out["status"] == "filed"
    assert len(out["filed"]) == 1
    assert out["filed"][0]["title"] == "Fix the thing"
    assert out["would_file"] == []
    assert out["text"] == "All done."


def test_would_file_only_entries_give_status_practice(mod, tmp_path):
    ledger = str(tmp_path / "ledger")
    mod.write_outcome_ledger_entry(ledger, "EPHEMERAL", {
        "bucket": "would_file", "title": "Would fix", "body": "draft", "component": "github_pr_tools",
    })
    outcome_asset = _outcome_fn(mod, ledger)
    ctx = dg.build_asset_context()
    out = outcome_asset(ctx, pipeline_result={"text": "Draft only."})

    assert out["status"] == "practice"
    assert len(out["would_file"]) == 1
    assert out["filed"] == []


def test_empty_ledger_gives_status_nothing_new(mod, tmp_path):
    ledger = str(tmp_path / "ledger")  # never written to -- directory doesn't even exist
    outcome_asset = _outcome_fn(mod, ledger)
    ctx = dg.build_asset_context()
    out = outcome_asset(ctx, pipeline_result={"text": "Nothing to report."})

    assert out["status"] == "nothing_new"
    assert out["filed"] == out["would_file"] == out["refused"] == []


def test_decisions_for_you_from_final_answer_marker(mod, tmp_path):
    """The answer's own "decisions for you" section always fills decisions_for_you, but (per
    summarize_outcome_ledger's own contract) never changes `status` by itself -- only a real
    ledger decision_needed entry does that."""
    ledger = str(tmp_path / "ledger")
    outcome_asset = _outcome_fn(mod, ledger)
    ctx = dg.build_asset_context()
    answer = "Summary text.\n\nDecisions for you:\n- approve the migration\n- confirm the schedule"
    out = outcome_asset(ctx, pipeline_result={"text": answer})

    assert out["status"] == "nothing_new"
    assert out["decisions_for_you"] == ["approve the migration", "confirm the schedule"]
    assert out["summary"] == "Summary text."


def test_ledger_decision_needed_entry_gives_status_needs_human(mod, tmp_path):
    ledger = str(tmp_path / "ledger")
    mod.write_outcome_ledger_entry(ledger, "EPHEMERAL", {
        "bucket": "decision_needed", "title": "Risky change", "reason": "touches prod config",
    })
    outcome_asset = _outcome_fn(mod, ledger)
    ctx = dg.build_asset_context()
    out = outcome_asset(ctx, pipeline_result={"text": "done"})

    assert out["status"] == "needs_human"
    assert out["decisions_for_you"][0]["title"] == "Risky change"


def test_required_tools_missing_passes_through_from_upstream(mod, tmp_path):
    ledger = str(tmp_path / "ledger")
    outcome_asset = _outcome_fn(mod, ledger)
    ctx = dg.build_asset_context()
    out = outcome_asset(ctx, pipeline_result={"text": "done", "required_tools_missing": ["svc__open_issue"]})

    assert out["required_tools_missing"] == ["svc__open_issue"]


@pytest.mark.parametrize("per_step_ops", [False, True])
def test_build_defs_attaches_outcome_asset_depending_on_last_output(mod, per_step_ops):
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "x"},
        outputs={"assets": ["a", "b"]},
        steps=[LLM_STEP, {"id": "b", "op": "llm_call", "model": "gpt-4o-mini", "source": "a"}],
        emit_outcome_asset=True, outcome_ledger_path="/tmp/whatever",
        per_step_ops=per_step_ops,
    )
    defs = comp.build_defs(None)
    keys = {ak.to_user_string() for a in defs.assets for ak in a.keys}
    assert "p_outcome" in keys
    outcome_def = next(a for a in defs.assets if dg.AssetKey(["p_outcome"]) in a.keys)
    dep_keys = {k.to_user_string() for k in outcome_def.dependency_keys}
    assert "p_b" in dep_keys  # last declared output, not the first
    assert "p_a" not in dep_keys


def test_emit_outcome_asset_false_means_no_outcome_asset(mod):
    comp = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "x"},
        outputs={"assets": ["a"]}, steps=[LLM_STEP],
    )
    defs = comp.build_defs(None)
    keys = {ak.to_user_string() for a in defs.assets for ak in a.keys}
    assert "p_outcome" not in keys
