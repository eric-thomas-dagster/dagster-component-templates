"""summarize_outcome_ledger's status / summary / answer-decisions behaviour. The function is inlined byte-identical into
six components (tests/test_agent_outcome_ledger_drift.py enforces that), so every copy is exercised here."""
import ast
import pathlib
from typing import Any, Dict, List

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent / "assets" / "ai"
COMPONENTS = ["github_pr_tools", "linear_ticket_tools", "openai_agent", "anthropic_agent", "litellm_agent", "gemini_agent"]


def _load(rel):
    src = (ROOT / rel / "component.py").read_text()
    wanted = {"_answer_decisions", "_answer_summary", "summarize_outcome_ledger"}
    body = [n for n in ast.parse(src).body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    ns: Dict[str, Any] = {"Any": Any, "List": List, "Dict": Dict}
    exec(compile(ast.Module(body=body, type_ignores=[]), rel, "exec"), ns)
    return ns["summarize_outcome_ledger"]


@pytest.fixture(params=COMPONENTS)
def summarize(request):
    return _load(request.param)


def _e(bucket, **kw):
    return {"bucket": bucket, "run_id": "r1", **kw}


def test_empty_ledger_is_nothing_new(summarize):
    assert summarize([])["status"] == "nothing_new"


def test_only_would_file_entries_is_practice(summarize):
    out = summarize([_e("would_file", title="a"), _e("would_file", title="b"), _e("would_file", title="c")])
    assert out["status"] == "practice" and len(out["would_file"]) == 3 and out["filed"] == []


def test_any_filed_entry_is_filed_even_with_would_file_or_refused(summarize):
    assert summarize([_e("filed", url="u")])["status"] == "filed"
    assert summarize([_e("would_file"), _e("filed"), _e("refused")])["status"] == "filed"


def test_refused_only_is_refused_and_a_ledger_decision_is_needs_human(summarize):
    assert summarize([_e("refused", reason="x")])["status"] == "refused"
    out = summarize([_e("filed"), _e("decision_needed", question="merge?")])
    assert out["status"] == "needs_human" and out["decisions_for_you"] == [{"question": "merge?"}]


def test_decisions_are_parsed_from_the_answer_text(summarize):
    out = summarize([], "Done.\n\nDecisions for you: - merge the dependency bump")
    assert out["decisions_for_you"] == ["merge the dependency bump"]
    assert summarize([], "Done.\n\nDecisions for you: none")["decisions_for_you"] == []
    assert summarize([], "Done.\n\nDecisions for you:\nNone.")["decisions_for_you"] == []


def test_multiline_markdown_decisions_and_headings(summarize):
    answer = "Fixed two bugs.\n\n**Decisions for you:**\n- bump dagster to 1.9\n2. rotate the token\n* \n"
    assert summarize([], answer)["decisions_for_you"] == ["bump dagster to 1.9", "rotate the token"]
    assert summarize([], "x\n\n## Decisions for you\n- one")["decisions_for_you"] == ["one"]


def test_answer_decisions_do_not_change_status_and_follow_ledger_decisions(summarize):
    out = summarize([_e("filed")], "ok\n\nDecisions for you: - approve")
    assert out["status"] == "filed" and out["decisions_for_you"] == ["approve"]
    out = summarize([_e("decision_needed", q="a")], "ok\n\nDecisions for you: - b")
    assert out["decisions_for_you"] == [{"q": "a"}, "b"]


def test_custom_and_disabled_marker(summarize):
    assert summarize([], "x\n\nFor a human: - look", decisions_marker="for a human")["decisions_for_you"] == ["look"]
    out = summarize([], "x\n\nDecisions for you: - y", decisions_marker="")
    assert out["decisions_for_you"] == [] and "Decisions for you" in out["summary"]


def test_summary_is_the_answer_before_the_decisions_section_capped_at_700(summarize):
    assert summarize([], "Fixed it.\n\nDecisions for you: none")["summary"] == "Fixed it."
    assert summarize([], "x" * 900)["summary"] == "x" * 700 + "…"
    assert summarize([], "Intro.\n\n## Decisions for you\n- a")["summary"] == "Intro."
    assert summarize([], None)["summary"] == "" and summarize([], 5)["decisions_for_you"] == []
