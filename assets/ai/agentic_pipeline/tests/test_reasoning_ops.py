"""verify / eval / best_of_n / cascade / plan / decide / approve + the `when:` guard, per-step policy and versioning."""
import json
import threading
import time

import dagster as dg
import pytest

from .conftest import new_state, tool_call


def run(mod, ctx, step, state=None):
    state = state if state is not None else new_state()
    mod._run_step(step, state, ctx)
    return state[step["id"]], state


# ── verify ────────────────────────────────────────────────────────────────────

def test_verify_supported(mod, fake_litellm, ctx):
    fake_litellm.reply(json.dumps({"verdict": "supported", "score": 0.95, "unsupported_claims": [], "issues": []}))
    state = new_state(text="Paris is the capital of France.")
    state["docs"] = {"text": "France's capital city is Paris."}
    out, _ = run(mod, ctx, {"id": "v", "op": "verify", "model": "gpt-4o-mini", "evidence_source": "docs"}, state)
    assert out["passed"] and out["verdict"] == "supported" and out["score"] == 0.95
    assert out["text"] == "Paris is the capital of France."          # the answer passes through unchanged
    assert "France's capital city" in fake_litellm.calls[0]["messages"][1]["content"]   # evidence was shown to the model


def test_verify_unsupported_claims_and_gate(mod, fake_litellm, ctx):
    reply = json.dumps({"verdict": "unsupported", "score": 0.2, "unsupported_claims": ["pop. 90M"], "issues": ["made up"]})
    fake_litellm.reply(reply)
    state = new_state(text="Paris has 90M people.")
    state["docs"] = {"text": "Paris is the capital."}
    out, _ = run(mod, ctx, {"id": "v", "op": "verify", "model": "m", "evidence_source": ["docs"]}, state)
    assert not out["passed"] and out["unsupported_claims"] == ["pop. 90M"]
    fake_litellm.reply(reply)
    with pytest.raises(RuntimeError, match="pop. 90M"):
        run(mod, ctx, {"id": "v", "op": "verify", "model": "m", "evidence_source": ["docs"], "on_fail": "fail"}, state)


def test_verify_requires_evidence(mod, fake_litellm, ctx):
    with pytest.raises(ValueError, match="needs evidence"):
        run(mod, ctx, {"id": "v", "op": "verify", "model": "m"})


def test_verify_tolerates_fenced_json_and_garbage(mod, fake_litellm, ctx):
    fake_litellm.reply('Sure!\n```json\n{"verdict": "supported", "score": 0.9}\n```')
    state = new_state(); state["e"] = {"text": "x"}
    assert run(mod, ctx, {"id": "v", "op": "verify", "model": "m", "evidence_source": "e"}, state)[0]["passed"]
    fake_litellm.reply("I cannot comply")                         # unparseable -> fails closed, never silently passes
    assert not run(mod, ctx, {"id": "v2", "op": "verify", "model": "m", "evidence_source": "e"}, state)[0]["passed"]


# ── eval ──────────────────────────────────────────────────────────────────────

CRITERIA = [{"name": "accuracy", "weight": 3}, {"name": "tone", "weight": 1}]


def test_eval_weighted_score(mod, fake_litellm, ctx):
    fake_litellm.reply(json.dumps({"scores": {"accuracy": {"score": 5, "rationale": "exact"}, "tone": {"score": 1, "rationale": "curt"}}}))
    out, _ = run(mod, ctx, {"id": "e", "op": "eval", "model": "m", "criteria": CRITERIA})
    assert out["overall"] == 4.0 and out["scores"] == {"accuracy": 5.0, "tone": 1.0} and out["rationales"]["tone"] == "curt"


def test_eval_gate_and_unscored_criterion_counts_against(mod, fake_litellm, ctx):
    fake_litellm.reply(json.dumps({"scores": {"accuracy": {"score": 5}}}))          # tone missing -> 1
    with pytest.raises(RuntimeError, match="below fail_below"):
        run(mod, ctx, {"id": "e", "op": "eval", "model": "m", "criteria": CRITERIA, "fail_below": 4.5})


# ── best_of_n ─────────────────────────────────────────────────────────────────

def test_best_of_n_with_verifier(mod, fake_litellm, ctx):
    for t in ("a plain answer", "ANSWER: 42", "another"):
        fake_litellm.reply(t)
    out, _ = run(mod, ctx, {"id": "b", "op": "best_of_n", "model": "m", "n": 3, "parallel": False,
                            "verifier": {"kind": "regex", "pattern": r"ANSWER: \d+"}})
    assert out["text"] == "ANSWER: 42" and out["selected_index"] == 1 and out["any_passed"]
    assert [c["passed"] for c in out["candidates"]] == [False, True, False]


def test_best_of_n_vote_is_self_consistency(mod, fake_litellm, ctx):
    for t in ("Paris", "paris.", "Lyon", "Paris "):
        fake_litellm.reply(t)
    out, _ = run(mod, ctx, {"id": "b", "op": "best_of_n", "model": "m", "n": 4, "parallel": False, "select": "vote"})
    assert out["text"].strip().lower().startswith("paris") and out["agreement"] == 0.75


def test_best_of_n_none_pass(mod, fake_litellm, ctx):
    for t in ("x", "y"):
        fake_litellm.reply(t)
    step = {"id": "b", "op": "best_of_n", "model": "m", "n": 2, "parallel": False, "verifier": {"kind": "regex", "pattern": "^never$"}}
    assert run(mod, ctx, dict(step))[0]["any_passed"] is False        # default: return the best anyway, flagged
    with pytest.raises(RuntimeError, match="none of 2 candidates"):
        run(mod, ctx, {**step, "on_none_pass": "fail"})


def test_best_of_n_llm_verifier_scores_each_candidate(mod, fake_litellm, ctx):
    for t in ("weak", "strong"):
        fake_litellm.reply(t)
    fake_litellm.reply(json.dumps({"score": 0.2, "reason": "thin"}))
    fake_litellm.reply(json.dumps({"score": 0.9, "reason": "thorough"}))
    out, _ = run(mod, ctx, {"id": "b", "op": "best_of_n", "model": "m", "n": 2, "parallel": False, "verifier": {"kind": "llm"}})
    assert out["text"] == "strong" and out["n_llm_calls"] == 4


def test_best_of_n_parallel_workers_still_obey_the_policy(mod, fake_litellm, ctx):
    state = new_state({"budget": {"max_llm_calls": 2}})
    with pytest.raises(mod.PolicyViolation, match="max_llm_calls=2"):
        run(mod, ctx, {"id": "b", "op": "best_of_n", "model": "m", "n": 4, "parallel": True}, state)
    assert len(fake_litellm.calls) <= 3                                 # threads inherited the active policy


# ── cascade ───────────────────────────────────────────────────────────────────

def test_cascade_escalates_until_accepted(mod, fake_litellm, ctx):
    fake_litellm.reply("dunno")
    fake_litellm.reply("ANSWER: 7")
    out, _ = run(mod, ctx, {"id": "c", "op": "cascade", "models": ["ollama/qwen3:8b", "gpt-4o"], "accept": {"kind": "regex", "pattern": "ANSWER"}})
    assert out["model_used"] == "gpt-4o" and out["n_escalations"] == 1 and out["accepted"]
    assert [e["accepted"] for e in out["escalations"]] == [False, True]


def test_cascade_cheap_model_wins_and_stronger_is_never_called(mod, fake_litellm, ctx):
    fake_litellm.reply("ANSWER: 7")
    out, _ = run(mod, ctx, {"id": "c", "op": "cascade", "models": ["cheap", "strong"], "accept": {"kind": "regex", "pattern": "ANSWER"}})
    assert out["model_used"] == "cheap" and out["n_escalations"] == 0 and len(fake_litellm.calls) == 1


def test_cascade_escalates_on_model_error_and_can_fail_when_exhausted(mod, fake_litellm, ctx):
    fake_litellm.reply(ConnectionError("ollama is down"))
    fake_litellm.reply("fine")
    out, _ = run(mod, ctx, {"id": "c", "op": "cascade", "models": ["ollama/qwen3", "gpt-4o-mini"]})
    assert out["model_used"] == "gpt-4o-mini" and "ollama is down" in out["escalations"][0]["error"]
    fake_litellm.reply("nope")
    with pytest.raises(RuntimeError, match="no model"):
        run(mod, ctx, {"id": "c", "op": "cascade", "models": ["a"], "accept": {"kind": "regex", "pattern": "YES"}, "on_exhausted": "fail"})


# ── plan ──────────────────────────────────────────────────────────────────────

def plan_json(*tasks):
    return json.dumps({"steps": [{"id": f"s{i + 1}", "task": t} for i, t in enumerate(tasks)]})


def test_plan_executes_in_order_and_synthesizes(mod, fake_litellm, ctx):
    fake_litellm.reply(plan_json("find the facts", "write the summary"))
    fake_litellm.reply("fact: 42")
    fake_litellm.reply("summary of 42")
    fake_litellm.reply("FINAL: the answer is 42")
    out, _ = run(mod, ctx, {"id": "p", "op": "plan", "model": "m"})
    assert out["text"] == "FINAL: the answer is 42" and out["status"] == "completed" and out["n_replans"] == 0
    assert [s["id"] for s in out["steps"]] == ["s1", "s2"]
    assert "fact: 42" in fake_litellm.calls[2]["messages"][1]["content"]          # s2 saw s1's result


def test_plan_replans_when_a_step_fails_verification(mod, fake_litellm, ctx):
    fake_litellm.reply(plan_json("try the flaky approach", "never reached"))
    fake_litellm.reply("garbage")                                       # s1 output fails the verifier
    fake_litellm.reply(plan_json("take the robust approach"))            # the replan
    fake_litellm.reply("OK: robust result")
    fake_litellm.reply("FINAL")
    out, _ = run(mod, ctx, {"id": "p", "op": "plan", "model": "m", "verifier": {"kind": "regex", "pattern": "^OK"}})
    assert out["n_replans"] == 1 and out["status"] == "completed"
    assert [s["ok"] for s in out["steps"]] == [False, True]
    replan_prompt = fake_litellm.calls[2]["messages"][1]["content"]
    assert "FAILED STEP: try the flaky approach" in replan_prompt and "garbage" in replan_prompt


def test_plan_gives_up_after_max_replans(mod, fake_litellm, ctx):
    fake_litellm.reply(plan_json("a"))
    for _ in range(3):
        fake_litellm.reply("bad")
        fake_litellm.reply(plan_json("again"))
    out, _ = run(mod, ctx, {"id": "p", "op": "plan", "model": "m", "max_replans": 1, "verifier": {"kind": "regex", "pattern": "^OK"}})
    assert out["status"] == "failed" and out["n_replans"] == 1
    fake_litellm.script.clear()
    fake_litellm.reply(plan_json("a"))
    fake_litellm.reply("bad")
    with pytest.raises(RuntimeError, match="failed after 0 replan"):
        run(mod, ctx, {"id": "p2", "op": "plan", "model": "m", "max_replans": 0, "on_fail": "fail", "verifier": {"kind": "regex", "pattern": "^OK"}})


def test_plan_falls_back_to_one_step_when_the_planner_is_unparseable(mod, fake_litellm, ctx):
    fake_litellm.reply("I think you should just do the thing")
    fake_litellm.reply("done")
    fake_litellm.reply("FINAL")
    out, _ = run(mod, ctx, {"id": "p", "op": "plan", "model": "m"})
    assert out["status"] == "completed" and len(out["steps"]) == 1


def test_plan_executor_can_use_tools_under_the_policy(mod, fake_litellm, ctx, monkeypatch):
    called = []

    async def discover(log, servers):
        return [{"server": "fs", "name": "read_file", "description": "", "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}}}]

    async def call(**kw):
        called.append(kw["tool_args"])
        return {"raw": "file says hi", "kind": "text"}

    monkeypatch.setattr(mod, "_discover_mcp_tools_async", discover)
    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    fake_litellm.reply(plan_json("read the file"))
    fake_litellm.reply("", tool_calls=[tool_call("fs__read_file", {"path": "/etc/passwd"}, id="1")])   # outside the allowed root
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "could not read"}, id="2")])
    fake_litellm.reply("FINAL")
    policy = {"tools": {"arg_rules": [{"tool": "fs__*", "arg": "path", "under": ["/srv/data"]}]}}
    out, state = run(mod, ctx, {"id": "p", "op": "plan", "model": "m",
                                "executor": {"mcp_servers": [{"name": "fs", "type": "stdio", "command": ["x"]}]}}, new_state(policy))
    assert called == []                                                 # the sandbox held inside the plan's tool loop
    assert out["steps"][0]["output"] == "could not read"


# ── decide ────────────────────────────────────────────────────────────────────

OUTCOMES = {"auto_close": "safe to close", "needs_review": "a human should look", "escalate": "urgent"}


def test_decide_records_outcome_reason_and_evidence(mod, fake_litellm, ctx):
    fake_litellm.reply(json.dumps({"outcome": "needs_review", "reason": "PO number missing", "confidence": 0.82,
                                   "evidence": [{"source": "facts", "quote": "PO: (blank)"}]}))
    state = new_state(text="invoice 17")
    state["facts"] = {"text": "PO: (blank)"}
    out, _ = run(mod, ctx, {"id": "d", "op": "decide", "model": "m", "outcomes": OUTCOMES, "evidence_source": "facts"}, state)
    assert out["outcome"] == "needs_review" and out["confidence"] == 0.82 and out["evidence"][0]["quote"] == "PO: (blank)"
    assert out["text"] == "needs_review: PO number missing"


def test_decide_rejects_unknown_outcome_unless_a_default_is_set(mod, fake_litellm, ctx):
    fake_litellm.reply(json.dumps({"outcome": "shrug", "reason": "?"}))
    with pytest.raises(RuntimeError, match="not one of"):
        run(mod, ctx, {"id": "d", "op": "decide", "model": "m", "outcomes": OUTCOMES})
    fake_litellm.reply(json.dumps({"outcome": "shrug", "reason": "?"}))
    out, _ = run(mod, ctx, {"id": "d", "op": "decide", "model": "m", "outcomes": OUTCOMES, "default_outcome": "needs_review"})
    assert out["outcome"] == "needs_review" and out["used_default_outcome"]
    with pytest.raises(ValueError, match="at least two"):
        run(mod, ctx, {"id": "d", "op": "decide", "model": "m", "outcomes": {"only": "x"}})


# ── when: guard ───────────────────────────────────────────────────────────────

def test_when_guard_skips_and_passes_text_through(mod, fake_litellm, ctx):
    state = new_state(text="hello")
    state["d"] = {"text": "x", "outcome": "auto_close", "confidence": 0.95}
    out, _ = run(mod, ctx, {"id": "s", "op": "llm_call", "model": "m", "source": "d", "when": {"source": "d", "field": "outcome", "equals": "needs_review"}}, state)
    assert out["skipped"] and out["text"] == "x" and fake_litellm.calls == []        # no model call was made
    fake_litellm.reply("ran")
    out, _ = run(mod, ctx, {"id": "s2", "op": "llm_call", "model": "m", "when": {"source": "d", "field": "outcome", "in": ["auto_close", "x"]}}, state)
    assert out["text"] == "ran" and "skipped" not in out


@pytest.mark.parametrize("when,expected_runs", [
    ({"field": "confidence", "lt": 0.9}, False), ({"field": "confidence", "gte": 0.9}, True),
    ({"field": "confidence", "gte": 0.9, "not": True}, False), ({"field": "ok", "truthy": True}, True),
    ({"field": "ok", "equals": False}, False), ({"regex": "^hello"}, True), ({"contains": "WORLD"}, True),
])
def test_when_predicates(mod, when, expected_runs):
    state = {"d": {"text": "hello world", "confidence": 0.95, "ok": True}}
    assert mod._when_ok({"source": "d", **when}, state)[0] is expected_runs


def test_when_validation(mod):
    with pytest.raises(ValueError, match="needs `source"):
        mod._when_ok({"equals": "x"}, {})
    with pytest.raises(ValueError, match="not a prior step"):
        mod._when_ok({"source": "nope", "equals": "x"}, {"a": {"text": ""}})
    with pytest.raises(ValueError, match="needs a predicate"):
        mod._when_ok({"source": "a"}, {"a": {"text": ""}})


# ── approve (token contract shared with human_approval_gate) ─────────────────

def approve_step(tmp_path, **kw):
    return {"id": "gate", "op": "approve", "source": "draft", "approval_dir": str(tmp_path), "key": "t1", **kw}


def state_with_draft(**kw):
    s = new_state(**kw)
    s["draft"] = {"text": "Refund $120 to customer 9"}
    return s


def test_approved_token_passes_the_text_through(mod, ctx, tmp_path):
    (tmp_path / "t1.json").write_text(json.dumps({"approved": True, "approver": "eric", "reason": "ok"}))
    out, _ = run(mod, ctx, approve_step(tmp_path), state_with_draft())
    assert out["approved"] is True and out["status"] == "approved" and out["approver"] == "eric" and out["text"] == "Refund $120 to customer 9"


def test_rejected_stops_the_pipeline_or_continues_on_request(mod, ctx, tmp_path):
    (tmp_path / "t1.json").write_text(json.dumps({"approved": False, "reason": "wrong customer"}))
    with pytest.raises(RuntimeError, match="wrong customer"):
        run(mod, ctx, approve_step(tmp_path), state_with_draft())
    out, _ = run(mod, ctx, approve_step(tmp_path, on_reject="continue"), state_with_draft())
    assert out["approved"] is False and out["text"] == ""                  # nothing flows downstream


def test_no_token_defaults_to_reject_the_safe_default(mod, ctx, tmp_path):
    with pytest.raises(RuntimeError, match="timeout_default_reject"):
        run(mod, ctx, approve_step(tmp_path), state_with_draft())
    assert not (tmp_path / "pending" / "t1.json").exists()                 # decided (by default) -> left the queue


def test_timeout_policies(mod, ctx, tmp_path):
    out, _ = run(mod, ctx, approve_step(tmp_path, on_timeout="approve"), state_with_draft())
    assert out["approved"] is True and out["status"] == "timeout_default_approve"
    out, _ = run(mod, ctx, approve_step(tmp_path, on_timeout="pending"), state_with_draft())
    assert out["approved"] is None and out["status"] == "pending"
    pending = json.loads((tmp_path / "pending" / "t1.json").read_text())      # the review queue entry
    assert pending["summary"].startswith("Refund $120") and pending["approve_by_writing"]["path"].endswith("t1.json")


def test_token_arriving_during_the_window_is_honoured(mod, ctx, tmp_path):
    def reviewer():
        time.sleep(0.4)
        assert (tmp_path / "pending" / "t1.json").exists()                  # the request was visible while waiting
        (tmp_path / "t1.json").write_text(json.dumps({"approved": True, "approver": "slack:eric"}))

    t = threading.Thread(target=reviewer)
    t.start()
    out, _ = run(mod, ctx, approve_step(tmp_path, wait_seconds=5, poll_seconds=0.1), state_with_draft())
    t.join()
    assert out["status"] == "approved" and out["approver"] == "slack:eric" and 0.3 < out["waited_seconds"] < 3
    assert not (tmp_path / "pending" / "t1.json").exists()


def test_half_written_token_is_not_treated_as_a_decision(mod, ctx, tmp_path):
    (tmp_path / "t1.json").write_text('{"approved": tr')
    with pytest.raises(RuntimeError, match="timeout_default_reject"):
        run(mod, ctx, approve_step(tmp_path), state_with_draft())


def test_exception_based_review_only_asks_when_the_guard_says_so(mod, fake_litellm, ctx, tmp_path):
    state = state_with_draft()
    state["d"] = {"text": "auto_close: fine", "outcome": "auto_close"}
    step = approve_step(tmp_path, when={"source": "d", "field": "outcome", "equals": "needs_review"})
    out, _ = run(mod, ctx, step, state)
    assert out["skipped"]                                                   # routine case: no human, no queue entry, no error
    assert not (tmp_path / "pending").exists()
    state["d"] = {"text": "x", "outcome": "needs_review"}
    with pytest.raises(RuntimeError):                                       # an exception: a human is now required (and none answered)
        run(mod, ctx, step, state)


def test_approve_respects_the_filesystem_policy(mod, ctx, tmp_path):
    allowed = tmp_path / "ok"
    state = state_with_draft(policy={"filesystem": {"write": [str(allowed)]}})
    with pytest.raises(mod.PolicyViolation, match="outside policy.filesystem.write"):
        run(mod, ctx, approve_step(tmp_path / "elsewhere", on_timeout="pending"), state)


# ── per-step policy narrowing ─────────────────────────────────────────────────

def test_step_policy_can_only_narrow_never_loosen(mod, fake_litellm, ctx):
    from .test_policy import LOOP, TOOLS
    async def discover(log, servers):
        return TOOLS
    mod._discover_mcp_tools_async = discover
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "x"})])
    run(mod, ctx, {**LOOP, "policy": {"tools": {"deny": ["*delete*"]}}}, new_state())       # no pipeline policy: step narrows
    assert {t["function"]["name"] for t in fake_litellm.calls[0]["tools"]} == {"fs__read_file", "web__fetch", "finalize"}
    fake_litellm.calls.clear()
    fake_litellm.reply("", tool_calls=[tool_call("finalize", {"answer": "x"})])
    # the pipeline denies delete; a step that ALLOWS everything cannot bring it back
    run(mod, ctx, {**LOOP, "policy": {"tools": {"allow": ["*"]}}}, new_state({"tools": {"deny": ["*delete*"]}}))
    assert "fs__delete_file" not in {t["function"]["name"] for t in fake_litellm.calls[0]["tools"]}


def test_step_policy_budget_is_scoped_to_the_step(mod, fake_litellm, ctx):
    state = new_state({"budget": {"max_llm_calls": 10}})
    llm = {"op": "llm_call", "model": "m", "prompt_template": "{text}"}
    run(mod, ctx, {"id": "a", **llm, "policy": {"budget": {"max_llm_calls": 1}}}, state)
    run(mod, ctx, {"id": "b", **llm, "policy": {"budget": {"max_llm_calls": 1}}}, state)    # a fresh 1-call budget for this step
    run(mod, ctx, {"id": "c", **llm}, state)                                                  # the pipeline total (3 of 10) still counts
    assert state["__policy_usage__"]["llm_calls"] == 3
    # within ONE step the step budget bites
    with pytest.raises(mod.PolicyViolation, match="max_llm_calls=1"):
        run(mod, ctx, {"id": "d", "op": "best_of_n", "model": "m", "n": 3, "parallel": False, "policy": {"budget": {"max_llm_calls": 1}}}, state)


def test_step_policy_typos_fail_loudly(mod, fake_litellm, ctx):
    with pytest.raises(ValueError, match="unknown policy key"):
        run(mod, ctx, {"id": "a", "op": "llm_call", "model": "m", "policy": {"toools": {}}})


# ── versioning ────────────────────────────────────────────────────────────────

def _comp(mod, prompt_b="B"):
    return mod.AgenticPipelineComponent(
        asset_name_prefix="v", source={"kind": "literal", "text": "hi"},
        steps=[{"id": "a", "op": "llm_call", "model": "m", "prompt_template": "A {text}"},
               {"id": "b", "op": "llm_call", "model": "m", "prompt_template": prompt_b + " {a}", "source": "a"}],
        outputs={"assets": ["a", "b"]})


def test_code_version_changes_only_for_the_edited_step(mod):
    def versions(c):
        d = c.build_defs(None).assets[0]
        return {k.to_user_string(): v for k, v in d.code_versions_by_key.items()}
    v1, v2 = versions(_comp(mod)), versions(_comp(mod, prompt_b="B2"))
    assert v1["v_a"] == v2["v_a"] and v1["v_b"] != v2["v_b"] and all(v1.values())


def test_pipeline_version_is_on_every_materialization(mod, fake_litellm):
    result = dg.materialize(_comp(mod).build_defs(None).assets, raise_on_error=False)
    assert result.success
    versions = {e.materialization.metadata["a__pipeline_version"].value for e in result.get_asset_materialization_events()
                if "a__pipeline_version" in e.materialization.metadata}
    assert len(versions) == 1 and len(next(iter(versions))) == 12
