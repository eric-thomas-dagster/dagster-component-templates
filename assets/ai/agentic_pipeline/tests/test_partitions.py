"""Partition isolation + backfill_policy for agentic_pipeline.

Real Dagster materializations of a partitioned pipeline (both emitted shapes); only the model call is scripted."""
import json

import dagster as dg
import pytest


def comp(mod, tmp_path, *, per_step_ops=False, steps=None, partition_values="a,b", sinks=True, **kw):
    gate = {"id": "gate", "op": "approve", "source": "summ", "approval_dir": str(tmp_path / "approvals"),
            "on_timeout": "reject", "wait_seconds": 0}
    summ = {"id": "summ", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY",
            "prompt_template": "Summarize: {text}"}
    outputs = {"assets": ["gate"]}
    if sinks:
        outputs["text_sinks"] = [{"from": "gate", "path": str(tmp_path / "out" / "{partition_key}.txt")}]
    return mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "doc for {partition_key}"},
        steps=steps or [summ, gate], outputs=outputs, partition_type="static", partition_values=partition_values,
        per_step_ops=per_step_ops, **kw)


def run(c, key, raise_on_error=False):
    return dg.materialize(c.build_defs(None).assets, partition_key=key, raise_on_error=raise_on_error)


def approve(tmp_path, key, ok=True):
    d = tmp_path / "approvals"
    d.mkdir(exist_ok=True)
    (d / f"{key}.json").write_text(json.dumps({"approved": ok, "approver": "t"}))


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")


@pytest.mark.parametrize("per_step_ops", [False, True])
def test_two_partitions_share_no_state_approvals_or_sinks(mod, fake_litellm, tmp_path, per_step_ops):
    c = comp(mod, tmp_path, per_step_ops=per_step_ops)
    fake_litellm.reply("summary of a")
    fake_litellm.reply("summary of b")
    approve(tmp_path, "a")                                          # only partition a is approved
    ra = run(c, "a")
    rb = run(c, "b")
    assert ra.success and not rb.success                            # b has no token: the safe default stops it
    assert (tmp_path / "out" / "a.txt").read_text() == "summary of a"
    assert not (tmp_path / "out" / "b.txt").exists()                # b's sink never ran, a's file untouched
    prompts = [c_["messages"][-1]["content"] for c_ in fake_litellm.calls]
    assert prompts == ["Summarize: doc for a", "Summarize: doc for b"]   # each run saw only its own partition's text
    # approving b later does not need (or disturb) a's token
    approve(tmp_path, "b")
    fake_litellm.reply("summary of b2")
    assert run(c, "b").success
    assert (tmp_path / "out" / "a.txt").read_text() == "summary of a"
    assert (tmp_path / "out" / "b.txt").read_text() == "summary of b2"
    assert sorted(p.name for p in (tmp_path / "approvals").glob("*.json")) == ["a.json", "b.json"]


def test_a_rejection_of_one_partition_does_not_reject_another(mod, fake_litellm, tmp_path):
    c = comp(mod, tmp_path)
    approve(tmp_path, "a", ok=False)
    approve(tmp_path, "b", ok=True)
    assert not run(c, "a").success
    assert run(c, "b").success


def test_pending_review_records_are_per_partition(mod, fake_litellm, tmp_path):
    steps = [{"id": "gate", "op": "approve", "approval_dir": str(tmp_path / "approvals"), "on_timeout": "pending"}]
    c = comp(mod, tmp_path, steps=steps, sinks=False)
    assert run(c, "a").success and run(c, "b").success
    pending = tmp_path / "approvals" / "pending"
    assert sorted(p.name for p in pending.glob("*.json")) == ["a.json", "b.json"]
    assert json.loads((pending / "a.json").read_text())["key"] == "a"
    approve(tmp_path, "a")                                           # approving a leaves b's queue entry alone
    assert run(c, "a").success
    assert (pending / "b.json").exists() and not (pending / "a.json").exists()


def test_partition_keys_that_sanitize_alike_do_not_share_an_approval(mod, fake_litellm, tmp_path):
    """'x/y' and 'x_y' both sanitize to x_y; before the fix one token approved both partitions."""
    c = comp(mod, tmp_path, partition_values="x/y,x_y", sinks=False)
    approve(tmp_path, mod._approval_key("x/y"))
    assert run(c, "x/y").success
    assert not run(c, "x_y").success
    assert mod._approval_key("x_y") == "x_y" and mod._approval_key("INV-2291") == "INV-2291"   # safe keys are unchanged
    assert mod._approval_key("x/y") != mod._approval_key("x_y")


def test_two_approve_steps_under_partitioning_must_have_their_own_keys(mod, tmp_path):
    steps = [{"id": "g1", "op": "approve", "approval_dir": str(tmp_path)},
             {"id": "g2", "op": "approve", "approval_dir": str(tmp_path)}]
    with pytest.raises(ValueError, match="several approve steps"):
        comp(mod, tmp_path, steps=steps).build_defs(None)
    steps[0]["key"], steps[1]["key"] = "{partition_key}-g1", "{partition_key}-g2"
    comp(mod, tmp_path, steps=steps).build_defs(None)
    # unpartitioned: the default is the step id, so two gates are already distinct
    steps = [{"id": "g1", "op": "approve", "approval_dir": str(tmp_path)}, {"id": "g2", "op": "approve", "approval_dir": str(tmp_path)}]
    mod.AgenticPipelineComponent(asset_name_prefix="u", source={"kind": "literal", "text": "t"}, steps=steps,
                                 outputs={"assets": ["g1"]}).build_defs(None)


def test_a_fixed_approval_key_under_partitioning_is_rejected(mod, tmp_path):
    steps = [{"id": "gate", "op": "approve", "approval_dir": str(tmp_path), "key": "INV-1"}]
    with pytest.raises(ValueError, match="same for every partition"):
        comp(mod, tmp_path, steps=steps).build_defs(None)
    steps[0]["key"] = "INV-{partition_key}"
    comp(mod, tmp_path, steps=steps).build_defs(None)


# --- backfill_policy ---------------------------------------------------------------------------

@pytest.mark.parametrize("per_step_ops", [False, True])
def test_multi_run_policy_is_wired_onto_the_emitted_asset(mod, tmp_path, per_step_ops):
    c = comp(mod, tmp_path, per_step_ops=per_step_ops, backfill_policy="multi_run")
    (asset,) = c.build_defs(None).assets
    policy = asset.backfill_policy
    assert policy is not None and policy.max_partitions_per_run == 1
    assert comp(mod, tmp_path).build_defs(None).assets[0].backfill_policy is None     # unset: dagster's default


def test_single_run_is_rejected_with_a_reason(mod, tmp_path):
    with pytest.raises(ValueError, match="single_run.*no\\s+single partition_key"):
        comp(mod, tmp_path, backfill_policy="single_run").build_defs(None)


@pytest.mark.parametrize("n, msg", [(2, "not supported"), (0, ">= 1"), (-1, ">= 1")])
def test_multi_run_with_several_partitions_per_run_is_rejected(mod, tmp_path, n, msg):
    with pytest.raises(ValueError, match=msg):
        comp(mod, tmp_path, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=n).build_defs(None)


def test_cap_without_multi_run_and_unknown_policy_are_rejected(mod, tmp_path):
    with pytest.raises(ValueError, match="only applies"):
        comp(mod, tmp_path, backfill_policy_max_partitions_per_run=1).build_defs(None)
    with pytest.raises(Exception):
        comp(mod, tmp_path, backfill_policy="sometimes")


def test_policy_is_ignored_when_unpartitioned(mod):
    c = mod.AgenticPipelineComponent(asset_name_prefix="u", source={"kind": "literal", "text": "t"},
                                     steps=[{"id": "a", "op": "llm_call", "model": "m"}], outputs={"assets": ["a"]},
                                     backfill_policy="single_run")
    assert c.build_defs(None).assets


@pytest.mark.parametrize("per_step_ops", [False, True])
def test_a_multi_partition_run_fails_loudly_instead_of_resolving_as_unpartitioned(mod, fake_litellm, tmp_path, monkeypatch, per_step_ops):
    """Force ONE run over two partitions (what single_run / max_partitions_per_run>1 would do) by bypassing the load-time
    rejection. Without the guard partition_key is None, '{partition_key}' stays literal, and the partitions share one approval."""
    monkeypatch.setattr(mod, "_build_backfill_policy", lambda *a, **k: dg.BackfillPolicy.single_run())
    asset = comp(mod, tmp_path, per_step_ops=per_step_ops).build_defs(None).assets[0]
    from dagster._core.storage.tags import ASSET_PARTITION_RANGE_END_TAG, ASSET_PARTITION_RANGE_START_TAG
    res = dg.materialize([asset], raise_on_error=False,
                         tags={ASSET_PARTITION_RANGE_START_TAG: "a", ASSET_PARTITION_RANGE_END_TAG: "b"})
    assert not res.success
    msgs = " ".join(e.event_specific_data.error.message + (e.event_specific_data.error.cause.message if e.event_specific_data.error.cause else "")
                    for e in res.get_step_failure_events())
    assert "covers several partitions" in msgs, msgs
    assert not (tmp_path / "approvals").exists() and not (tmp_path / "out").exists()
