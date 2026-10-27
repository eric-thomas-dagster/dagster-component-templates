"""Cross-run budgets (durable ledger, breaker, unknown cost), spend metadata, and prompt versions.

Real Dagster materializations; only litellm.completion (and its price) is faked; the clock is a module seam."""
import concurrent.futures
import json
import logging
from datetime import datetime, timedelta, timezone

import dagster as dg
import pytest

STEP = {"op": "llm_call", "model": "gpt-4o-mini", "prompt_template": "{text}"}


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")


@pytest.fixture
def clock(mod, monkeypatch):
    c = {"t": datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)}
    monkeypatch.setattr(mod, "_ledger_now", lambda: c["t"])
    return c


def price(fake, usd):
    fake.completion_cost = lambda completion_response=None: usd


def comp(mod, ledger, budget=None, *, per_step_ops=False, n_steps=1, on_violation=None, partitioned=False, extra_policy=None):
    steps = [{"id": "a", **STEP, "api_key_env_var": "OPENAI_API_KEY"}]
    for i in range(1, n_steps):
        steps.append({"id": f"s{i}", **STEP, "source": steps[-1]["id"], "api_key_env_var": "OPENAI_API_KEY"})
    pol = {"budget": {"ledger": {"dir": str(ledger)}, **(budget or {})}}
    if on_violation:
        pol["on_violation"] = on_violation
    pol.update(extra_policy or {})
    kw = dict(partition_type="static", partition_values="p1,p2") if partitioned else {}
    return mod.AgenticPipelineComponent(
        asset_name_prefix="t", source={"kind": "literal", "text": "hi {partition_key}" if partitioned else "hi"}, steps=steps,
        outputs={"assets": [s["id"] for s in steps]}, per_step_ops=per_step_ops, policy=pol, **kw)


def run(c, key=None):
    return dg.materialize(c.build_defs(None).assets, partition_key=key, raise_on_error=False)


def md(result):
    return {k: v.value for e in result.get_asset_materialization_events() for k, v in e.materialization.metadata.items()}


def fail_text(result):
    return " ".join(str(e.event_specific_data) for e in result.all_events if e.is_failure or e.is_step_failure)


# ── validation ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad,msg", [
    ({"budget": {"max_cost_usd_per_day": 1}}, "need policy.budget.ledger"),
    ({"budget": {"ledger": {}}}, "non-empty `dir`"),
    ({"budget": {"ledger": {"dir": "x", "compact_after_days": 0}}}, "compact_after_days"),
    ({"budget": {"ledger": {"dir": "x"}, "breaker": {"trip_after_violations": 0, "cool_down_minutes": 5}}}, "trip_after_violations"),
    ({"budget": {"ledger": {"dir": "x"}, "breaker": {"trip_after_violations": 2}}}, "cool_down_minutes"),
    ({"budget": {"on_unknown_cost": "ignore"}}, "on_unknown_cost"),
    ({"budget": {"max_cost_usd_per_day": -1, "ledger": {"dir": "x"}}}, "non-negative"),
])
def test_validation(mod, bad, msg):
    with pytest.raises(ValueError, match=msg):
        mod._validate_policy(bad)


def test_ledger_is_pipeline_level_only(mod):
    with pytest.raises(ValueError, match="pipeline-level"):
        mod._validate_policy({"budget": {"ledger": {"dir": "x"}}}, root=False)


# ── daily / month / partition caps ────────────────────────────────────────────

@pytest.mark.parametrize("per_step_ops", [False, True])
def test_daily_cap_blocks_the_second_run_sharing_a_ledger(mod, fake_litellm, tmp_path, clock, per_step_ops):
    price(fake_litellm, 0.4)
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 1.0}, per_step_ops=per_step_ops, n_steps=1)
    assert run(c).success and run(c).success                     # 0.4 + 0.4 < 1.0
    r3 = run(c)                                                  # 0.8 < 1.0 -> allowed; ends at 1.2 -> "exceeded" after the call
    assert not r3.success and "max_cost_usd_per_day=1.0 exceeded" in fail_text(r3) and "day '2026-10-23'" in fail_text(r3)
    calls = len(fake_litellm.calls)
    r4 = run(c)                                                  # now blocked BEFORE any model call
    assert not r4.success and "reached" in fail_text(r4) and len(fake_litellm.calls) == calls


def test_cap_boundary_is_exact(mod, fake_litellm, tmp_path, clock):
    price(fake_litellm, 0.25)
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 0.5})
    assert run(c).success and run(c).success                     # spend lands exactly ON the cap: not "exceeded"
    n = len(fake_litellm.calls)
    r = run(c)
    assert not r.success and "reached" in fail_text(r) and len(fake_litellm.calls) == n   # but no further call starts


def test_a_step_with_many_calls_is_one_entry_not_one_per_call(mod, fake_litellm, tmp_path, clock):
    price(fake_litellm, 0.1)
    c = mod.AgenticPipelineComponent(
        asset_name_prefix="bo", source={"kind": "literal", "text": "q"},
        steps=[{"id": "a", "op": "best_of_n", "model": "gpt-4o-mini", "n": 3, "parallel": False, "select": "vote"}],
        outputs={"assets": ["a"]}, policy={"budget": {"ledger": {"dir": str(tmp_path / "led")}, "max_cost_usd_per_day": 50}})
    r = run(c)
    assert r.success, fail_text(r)
    files = list((tmp_path / "led" / "day" / "2026-10-23").glob("*.json"))
    n_calls = len(fake_litellm.calls)
    assert n_calls >= 3 and len(files) == 1                      # one (run, step) file, rewritten per call
    led = mod._SpendLedger({"dir": str(tmp_path / "led")})
    assert led.total("day", "2026-10-23")["llm_calls"] == n_calls
    assert led.total("day", "2026-10-23")["cost_usd"] == pytest.approx(0.1 * n_calls)


def test_daily_cap_resets_on_the_next_utc_day(mod, fake_litellm, tmp_path, clock):
    price(fake_litellm, 0.6)
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 1.0})
    assert run(c).success
    assert not run(c).success                                    # 0.6 then 1.2 -> exceeded
    clock["t"] += timedelta(days=1)
    assert run(c).success                                        # fresh day, fresh allowance


def test_token_cap_per_day(mod, fake_litellm, tmp_path, clock):
    c = comp(mod, tmp_path / "led", {"max_tokens_per_day": 25})
    assert run(c).success and run(c).success                     # 10 + 10 = 20 <= 25
    assert not run(c).success                                    # 30 > 25 after the call


def test_month_cap_spans_days(mod, fake_litellm, tmp_path, clock):
    price(fake_litellm, 1.0)
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_month": 2.5})
    for _ in range(2):
        assert run(c).success
        clock["t"] += timedelta(days=3)
    r = run(c)                                                   # 2.0 -> allowed, 3.0 > 2.5
    assert not r.success and "max_cost_usd_per_month=2.5 exceeded" in fail_text(r) and "'2026-10'" in fail_text(r)
    clock["t"] += timedelta(days=30)                             # next month
    assert run(c).success


def test_partition_cap_sums_all_runs_of_that_partition_only(mod, fake_litellm, tmp_path, clock):
    price(fake_litellm, 0.5)
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_partition": 0.9}, partitioned=True)
    assert run(c, "p1").success                                  # p1: 0.5
    assert not run(c, "p1").success                              # p1: 1.0 > 0.9
    assert run(c, "p2").success                                  # p2 untouched by p1's spend
    r = run(c, "p1")
    assert not r.success and "partition" in fail_text(r) and "p1" in fail_text(r)


def test_warn_mode_records_but_does_not_stop(mod, fake_litellm, tmp_path, clock, caplog):
    price(fake_litellm, 0.6)
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 1.0}, on_violation="warn")
    with caplog.at_level(logging.WARNING):
        assert run(c).success and run(c).success and run(c).success
    assert "max_cost_usd_per_day" in caplog.text
    assert md(run(c))["agentic_budget_violations"] >= 1


# ── circuit breaker ───────────────────────────────────────────────────────────

def breaker_comp(mod, tmp_path, **kw):
    return comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 0.0, "breaker": {"trip_after_violations": 2, "cool_down_minutes": 60}}, **kw)


def test_breaker_trips_then_reopens_after_cool_down(mod, fake_litellm, tmp_path, clock):
    price(fake_litellm, 0.01)
    c = breaker_comp(mod, tmp_path)
    for _ in range(2):                                           # two cap violations (cap 0 => refused before the call)
        r = run(c)
        assert not r.success and "reached" in fail_text(r)
    n = len(fake_litellm.calls)
    r = run(c)
    assert not r.success and "circuit breaker open" in fail_text(r)
    assert "2026-10-23T13:00:00" in fail_text(r)                 # names when it re-opens
    assert len(fake_litellm.calls) == n
    clock["t"] += timedelta(minutes=59)
    assert "circuit breaker open" in fail_text(run(c))
    clock["t"] += timedelta(minutes=2)
    r = run(c)                                                   # breaker closed: the cap itself (still 0) answers
    assert not r.success and "circuit breaker" not in fail_text(r) and "reached" in fail_text(r)


def test_breaker_window_ignores_old_violations(mod, fake_litellm, tmp_path, clock):
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 0.0,
                                     "breaker": {"trip_after_violations": 2, "cool_down_minutes": 5, "window_minutes": 10}})
    assert "reached" in fail_text(run(c))
    clock["t"] += timedelta(minutes=30)                          # first violation is outside the 10 min window
    r = run(c)
    assert "circuit breaker" not in fail_text(r) and "reached" in fail_text(r)
    clock["t"] += timedelta(minutes=1)
    assert "circuit breaker" not in fail_text(run(c))            # 3rd violation: 12:30 and 12:31 are now inside one window
    assert "circuit breaker open" in fail_text(run(c))           # tripped


def test_breaker_refuses_even_in_warn_mode(mod, fake_litellm, tmp_path, clock):
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 0.0, "breaker": {"trip_after_violations": 3, "cool_down_minutes": 60}},
             on_violation="warn")
    assert run(c).success and run(c).success                     # warn: violations recorded (2 per run), calls proceed
    assert "circuit breaker open" in fail_text(run(c))


# ── unknown cost ──────────────────────────────────────────────────────────────

def unpriced(fake):
    def boom(completion_response=None):
        raise RuntimeError("model not in price table")
    fake.completion_cost = boom


def test_unknown_cost_warns_once_per_run_and_token_caps_still_work(mod, fake_litellm, tmp_path, clock, caplog):
    unpriced(fake_litellm)
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 5.0, "max_tokens_per_day": 25}, n_steps=3)
    with caplog.at_level(logging.WARNING):
        r = run(c)                                               # 3 calls x 10 tokens = 30 > 25
    assert caplog.text.count("is unknown") == 1                  # warned once, not per call
    assert not r.success and "max_tokens_per_day=25" in fail_text(r)   # token cap still bites


def test_unknown_cost_fail_mode_refuses(mod, fake_litellm, tmp_path, clock):
    unpriced(fake_litellm)
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 5.0, "on_unknown_cost": "fail"})
    r = run(c)
    assert not r.success and "cost of model" in fail_text(r) and "unknown" in fail_text(r)


def test_unknown_cost_is_silent_without_a_cost_cap_and_counted(mod, fake_litellm, tmp_path, clock, caplog):
    unpriced(fake_litellm)
    c = comp(mod, tmp_path / "led", {"max_tokens_per_day": 1000, "on_unknown_cost": "fail"})
    with caplog.at_level(logging.WARNING):
        r = run(c)
    assert r.success and "is unknown" not in caplog.text         # no cost cap -> nothing to enforce, nothing to warn about
    assert md(r)["agentic_unknown_cost_calls"] == 1


# ── ledger mechanics ──────────────────────────────────────────────────────────

def test_concurrent_runs_on_one_ledger_lose_no_entries(mod, tmp_path):
    led = mod._SpendLedger({"dir": str(tmp_path / "led")})
    n = 40

    def one(i):
        led.write_entry("day", "2026-10-23", f"run{i}__a", {"cost_usd": 0.25, "tokens": 5, "llm_calls": 1, "unknown_cost_calls": 0})
    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        list(ex.map(one, range(n)))
    t = led.total("day", "2026-10-23")
    assert t["cost_usd"] == pytest.approx(n * 0.25) and t["llm_calls"] == n


def test_same_run_and_step_is_not_double_counted_but_never_shrinks(mod, tmp_path):
    led = mod._SpendLedger({"dir": str(tmp_path / "led")})
    v = {"cost_usd": 0.5, "tokens": 10, "llm_calls": 1, "unknown_cost_calls": 0}
    led.write_entry("day", "d", "r1__a", v)
    led.write_entry("day", "d", "r1__a", v)                                  # re-delivered
    assert led.total("day", "d")["cost_usd"] == 0.5
    led.write_entry("day", "d", "r1__a", {**v, "cost_usd": 0.2, "tokens": 4})  # retried attempt restarts lower
    assert led.total("day", "d")["cost_usd"] == 0.5 and led.total("day", "d")["tokens"] == 10
    led.write_entry("day", "d", "r1__b", v)                                  # a different step does add
    assert led.total("day", "d")["cost_usd"] == 1.0


def test_retried_step_in_a_real_run_counts_once(mod, fake_litellm, tmp_path, clock):
    price(fake_litellm, 0.3)
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 100}, n_steps=2)
    r = run(c)
    assert r.success
    led = mod._SpendLedger({"dir": str(tmp_path / "led")})
    files = sorted((tmp_path / "led" / "day" / "2026-10-23").glob("*.json"))
    assert len(files) == 2 and all("__" in f.name for f in files)           # one entry per (run, step)
    assert led.total("day", "2026-10-23")["cost_usd"] == pytest.approx(0.6)


def test_memory_fsspec_ledger(mod, fake_litellm, clock):
    fsspec = pytest.importorskip("fsspec")
    price(fake_litellm, 0.6)
    url = "memory://agentic-ledger-test"
    c = comp(mod, url, {"max_cost_usd_per_day": 1.0})
    assert run(c).success
    r = run(c)
    assert not r.success and "exceeded" in fail_text(r)
    fs = fsspec.filesystem("memory")
    assert len(fs.ls("/agentic-ledger-test/day/2026-10-23", detail=False)) == 2
    fs.rm("/agentic-ledger-test", recursive=True)


def test_compaction_folds_old_entries_without_changing_totals(mod, tmp_path, clock):
    led = mod._SpendLedger({"dir": str(tmp_path / "led")})
    old = (clock["t"] - timedelta(days=10)).isoformat()
    new = clock["t"].isoformat()
    for i in range(3):
        led.write_entry("partition", "k", f"r{i}__a", {"cost_usd": 1.0, "tokens": 1, "llm_calls": 1, "unknown_cost_calls": 0, "ts": old})
    led.write_entry("partition", "k", "r9__a", {"cost_usd": 1.0, "tokens": 1, "llm_calls": 1, "unknown_cost_calls": 0, "ts": new})
    before = led.total("partition", "k")
    led.compact(7)
    names = sorted(p.name for p in (tmp_path / "led" / "partition" / "k").iterdir())
    assert len(names) == 2 and any(n.startswith("_compacted-") for n in names) and "r9__a.json" in names
    assert led.total("partition", "k") == before
    led.compact(7)                                                           # idempotent
    assert led.total("partition", "k") == before
    # a second compactor racing on the same entries cannot double count: simulate by duplicating the compacted file
    comp_file = next(p for p in (tmp_path / "led" / "partition" / "k").iterdir() if p.name.startswith("_compacted-"))
    (comp_file.parent / "_compacted-dup.json").write_text(comp_file.read_text())
    assert led.total("partition", "k") == before


# ── metadata ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("per_step_ops", [False, True])
def test_spend_metadata_is_numeric_and_present(mod, fake_litellm, tmp_path, clock, per_step_ops):
    price(fake_litellm, 0.25)
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 2.0}, per_step_ops=per_step_ops, n_steps=2)
    r = run(c)
    assert r.success
    events = {e.materialization.asset_key.to_user_string(): e.materialization.metadata for e in r.get_asset_materialization_events()}
    first, second = events["t_a"], events["t_s1"]
    for k, kind in (("agentic_cost_usd", dg.FloatMetadataValue), ("agentic_tokens", dg.IntMetadataValue),
                    ("agentic_llm_calls", dg.IntMetadataValue), ("agentic_tool_calls", dg.IntMetadataValue),
                    ("agentic_latency_ms", dg.IntMetadataValue), ("agentic_budget_violations", dg.IntMetadataValue),
                    ("agentic_spent_today_usd", dg.FloatMetadataValue), ("agentic_remaining_today_usd", dg.FloatMetadataValue)):
        assert isinstance(first[k], kind), k
        assert k not in second                                   # pipeline totals ride one asset: no double counting in Insights
    assert first["agentic_cost_usd"].value == pytest.approx(0.5) and first["agentic_llm_calls"].value == 2
    assert first["agentic_tokens"].value == 20
    assert first["agentic_spent_today_usd"].value == pytest.approx(0.5)
    assert first["agentic_remaining_today_usd"].value == pytest.approx(1.5)
    assert first["agentic_step_cost_usd"].value == pytest.approx(0.25) and second["agentic_step_llm_calls"].value == 1
    assert first["agentic_budget_violations"].value == 0


def test_spend_metadata_without_any_policy(mod, fake_litellm):
    price(fake_litellm, 0.1)
    c = mod.AgenticPipelineComponent(asset_name_prefix="n", source={"kind": "literal", "text": "x"},
                                     steps=[{"id": "a", **STEP}], outputs={"assets": ["a"]})
    m = dg.materialize(c.build_defs(None).assets).get_asset_materialization_events()[0].materialization.metadata
    assert m["agentic_cost_usd"].value == pytest.approx(0.1) and m["agentic_llm_calls"].value == 1
    assert "agentic_spent_today_usd" not in m                    # no ledger configured -> no ledger snapshot


def test_insights_metrics_constant_matches_what_is_emitted(mod, fake_litellm, tmp_path, clock):
    import ast
    import pathlib
    price(fake_litellm, 0.1)
    src = (pathlib.Path(mod.__file__)).read_text()
    lit = next(n for n in ast.parse(src).body if isinstance(n, ast.Assign) and n.targets[0].id == "INSIGHTS_METRICS")
    listed = ast.literal_eval(lit.value)                         # plain literal: tools can read it without importing
    assert listed == mod.INSIGHTS_METRICS
    assert {m["unit_type"] for m in listed} <= {"INTEGER", "FLOAT", "TIME_MS", "TIME_SECONDS", "BYTES"}
    c = comp(mod, tmp_path / "led", {"max_cost_usd_per_day": 1.0})
    emitted = md(run(c))
    # `agentic_failures` is emitted only on the failure observation of a failed step (tests/test_failure_observation.py), never on success
    # `agentic_parked_runs` likewise only on a run that parks (tests/test_park_pending_and_config_change.py)
    assert {m["metadata_key"] for m in listed} - {"agentic_failures", "agentic_parked_runs"} <= set(emitted)
    assert "agentic_failures" not in emitted and "agentic_parked_runs" not in emitted


def test_tool_calls_are_counted(mod, fake_litellm, tmp_path, monkeypatch):
    async def call(**kw):
        return {"raw": "x", "kind": "text", "is_error": False, "value": "x"}
    monkeypatch.setattr(mod, "_call_mcp_tool_async", call)
    c = mod.AgenticPipelineComponent(
        asset_name_prefix="m", source={"kind": "literal", "text": "x"},
        steps=[{"id": "m", "op": "mcp_call", "server": {"name": "fs", "type": "stdio", "command": ["x"]},
                "mcp_tool_name": "read", "tool_args": {}}], outputs={"assets": ["m"]})
    m = dg.materialize(c.build_defs(None).assets).get_asset_materialization_events()[0].materialization.metadata
    assert m["agentic_tool_calls"].value == 1


def test_map_with_threads_sees_policy_and_spend(mod, fake_litellm, ctx_=None):
    from types import SimpleNamespace
    ctx = SimpleNamespace(log=logging.getLogger("t"))
    state = {"source": {"text": json.dumps(["a", "b", "c", "d"])}, "__ctx__": {"partition_key": None, "partition_fields": None},
             "__personas__": {}, "__policy__": {"budget": {"max_llm_calls": 2}}}
    step = {"id": "m", "op": "map", "model": "gpt-4o-mini", "prompt_template": "{item}", "max_concurrent": 2, "items_from": "text"}
    with pytest.raises(mod.PolicyViolation):
        mod._run_step(step, state, ctx)
    assert len(fake_litellm.calls) <= 3                          # the budget bit inside worker threads (it never did)


# ── prompt versions ───────────────────────────────────────────────────────────

def attrs(**over):
    base = {"steps": [{"id": "a", "op": "llm_call", "model": "m", "prompt_template": "A {text}", "temperature": 0.1},
                      {"id": "b", "op": "llm_call", "model": "m", "prompt_template": "B {a}", "source": "a", "persona": "p"},
                      {"id": "c", "op": "approve", "source": "b"}],
            "personas": {"p": {"system_prompt": "be brief", "model": "m"}, "q": {"system_prompt": "other"}}}
    base.update(over)
    return base


def test_prompt_versions_are_stable_and_only_for_llm_steps(mod):
    v = mod.prompt_versions(attrs())
    assert set(v) == {"a", "b"} and v == mod.prompt_versions(attrs())
    assert v["a"]["version"] == v["a"]["hash"] and v["a"]["hash"].startswith("p-")


def test_prompt_version_ignores_wiring_and_sensitive_to_prompt_model_params_persona(mod):
    base = mod.prompt_versions(attrs())
    a = attrs()
    a["steps"][0]["id"] = "a"
    a["steps"][0]["when"] = {"source": "source", "truthy": True}
    a["steps"][0]["source"] = "source"
    assert mod.prompt_versions(a)["a"] == base["a"]                          # wiring/gating is not the prompt
    for field, val in (("prompt_template", "A2 {text}"), ("model", "m2"), ("temperature", 0.9), ("system_prompt", "sys")):
        a = attrs()
        a["steps"][0][field] = val
        pv = mod.prompt_versions(a)
        assert pv["a"]["hash"] != base["a"]["hash"] and pv["b"] == base["b"], field   # exactly that step
    a = attrs()
    a["personas"]["p"]["system_prompt"] = "be verbose"                        # persona edit moves the steps that use it
    pv = mod.prompt_versions(a)
    assert pv["b"]["hash"] != base["b"]["hash"] and pv["a"] == base["a"]
    a = attrs()
    a["personas"]["q"]["system_prompt"] = "changed"                          # unused persona: nobody's version moves
    assert mod.prompt_versions(a) == base


def test_explicit_version_names(mod):
    a = attrs()
    a["steps"][0]["version"] = "v3"
    a["personas"]["p"]["version"] = "tone-2"
    v = mod.prompt_versions(a)
    assert v["a"]["version"] == "v3" and v["b"]["version"] == "tone-2"
    assert v["a"]["hash"].startswith("p-")                                   # hash still available to detect un-bumped edits


def _vcomp(mod, **kw):
    steps = [{"id": "a", "op": "llm_call", "model": "m", "prompt_template": "A {text}", "persona": "p"},
             {"id": "b", "op": "llm_call", "model": "m", "prompt_template": "B {a}", "source": "a", "persona": "q"},
             {"id": "c", "op": "llm_call", "model": "m", "prompt_template": "C {b}", "source": "b"}]
    personas = {"p": {"system_prompt": "P"}, "q": {"system_prompt": "Q"}}
    for k, v in kw.items():
        if k == "b_prompt":
            steps[1]["prompt_template"] = v
        if k == "q_system":
            personas["q"]["system_prompt"] = v
        if k == "budget":
            pass
    pol = {"budget": {"max_cost_usd": kw.get("budget", 1.0)}}
    return mod.AgenticPipelineComponent(asset_name_prefix="v", source={"kind": "literal", "text": "hi"}, steps=steps,
                                        outputs={"assets": ["a", "b", "c"]}, personas=personas, policy=pol)


def versions(c):
    return {k.to_user_string(): v for k, v in c.build_defs(None).assets[0].code_versions_by_key.items()}


def test_code_version_changes_exactly_the_edited_step(mod):
    base = versions(_vcomp(mod))
    p = versions(_vcomp(mod, b_prompt="B2 {a}"))
    assert [k for k in base if base[k] != p[k]] == ["v_b"]
    q = versions(_vcomp(mod, q_system="Q2"))                                 # persona used only by b
    assert [k for k in base if base[k] != q[k]] == ["v_b"]
    assert versions(_vcomp(mod, budget=99.0)) == base                         # tuning a cap does not stale every asset


def test_downstream_is_stale_per_dagster_when_one_prompt_changes(mod, fake_litellm):
    from dagster._core.definitions.data_version import StaleStatus
    from dagster._core.definitions.data_version import CachingStaleStatusResolver
    inst = dg.DagsterInstance.ephemeral()
    d1 = _vcomp(mod).build_defs(None)
    assert dg.materialize(d1.assets, instance=inst).success
    d2 = _vcomp(mod, b_prompt="B2 {a}").build_defs(None)
    from dagster._core.loader import LoadingContextForTest
    resolver = CachingStaleStatusResolver(inst, d2.resolve_asset_graph(), LoadingContextForTest(inst))
    st = {k: resolver.get_status(dg.AssetKey(k)) for k in ("v_a", "v_b", "v_c")}
    assert st["v_a"] == StaleStatus.FRESH and st["v_b"] == StaleStatus.STALE        # the edited step, by code version
    assert st["v_c"] == StaleStatus.FRESH         # Dagster only propagates once the edited step is re-materialized (real behaviour)
    assert dg.materialize(d2.assets, instance=inst, selection=["v_b"]).success      # re-run just b (can_subset)
    resolver = CachingStaleStatusResolver(inst, d2.resolve_asset_graph(), LoadingContextForTest(inst))
    st = {k: resolver.get_status(dg.AssetKey(k)) for k in ("v_a", "v_b", "v_c")}
    assert st["v_a"] == StaleStatus.FRESH and st["v_b"] == StaleStatus.FRESH
    assert st["v_c"] == StaleStatus.STALE         # downstream of the changed prompt is now stale


@pytest.mark.parametrize("per_step_ops", [False, True])
def test_versions_flow_into_materialization_metadata(mod, fake_litellm, per_step_ops):
    c = _vcomp(mod)
    c.per_step_ops = per_step_ops
    m = {e.materialization.asset_key.to_user_string(): e.materialization.metadata
         for e in dg.materialize(c.build_defs(None).assets).get_asset_materialization_events()}
    pv = mod.prompt_versions({"steps": c.steps, "personas": c.personas})
    assert m["v_a"]["agentic_prompt_version"].value == pv["a"]["version"]
    assert m["v_b"]["agentic_prompt_hash"].value == pv["b"]["hash"]
    assert m["v_c"]["agentic_prompt_versions"].value == {k: v["version"] for k, v in pv.items()}
