"""Real tests for ScriptEvaluatorComponent -- scripts actually execute (Jython via a
real subprocess, JavaScript via the real quickjs-ng engine), nothing mocked. Only the
subprocess boundary itself is a real OS process, which is the point being tested."""
import importlib.util
import json
import os
import sys
import time

import pandas as pd
import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_COMPONENT_PATH = os.path.join(_HERE, "..", "component.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("script_evaluator_under_test", _COMPONENT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["script_evaluator_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


# ---- Jython mode: Java-collection semantics + integer-division floor ----

def test_jython_integer_division_floors(mod):
    records = [{"a": 7, "b": 2}, {"a": 10, "b": 4}, {"a": -7, "b": 2}]
    script = 'record.put("c", record.get("a") / record.get("b"))'
    r = mod._execute_isolated("jython", None, script, None, records, {}, {}, "record", "fail", 10, 64_000_000)
    assert r["records"][0]["c"] == 3
    assert r["records"][1]["c"] == 2
    assert r["records"][2]["c"] == -4  # floor, not truncation: -7 // 2 == -4


def test_jython_float_division_is_true_division(mod):
    records = [{"a": 7.0, "b": 2}]
    script = 'record.put("c", record.get("a") / record.get("b"))'
    r = mod._execute_isolated("jython", None, script, None, records, {}, {}, "record", "fail", 10, 64_000_000)
    assert abs(r["records"][0]["c"] - 3.5) < 1e-9


def test_jython_java_list_map_methods(mod):
    records = [{"items": [1, 2, 3], "meta": {"x": 1}}]
    script = """
items = record.get("items")
items.add(4)
record.put("size", items.size())
meta = record.get("meta")
meta.put("y", 2)
record.put("has_x", meta.containsKey("x"))
record.put("has_z", meta.containsKey("z"))
"""
    r = mod._execute_isolated("jython", None, script, None, records, {}, {}, "record", "fail", 10, 64_000_000)
    out = r["records"][0]
    assert out["items"] == [1, 2, 3, 4]
    assert out["size"] == 4
    assert out["meta"] == {"x": 1, "y": 2}
    assert out["has_x"] is True
    assert out["has_z"] is False


def test_jython_list_has_no_push(mod):
    # A real StreamSets Jython script never has .push() on a Java List -- confirm
    # this shim doesn't accidentally provide it.
    script = 'record.get("items").push(1)'
    r = mod._execute_isolated("jython", None, script, None, [{"items": [1]}], {}, {}, "record", "record_error", 10, 64_000_000)
    assert len(r["errors"]) == 1
    assert "push" in r["errors"][0]["message"]


# ---- Cross-invocation primitives: userParams, state, events, init/destroy ----

def test_jython_user_params_and_state(mod):
    script = 'record.put("threshold_hit", record.get("a") > userParams.get("threshold"))\nstate.put("seen", state.get("seen", 0) + 1)'
    r = mod._execute_isolated(
        "jython", None, script, None, [{"a": 5}, {"a": 15}],
        {"threshold": 10}, {"seen": 100}, "record", "fail", 10, 64_000_000,
    )
    assert r["records"][0]["threshold_hit"] is False
    assert r["records"][1]["threshold_hit"] is True
    assert r["state"]["seen"] == 102  # 100 + 2 records


def test_jython_init_and_destroy_scripts_run(mod):
    init = 'state.put("init_ran", True)'
    destroy = 'state.put("destroy_ran", True)'
    script = "pass"
    r = mod._execute_isolated("jython", init, script, destroy, [{"a": 1}], {}, {}, "record", "fail", 10, 64_000_000)
    assert r["state"]["init_ran"] is True
    assert r["state"]["destroy_ran"] is True


def test_jython_events(mod):
    script = 'events.add({"kind": "processed", "index": record.get("a")})'
    r = mod._execute_isolated("jython", None, script, None, [{"a": 1}, {"a": 2}], {}, {}, "record", "fail", 10, 64_000_000)
    assert len(r["events"]) == 2
    assert r["events"][0]["kind"] == "processed"


# ---- Batch mode ----

def test_jython_batch_mode_sees_whole_batch(mod):
    script = 'records.add({"sum": sum(r.get("a") for r in records)})'
    r = mod._execute_isolated("jython", None, script, None, [{"a": 1}, {"a": 2}, {"a": 3}], {}, {}, "batch", "fail", 10, 64_000_000)
    assert r["records"][-1]["sum"] == 6


# ---- Error handling ----

def test_jython_record_error_mode_skips_bad_record(mod):
    script = 'if record.get("a") == 1: raise ValueError("boom")\nrecord.put("ok", True)'
    r = mod._execute_isolated("jython", None, script, None, [{"a": 1}, {"a": 2}], {}, {}, "record", "record_error", 10, 64_000_000)
    assert len(r["errors"]) == 1 and r["errors"][0]["index"] == 0
    assert len(r["records"]) == 1 and r["records"][0]["a"] == 2


def test_jython_fail_mode_propagates(mod):
    script = 'raise ValueError("boom")'
    with pytest.raises(RuntimeError, match="boom"):
        mod._execute_isolated("jython", None, script, None, [{"a": 1}], {}, {}, "record", "fail", 10, 64_000_000)


def test_jython_timeout_is_enforced(mod):
    script = "while True: pass"
    start = time.time()
    with pytest.raises(TimeoutError):
        mod._execute_isolated("jython", None, script, None, [{"a": 1}], {}, {}, "batch", "fail", 2, 64_000_000)
    assert time.time() - start < 5  # didn't hang past the timeout + small overhead


# ---- JavaScript mode (real quickjs-ng, not mocked) ----

def test_javascript_basic_transform(mod):
    script = "record.c = record.a + record.b;"
    r = mod._execute_isolated("javascript", None, script, None, [{"a": 2, "b": 3}], {}, {}, "record", "fail", 10, 64_000_000)
    assert r["records"][0]["c"] == 5


def test_javascript_division_is_true_division_not_floored(mod):
    script = "record.c = record.a / record.b;"
    r = mod._execute_isolated("javascript", None, script, None, [{"a": 7, "b": 2}], {}, {}, "record", "fail", 10, 64_000_000)
    assert abs(r["records"][0]["c"] - 3.5) < 1e-9


def test_javascript_state_and_user_params(mod):
    script = "record.hit = record.a > userParams.threshold; state.seen = (state.seen || 0) + 1;"
    r = mod._execute_isolated(
        "javascript", None, script, None, [{"a": 5}, {"a": 15}],
        {"threshold": 10}, {}, "record", "fail", 10, 64_000_000,
    )
    assert r["records"][0]["hit"] is False
    assert r["records"][1]["hit"] is True
    assert r["state"]["seen"] == 2


def test_javascript_events(mod):
    script = "events.push({kind: 'processed'});"
    r = mod._execute_isolated("javascript", None, script, None, [{"a": 1}], {}, {}, "record", "fail", 10, 64_000_000)
    assert len(r["events"]) == 1 and r["events"][0]["kind"] == "processed"


def test_javascript_timeout_is_enforced(mod):
    script = "while (true) {}"
    with pytest.raises((TimeoutError, RuntimeError)):
        # QuickJS's own set_time_limit may raise inside the worker before the outer
        # subprocess timeout even triggers -- either is a correctly-enforced bound.
        mod._execute_isolated("javascript", None, script, None, [{"a": 1}], {}, {}, "batch", "fail", 5, 64_000_000)


def test_javascript_record_error_mode(mod):
    script = "if (record.a === 1) { throw new Error('boom'); } record.ok = true;"
    r = mod._execute_isolated("javascript", None, script, None, [{"a": 1}, {"a": 2}], {}, {}, "record", "record_error", 10, 64_000_000)
    assert len(r["errors"]) == 1
    assert len(r["records"]) == 1 and r["records"][0]["a"] == 2


# ---- Cross-run state persistence (file-backed) ----

def test_state_persists_across_runs_via_state_path(mod, tmp_path):
    state_path = str(tmp_path / "state.json")
    script = 'state.put("count", state.get("count", 0) + 1)'

    for expected in (1, 2, 3):
        state = mod._load_state(state_path)
        r = mod._execute_isolated("jython", None, script, None, [{"a": 1}], {}, state, "record", "fail", 10, 64_000_000)
        mod._write_state(state_path, r["state"])

    final = json.loads(open(state_path).read())
    assert final["count"] == 3


# ---- Full asset integration (DataFrame in, DataFrame out) ----

def test_component_materializes_as_real_asset(mod):
    import dagster as dg

    component = mod.ScriptEvaluatorComponent(
        asset_name="enriched",
        upstream_asset_key="raw",
        language="jython",
        mode="record",
        script='record.put("doubled", record.get("a") * 2)',
    )

    @dg.asset(key=dg.AssetKey("raw"))
    def raw() -> pd.DataFrame:
        return pd.DataFrame({"a": [1, 2, 3]})

    defs = component.build_defs(None)
    enriched_asset = next(iter(defs.assets))

    result = dg.materialize([raw, enriched_asset])
    assert result.success
    out = result.asset_value(dg.AssetKey("enriched"))
    assert list(out["doubled"]) == [2, 4, 6]
