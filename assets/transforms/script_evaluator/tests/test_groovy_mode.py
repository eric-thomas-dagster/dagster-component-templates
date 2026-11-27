"""Real tests for ScriptEvaluatorComponent's language='groovy' mode -- scripts actually
execute via a real `groovy` CLI subprocess (a real JVM), nothing mocked except the one
test that simulates `groovy` being absent from PATH. Skipped entirely if `groovy` isn't
installed in this environment (install via e.g. `brew install groovy` on macOS)."""
import importlib.util
import os
import shutil
import sys
import time

import pandas as pd
import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_COMPONENT_PATH = os.path.join(_HERE, "..", "component.py")

pytestmark = pytest.mark.skipif(
    shutil.which("groovy") is None,
    reason="groovy CLI not installed on PATH (e.g. `brew install groovy`)",
)


def _load_module():
    spec = importlib.util.spec_from_file_location("script_evaluator_groovy_under_test", _COMPONENT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["script_evaluator_groovy_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


# ---- Basic transform ----

def test_groovy_basic_transform(mod):
    script = "record.c = record.a + record.b"
    r = mod._execute_isolated("groovy", None, script, None, [{"a": 2, "b": 3}], {}, {}, "record", "fail", 30, 0)
    assert r["records"][0]["c"] == 5


def test_groovy_division_promotes_to_decimal_not_floored(mod):
    # Real Groovy semantics: Integer/Integer division returns a BigDecimal, unlike
    # Jython's floor division -- this is genuine behavior, not something to "fix".
    script = "record.c = record.a / record.b"
    r = mod._execute_isolated("groovy", None, script, None, [{"a": 7, "b": 2}], {}, {}, "record", "fail", 30, 0)
    assert abs(r["records"][0]["c"] - 3.5) < 1e-9


# ---- Native java.util.Map/List methods -- no JMap/JList shim needed ----

def test_groovy_native_map_list_methods(mod):
    records = [{"items": [1, 2, 3], "meta": {"x": 1}}]
    script = """
record.items.add(4)
record.size = record.items.size()
record.meta.put("y", 2)
record.has_x = record.meta.containsKey("x")
record.has_z = record.meta.containsKey("z")
"""
    r = mod._execute_isolated("groovy", None, script, None, records, {}, {}, "record", "fail", 30, 0)
    out = r["records"][0]
    assert out["items"] == [1, 2, 3, 4]
    assert out["size"] == 4
    assert out["meta"] == {"x": 1, "y": 2}
    assert out["has_x"] is True
    assert out["has_z"] is False


def test_groovy_bracket_syntax_also_works(mod):
    script = 'record["c"] = record["a"] + record["b"]'
    r = mod._execute_isolated("groovy", None, script, None, [{"a": 10, "b": 5}], {}, {}, "record", "fail", 30, 0)
    assert r["records"][0]["c"] == 15


# ---- Cross-invocation primitives: userParams, state, events, init/destroy ----

def test_groovy_user_params_and_state(mod):
    script = "record.threshold_hit = record.a > userParams.threshold\nstate.seen = (state.seen ?: 0) + 1"
    r = mod._execute_isolated(
        "groovy", None, script, None, [{"a": 5}, {"a": 15}],
        {"threshold": 10}, {"seen": 100}, "record", "fail", 30, 0,
    )
    assert r["records"][0]["threshold_hit"] is False
    assert r["records"][1]["threshold_hit"] is True
    assert r["state"]["seen"] == 102


def test_groovy_init_and_destroy_scripts_run(mod):
    init = 'state.init_ran = true'
    destroy = 'state.destroy_ran = true'
    script = "record.touched = true"
    r = mod._execute_isolated("groovy", init, script, destroy, [{"a": 1}], {}, {}, "record", "fail", 30, 0)
    assert r["state"]["init_ran"] is True
    assert r["state"]["destroy_ran"] is True


def test_groovy_events(mod):
    script = 'events.add([kind: "processed", index: record.a])'
    r = mod._execute_isolated("groovy", None, script, None, [{"a": 1}, {"a": 2}], {}, {}, "record", "fail", 30, 0)
    assert len(r["events"]) == 2
    assert r["events"][0]["kind"] == "processed"


# ---- Batch mode ----

def test_groovy_batch_mode_sees_whole_batch(mod):
    script = "def total = records.sum { it.a }\nrecords.add([sum: total])"
    r = mod._execute_isolated("groovy", None, script, None, [{"a": 1}, {"a": 2}, {"a": 3}], {}, {}, "batch", "fail", 30, 0)
    assert r["records"][-1]["sum"] == 6


# ---- Error handling ----

def test_groovy_record_error_mode_skips_bad_record(mod):
    script = "if (record.a == 1) { throw new RuntimeException('boom') }\nrecord.ok = true"
    r = mod._execute_isolated("groovy", None, script, None, [{"a": 1}, {"a": 2}], {}, {}, "record", "record_error", 30, 0)
    assert len(r["errors"]) == 1 and r["errors"][0]["index"] == 0
    assert len(r["records"]) == 1 and r["records"][0]["a"] == 2


def test_groovy_fail_mode_propagates(mod):
    script = "throw new RuntimeException('boom')"
    with pytest.raises(RuntimeError, match="boom"):
        mod._execute_isolated("groovy", None, script, None, [{"a": 1}], {}, {}, "record", "fail", 30, 0)


def test_groovy_timeout_is_enforced(mod):
    script = "while (true) {}"
    start = time.time()
    with pytest.raises(TimeoutError):
        mod._execute_isolated("groovy", None, script, None, [{"a": 1}], {}, {}, "batch", "fail", 3, 0)
    assert time.time() - start < 15  # didn't hang past the timeout + JVM-exit overhead


def test_groovy_missing_binary_raises_clear_runtime_error(mod, monkeypatch):
    monkeypatch.setattr(mod.shutil, "which", lambda name: None)
    with pytest.raises(RuntimeError, match="requires the 'groovy' CLI"):
        mod._execute_isolated("groovy", None, "record.c = 1", None, [{"a": 1}], {}, {}, "record", "fail", 10, 0)


# ---- Full asset integration (DataFrame in, DataFrame out) ----

def test_component_materializes_as_real_asset_groovy(mod):
    import dagster as dg

    component = mod.ScriptEvaluatorComponent(
        asset_name="enriched_groovy",
        upstream_asset_key="raw_groovy",
        language="groovy",
        mode="record",
        script="record.doubled = record.a * 2",
    )

    @dg.asset(key=dg.AssetKey("raw_groovy"))
    def raw() -> pd.DataFrame:
        return pd.DataFrame({"a": [1, 2, 3]})

    defs = component.build_defs(None)
    enriched_asset = next(iter(defs.assets))

    result = dg.materialize([raw, enriched_asset])
    assert result.success
    out = result.asset_value(dg.AssetKey("enriched_groovy"))
    assert list(out["doubled"]) == [2, 4, 6]


def test_build_defs_raises_import_error_when_groovy_missing(mod, monkeypatch):
    monkeypatch.setattr(mod.shutil, "which", lambda name: None)
    component = mod.ScriptEvaluatorComponent(
        asset_name="enriched_groovy_missing",
        upstream_asset_key="raw_groovy_missing",
        language="groovy",
        mode="record",
        script="record.doubled = record.a * 2",
    )
    with pytest.raises(ImportError, match="requires the 'groovy' CLI"):
        component.build_defs(None)
