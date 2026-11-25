"""Script Evaluator.

Runs a customer-authored transform script against each row (mode: record) or the
whole batch at once (mode: batch) of an upstream DataFrame -- the Dagster analog
of StreamSets' Groovy/JavaScript/Jython Script Evaluator processor.

SECURITY MODEL -- read this before pointing this component at untrusted script
content. This component executes ARBITRARY code a script author wrote. It is NOT
a security sandbox against a malicious author:

  - `language: javascript` runs inside QuickJS (quickjs-ng), which has no
    filesystem, network, process, or module-loading bindings by construction --
    there is nothing for a JS script to reach outside its own data, and a real,
    verified `set_time_limit`/`set_memory_limit` backstop is enforced (confirmed
    directly: an infinite `while(true){}` loop is interrupted at the configured
    limit, not just documented-but-unenforced).
  - `language: jython` runs real CPython in a separate subprocess (never in the
    Dagster run's own process), with a hard wall-clock timeout
    (`timeout_seconds`) enforced by terminating the subprocess if it doesn't
    return in time. This gives process isolation and a DoS backstop -- it does
    NOT prevent a script from reading environment variables, the filesystem, or
    network access available to that subprocess's user. Treat script content
    with the same trust level you'd give any other custom code you deploy.

  Groovy is NOT supported. Unlike this registry's PySpark/Snowpark pipeline
  components (which bundle their own JVM/driver via a pure `pip install`),
  running Groovy needs a system-level JVM *and* a separately-installed Groovy
  distribution -- neither is pip-installable, so this registry has no way to
  package or verify it. Revisit if real demand shows up with a concrete,
  reproducible install story.

Semantics matched to the real StreamSets Script Evaluator stage:
  - Jython mode: record fields that are lists/dicts are exposed as Java-collection-
    style objects (`.add`/`.get`/`.size`/`.put`/`.containsKey` method calls, not
    Python `list.append`/dict `[]` syntax) -- `list.push(...)` does not exist in
    this semantic and is deliberately not implemented. Integer division (`/` on
    two ints) floors like Jython/Python 2, not CPython 3's true division --
    implemented via an AST rewrite (`ast.Div` -> a runtime-type-checking helper),
    not a static guess, so it's correct regardless of what types a variable holds
    at the point division actually runs.
  - `userParams` (a plain dict) and `state` (persisted across RUNS via a small
    local JSON file at `state_path` -- same pattern as `s3_monitor`'s dynamic-
    partition pruning state) are both available in script scope under those
    names.
  - `init_script` runs once before the first record/batch; `destroy_script` runs
    once after the last. Both optional.
  - `on_script_error: record_error` (default) catches a script exception per
    record (mode: record) or for the whole batch (mode: batch) and records it in
    metadata instead of failing the run; `on_script_error: fail` re-raises.
  - Events: a script can call `events.add(...)` (Jython) / `events.push(...)`
    (JavaScript) to emit an out-of-band event dict -- surfaced in this asset's
    MaterializeResult metadata (`events`), not as a second physical Dagster
    output. This repo has no existing multi-output-asset precedent for transform
    components, so rich metadata on one asset is the convention followed here,
    not three separate Dagster outputs.

Example:
    ```yaml
    type: dagster_community_components.ScriptEvaluatorComponent
    attributes:
      asset_name: enriched_orders
      upstream_asset_key: raw_orders
      language: jython
      mode: record
      script: |
        record['total'] = record['price'] * record['qty']
        if record['total'] > 1000:
            record['tier'] = 'gold'
        state['count'] = state.get('count', 0) + 1
      state_path: ./script_evaluator_state.json
      on_script_error: record_error
      timeout_seconds: 60
    ```
"""
import ast
import json
import os
from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    BackfillPolicy,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field, model_validator


# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


# BEGIN Jython integer-division AST rewrite (inlined per component; keep copies identical — see tests/test_jython_div_rewrite.py)
class _JythonDivTransformer(ast.NodeTransformer):
    """Rewrites every `a / b` into `_jython_div(a, b)` so division floors when both
    operands are ints at RUNTIME (Jython/Python 2 semantics), true-divides otherwise --
    a static guess at parse time can't know the operand types, so this defers the
    check to the helper, which runs on every actual division regardless of what the
    script's variables turn out to hold."""

    def visit_BinOp(self, node):
        self.generic_visit(node)
        if isinstance(node.op, ast.Div):
            return ast.copy_location(
                ast.Call(
                    func=ast.Name(id="_jython_div", ctx=ast.Load()),
                    args=[node.left, node.right],
                    keywords=[],
                ),
                node,
            )
        return node


def _jython_div(a, b):
    if isinstance(a, int) and isinstance(b, int) and not isinstance(a, bool) and not isinstance(b, bool):
        return a // b
    return a / b
# END Jython integer-division AST rewrite


# BEGIN Java-collection emulation (inlined per component; keep copies identical — see tests/test_java_collection_shim.py)
class JList:
    """Java `java.util.List`-style wrapper over a plain Python list -- `.add`/`.get`/
    `.size`/`.remove`, not Python `list.append`/`[]`/`len()`. Deliberately has no
    `.push` (that's a JS Array method, not a Java List one -- a real StreamSets
    Jython script never has it either)."""

    def __init__(self, data: Optional[list] = None):
        self._data = data if data is not None else []

    def add(self, value):
        self._data.append(_wrap_for_script(value))
        return True

    def get(self, i):
        return self._data[i]

    def set(self, i, value):
        self._data[i] = _wrap_for_script(value)

    def size(self):
        return len(self._data)

    def remove(self, i):
        return self._data.pop(i)

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def unwrap(self):
        return [_unwrap_from_script(v) for v in self._data]


class JMap:
    """Java `java.util.Map`-style wrapper over a plain Python dict -- `.put`/`.get`/
    `.containsKey`/`.remove`, not Python dict `[]`/`in`."""

    def __init__(self, data: Optional[dict] = None):
        self._data = data if data is not None else {}

    def put(self, key, value):
        self._data[key] = _wrap_for_script(value)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def containsKey(self, key):
        return key in self._data

    def remove(self, key):
        return self._data.pop(key, None)

    def keySet(self):
        return JList(list(self._data.keys()))

    def size(self):
        return len(self._data)

    def unwrap(self):
        return {k: _unwrap_from_script(v) for k, v in self._data.items()}


def _wrap_for_script(value):
    if isinstance(value, list):
        return JList([_wrap_for_script(v) for v in value])
    if isinstance(value, dict):
        return JMap({k: _wrap_for_script(v) for k, v in value.items()})
    return value


def _unwrap_from_script(value):
    if isinstance(value, JList):
        return value.unwrap()
    if isinstance(value, JMap):
        return value.unwrap()
    if isinstance(value, list):
        return [_unwrap_from_script(v) for v in value]
    if isinstance(value, dict):
        return {k: _unwrap_from_script(v) for k, v in value.items()}
    return value
# END Java-collection emulation


# BEGIN cross-run state persistence (inlined per component; keep copies identical — see tests/test_state_persistence.py)
def _load_state(state_path: Optional[str]) -> dict:
    if not state_path or not os.path.exists(state_path):
        return {}
    try:
        with open(state_path) as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return {}


def _write_state(state_path: Optional[str], state: dict) -> None:
    if not state_path:
        return
    try:
        with open(state_path, "w") as f:
            json.dump(state, f)
    except Exception:  # noqa: BLE001
        pass
# END cross-run state persistence


def _run_jython(init_script, script, destroy_script, records, user_params, state, mode, on_script_error):
    """Executes inside the subprocess spawned by `_execute_isolated` below. Returns a
    plain-JSON-safe dict -- everything here must survive pickling across the
    multiprocessing queue."""
    out_records: List[dict] = []
    errors: List[dict] = []
    events: List[dict] = []

    events_list = JList([])
    globals_ns = {
        "_jython_div": _jython_div,
        "userParams": JMap(dict(user_params or {})),
        "state": JMap(dict(state or {})),
        "events": events_list,
        "JList": JList,
        "JMap": JMap,
    }

    def _compile(src):
        tree = ast.parse(src)
        _JythonDivTransformer().visit(tree)
        ast.fix_missing_locations(tree)
        return compile(tree, "<script_evaluator>", "exec")

    if init_script:
        exec(_compile(init_script), globals_ns)  # noqa: S102 -- see module docstring security model

    def _run_one(record_dict):
        globals_ns["record"] = _wrap_for_script(record_dict)
        exec(_compile(script), globals_ns)  # noqa: S102 -- see module docstring security model
        return _unwrap_from_script(globals_ns["record"])

    if mode == "batch":
        globals_ns["records"] = JList([_wrap_for_script(r) for r in records])
        try:
            exec(_compile(script), globals_ns)  # noqa: S102
            out_records = [_unwrap_from_script(r) for r in globals_ns["records"]]
        except Exception as e:  # noqa: BLE001
            if on_script_error == "fail":
                raise
            errors.append({"index": None, "message": str(e)})
            out_records = list(records)
    else:
        for i, rec in enumerate(records):
            try:
                out_records.append(_run_one(rec))
            except Exception as e:  # noqa: BLE001
                if on_script_error == "fail":
                    raise
                errors.append({"index": i, "message": str(e)})

    if destroy_script:
        exec(_compile(destroy_script), globals_ns)  # noqa: S102

    events = events_list.unwrap()
    final_state = globals_ns["state"].unwrap() if isinstance(globals_ns.get("state"), JMap) else {}
    return {"records": out_records, "errors": errors, "events": events, "state": final_state}


def _run_javascript(init_script, script, destroy_script, records, user_params, state, mode, on_script_error, memory_limit_bytes):
    import quickjs

    ctx = quickjs.Context()
    ctx.set_memory_limit(memory_limit_bytes)
    ctx.set("__user_params_json", json.dumps(user_params or {}))
    ctx.set("__state_json", json.dumps(state or {}))
    ctx.eval(
        "globalThis.userParams = JSON.parse(__user_params_json); "
        "globalThis.state = JSON.parse(__state_json); "
        "globalThis.events = [];"
    )

    if init_script:
        ctx.eval(init_script)

    out_records: List[dict] = []
    errors: List[dict] = []

    def _run_one(record_dict):
        ctx.set("__record_json", json.dumps(record_dict))
        ctx.eval("globalThis.record = JSON.parse(__record_json);")
        ctx.eval(script)
        return json.loads(ctx.eval("JSON.stringify(record)"))

    if mode == "batch":
        ctx.set("__records_json", json.dumps(list(records)))
        ctx.eval("globalThis.records = JSON.parse(__records_json);")
        try:
            ctx.eval(script)
            out_records = json.loads(ctx.eval("JSON.stringify(records)"))
        except Exception as e:  # noqa: BLE001
            if on_script_error == "fail":
                raise
            errors.append({"index": None, "message": str(e)})
            out_records = list(records)
    else:
        for i, rec in enumerate(records):
            try:
                out_records.append(_run_one(rec))
            except Exception as e:  # noqa: BLE001
                if on_script_error == "fail":
                    raise
                errors.append({"index": i, "message": str(e)})

    if destroy_script:
        ctx.eval(destroy_script)

    events = json.loads(ctx.eval("JSON.stringify(events)"))
    final_state = json.loads(ctx.eval("JSON.stringify(state)"))
    return {"records": out_records, "errors": errors, "events": events, "state": final_state}


def _execute_isolated(language, init_script, script, destroy_script, records, user_params, state, mode, on_script_error, timeout_seconds, memory_limit_bytes):
    """Runs the whole materialization's worth of script execution in ONE real OS
    subprocess (not one per record -- per-record spawning is real isolation too,
    but prohibitively slow for anything beyond a handful of rows, and isn't what
    most real sandboxes do either): invokes THIS SAME FILE as a standalone script
    (`python3 <this file> --script-evaluator-worker`), feeding the payload as JSON
    over stdin and reading the result as JSON from stdout. This is deliberately
    NOT `multiprocessing` -- a spawn-context subprocess needs to re-import the
    parent module by reference, which breaks whenever that module was loaded by
    path rather than as a real installed package (exactly how this repo's own
    test harness loads every component.py) -- invoking the file directly as a
    subprocess sidesteps that entirely, with a textbook `subprocess.run(...,
    timeout=...)` for the hard wall-clock kill."""
    import subprocess
    import sys

    payload = json.dumps({
        "language": language, "init_script": init_script, "script": script,
        "destroy_script": destroy_script, "records": records, "user_params": user_params,
        "state": state, "mode": mode, "on_script_error": on_script_error,
        "memory_limit_bytes": memory_limit_bytes,
    })
    try:
        proc = subprocess.run(
            [sys.executable, os.path.abspath(__file__), "--script-evaluator-worker"],
            input=payload, capture_output=True, text=True, timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as e:
        raise TimeoutError(f"script_evaluator: script execution exceeded timeout_seconds={timeout_seconds}") from e

    if proc.returncode != 0:
        raise RuntimeError(f"script_evaluator: worker subprocess failed (exit {proc.returncode}): {proc.stderr[-4000:]}")
    try:
        out = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"script_evaluator: worker subprocess produced non-JSON output: {proc.stdout[-2000:]!r} / stderr: {proc.stderr[-2000:]}") from e
    if not out["ok"]:
        raise RuntimeError(f"script_evaluator: {out['error']}")
    return out["result"]


def _worker_main():
    """Entry point when this file is invoked directly as a subprocess (see
    `_execute_isolated` above) -- reads the execution payload as JSON from stdin,
    runs it, writes the result as JSON to stdout. Never imported for this path;
    only reached via `python3 component.py --script-evaluator-worker`."""
    import sys

    payload = json.loads(sys.stdin.read())
    try:
        if payload["language"] == "jython":
            result = _run_jython(
                payload["init_script"], payload["script"], payload["destroy_script"],
                payload["records"], payload["user_params"], payload["state"],
                payload["mode"], payload["on_script_error"],
            )
        else:
            result = _run_javascript(
                payload["init_script"], payload["script"], payload["destroy_script"],
                payload["records"], payload["user_params"], payload["state"],
                payload["mode"], payload["on_script_error"], payload["memory_limit_bytes"],
            )
        sys.stdout.write(json.dumps({"ok": True, "result": result}))
    except Exception as e:  # noqa: BLE001
        sys.stdout.write(json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}"}))


if __name__ == "__main__":
    import sys as _sys
    if "--script-evaluator-worker" in _sys.argv:
        _worker_main()


class ScriptEvaluatorComponent(Component, Model, Resolvable):
    """Runs a customer-authored transform script per record or per batch against an
    upstream DataFrame -- the Dagster analog of StreamSets' Script Evaluator
    processor. See the module docstring for the full security model and the
    Jython/JavaScript semantics this matches.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(default=None, description="Upstream asset providing the input DataFrame")

    language: Literal["jython", "javascript"] = Field(
        description="jython: real CPython in a subprocess, with a Java-collection emulation shim and "
                    "Jython-style integer division. javascript: QuickJS (quickjs-ng) -- no filesystem/network/"
                    "process access by construction. Groovy is not supported (see module docstring)."
    )
    mode: Literal["record", "batch"] = Field(
        default="record",
        description="record: script runs once per row, sees `record` (a dict, or Java-collection-wrapped "
                    "for Jython). batch: script runs once, sees `records` (a list of all rows this "
                    "materialization is processing)."
    )
    script: str = Field(description="The transform script body.")
    init_script: Optional[str] = Field(default=None, description="Runs once before the first record/batch.")
    destroy_script: Optional[str] = Field(default=None, description="Runs once after the last record/batch.")
    user_params: Optional[Dict[str, Any]] = Field(
        default=None, description="Exposed to the script as `userParams` (a dict/JMap)."
    )
    state_path: Optional[str] = Field(
        default=None,
        description="Where `state` is persisted across RUNS as JSON (same pattern as s3_monitor's dynamic-"
                    "partition pruning state). Unset: state starts empty every run and isn't persisted."
    )
    on_script_error: Literal["record_error", "fail"] = Field(
        default="record_error",
        description="record_error: a script exception is caught per record (mode: record) or for the whole "
                    "batch (mode: batch) and recorded in this asset's metadata; the run continues. fail: the "
                    "exception propagates and fails the run."
    )
    timeout_seconds: float = Field(
        default=60.0,
        description="Hard wall-clock timeout for the WHOLE materialization's script execution (not per "
                    "record) -- the subprocess is terminated if it doesn't finish in time."
    )
    javascript_memory_limit_mb: int = Field(
        default=64,
        description="QuickJS heap limit in MB (javascript mode only) -- a real, enforced backstop, not "
                    "documentation-only (verified: an infinite loop is interrupted at the configured limit)."
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags")

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component="ScriptEvaluatorComponent", mode="single", optional=True)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), tags=('tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Script Evaluator", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        language = self.language
        mode = self.mode
        script = self.script
        init_script = self.init_script
        destroy_script = self.destroy_script
        user_params = self.user_params or {}
        state_path = self.state_path
        on_script_error = self.on_script_error
        timeout_seconds = self.timeout_seconds
        memory_limit_bytes = int(self.javascript_memory_limit_mb) * 1_000_000

        if language == "javascript":
            try:
                import quickjs  # noqa: F401
            except ImportError as e:
                raise ImportError(
                    "ScriptEvaluatorComponent language='javascript' requires the 'quickjs-ng' package. "
                    "Run: pip install quickjs-ng"
                ) from e

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins=({"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))} if upstream_asset_key else None),
            group_name=self.group_name,
            owners=self.owners,
            tags=self.tags,
            backfill_policy=BackfillPolicy.single_run(),
        )
        def _script_evaluator_asset(context: AssetExecutionContext, upstream: pd.DataFrame = None) -> pd.DataFrame:
            df = upstream if upstream is not None else pd.DataFrame()
            records = df.to_dict(orient="records")
            state = _load_state(state_path)

            result = _execute_isolated(
                language, init_script, script, destroy_script, records, user_params, state,
                mode, on_script_error, timeout_seconds, memory_limit_bytes,
            )

            _write_state(state_path, result["state"])
            out_df = pd.DataFrame(result["records"]) if result["records"] else df.iloc[0:0]

            context.log.info(
                f"script_evaluator: {len(result['records'])} record(s), "
                f"{len(result['errors'])} error(s), {len(result['events'])} event(s)"
            )

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(out_df)),
                "error_count": MetadataValue.int(len(result["errors"])),
                "errors_sample": MetadataValue.json(result["errors"][:20]),
                "events_sample": MetadataValue.json(result["events"][:20]),
                "language": MetadataValue.text(language),
                "mode": MetadataValue.text(mode),
                "state": MetadataValue.json(result["state"]),
            })
            return out_df.reset_index(drop=True)

        return Definitions(assets=[_script_evaluator_asset])
