# `ScriptEvaluatorComponent`

Runs a customer-authored transform script against each row (`mode: record`) or the whole
batch at once (`mode: batch`) of an upstream DataFrame — the Dagster analog of StreamSets'
Groovy/JavaScript/Jython Script Evaluator processor.

## Security model — read this before pointing this at untrusted script content

This component executes **arbitrary code a script author wrote**. It is not a security
sandbox against a malicious author:

- **`language: javascript`** runs inside QuickJS (`quickjs-ng`), which has no filesystem,
  network, process, or module-loading bindings by construction — there is nothing for a JS
  script to reach outside its own data. A real, verified `set_time_limit`/`set_memory_limit`
  backstop is enforced (confirmed directly: an infinite `while(true){}` loop is interrupted at
  the configured limit, not just documented-but-unenforced).
- **`language: jython`** runs real CPython in a separate OS subprocess (never in the Dagster
  run's own process), with a hard wall-clock timeout (`timeout_seconds`) enforced by
  terminating the subprocess if it doesn't return in time. This gives process isolation and a
  DoS backstop — it does **not** prevent a script from reading environment variables, the
  filesystem, or network access available to that subprocess's user. Treat script content with
  the same trust level you'd give any other custom code you deploy.

- **`language: groovy`** shells out to a system-installed `groovy` CLI (and JVM) via a small,
  fixed Groovy driver script, one subprocess per materialization, with the same
  `timeout_seconds` wall-clock backstop as jython mode (enforced directly on the `groovy`
  subprocess call, so a timeout can't orphan a hung JVM). **Not pip-installable** — unlike this
  registry's PySpark/Snowpark pipeline components (which bundle their own JVM/driver via a plain
  `pip install`), Groovy needs a system-level JVM *and* a separately-installed Groovy
  distribution. Install it yourself before using this mode (e.g. `brew install groovy` on
  macOS, which also installs a JVM) — `build_defs` raises a clear `ImportError` at
  definition-load time if the `groovy` binary isn't found on `PATH`. Same trust model as jython:
  no sandboxing, treat script content like any other custom code you deploy.

## Jython mode semantics (matched to the real StreamSets engine)

- Record fields that are lists/dicts are exposed as **Java-collection-style objects**:
  `.add(x)` / `.get(i)` / `.size()` (not Python `list.append`/`[]`/`len()`) for list fields,
  `.put(k, v)` / `.get(k)` / `.containsKey(k)` (not Python dict `[]`/`in`) for map fields.
  `list.push(...)` does **not** exist in this semantic and is deliberately not implemented —
  confirmed by a test that asserts it raises.
- Integer division (`/` on two ints) **floors**, matching Jython/Python 2 — implemented via an
  AST rewrite (every `ast.Div` becomes a call to a helper that checks operand types **at
  runtime**, not a static guess at parse time), so it's correct regardless of what types a
  variable actually holds when division runs. Division involving a float still true-divides.

## JavaScript mode semantics

Standard JS: `/` always true-divides (no floor), objects/arrays use normal JS syntax
(`record.c = record.a + record.b`, `list.push(...)`).

## Groovy mode semantics

`record`/`records` are **real `java.util.Map`/`List` objects** (parsed by Groovy's own
`JsonSlurper`), so both dot/bracket syntax (`record.foo`, `record['foo']`) *and* Java
collection methods (`.get()`/`.put()`/`.containsKey()`/`.add()`) work natively — unlike jython
mode, no `JMap`/`JList` emulation shim is needed here. `/` uses Groovy's own real semantics:
Integer/Integer division promotes to a `BigDecimal` (e.g. `7 / 2 == 3.5`), which is **different
from Jython's floor division** — that's genuine Groovy behavior, not something normalized to
match jython mode.

## Shared primitives (all languages)

- **`userParams`** — a plain dict (JMap in Jython, a plain JS object in JavaScript, a real
  `Map` in Groovy) from the `user_params` config field.
- **`state`** — persisted across **runs** (not just batches) via a small local JSON file at
  `state_path`, the same pattern `s3_monitor` already uses for its dynamic-partition pruning
  state. Leave `state_path` unset and `state` starts empty every run with no persistence.
- **`init_script`** runs once before the first record/batch; **`destroy_script`** runs once
  after the last. Both optional.
- **Events**: a script can call `events.add({...})` (Jython, Groovy) / `events.push({...})`
  (JavaScript) to emit an out-of-band event dict. These surface in this asset's
  `MaterializeResult` metadata (`events_sample`), not as a second physical Dagster output —
  this repo has no existing multi-output-asset precedent for transform components, so rich
  metadata on one asset is the convention followed here.
- **`on_script_error: record_error`** (default) catches a script exception per record (`mode:
  record`) or for the whole batch (`mode: batch`) and records it in metadata (`errors_sample`)
  instead of failing the run. **`on_script_error: fail`** re-raises immediately.

## Example

```yaml
type: dagster_community_components.ScriptEvaluatorComponent
attributes:
  asset_name: enriched_orders
  upstream_asset_key: raw_orders
  language: jython
  mode: record
  script: |
    record.put("total", record.get("price") * record.get("qty"))
    if record.get("total") > 1000:
        record.put("tier", "gold")
    state.put("count", state.get("count", 0) + 1)
  state_path: ./script_evaluator_state.json
  on_script_error: record_error
  timeout_seconds: 60
```

```yaml
type: dagster_community_components.ScriptEvaluatorComponent
attributes:
  asset_name: enriched_orders_js
  upstream_asset_key: raw_orders
  language: javascript
  mode: batch
  script: |
    for (const r of records) {
      r.total = r.price * r.qty;
    }
  timeout_seconds: 30
```

```yaml
type: dagster_community_components.ScriptEvaluatorComponent
attributes:
  asset_name: enriched_orders_groovy
  upstream_asset_key: raw_orders
  language: groovy
  mode: record
  script: |
    record.total = record.price * record.qty
    if (record.total > 1000) {
      record.tier = 'gold'
    }
    state.count = (state.count ?: 0) + 1
  state_path: ./script_evaluator_state.json
  on_script_error: record_error
  timeout_seconds: 60
```

## Fields

<!-- FIELDS:START -->
| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | no | — | Upstream asset providing the input DataFrame |
| `upstream_asset_keys` | array of string | no | — | Deprecated alias; use `upstream_asset_key` |
| `language` | `jython` \| `javascript` \| `groovy` | yes | — | Execution engine; see Security model above |
| `mode` | `record` \| `batch` | no | `record` | Per-row vs. whole-batch script execution |
| `script` | string | yes | — | The transform script body |
| `init_script` | string | no | — | Runs once before the first record/batch |
| `destroy_script` | string | no | — | Runs once after the last record/batch |
| `user_params` | object | no | — | Exposed to the script as `userParams` |
| `state_path` | string | no | — | Cross-run state JSON file path |
| `on_script_error` | `record_error` \| `fail` | no | `record_error` | Error-handling mode |
| `timeout_seconds` | number | no | `60.0` | Hard wall-clock timeout for the whole materialization |
| `javascript_memory_limit_mb` | integer | no | `64` | QuickJS heap limit (javascript mode only) |
| `group_name` | string | no | — | Dagster asset group |
| `owners` | array of string | no | — | Asset owners |
| `tags` | object | no | — | Asset tags |
<!-- FIELDS:END -->
