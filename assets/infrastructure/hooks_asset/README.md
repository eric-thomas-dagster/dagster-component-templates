# `HooksAssetComponent` + `@on_hooks` decorator

Asset-scoped `on_success` / `on_failure` callbacks. Prefect's `@task(on_completion=..., on_failure=...)` — Dagster's built-in `@dg.success_hook` / `@dg.failure_hook` are job-scoped (attached in job wiring); this component gives you callbacks right next to the asset.

## Two shapes

| Shape | Use when |
|---|---|
| **`HooksAssetComponent`** (YAML) | New asset with hooks |
| **`@on_hooks` decorator** (Python) | Wrap an existing `@dg.asset` |

## Callback signatures

- **`on_start`** callbacks:   `fn(context) -> None`                        (fires BEFORE compute)
- **`on_success`** callbacks: `fn(context, result) -> None`                (fires on successful compute)
- **`on_failure`** callbacks: `fn(context, exception) -> None`             (fires on failed compute)
- **`on_end`** callbacks:     `fn(context, outcome, result_or_exc) -> None` (fires FINALLY-STYLE after on_success/on_failure; `outcome in ("success", "failure")`)

Callbacks are ordinary Python `mod:fn` references — same shape as `@lifecycle`'s custom checks or `@data_contract`'s probes.

## Semantics

- Callbacks are called SEQUENTIALLY, in list order, within each phase.
- Any exception raised by a callback is **LOGGED**, not re-raised. Hooks don't change the compute's outcome. Matches Prefect.
- Firing order for a SUCCESS: `on_start` → compute → `on_success` → `on_end("success", result)`.
- Firing order for a FAILURE: `on_start` → compute (raises) → `on_failure` → `on_end("failure", exc)` → exception re-raises.
- `dg.Failure` counts as failure (fires `on_failure` + `on_end("failure", ...)`).

## Full lifecycle example

```python
import dagster as dg
from dagster_community_components import on_hooks

def log_start(context):
    context.log.info("[hook] on_start fired")

def notify_success(context, result):
    context.log.info(f"[hook] on_success — result rows={len(result)}")

def create_ticket(context, exc):
    context.log.info(f"[hook] on_failure — {type(exc).__name__}: {exc}")

def emit_metric(context, outcome, result_or_exc):
    context.log.info(f"[hook] on_end — outcome={outcome}")

@dg.asset
@on_hooks(
    on_start=["my_project.hooks:log_start"],
    on_success=["my_project.hooks:notify_success"],
    on_failure=["my_project.hooks:create_ticket"],
    on_end=["my_project.hooks:emit_metric"],
)
def critical_report(context):
    return build_report()
```

## Full YAML example

```yaml
type: dagster_community_components.HooksAssetComponent
attributes:
  asset_name: critical_report

  compute:
    kind: python
    python: "my_project.reports:build_critical"

  on_start:
    - "my_project.hooks:log_run_start"

  on_success:
    - "my_project.hooks:notify_slack_success"
    - "my_project.hooks:update_dashboard"

  on_failure:
    - "my_project.hooks:create_jira_ticket"
    - "my_project.hooks:page_oncall"

  on_end:
    - "my_project.hooks:emit_lifecycle_metric"
```

Callback module:

```python
# my_project/hooks.py
def notify_slack_success(context, result):
    # context is a Dagster AssetExecutionContext
    # result is whatever compute returned
    ...

def create_jira_ticket(context, exception):
    # exception is the raised BaseException (usually dg.Failure)
    ...
```

## `@on_hooks` decorator

```python
import dagster as dg
from dagster_community_components import on_hooks

@dg.asset
@on_hooks(
    on_success=["my_project.hooks:notify_slack_success"],
    on_failure=["my_project.hooks:create_jira_ticket",
                "my_project.hooks:page_oncall"],
)
def critical_report(context):
    return build_report()
```

## Composes with

- **`@smart_retry`** — hooks fire ONCE per materialization outcome, not per retry attempt. Retry succeeds after 2 tries → one `on_success` call.
- **`@sla`** — pair `on_failure=notify_slack` with `@sla(on_breach='fail')` for automatic SLA-breach alerting.
- **`@data_contract`** — `on_failure=create_jira_ticket` fires when the contract violation raises `dg.Failure`.
- **`@lifecycle`** — hooks fire AFTER publish/quarantine finishes.

## Why this vs. Dagster's built-in hooks

`@dg.success_hook` + `@dg.failure_hook` require:
1. Define the hook function with the decorator.
2. Attach it to a job or an op via `hooks_on_success=[my_hook]`.

For an asset-first project where you don't build jobs by hand, that's awkward. `@on_hooks` moves the wiring to right next to the asset: same YAML block, same decorator stack. Everything visible in one place.

## CLI demos using this template

| Demo | Setup script | What it shows |
|---|---|---|
| [`hooks_asset.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/hooks_asset.md) | [`setup_hooks_asset_demo.sh`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/setup_hooks_asset_demo.sh) | 100% offline. Demonstrates BOTH shapes AND both callback paths: (1a) `@on_hooks` Python decorator on a succeeding asset (multiple `on_success` hooks fire in list order), (1b) same on a failing asset (`on_failure` fires, then `RuntimeError` re-raised → STEP_FAILURE), (2) `HooksAssetComponent { wraps: SyntheticDataGeneratorComponent }` — outer hooks fire with the inner's DataFrame result. Callbacks are `mod:fn` refs so both shapes reference the same shared `src/<pkg>/hooks.py`. |

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_hooks_asset_demo.sh | bash
```

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## What's not in v1 (roadmap)

- **Async callbacks** — v1 runs synchronously.
- **Cross-asset hook sharing** — declare a hook once, reference from N assets.

## Fields

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — | Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode). |
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Default: ['python', 'hooks']. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | — |
| `compute` | `Dict[str, Any]` | — | `{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`. |
| `wraps` | `Dict[str, Any]` | — | Wrap another DCC component's assets with success/failure hooks instead of defining new compute. Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. Mutually exclusive with `compute`. |
| `on_success` | `List[str]` | — | List of 'mod:fn' refs called with (context, result) after successful compute. |
| `on_failure` | `List[str]` | — | List of 'mod:fn' refs called with (context, exception) on failure. Doesn't change the outcome. |
| `on_start` | `List[str]` | — | Callbacks fired BEFORE compute (in list order). Signature: `(context) -> None`. Each is a `mod:fn` reference. |
| `on_end` | `List[str]` | — | Callbacks fired AFTER compute (finally-style — regardless of success or failure, in list order). Signature: `(context, outcome: str, result_or_exc) -> None`. Runs after on_success/on_failure. |

[//]: # (FIELDS:END)
