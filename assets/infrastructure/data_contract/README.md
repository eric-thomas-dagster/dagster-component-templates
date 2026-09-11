# `DataContractComponent` + `@data_contract` decorator

Enforce **data contracts** at materialization. A data contract is the formal agreement between a producer and its consumers: schema, freshness, SLAs, ownership, versioning. This component makes contracts CODE — every materialization validates the produced DataFrame against the contract, emits proper Dagster events, and either blocks publish on violation OR materializes with failing checks so downstream blocks via `AutomationCondition`.

Producer + consumer + import shapes:

| Shape | Use when |
|---|---|
| **`DataContractComponent`** (YAML) | Define a new asset with a contract |
| **`@data_contract` decorator** (Python) | Wrap an EXISTING `@dg.asset` in Python |
| **`RequiresContractComponent`** (YAML) | Consumer-side — assert an upstream contract before compute |
| **`@requires_contract` decorator** (Python) | Consumer-side Python decorator |
| **`contract_from_json_schema()`** (helper) | Import a JSON Schema file as a DCC contract |

## What gets enforced — the contract IS the asset's checks

Every rule in the contract becomes a first-class Dagster asset check:

- **Per-column schema rule** → one `AssetCheckSpec(name="schema_<col>")` + one `AssetCheckResult` per materialization (type / nullable / unique / min / max / allowed_values / regex).
- **Row-count SLA rule** → `AssetCheckSpec(name="sla_row_count")` + a runtime `AssetCheckResult` comparing today's row count against the last successful materialization.
- **Freshness rule** → `AssetCheckSpec(name="freshness")` + a runtime `AssetCheckResult` comparing time-since-last-materialization against the contract's max lag.

Plus two things that ride alongside — attached to the asset itself rather than as separate checks:

- **Contract version** → set as the asset's `code_version`. Dagster's UI shows version bumps automatically; downstream can trigger re-materialization via `AutomationCondition.code_version_changed()`.
- **Ownership + consumer registry** → emitted as an `AssetObservation` with `contract_owners` / `contract_consumers` tags. Searchable via the event log.

You never declare `AssetCheckSpec` objects yourself — Shape A of the decorator derives them from the contract at import time.

## Why this belongs in Dagster (and not a separate contract tool)

Every enforcement primitive here is a Dagster event:
- **Schema violations** → `AssetCheckResult(severity=ERROR)` per column.
- **Prior-materialization lookup** → `context.instance.get_event_records` — no external state store.
- **Contract version detection** → `code_version` on the `AssetOut`; Dagster's UI shows the version bump.
- **Downstream gating** → `AutomationCondition.eager()` on any check fail blocks downstream from firing until the contract is satisfied.
- **Ownership metadata** → `AssetObservation` tags — searchable via `context.instance.get_event_records`.

You can't build this outside Dagster without reimplementing the event log, the check panel, the automation-condition engine, and change detection. That's the point.

## Schema rules in v1

Each column entry supports (all optional except `name`):

| Field | Meaning |
|---|---|
| `name` | Column name (required) |
| `type` | pandas dtype (`int64`, `float64`, `string`, `bool`, `datetime64[ns]`, etc.) — matches types with sensible family rules (`int` matches `int8/16/32/64`; `string` matches `object` and `string[python]`) |
| `nullable` | Default `true`. If `false`, any null → check FAIL |
| `unique` | Default `false`. If `true`, any duplicate → check FAIL |
| `min` / `max` | Numeric bounds |
| `allowed_values` | List of accepted values (categoricals) |
| `regex` | Pattern the column must match (strings only) |

## Freshness + SLA

```yaml
freshness_max_lag_minutes: 60         # must materialize within 1 hour of prior
sla_max_row_count_drop_pct: 20        # today's rows >= 80% of yesterday's
```

Both look up the LAST successful materialization from `context.instance.get_event_records`. First materialization skips these checks with a "no prior to compare" message.

## Enforcement modes

- `on_violation: block` (default) — raise `dg.Failure` on any check fail; asset does NOT materialize; downstream doesn't fire.
- `on_violation: warn` — asset materializes anyway; failing checks are visible in the UI; downstream can block via `AutomationCondition.eager()` on the specific failing check.

`block` is the CI/CD-friendly default: bad data never lands in prod. `warn` is useful for backfills / rehydration where you WANT the asset materialized but need visibility on quality issues.

## Full YAML example

```yaml
type: dagster_community_components.DataContractComponent
attributes:
  asset_name: orders

  compute:
    kind: python
    python: "my_project.orders:build_daily"

  contract:
    version: "1.2.0"
    owners: [data-platform@example.com]
    consumers: [analytics-team, ml-team]

    schema:
      - {name: order_id,   type: int64,             nullable: false, unique: true}
      - {name: user_id,    type: int64,             nullable: false}
      - {name: amount,     type: float64,           nullable: false, min: 0}
      - {name: currency,   type: string,            allowed_values: [USD, EUR, GBP]}
      - {name: email,      type: string,            regex: "^[^@]+@[^@]+\\.[^@]+$"}
      - {name: created_at, type: "datetime64[ns]",  nullable: false}

    freshness_max_lag_minutes: 60
    sla_max_row_count_drop_pct: 20

  on_violation: block
```

## `@data_contract` decorator

Everything about the asset — the contract itself AND the fact that it's a
Dagster asset — declared in one visible block. No module-level variable
to define, no `check_specs=` list to mirror, no hand-written
`AssetCheckSpec` literals. `@data_contract` reads the AssetsDefinition,
derives the check specs from the contract, and rebuilds the asset with
them merged in.

```python
import dagster as dg
import pandas as pd
from dagster_community_components import data_contract

@data_contract(
    contract={
        'version': '1.2.0',
        'owners': ['data-platform@example.com'],
        'consumers': ['analytics-team'],
        'schema': [
            {'name': 'order_id', 'type': 'int64',   'nullable': False, 'unique': True},
            {'name': 'amount',   'type': 'float64', 'nullable': False, 'min': 0, 'max': 1_000_000},
            {'name': 'currency', 'type': 'string',  'allowed_values': ['USD', 'EUR', 'GBP']},
            {'name': 'email',    'type': 'string',  'regex': '^[^@]+@[^@]+[.][^@]+$'},
        ],
        'checks': [   # custom asset checks — any Python callable
            {
                'name': 'orders_total_matches_line_items',
                'description': 'order.amount equals sum of line items',
                'python': 'my_project.checks:validate_order_totals',
            },
        ],
        'freshness_max_lag_minutes': 60,
        'sla_max_row_count_drop_pct': 20,
    },
    on_violation='block',
)
@dg.asset(group_name='revenue', owners=['data-team@example.com'])
def orders(context) -> pd.DataFrame:
    return build_orders()
```

`@dg.asset` keeps all its normal power (`group_name`, `owners`, `tags`,
`partitions_def`, `code_version`, `metadata`, `kinds`,
`automation_condition`, `ins`, …). Add a column to the contract, delete
one, tweak the freshness window — the check specs regenerate on the
next import. Contract is the single source of truth.

### `contract['checks']` — custom asset checks

Any per-column rule (`min`, `max`, `nullable`, `unique`, `allowed_values`,
`regex`) that doesn't fit your semantics? Drop a Python callable into
`contract['checks']`. Each entry becomes its own `AssetCheckSpec` +
runtime `AssetCheckResult`. Same panel, same severity, same metadata story
— just user code you wrote.

The callable receives the DataFrame and returns either:

- `bool` — `True` = passed, `False` = failed (description falls back to
  the entry's `description`)
- `dict` — `{'passed': bool, 'description'?: str, 'metadata'?: dict}` for
  richer failure reporting

```python
# my_project/checks.py
def validate_order_totals(df):
    mismatched = df[df.amount != df.line_items.map(sum)]
    if mismatched.empty:
        return True
    return {
        'passed': False,
        'description': f'{len(mismatched)} orders with amount ≠ sum(line_items)',
        'metadata': {
            'mismatched_ids': mismatched.order_id.tolist()[:10],
            'mismatched_count': len(mismatched),
        },
    }
```

Exceptions from the callable → check fails with the exception message as
its description. Other checks still run — one bad check doesn't block
the panel.

### Custom checks alongside the contract

If you need `AssetCheckSpec`s beyond what the contract implies (custom
downstream reconciliation, an integrity check that doesn't fit the
schema/sla/freshness shape, etc.), pull the contract out to a variable
and splat the derived specs alongside your custom ones:

```python
from dagster_community_components import data_contract, check_specs_for_contract

CONTRACT = {
    'version': '1.2.0',
    'schema': [...],
    'freshness_max_lag_minutes': 60,
}

@dg.asset(check_specs=[
    *check_specs_for_contract(CONTRACT, 'orders'),
    dg.AssetCheckSpec(name='downstream_reconciliation', asset='orders'),
])
@data_contract(CONTRACT, on_violation='block')
def orders(context) -> pd.DataFrame:
    df = build_orders()
    yield dg.AssetCheckResult(check_name='downstream_reconciliation', passed=reconcile(df))
    return df
```

Use this shape only when you actually need the extra specs — the inline
shape above is what most contracts want.

## Metadata reported per materialization

- `contract_version` — the pinned version
- `contract_row_count` — used by NEXT materialization to compute row-count SLA drop
- `contract_check_summary` — `N/M passed`
- `contract_owners` / `contract_consumers` — JSON lists
- `all_passed` — bool
- Per-check metadata via `AssetCheckResult` (typed): `actual_dtype`, `null_count`, `distinct`, `actual_min`, `actual_max`, `drop_pct`, `lag_minutes`, etc.

Plus one `AssetObservation` tagged with `contract_version`, `contract_owners`, `contract_consumers` — searchable via the event log.

## Composes with

- **`@lifecycle`** (WAP) — contract enforcement IS an audit check. Both decorators stack:
  ```python
  @dg.asset
  @lifecycle(write={...}, audit=[...])
  @data_contract(contract={...})
  def orders(context): return build()
  ```
- **`@smart_retry`** — retry transient failures during compute; if compute succeeds but contract fails, that's not a retry — that's a Failure.
- **`SlackApprovalGate` / `MultiApproverGate`** — on contract-version bumps, require sign-off before publishing. Route the `dg.Failure` metadata into an approval flow.

## Contract versioning

The contract's `version` becomes the asset's `code_version` — Dagster automatically detects when it changes and marks downstream assets as "code version changed" (visible in the UI). Combined with `AutomationCondition.code_version_changed()`, you get FREE re-materialization of downstream on contract bumps.

## `@requires_contract` — consumer-side

The producer emits an `AssetObservation` describing the contract on every materialization. The consumer asserts a semver requirement on that observation BEFORE its compute runs. If the upstream contract is missing, older than `min_version`, or missing a required column, `@requires_contract` raises `dg.Failure` before the downstream step spends any compute — the failure surfaces in the run/asset view.

```python
import dagster as dg
from dagster_community_components import requires_contract

@dg.asset(deps=["orders"])
@requires_contract(
    upstream="orders",
    min_version="1.2.0",                        # semver requirement
    require_columns=["order_id", "user_id", "amount"],
)
def daily_revenue(context, orders):
    ...
```

YAML form — `RequiresContractComponent`:

```yaml
type: dagster_community_components.RequiresContractComponent
attributes:
  asset_name: daily_revenue
  upstream: orders
  min_version: "1.2.0"
  require_columns: [order_id, user_id, amount]
  compute:
    kind: python
    python: "my_project.revenue:compute_daily"
```

Or wrap an existing DCC component's asset with a contract gate (same
composability pattern as `throttle_asset.wraps`):

```yaml
type: dagster_community_components.RequiresContractComponent
attributes:
  upstream: orders
  min_version: "1.2.0"
  require_columns: [order_id, user_id, amount]
  wraps:
    type: dagster_community_components.DataframeTransformerComponent
    attributes:
      asset_name: daily_revenue
      # ... inner component's normal config ...
```

On success, emits `AssetObservation(requires_contract_satisfied=true, upstream=…, upstream_contract_version=…, version_ok=…)` on the DOWNSTREAM asset — searchable via the event log alongside the producer's `data_contract` observations.

## Breaking-change detection

Set `detect_breaking_changes: true` on `@data_contract` or `DataContractComponent` and every materialization also diffs the current contract against the PRIOR contract observation for the same asset (via the event log). Breaking flags:

- **Dropped column** — column present in prior, missing in current.
- **Narrowed type** — column type narrowed (float→int, string→int, etc.).
- **Nullability narrowed** — nullable True → False.

Any breaking flag emits an ADDITIONAL `AssetObservation` tagged `contract_breaking_change=true` with a `breaking_changes: [...]` JSON list and a rendered markdown summary. Set `on_breaking_change="fail"` to promote breaking flags to a hard `dg.Failure`.

```python
@data_contract(
    contract={
        'version': '2.0.0',                           # bumped from 1.2.0
        'schema': [
            {'name': 'order_id', 'type': 'int64', 'nullable': False},
            # `email` column dropped — will trigger breaking flag.
        ],
    },
    detect_breaking_changes=True,
    on_breaking_change='warn',       # or 'fail' to block on breaking change
)
@dg.asset
def orders(context): ...
```

Resulting observation:

```
tags:
  contract_breaking_change: true
  contract_prior_version:  1.2.0
  contract_current_version: 2.0.0
metadata:
  breaking_changes:
    - {kind: dropped_column, column: email, detail: "column 'email' removed"}
  breaking_change_count: 1
  summary: |
    # Contract breaking change: `1.2.0` → `2.0.0`
    ...
```

## Loading contracts from JSON Schema

Teams that already publish schemas as JSON Schema (event bus, OpenAPI, contract-registry, etc.) can load them directly:

```python
from dagster_community_components import contract_from_json_schema, data_contract

CONTRACT = contract_from_json_schema(
    "orders.schema.json",
    version="1.0.0",
    owners=["data-platform@example.com"],
)

@data_contract(contract=CONTRACT)
@dg.asset
def orders(context): ...
```

Supported top-level shape: `{"type": "object", "properties": {...}, "required": [...]}`. Per-property `type` maps to Dagster/pandas dtypes (`string`→`string`, `integer`→`int`, `number`→`float`, `boolean`→`bool`, `array`→`list`, `object`→`dict`). Union types like `["string", "null"]` pick the non-null member and force `nullable=True`. `pattern` → contract `regex`, `enum` → `allowed_values`, `minimum`/`maximum` → `min`/`max`.

## Roadmap

- **Cross-asset foreign keys** — `foreign_key: users.user_id` — validate referential integrity across assets.
- **dbt / Great Expectations delegation** — treat those suites as one contract check.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name. |
| `compute` | `Dict[str, Any]` | `{kind: python, python: 'mod:fn'}`. Returns pandas DataFrame. |
| `contract` | `Dict[str, Any]` | Contract config: `{version, owners, consumers, schema, freshness_max_lag_minutes?, sla_max_row_count_drop_pct?}`. schema is a list of `{name, type?, nullable?, unique?, min?, max?, allowed_values?, regex?}` entries — one per column. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Asset kinds. Default: ['python', 'contract', 'governance']. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | Optional upstream asset passed to compute. |
| `on_violation` | `str` | `"block"` | 'block' (default) raises dg.Failure on any check fail — asset does not materialize. 'warn' materializes anyway; downstream can block via AutomationCondition.eager() on the failing check. |
| `detect_breaking_changes` | `bool` | `false` | Compare current contract to the prior emission via event log. Emits `contract_breaking_change` observation on dropped columns / narrowed types / nullable→non-nullable transitions. |
| `on_breaking_change` | `str` | `"warn"` | When `detect_breaking_changes: true`, controls what happens on a breaking flag: 'warn' (default — just emit observation) or 'fail' (also raise dg.Failure and block materialization). |

[//]: # (FIELDS:END)
