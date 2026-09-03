# `DataContractComponent` + `@data_contract` decorator

Enforce **data contracts** at materialization. A data contract is the formal agreement between a producer and its consumers: schema, freshness, SLAs, ownership, versioning. This component makes contracts CODE — every materialization validates the produced DataFrame against the contract, emits proper Dagster events, and either blocks publish on violation OR materializes with failing checks so downstream blocks via `AutomationCondition`.

Two shapes, one engine:

| Shape | Use when |
|---|---|
| **`DataContractComponent`** (YAML) | Define a new asset with a contract |
| **`@data_contract` decorator** (Python) | Wrap an EXISTING `@dg.asset` in Python |

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

Two shapes — both derive the check specs from the contract, both use the
same enforcement engine. Pick based on which side of `@dg.asset` you want
to sit.

### Shape A (recommended) — applied AFTER `@dg.asset`

The decorator wraps the `AssetsDefinition`. It reads the asset key, derives
`check_specs` from the contract, and rebuilds the asset with them added.
All the standard `@dg.asset` kwargs (`group_name`, `owners`, `tags`,
`partitions_def`, `code_version`, `metadata`, `kinds`, `automation_condition`,
`ins`, etc.) are preserved unchanged. You never declare a single
`AssetCheckSpec` yourself.

```python
import dagster as dg
import pandas as pd
from dagster_community_components import data_contract

CONTRACT = {
    'version': '1.2.0',
    'owners': ['data-platform@example.com'],
    'consumers': ['analytics-team'],
    'schema': [
        {'name': 'order_id', 'type': 'int64',   'nullable': False, 'unique': True},
        {'name': 'amount',   'type': 'float64', 'nullable': False, 'min': 0},
    ],
    'freshness_max_lag_minutes': 60,
    'sla_max_row_count_drop_pct': 20,
}

@data_contract(CONTRACT, on_violation='block')
@dg.asset(group_name='revenue', owners=['data-team@example.com'])
def orders(context) -> pd.DataFrame:
    return build_orders()
```

Add a column to `CONTRACT['schema']`, delete one, tweak the freshness
window — the check specs regenerate on the next import. Contract is the
single source of truth.

### Shape B (escape hatch) — applied BEFORE `@dg.asset`

Reach for this only when you need `AssetCheckSpec`s beyond what the
contract implies (e.g., custom checks alongside the contract-derived ones).
You declare `check_specs` on `@dg.asset` yourself; `check_specs_for_contract`
generates the contract-derived ones for you to include:

```python
from dagster_community_components import data_contract, check_specs_for_contract

@dg.asset(check_specs=[
    *check_specs_for_contract(CONTRACT, 'orders'),
    dg.AssetCheckSpec(name='custom_downstream_reconciliation', asset='orders'),
])
@data_contract(CONTRACT, on_violation='block')
def orders(context) -> pd.DataFrame:
    return build_orders()
```

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

## What's not in v1 (roadmap)

- **`@requires_contract` decorator** — consumer-side. Pin a semver requirement on an upstream contract; block downstream if the upstream contract version doesn't match.
- **Breaking-change detection** — compare current contract to prior via event log; emit a `contract_breaking_change` observation on breaking type changes / dropped columns.
- **Cross-asset foreign keys** — `foreign_key: users.user_id` — validate referential integrity across assets.
- **dbt / Great Expectations delegation** — treat those suites as one contract check.
- **JSON Schema import** — read a JSON Schema file and generate contract rules.

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

[//]: # (FIELDS:END)
