# dbt Docs Enriched Project

A drop-in extension of `DbtProjectComponent` that surfaces rich metadata from the dbt manifest onto every Dagster asset — without changing how dbt runs.

## The problem

The standard `DbtProjectComponent` exposes model descriptions, column schemas, tags, and owners. But the dbt manifest contains much more that's invisible in the Dagster UI: which BI dashboards consume a model, whether a contract is enforced, what metrics are defined on it, source freshness SLAs, and more.

This component adds all of that as asset metadata, visible in the **Asset details → Metadata** tab in the Dagster UI.

---

## Quick start

```yaml
# defs/components/dbt.yaml
type: dagster_component_templates.DbtDocsEnrichedProjectComponent
attributes:
  project: "{{ project_root }}/dbt_project"
  dbt_docs_url: "https://dbt-docs.internal.mycompany.com"
  include_exposures: true
  include_metrics: true
  include_semantic_models: true
  include_contracts: true
  include_source_freshness: true
  cli_args:
    - build
```

All `include_*` flags default to `false` — opt in to only what you need.

---

## Migration from DbtProjectComponent

```yaml
# Before
type: dagster_dbt.DbtProjectComponent
attributes:
  project: "{{ project_root }}/dbt_project"
  cli_args:
    - build

# After — dbt execution is identical, metadata is enriched
type: dagster_component_templates.DbtDocsEnrichedProjectComponent
attributes:
  project: "{{ project_root }}/dbt_project"
  dbt_docs_url: "https://dbt-docs.internal.mycompany.com"
  include_exposures: true
  cli_args:
    - build
```

---

## Fields

### Enrichment fields (new)

| Field | Type | Default | Description |
|---|---|---|---|
| `dbt_docs_url` | str | `null` | Base URL of your hosted dbt docs site |
| `include_exposures` | bool | `false` | Add downstream exposure info (BI dashboards, notebooks, etc.) to each model |
| `include_metrics` | bool | `false` | Add metric names referencing each model |
| `include_semantic_models` | bool | `false` | Add semantic model definitions referencing each model |
| `include_contracts` | bool | `false` | Add contract enforcement status and column-level constraints |
| `include_meta` | bool | `false` | Add full `meta` dict (non-dagster keys) |
| `include_source_freshness` | bool | `false` | Add freshness SLA thresholds on source assets |
| `include_doc_blocks` | bool | `false` | Resolve and embed doc block contents (verbose — use sparingly) |
| `manifest_path` | str | `null` | Override path to `manifest.json`. Defaults to `{project_dir}/target/manifest.json` |

### Base DbtProjectComponent fields (unchanged)

All fields from `DbtProjectComponent` work identically: `project`, `cli_args`, `select`, `exclude`, `selector`, `op`, `translation`, `include_metadata`, etc.

---

## What gets added

| Metadata key | Source | Shown when |
|---|---|---|
| `dbt_docs/url` | `dbt_docs_url` + node type/unique_id | `dbt_docs_url` is set |
| `dbt_docs/exposures` | `manifest.exposures` via `child_map` | Model has downstream exposures |
| `dbt_docs/metrics` | `manifest.metrics` via `child_map` | Model has referenced metrics |
| `dbt_docs/semantic_models` | `manifest.semantic_models` via `child_map` | Model is in a semantic model |
| `dbt_docs/contract_enforced` | `node.contract.enforced` | Contract is enforced |
| `dbt_docs/column_constraints` | `node.columns[*].constraints` | Contract + column constraints defined |
| `dbt_docs/meta` | `node.meta` (non-dagster keys) | `meta` dict has non-dagster keys |
| `dbt_docs/freshness` | `node.freshness` | Source node with freshness SLA |
| `dbt_docs/loaded_at_field` | `node.loaded_at_field` | Source node |
| `dbt_docs/loader` | `node.loader` | Source node with loader set |
| `dbt_docs/access` | `node.config.access` | Model is `public` or `private` (not `protected`) |
| `dbt_docs/language` | `node.language` | Python dbt model (non-SQL) |
| `dbt_docs/patch_path` | `node.patch_path` | Model has a YAML docs file |
| `dbt_docs/doc_blocks` | `manifest.docs` | `include_doc_blocks: true` |

---

## Examples by feature

### `dbt_docs_url` — clickable link per asset

When set, every model, source, and snapshot asset gets a direct link in the Dagster UI:

```
{dbt_docs_url}/#!/model/model.my_project.fct_revenue
{dbt_docs_url}/#!/source/source.my_project.raw__orders
{dbt_docs_url}/#!/snapshot/snapshot.my_project.orders_snapshot
```

Works with any dbt docs hosting:
- `dbt docs serve` (local): `http://localhost:8080`
- dbt Cloud: `https://cloud.getdbt.com/accounts/{id}/projects/{id}/docs`
- Self-hosted (S3, GitHub Pages, Netlify): your custom URL

---

### `include_exposures` — downstream BI dashboards

If your dbt project has an exposure:

```yaml
# models/exposures.yml
exposures:
  - name: weekly_revenue_dashboard
    type: dashboard
    owner:
      email: analytics@company.com
    url: https://looker.company.com/dashboards/42
    maturity: high
    depends_on:
      - ref('fct_revenue')
```

Then `fct_revenue` in Dagster shows:

```json
"dbt_docs/exposures": [
  {
    "name": "weekly_revenue_dashboard",
    "type": "dashboard",
    "owner": "analytics@company.com",
    "url": "https://looker.company.com/dashboards/42",
    "maturity": "high",
    "description": null
  }
]
```

---

### `include_metrics` — dbt metrics

```yaml
# models/metrics.yml
metrics:
  - name: monthly_revenue
    label: Monthly Revenue
    type: sum
    description: "Total revenue by month"
    depends_on:
      - ref('fct_revenue')
```

`fct_revenue` in Dagster shows:

```json
"dbt_docs/metrics": [
  {
    "name": "monthly_revenue",
    "label": "Monthly Revenue",
    "type": "sum",
    "description": "Total revenue by month",
    "time_granularity": null
  }
]
```

---

### `include_semantic_models` — semantic layer

```yaml
# models/semantic_models.yml
semantic_models:
  - name: revenue_model
    description: "Revenue semantic model"
    model: ref('fct_revenue')
    primary_entity: transaction
    measures:
      - name: total_revenue
        agg: sum
    dimensions:
      - name: created_at
        type: time
```

`fct_revenue` in Dagster shows:

```json
"dbt_docs/semantic_models": [
  {
    "name": "revenue_model",
    "description": "Revenue semantic model",
    "primary_entity": "transaction",
    "measures": ["total_revenue"],
    "dimensions": ["created_at"],
    "entities": []
  }
]
```

---

### `include_contracts` — model contracts

```yaml
# models/schema.yml
models:
  - name: fct_revenue
    contract:
      enforced: true
    columns:
      - name: order_id
        data_type: integer
        constraints:
          - type: not_null
          - type: unique
      - name: revenue
        data_type: numeric
        constraints:
          - type: not_null
```

`fct_revenue` in Dagster shows:

```json
"dbt_docs/contract_enforced": true,
"dbt_docs/column_constraints": {
  "order_id": [{"type": "not_null"}, {"type": "unique"}],
  "revenue": [{"type": "not_null"}]
}
```

---

### `include_source_freshness` — source SLAs

```yaml
# models/sources.yml
sources:
  - name: raw
    tables:
      - name: orders
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
        loader: fivetran
```

The `raw__orders` source asset in Dagster shows:

```json
"dbt_docs/freshness": {
  "warn_after": {"count": 12, "period": "hour"},
  "error_after": {"count": 24, "period": "hour"}
},
"dbt_docs/loaded_at_field": "_loaded_at",
"dbt_docs/loader": "fivetran"
```

---

### `include_meta` — custom metadata

```yaml
# models/schema.yml
models:
  - name: fct_revenue
    meta:
      owner: finance-team
      tier: gold
      pii: false
      sla_hours: 4
```

`fct_revenue` in Dagster shows:

```json
"dbt_docs/meta": {
  "owner": "finance-team",
  "tier": "gold",
  "pii": false,
  "sla_hours": 4
}
```

> The `dagster` sub-key inside `meta` is excluded — it's already handled by `dagster-dbt` natively.

---

## How it works

This component is a `@dataclass` subclass of `DbtProjectComponent`. It overrides `build_defs_from_state` to:

1. Call `super().build_defs_from_state()` — all standard dbt behaviour, unchanged
2. Load `target/manifest.json` from disk (already compiled by dbt prepare)
3. Call `defs.map_resolved_asset_specs()` to enrich each `AssetSpec` with additional metadata
4. The `dagster_dbt/unique_id` key (set internally by dagster-dbt) is used to look up each node in the manifest

If the manifest cannot be loaded, the component falls back to base `DbtProjectComponent` behaviour with a warning — **it never breaks the Dagster load.**

---

## Requirements

```
dagster-dbt>=0.23.0
```

The component is a thin subclass — it requires `dagster-dbt` but adds no other dependencies. If `dagster-dbt` is not installed, an `ImportError` stub is provided that raises a clear error message at runtime.

---

## Troubleshooting

**"could not load manifest.json"** — dbt hasn't been compiled yet, or the project path is non-standard. Run `dbt compile` or `dbt build` first, or set `manifest_path` explicitly:

```yaml
attributes:
  manifest_path: "/absolute/path/to/target/manifest.json"
```

**No metadata appearing for a model** — check that the model appears in `manifest.json` under `nodes` or `sources`. Models excluded from the dbt selection won't have entries.

**`include_exposures: true` but no exposures showing** — the model must appear in `child_map` in the manifest. Run `dbt compile` to regenerate if you've recently added exposures.

**`include_contracts: true` but no contract metadata** — contracts require `contract: enforced: true` in the model's YAML schema file AND a supported dbt adapter.

---

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  project: "{{ project_root }}/dbt_project"
  deps:
    - raw_orders
    - raw/schema/orders
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that your dbt project depends on upstream tables produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
