# dbt Docs Enriched Project

A drop-in extension of `DbtProjectComponent` that surfaces rich metadata from the dbt manifest onto every Dagster asset — without changing how dbt runs.

## The problem

The standard `DbtProjectComponent` exposes model descriptions, column schemas, tags, and owners. But the dbt manifest contains much more that's invisible in the Dagster UI: which BI dashboards consume a model, whether a contract is enforced, what metrics are defined on it, source freshness SLAs, and more.

This component adds all of that as asset metadata, visible in the **Asset details → Metadata** tab in the Dagster UI.

## What gets added

| Metadata key | Source | Shown when |
|---|---|---|
| `dbt_docs/url` | `dbt_docs_url` + node type/id | `dbt_docs_url` is set |
| `dbt_docs/exposures` | `manifest.exposures` via `child_map` | Model has downstream exposures |
| `dbt_docs/metrics` | `manifest.metrics` via `child_map` | Model has referenced metrics |
| `dbt_docs/semantic_models` | `manifest.semantic_models` via `child_map` | Model is in a semantic model |
| `dbt_docs/contract_enforced` | `node.contract.enforced` | Contract is enforced |
| `dbt_docs/column_constraints` | `node.columns[*].constraints` | Contract + constraints defined |
| `dbt_docs/meta` | `node.meta` (non-dagster keys) | `meta` dict has non-dagster keys |
| `dbt_docs/freshness` | `node.freshness` | Source node with freshness SLA |
| `dbt_docs/loaded_at_field` | `node.loaded_at_field` | Source node |
| `dbt_docs/loader` | `node.loader` | Source node with loader set |
| `dbt_docs/access` | `node.config.access` | Model is `public` or `private` |
| `dbt_docs/language` | `node.language` | Python dbt model |
| `dbt_docs/patch_path` | `node.patch_path` | Model has a YAML docs file |
| `dbt_docs/doc_blocks` | `manifest.docs` | `include_doc_blocks: true` |

## Required packages

```
dagster-dbt>=0.23.0
```

## Usage

Replace `DbtProjectComponent` with `DbtDocsEnrichedProjectComponent` in your `defs.yaml`. All other attributes are identical — dbt execution is unchanged.

```yaml
# Before
type: dagster_dbt.DbtProjectComponent
attributes:
  project: "{{ project_root }}/dbt_project"
  cli_args:
    - build

# After
type: dagster_component_templates.DbtDocsEnrichedProjectComponent
attributes:
  project: "{{ project_root }}/dbt_project"
  dbt_docs_url: "https://dbt-docs.internal.mycompany.com"
  cli_args:
    - build
```

## Fields

### Enrichment fields (new)

| Field | Type | Default | Description |
|---|---|---|---|
| `dbt_docs_url` | str | null | Base URL of your hosted dbt docs site |
| `include_exposures` | bool | true | Add downstream exposure info to each model |
| `include_metrics` | bool | true | Add metric names referencing each model |
| `include_semantic_models` | bool | true | Add semantic model definitions |
| `include_contracts` | bool | true | Add contract enforcement + column constraints |
| `include_meta` | bool | true | Add full `meta` dict (non-dagster keys) |
| `include_source_freshness` | bool | true | Add freshness SLA on source assets |
| `include_doc_blocks` | bool | false | Resolve and embed doc block contents (verbose) |
| `manifest_path` | str | null | Override path to `manifest.json` |

### Base DbtProjectComponent fields (unchanged)

All fields from `DbtProjectComponent` work identically: `project`, `cli_args`, `select`, `exclude`, `selector`, `op`, `translation`, `include_metadata`, etc.

## The `dbt_docs_url` field

When set, every model, source, and snapshot asset gets a clickable URL in the Dagster UI:

```
{dbt_docs_url}/#!/model/model.my_project.my_model
{dbt_docs_url}/#!/source/source.my_project.raw_orders
{dbt_docs_url}/#!/snapshot/snapshot.my_project.orders_snapshot
```

Works with any dbt docs hosting:
- `dbt docs serve` (local): `http://localhost:8080`
- dbt Cloud: `https://cloud.getdbt.com/accounts/{id}/projects/{id}/docs`
- Self-hosted (S3, GitHub Pages, Netlify): your custom URL

## Exposures example

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

Then `fct_revenue` in Dagster will have:

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

## How it works

This component is a `@dataclass` subclass of `DbtProjectComponent`. It overrides `build_defs_from_state` to:

1. Call `super().build_defs_from_state()` — all standard dbt behaviour, unchanged
2. Load `target/manifest.json` from disk (already compiled by dbt prepare)
3. Call `defs.map_resolved_asset_specs()` to enrich each `AssetSpec` with metadata
4. The unique_id is read back from the `dagster_dbt/unique_id` internal metadata key set by dagster-dbt

If the manifest cannot be loaded, the component falls back to the base behaviour with a warning — it never breaks the Dagster load.

## Answering "do we have a dbt docs tab?"

Not as a dedicated UI tab — but with this component, every dbt model in the **Asset Catalog** and **Asset Details** view shows:
- A direct link to the dbt docs page
- Which BI tools / dashboards consume this model
- Contract status
- Source freshness SLAs
- Metrics defined on the model

This makes the Dagster UI a **superset** of the dbt docs site for lineage and metadata.

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
