# Polytomic Sync Asset

Connects to the [Polytomic](https://www.polytomic.com/) API at component load time, discovers all syncs in your organization, and creates one Dagster asset per sync. At execution time each asset triggers the corresponding Polytomic sync run and (by default) polls until the run completes.

## What Polytomic does

Polytomic is a reverse ETL platform. It moves data from your data warehouse into business tools — Salesforce, HubSpot, Intercom, Marketo, and 100+ others. Each **sync** has:

- A **source model** — a SQL query, table, or view from your warehouse.
- A **destination target** — a specific object in a SaaS tool (e.g. `Salesforce / Contact`).
- A **mode** — typically `full` (replace all records) or `incremental` (upsert changes only).

## Why StateBackedComponent

The component uses Dagster's `StateBackedComponent` pattern so the Polytomic API is called **once** at prepare/build time and the sync list is cached to disk. Subsequent code-server reloads read from the cache without any network calls, keeping startup fast and not requiring a live API key at import time.

To populate or refresh the cache:

```bash
# During local development (automatic):
dagster dev

# In CI/CD or when building a Docker image:
dg utils refresh-defs-state
```

## API key setup

The component reads your Polytomic API key from an environment variable you specify via `api_key_env_var`. Create a Polytomic API key in **Settings → API Keys** and export it:

```bash
export POLYTOMIC_API_KEY="pt_live_..."
```

## Required packages

```
requests>=2.28.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `api_key_env_var` | Yes | — | Name of the env var containing the Polytomic API key |
| `organization_id` | No | `null` | Filter syncs to a specific Polytomic organization ID (useful for multi-org accounts) |
| `name_filter` | No | `null` | Substring filter applied to sync names |
| `exclude_sync_ids` | No | `null` | List of Polytomic sync IDs to exclude from asset generation |
| `group_name` | No | `polytomic` | Dagster asset group name assigned to all generated assets |
| `key_prefix` | No | `null` | Optional prefix prepended to every generated asset key |
| `wait_for_completion` | No | `true` | Poll until the sync run finishes; set to `false` to fire-and-forget |
| `poll_interval_seconds` | No | `15` | Seconds between status-poll requests |
| `sync_timeout_seconds` | No | `7200` | Maximum seconds to wait before raising a timeout error (2 hours) |

## Example YAML

```yaml
type: dagster_component_templates.PolytomicAssetComponent
attributes:
  api_key_env_var: POLYTOMIC_API_KEY
  group_name: reverse_etl
  wait_for_completion: true
  poll_interval_seconds: 15
```

Advanced example with filtering:

```yaml
type: dagster_component_templates.PolytomicAssetComponent
attributes:
  api_key_env_var: POLYTOMIC_API_KEY
  group_name: reverse_etl
  key_prefix: polytomic
  name_filter: "Salesforce"
  exclude_sync_ids:
    - "abc123"
    - "def456"
  wait_for_completion: true
  poll_interval_seconds: 30
  sync_timeout_seconds: 3600
```

## Sync mode types

Polytomic supports two primary sync modes:

- **`full`** — Replaces all records in the destination on every run. Suitable for smaller datasets or destinations that do not support upserts.
- **`incremental`** — Upserts only new or changed records using a configured identity field. More efficient for large datasets.

The mode is surfaced as a tag in the Dagster asset metadata (`polytomic/mode`) and included in the asset description.

## Refreshing state

If you add, remove, or rename syncs in Polytomic, re-run the state refresh to pick up the changes:

```bash
dg utils refresh-defs-state
```

This re-calls `GET /api/syncs`, applies any configured filters, and rewrites the cache file. The next time Dagster loads your code location it will reflect the updated sync list.
