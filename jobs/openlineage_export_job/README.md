# OpenLineageExportJobComponent

Op-shaped job that **walks the live Dagster asset graph** on each run and
emits one OpenLineage `RunEvent` per asset to a configured collector
(Marquez, OpenLineage Proxy, Datakin, or any consumer of the
[OpenLineage v2 spec](https://openlineage.io/spec/2-0-2/OpenLineage.json)).

Each emitted event has:

- `outputs[0]` = the asset, in your configured `namespace`
- `inputs[]` = the asset's parent assets (canonical OpenLineage lineage edges)
- `run.runId` = the Dagster run id (so collectors can correlate Dagster
  runs with OL events)
- `job.name` = `dagster.<asset_key>` (one OL "job" per asset, for clear
  per-asset history)
- `dagster_metadata` facet on each Dataset = group, kinds, freshness policy

## When to use this vs `lineage_to_openlineage`

Two complementary OpenLineage paths in the registry:

| Component | Shape | Use case |
|---|---|---|
| `lineage_graph_extractor` + `lineage_to_openlineage` | **3-asset chain** | Lineage as first-class assets. Snapshot is itself materialized + tracked, downstream sinks fan out to multiple catalogs from the same upstream payload. |
| `openlineage_export_job` (this) | **single op-job** | One scheduled job; doesn't add lineage as assets to your graph. Simpler when you just want lineage events emitted on a cadence. |

Pick the asset chain when you want the lineage payload itself to be
versioned + visible in Dagster. Pick this op-job when you want zero
asset overhead and a single dial to schedule.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `job_name` | `str` | required | Dagster job name |
| `schedule` | `Optional[str]` | `null` | Cron schedule; `null` = manual / sensor-triggered |
| `default_status` | `str` | `STOPPED` | `STOPPED` \| `RUNNING` |
| `tags` | `Optional[dict]` | `null` | Dagster job tags |
| `endpoint_env_var` | `str` | `OPENLINEAGE_URL` | Env var with collector base URL. The job appends `/api/v1/lineage` if no path is set. |
| `api_key_env_var` | `Optional[str]` | `null` | Env var with bearer token (for collectors that require auth) |
| `namespace` | `str` | `dagster` | OpenLineage namespace — all datasets and jobs from this code location group under this string |
| `producer` | `str` | this component's URI | OpenLineage producer URI |
| `only_export_on_change` | `bool` | `true` | Hash the payload structurally and skip emit if unchanged from the previous successful run |
| `fail_on_collector_error` | `bool` | `false` | If `true`, fail the run when the collector is unreachable |

## YAML example

```yaml
type: dagster_component_templates.OpenLineageExportJobComponent
attributes:
  job_name: emit_openlineage_lineage
  schedule: "0 */4 * * *"             # every 4 hours
  default_status: RUNNING
  endpoint_env_var: OPENLINEAGE_URL   # http://marquez:5000  (suffix appended automatically)
  namespace: dagster.prod
  only_export_on_change: true
  fail_on_collector_error: false      # best-effort
```

## Required env vars

```bash
OPENLINEAGE_URL=http://marquez:5000   # or full URL with /api/v1/lineage
# Optional, if your collector requires auth:
OPENLINEAGE_API_KEY=...
```

## Behavior

1. Walks `context.repository_def.asset_graph` to enumerate every asset
   in the current code location.
2. Hashes the structural payload (nodes + edges, no timestamps). If
   `only_export_on_change=true` and the hash matches the previous
   successful run's tag, the export is skipped — your collector
   doesn't see redundant events on every tick.
3. Builds one OpenLineage `RunEvent` per asset, populating `inputs[]`
   with the asset's upstream parents and `outputs[0]` with the asset
   itself, both in the configured `namespace`.
4. POSTs events sequentially to `{endpoint}/api/v1/lineage`. Bearer
   token added if `api_key_env_var` is set.
5. On success, tags the Dagster run with the structural hash so the
   next run's change-detection works.

## Limitations

- **Sequential POSTs**: events are emitted one at a time, no batching.
  Marquez happily accepts batches; a future iteration can group events.
- **No FAIL events**: only `COMPLETE` events are emitted. The job is
  fire-and-forget — collectors learn that the asset graph exists, not
  whether each asset's most recent run succeeded.
- **No column-level lineage**: the event includes a `schema` facet with
  metadata-derived field names, but Dagster's `column_lineage` metadata
  isn't yet propagated as a `columnLineage` facet.
- **Stdlib HTTP**: uses Python's `urllib.request`. No retry, no batching.
  For prod, swap in the official `openlineage-python` client.

## Roadmap (see [TODO.md](../../TODO.md))

- Batched event emission (`/api/v1/lineage` accepts arrays)
- `columnLineage` facet from Dagster's `column_lineage` metadata
- FAIL events when an asset's most recent run failed
- Optional Dagster+ deployment-wide scope (via GraphQL, like
  `lineage_graph_extractor` does today)
