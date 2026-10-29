# InsightsCustomMetricsSyncComponent

Registers Dagster+ Insights **custom metrics** (defined by `metadata_key` + unit) through the Dagster+ GraphQL API, as an
idempotent asset, so you don't create them by hand in the UI. Once defined, Insights aggregates a metric across every asset
whose materialization emits a numeric `MetadataValue` under that key. The GraphQL operations are ported unchanged from
`cli/sync_custom_metrics/sync_custom_metrics.py`.

## Behaviour
- Match by `metadata_key`. Create when missing; update only if `display_name` / `description` / `unit_type` differ (unspecified
  optional fields keep the server's value, never nulled). An unchanged run performs zero mutations.
- **Never deletes** unless `prune: true` **and** `prune_prefixes` is non-empty; only keys starting with a prefix (and not desired)
  are deleted, so UI-created metrics survive. `prune: true` without prefixes fails at build time.
- `dry_run` defaults to **true**: it lists current definitions and reports the plan (`planned` metadata) with no mutation. Metric
  definitions are org-visible settings, so the first run is a preview; set `dry_run: false` to write.
- `on_error` defaults to **warn**: a Dagster+ outage/bad token logs a warning and records `status: failed` + `errors` in metadata
  instead of failing the run (this is a sidecar asset). Use `fail` in CI.
- HTTP 429/5xx/network errors are retried (`max_retries`, jittered exponential backoff, honours `Retry-After`); 401/403 and 4xx are
  not. `CustomMetricError` for one metric is reported and the other metrics still sync.
- The token (`token_env_var`, default `DAGSTER_CLOUD_API_TOKEN`) is read at run time and never logged or put in metadata/errors.
- `deployment_url` is **required**: either an explicit `https://<org>.dagster.cloud/<deployment>` (plain http only for localhost; an
  explicit value always wins) or `auto`, resolved at run time from sources confirmed in the Dagster+ code:
  deployment = env `DAGSTER_CLOUD_DEPLOYMENT_NAME` (set in `dagster_cloud_backend/user_code/workspace.py:169`, delivered to run workers
  through the container-context env vars, `dagster_cloud_serverless_k8s_agent/run_launcher.py:40-47`; documented in
  `built-in.md:17`; for a branch deployment it is the branch deployment's name). The **organization name is not an env var** in run
  workers (`DAGSTER_CLOUD_ORGANIZATION` is a CLI/CI variable; the injected `INTERNAL_DAGSTER_CLOUD_ORGANIZATION_ID` is an opaque id),
  so it comes from the instance: `DagsterCloudAgentInstance.dagit_url` (`dagster_cloud/instance/__init__.py:387-401`) decodes it, and
  the region, from the agent token -> `https://<org>.dagster.cloud` or `https://<org>.<region>.dagster.cloud` (e.g. `.eu.`). If the
  env var or the instance property is missing (plain OSS instance, legacy agent token, local run) `auto` raises an error telling you
  to set the URL explicitly. Custom domains cannot be derived. Not verified end to end: that a run worker's instance carries the
  agent token (from reading the code only); if it does not, you get the explicit-URL error, never a guessed URL.

## Schema-drift guard
`verify_schema: off | warn | strict` (default **warn**; legacy true=strict, false=off). Before any write the asset introspects the
server once and validates every operation we send (fields, union membership, argument/variable types, input keys and
required-ness, unit enum values). `warn`: records `schema_check` (`pass|drift|unavailable`) + `schema_problems`, logs a WARNING on
drift, and syncs anyway. `strict`: on drift makes **no** writes (not even the list call) and reports `status: schema_drift`
(raises when `on_error: fail`). Introspection being unavailable (permissions, disabled) never blocks in any mode. Start with
`warn`; move to `strict` after one clean live `--verify-schema` run, since the validator can only be as right as the schema it
sees.
CLI: `python component.py --verify-schema --deployment-url https://acme.dagster.cloud/prod` prints PASS or the problems
(exit 1 on drift, 2 if introspection is impossible) -- handy in CI.

## Presets
`agentic_pipeline`, `context_engineering_pipeline`, `receipts_sync`, `receipt_outcome_ingestion`, `rag_pipeline` -- the numeric
metadata keys those components emit (a test AST-checks each key against the component source). `run_failure_remediation`: TODO.
**Collision risk:** Insights keys are global per deployment. `retries`, `lag_seconds`, `load_jobs`, `total_tokens`,
`total_cost_usd`, `total_latency_ms`, `cache_*_tokens` are emitted under those exact names, so any other asset emitting them is
aggregated too. Pick presets selectively, or use explicit `metrics:`.

## `presets: auto` (opt-in)
1. Add this component to your project.
2. Set `presets: auto` (a string or `[auto, ...]`); components tag their assets `insights/metrics: <preset name>`.
3. Set the token env var (`DAGSTER_CLOUD_API_TOKEN`) on the code location.
4. Materialize once: check `planned` / `auto_discovered` in the metadata.
5. Flip `dry_run: false`.

At run time the asset scans asset tags and unions the preset names found (`auto_discovered`, `auto_scope` in metadata; unknown
tag values are listed in `auto_unknown_tag_values`). If nothing is tagged it is a no-op with a message. Scope: in a run launched
from a code location (Dagster+) `context.repository_def` exposes the whole location (`auto_scope: repository`). In in-process
execution (`dg.materialize`, `execute_in_process`) it is absent and only the run's own job graph is visible
(`auto_scope: job`); a run selecting just this asset then sees only itself. Build-time scanning is not possible: a component's
`build_defs` cannot see sibling components' definitions.

## CLI (GitOps)
`python component.py --manifest-only --preset agentic_pipeline [--preset ...]` prints a manifest for `cli/sync_custom_metrics`.
`--list-presets` lists presets.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `deployment_url` | `str` | Full Dagster+ deployment URL, e.g. https://acme.dagster.cloud/prod. `auto` derives it at run time from DAGSTER_CLOUD_DEPLOYMENT_NAME + the instance's Dagster+ organization, and errors if it cannot (see README); an explic… _(full docs in schema.json + component README)_ |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `token_env_var` | `str` | `"DAGSTER_CLOUD_API_TOKEN"` | Env var holding a Dagster+ API token with permission to manage custom metrics. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout_seconds` | `float` | `30.0` | Per-request HTTP timeout. |
| `max_retries` | `int` | `4` | Retries for HTTP 429/5xx/network errors (jittered exponential backoff, honours Retry-After). |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | `"insights_custom_metrics_sync"` | Dagster asset name |
| `group_name` | `str` | `"agents"` | Asset group |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_backoff_seconds` | `float` | `1.0` | Base backoff. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `presets` | `List[str]` | `list()` | `f"Preset names: {', '.join(sorted(PRESETS))}. `auto` (alone or in the list) = at run time, register the presets named by the `{INSIGHTS_TAG_KEY}` tag of the assets in the code location."` |
| `metrics` | `List[Dict[str, Any]]` | `list()` | Explicit entries: {metadata_key, unit_type, display_name?, description?}. |
| `dry_run` | `bool` | `true` | Plan only: reads current definitions, performs NO mutation. Set false to write. |
| `prune` | `bool` | `false` | Delete metrics not in the desired set -- ONLY keys starting with a prune_prefixes entry. |
| `prune_prefixes` | `List[str]` | `list()` | Safelist for prune. Required (non-empty) when prune is true. |
| `on_error` | `str` | `"warn"` | warn (log + record in metadata, run succeeds) or fail (raise). |
| `verify_schema` | `str` | `"warn"` | off \| warn \| strict (legacy true=strict, false=off). warn (default): introspect once, record schema_check/schema_problems and log a WARNING on drift, but still sync. strict: on drift make no writes, status schema_drift… _(full docs in schema.json + component README)_ |
| `cron_schedule` | `str` | — | Optional cron expression to re-run the sync. |

[//]: # (FIELDS:END)
