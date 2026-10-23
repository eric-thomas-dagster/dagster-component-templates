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
- `deployment_url` is **required** (`https://<org>.dagster.cloud/<deployment>`; plain http only for localhost). It is not derived
  from the agent environment: the agent exposes the deployment name but I found no reliable organization variable.

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
