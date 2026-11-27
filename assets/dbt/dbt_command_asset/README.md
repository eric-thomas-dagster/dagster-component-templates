# DbtCommandAsset

Run an arbitrary `dbt` CLI command (`run`, `build`, `test`, `snapshot`, `seed`, `source` freshness) as a **single materializable Dagster asset** — no dbt manifest, no per-model asset graph required.

This fills the gap between two existing dbt components:

- [`jobs/dbt_run_job`](../../../jobs/dbt_run_job) runs the same kind of command, but is explicitly **job-shaped** ("no asset modeling" per its own docstring) — materializing it doesn't produce a trackable asset.
- [`assets/dbt/enriched_dbt_project`](../enriched_dbt_project) is asset-shaped, but requires a real dbt manifest and emits **one asset per dbt model** — much richer, but a bigger commitment than "just run this command and materialize one thing."

`DbtCommandAssetComponent` is the lightweight middle ground — the same relationship [`shell_command_asset`](../../infrastructure/shell_command_asset) has to a full per-step pipeline component, specialized for dbt.

## How it runs

Builds and runs:

```
dbt <command> --project-dir <project_dir> [--profiles-dir ...] [--target ...] \
    [--select ...] [--exclude ...] [--full-refresh] [--vars '{...}'] [extra_args...]
```

via `subprocess.run`, streaming stdout/stderr to the run log. A nonzero exit code fails the asset (via `dg.Failure`).

## `parse_run_results` — best-effort metadata from dbt's own artifact

Every `dbt run|build|test|snapshot|seed` invocation writes `target/run_results.json`. When `parse_run_results: true` (the default), this component reads it after a real (non-dry-run) invocation and attaches:

- `dbt_node_count` — total nodes in the run
- `dbt_status_counts` — a JSON object of `{status: count}` (e.g. `{"pass": 40, "error": 2, "skip": 1}`)
- `dbt_elapsed_seconds` — total elapsed time dbt reported

If the file is missing or unparseable, these keys are simply omitted — this is enrichment, not a requirement, and never fails the asset on its own.

## Partitioning

`{partition_key}` is substituted into `select`, `cwd`, and any string value in `vars` when a `partition_type` is set — e.g. a daily-partitioned asset with `select: "tag:daily_{partition_key}"`.

## Example

```yaml
type: dagster_community_components.DbtCommandAssetComponent
attributes:
  asset_name: run_marts_models
  project_dir: "{{ project_root }}/dbt_project"
  target: prod
  command: build
  select: "marts.*"
  group_name: dbt
  description: "Builds the marts layer via a plain dbt build command, no per-model asset graph."
```

## `dry_run`

Logs the fully-resolved command (after `{partition_key}` substitution) and materializes successfully without spawning dbt at all. Metadata always carries `dry_run: true`/`false` so history shows which runs were real.

## `run_window`

`'HH:MM-HH:MM tz [day,day,...]'` — outside this window the asset skips cleanly instead of running or failing. A malformed spec fails open (never blocks).
