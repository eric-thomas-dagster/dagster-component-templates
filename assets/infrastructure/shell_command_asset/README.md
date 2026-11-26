# ShellCommandAsset

Run a shell command (or multiline script) as a Dagster asset via `subprocess.Popen(cmd, shell=True, executable="/bin/bash")` — the Airflow `BashOperator` / `@task.bash` equivalent. **Not** `dagster-shell`'s `execute_shell_command` (an earlier version of this README claimed that, incorrectly — the code has always used `subprocess` directly, and `dagster-shell`'s last release pins `dagster==1.9.13`, so it isn't even installable alongside a current Dagster). Captured stdout/stderr stream to the run log live, exit code → asset success/failure, optional `cwd` + `env_vars` (merged with the parent process's own environment, same as Airflow).

Runs specifically via `bash -c`, not the OS default `/bin/sh` that bare `shell=True` would otherwise pick — on many Linux distros `/bin/sh` is `dash`, which rejects bashisms like `[[ ]]` / `${var:-default}` / arrays. This matches Airflow's `BashOperator` default executable.

## `command_asset_key` — command from an upstream asset (`@task.bash`-style)

```yaml
command_asset_key: build_deploy_command   # upstream asset's materialized value IS the command
command: echo fallback                    # used only if the upstream value is falsy
```

Mirrors Airflow's `@task.bash`, where the decorated Python callable's return value is the command to run. `command_asset_key` adds that asset as a dependency automatically; `command` becomes optional (but at least one of the two is required).

## `skip_on_exit_code` — a matching exit code succeeds instead of failing

Airflow's `BashOperator` defaults `skip_exit_code` to `99` (a no-op task is marked skipped, not failed). This field is **not defaulted to 99** here — this component predates it, and existing YAML may rely on every nonzero exit code failing the run. Set it explicitly to opt in:

```yaml
skip_on_exit_code: 99
```

The asset still materializes successfully (Dagster has no native "skipped" status for a single-asset op); metadata records `skipped: true` and the real exit code so you can tell a real no-op apart from a plain success.

## `success_exit_codes` — treat more nonzero codes as plain success

Some legacy scripts exit nonzero on "nothing to do," and that should count as a normal success, not a skip. `success_exit_codes` is distinct from `skip_on_exit_code`: it adds no `skipped` metadata marker at all, just an ordinary successful materialization. Checked before `skip_on_exit_code` and before the exit-code-nonzero failure, so a code can only match one of the two:

```yaml
success_exit_codes: [5, 6]
```

## `dry_run` — log the command without running it

```yaml
dry_run: true
```

Logs the fully-resolved command (after `{partition_key}` substitution) and returns a successful materialization with no `subprocess.Popen` call at all — no `exit_code`/`stdout_preview` metadata either, since nothing ran. Every materialization (dry or real) always carries a `dry_run: true`/`false` metadata key, so a history shows which runs were real.

## `return_last_stdout_line` — feed a downstream asset the command's output

A source system's shell step is often read by its downstream steps as its last line of output.
Set `return_last_stdout_line: true` to return that line as this asset's materialized value (off
by default — the existing metadata-only behavior is unchanged). Add `parse_json_output: true` to
parse that line as JSON when it is valid JSON (falls back to the raw string otherwise):

```yaml
command: ./scripts/compute_row_count.sh
return_last_stdout_line: true
parse_json_output: true
```

## `timeout_seconds`

Kills the process and fails the run if it runs longer than this many seconds. Unset (default): no timeout.

## `run_window` — skip cleanly, don't fail, outside a time window

```yaml
run_window: "09:00-17:00 America/New_York mon,tue,wed,thu,fri"
```

Checked once per materialization, before `dry_run` or the command itself runs. Outside the
window, the asset **skips cleanly** — no subprocess is spawned, the run succeeds, and real
downstream dependents skip too, rather than failing or blocking the DAG. A malformed spec fails
open (always runs) rather than silently blocking the command forever. Unset (default): always
runs.

**Sharing one `run_window` across several assets** instead of repeating the same literal string
in every component instance: Dagster's own `template_vars_module` mechanism (real, built-in --
not anything new here) lets several `defs.yaml` files reference one Python-defined value by
name. Define it once:

```python
# template_vars.py (sibling to the defs.yaml files that reference it)
import dagster as dg

@dg.template_var
def business_hours() -> str:
    return "09:00-17:00 America/New_York mon,tue,wed,thu,fri"
```

then in each asset's `defs.yaml`:

```yaml
template_vars_module: .template_vars   # relative to this defs.yaml; or an absolute module path
                                        # (e.g. my_project.shared.template_vars) to share across
                                        # defs.yaml files in different directories
attributes:
  run_window: "{{ business_hours }}"   # no parens -- business_hours() already returned the string at load time
```

Changing the window means editing one function, not every asset that uses it. (Confirmed from
Dagster's own `dagster.components.component.template_vars` docstring and source directly -- not
live-tested against a running `dg` project in this environment.)

## Partitioning

With `partition_type` set, `{partition_key}` is substituted into both `command` and `cwd` before running:

```yaml
command: "./export.sh --date={partition_key}"
cwd: "/data/{partition_key}"
partition_type: daily
partition_start: "2024-01-01"
```

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout_seconds` | `int` | — | Kill the command and fail the run if it runs longer than this many seconds. Unset: no timeout. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"shell"` | Asset group. |
| `description` | `str` | — | Asset description. |
| `deps` | `List[str]` | — | Upstream Dagster asset keys for lineage (in addition to command_asset_key, which is added automatically). |
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | Backfill execution strategy for a partitioned asset. |
| `owners` | `List[str]` | — | Asset owners -- team names ('team:analytics') or email addresses. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags applied to the asset in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog. Auto-inferred from component name when unset. |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'dynamic', 'multi', or None for unpartitioned. With a partition type set, `{partition_key}` is substituted into `command`/`cwd`. |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. |
| `partition_end_offset` | `int` | `0` | 0 (default): a time-based partition set only ever holds COMPLETED windows. 1: include the current, still-in-progress window. |
| `partition_timezone` | `str` | — | IANA timezone for the partitions definition (defaults to UTC). |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is time-based. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec. Overrides flat fields when set. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static or multi partitioning. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `command` | `str` | — | Shell command (or multiline script) to execute, run via `bash -c`. Supports `{partition_key}` when `partition_type` is set. Required unless `command_asset_key` is set (in which case the upstream asset's value IS the comm… _(full docs in schema.json + component README)_ |
| `command_asset_key` | `str` | — | Upstream asset key whose materialized value becomes the command string to run -- mirrors Airflow's `@task.bash`, where the decorated Python callable's return value IS the command. Adds that asset as a dependency automatically. |
| `cwd` | `str` | — | Working directory for the command. |
| `env_vars` | `Dict[str, str]` | — | Additional env vars to pass to the shell (merged with the parent process's own environment). |
| `skip_on_exit_code` | `int` | — | Exit code treated as a no-op success instead of a failure (Airflow's BashOperator default is 99 -- not defaulted here since this component predates this field and existing YAML may rely on every nonzero exit code failing… _(full docs in schema.json + component README)_ |
| `success_exit_codes` | `List[int]` | — | Additional exit codes treated as a normal, successful materialization -- unlike skip_on_exit_code, no 'skipped' metadata marker, just success. For legacy scripts that exit nonzero on 'nothing to do' and should count as s… _(full docs in schema.json + component README)_ |
| `dry_run` | `bool` | `false` | If True, log the fully-resolved command (after {partition_key} substitution) and skip actually running it -- no subprocess is spawned. Output metadata always includes dry_run so a materialization history shows which runs were real. |
| `run_window` | `str` | — | 'HH:MM-HH:MM tz [day,day,...]' (e.g. '09:00-17:00 America/New_York mon,tue,wed,thu,fri'). Outside this window, the asset SKIPS cleanly instead of running or failing -- the command does not run, and real downstream depend… _(full docs in schema.json + component README)_ |
| `pool` | `str` | — | Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed number of slots shared across every asset/op using the same pool name). The pool's own slot count is configured at the deployment level (dagster.y… _(full docs in schema.json + component README)_ |
| `return_last_stdout_line` | `bool` | `false` | Return the command's last non-empty stdout line as this asset's materialized value (via the IO manager), not just metadata, so a downstream asset can take it as an input the usual way. Mirrors what a source system's last… _(full docs in schema.json + component README)_ |
| `parse_json_output` | `bool` | `false` | Only with return_last_stdout_line: if that last line parses as valid JSON, return the parsed value instead of the raw string. Falls back to the raw string when it isn't valid JSON. |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (partition_type='dynamic'). |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only. |

[//]: # (FIELDS:END)

## Example

```yaml
type: dagster_community_components.ShellCommandAssetComponent
attributes:
  asset_name: refresh_reports
  command: ./scripts/refresh_reports.sh
  cwd: /opt/reports
  env_vars:
    REPORT_ENV: production
  skip_on_exit_code: 99
  timeout_seconds: 600
  group_name: shell
  description: Rebuild the nightly report files
  deps:
    - raw/orders
```

## `pool` — Dagster's native concurrency pool (Airflow's `pool`)

Set `pool: <name>` to share a fixed number of concurrent slots across every asset/op using the
same pool name — the direct analog of Airflow's `pool`. The slot count itself is configured at
the deployment level (`dagster.yaml`'s `concurrency: pools:`, or the Dagster+ UI), not on this
component.

For an Airflow trigger-rule equivalent (`automation_condition`) instead of a slot limit, see
[`automation_condition_applicator`](https://dagster-component-ui.vercel.app/c/automation_condition_applicator)
— it attaches a named preset to any asset by selection, so there's no separate
`automation_condition` field needed here.

## Requirements

```
dagster
```

No third-party dependency — `subprocess` is standard library.
