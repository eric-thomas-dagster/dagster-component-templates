# PythonCallableAsset

Run an arbitrary Python callable as a Dagster asset — the Airflow `PythonOperator` / `@task` equivalent. With `context_mode: "airflow"`, the callable is given an Airflow-compatible execution context so a converted callable body can run largely unmodified, without needing a real Airflow installation.

## The two modes

- **`context_mode: "none"`** (default): calls `callable_path(*args, **kwargs)`. `inputs` (upstream asset values) are still bound directly to matching parameter names — this works independently of `context_mode`.
- **`context_mode: "airflow"`**: additionally builds an Airflow-shaped context and injects **only the names the callable's own signature declares** (or everything, if it declares `**kwargs`) — checked via `inspect.signature`, matching how Airflow callables opt into specific context keys by naming them as parameters.

## The Airflow context

| Name | Value |
|---|---|
| `ds` | `logical_date.strftime("%Y-%m-%d")` |
| `ds_nodash` | `logical_date.strftime("%Y%m%d")` |
| `ts` | `logical_date.isoformat()` |
| `logical_date` | see below |
| `data_interval_start` / `data_interval_end` | see below — run tags, then the partition window, then a loud fallback |
| `run_id` | `context.run.run_id` |
| `params` | the component's static `params` field |
| `dag_run` | object with `.conf` — JSON-decoded from the `dag_run_conf_tag` run tag, or `{}` |
| `ti` | object with `.xcom_pull(task_ids=name)` (resolves from `inputs`) and `.try_number` (`context.retry_number + 1`) |
| `Variable` | `Variable.get(key, default_var=None)` reads env var `AIRFLOW_VAR_<KEY upper>` — Airflow's own real, documented env-var-backing convention |
| `S3Hook` | a small, **explicitly partial** shim over boto3: `get_key`, `check_for_key`, `load_string`, `read_key` only |

**Where `logical_date`/`data_interval_start`/`data_interval_end` come from — in this priority order:**

1. **Run tags**, if the triggering schedule set them: `logical_date` / `data_interval_start` / `data_interval_end` (ISO8601) — exactly the tags `cron_schedule`'s/`interval_schedule`'s own `add_logical_date_tag`/`add_interval_tags` produce. Trusted over everything else, since the schedule component already resolved the real date for this run.
2. **The asset's own partition window**, via Dagster's native `context.partition_time_window`, if no run tags are present. `airflow_version: 2` (default) sets `logical_date` to the interval **start** (Airflow 2's `execution_date` semantics); `airflow_version: 3` sets it to the interval **end** (approximates Airflow 3's `CronTriggerTimetable`, not a full timetable port).
3. **`on_missing_date`**, if neither is available (an un-partitioned asset with no date-tagging schedule): `"warn"` (default) logs a clear warning and falls back to `datetime.now(utc)` — this used to happen **silently**, which was a real bug; `"fail"` raises instead, for pipelines where a wrong date silently flowing into a query or filename is worse than not running.

```yaml
# Paired with a schedule's add_logical_date_tag/add_interval_tags — no partition needed
attributes:
  context_mode: airflow
  on_missing_date: fail   # refuse rather than silently use now() if the tag is somehow missing
```

## `inputs` — replacing XCom

```yaml
inputs:
  upstream_a: mydag/extract   # {name: upstream_asset_key}
```

Each entry's value is loaded via the IO manager. `name` doubles as the Airflow task_id for `ti.xcom_pull(task_ids=name)`, and — if the callable's signature declares a parameter called `name` — is bound directly to it (TaskFlow-style), on top of (or instead of) using `ti.xcom_pull` inside the function body. `ti.xcom_pull` on a `name` not declared in `inputs` raises a clear error at run time naming the fix. Use `deps` instead of `inputs` for a lineage-only upstream with no value needed.

### Reading one named value instead of the whole upstream value

A `.` in the value switches to a metadata lookup instead of an IO-manager value load — the
multi-value XCom pattern, where a task pushes several named values (as metadata on its own
materialization) and a later task reads one of them by name:

```yaml
inputs:
  row_count: mydag/extract.row_count   # {name: 'upstream_asset_key.metadata_key'}
input_defaults:
  row_count: 0   # used if `extract`'s materialization THIS RUN has no 'row_count' metadata
```

The lookup only sees the upstream's materialization event from the **current run** (not any
earlier run) — if the upstream hasn't actually run yet in this run, or didn't set that metadata
key, the entry falls back to `input_defaults[name]` if set, else fails loudly rather than
silently passing `None`.

## Jinja templates in `args`/`kwargs`

`context_mode: "airflow"` only. String values in `args`/`kwargs` containing `{{ }}` are rendered with exactly these variables — anything else **raises at component-construction time**, not at run time. Rendered through Jinja's `SandboxedEnvironment`, not a plain `Environment`: the identifier whitelist only checks the top-level name (`{{ ds }}`), not what a template does with it once bound, so the sandbox is what actually stops something like `{{ params.__class__.__mro__ }}` from reaching into Python internals at render time.

- `{{ ds }}`, `{{ ds_nodash }}`
- `{{ logical_date }}`, and `{{ logical_date | ds }}` / `{{ logical_date | ds_nodash }}` (custom filters)
- `{{ params.x }}`
- `{{ var.value.FOO }}` (same `AIRFLOW_VAR_FOO` env var as the `Variable` shim)

## What this does NOT do

- No real Airflow metadata database — `Variable`/`dag_run.conf` are env-var/run-tag-backed shims, not a live Airflow deployment.
- No multiple runs per data interval, no `logical_date=None` ad-hoc runs, and `airflow_version: 3` is a documented approximation of Airflow 3's timetable change (AIP-83), not a full port.
- `S3Hook` covers 4 methods, not the full `airflow.providers.amazon.aws.hooks.s3.S3Hook` surface — extend `_AirflowS3Hook` in `component.py` as real converted callables need more.
- `Variable`/`S3Hook` are available only through the injected context (a parameter the callable declares, or `**kwargs`) — there is no separately-importable `dagster_community_components.airflow_compat` package for a callable's own top-level `import` statements.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_key` | `str` | Dagster asset key, slash-delimited (e.g. 'mydag/mytask'). |
| `callable_path` | `str` | 'module.path:function_name' (e.g. 'mypkg.tasks:extract'). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `params` | `Dict[str, Any]` | — | Static dict exposed as `params` (mirrors Airflow's DAG-level params). |
| `timeout_seconds` | `int` | — | Unused by this component today (no built-in call timeout) -- kept for schema consistency with sibling infrastructure components. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `deps` | `List[str]` | — | Lineage-only upstream asset keys -- no value loaded (use `inputs` to load a value). |
| `group_name` | `str` | `"infrastructure"` | Dagster asset group name. |
| `description` | `str` | — | Human-readable description surfaced in the Dagster UI. |
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
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'dynamic', 'multi', or None for unpartitioned. |
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

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `input_defaults` | `Dict[str, Any]` | — | Default value for an `inputs` metadata-lookup entry ('asset_key.metadata_key') whose metadata key is missing from the upstream's materialization in this run. Entries with no default raise instead of silently passing None. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `args` | `List[Any]` | — | Static positional arguments passed to the callable. |
| `kwargs` | `Dict[str, Any]` | — | Static keyword arguments passed to the callable. |
| `inputs` | `Dict[str, str]` | — | {name: upstream_asset_key} loads the whole value via the IO manager. {name: 'upstream_asset_key.metadata_key'} (a '.' in the value) instead reads just that named metadata entry off the upstream asset's materialization ev… _(full docs in schema.json + component README)_ |
| `context_mode` | `Literal['none', 'airflow']` | `"none"` | 'airflow' injects the Airflow-compatible context described above; 'none' calls the callable with just `args`/`kwargs`. |
| `dag_run_conf_tag` | `str` | `"airflow/dag_run_conf"` | Run tag name carrying a JSON-encoded `dag_run.conf` dict -- set `dg launch --tags 'airflow/dag_run_conf={"key": "value"}'` (or your own tag name). Absent tag -> `{}`. Set to None/null to disable. |
| `airflow_version` | `Literal[2, 3]` | `2` | Controls only `logical_date`: 2 (default) = partition interval start (Airflow 2's execution_date semantics -- the overwhelmingly common case for already-written DAGs). 3 = partition interval end (approximates Airflow 3's… _(full docs in schema.json + component README)_ |
| `on_missing_date` | `Literal['warn', 'fail']` | `"warn"` | Only matters for context_mode='airflow' on an UN-PARTITIONED asset with no logical_date/data_interval_* run tags available (neither cron_schedule's/interval_schedule's add_logical_date_tag/add_interval_tags nor a partiti… _(full docs in schema.json + component README)_ |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (partition_type='dynamic'). |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only. |

[//]: # (FIELDS:END)

## Example

```yaml
type: dagster_community_components.PythonCallableAssetComponent
attributes:
  asset_key: mydag/transform
  callable_path: myproject.tasks:transform
  context_mode: airflow
  inputs:
    upstream_a: mydag/extract
  params:
    threshold: 10
  partition_type: daily
  partition_start: "2024-01-01"
  kwargs:
    output_prefix: "{{ ds }}"
```

A converted Airflow callable using the classic context style runs unmodified:

```python
# myproject/tasks.py
def transform(ti, params, ds, **_):
    upstream = ti.xcom_pull(task_ids="upstream_a")
    return do_transform(upstream, threshold=params["threshold"], run_date=ds)
```

## Requirements

```
dagster
jinja2
boto3
```

`boto3` is only needed if a callable actually uses the injected `S3Hook`.
