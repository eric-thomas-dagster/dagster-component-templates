# PerPartitionBackfillJobComponent

Cron-driven parallel backfill of N partitions of an existing partitioned
asset. Complements Dagster's first-class backfill machinery for
predictable recurring rebuilds — e.g. "every Monday at 9am, refresh the
last 7 days of sales_daily" or "every hour, refresh every registered
tenant's daily rollup".

## Partition-selection strategies

| Strategy | When to use | Required fields |
|---|---|---|
| `last_n_days` | Time-window backfill of the most recent N daily partitions | `partition_count` |
| `explicit` | Hand-pick a known set of keys | `partition_keys` |
| `callable` | Compute the keys at job runtime (e.g. from an external system) | `partition_callable_path` |
| `dynamic_all` | Multi-tenant SaaS: rebuild every currently-registered tenant | `dynamic_partitions_def_name` |
| `dynamic_subset` | Same, filtered by a regex (only enterprise tenants, only EU customers, etc.) | `dynamic_partitions_def_name`, `dynamic_filter` |

## Concurrency

Two ways to throttle execution:

| Field | Behavior |
|---|---|
| `concurrency_key_template` (recommended) | Each per-partition op gets a `dagster/concurrency_key` tag rendered from this template. Available placeholder: `{partition_key}`. e.g. `tenant-{partition_key}` makes same-tenant runs serialize but cross-tenant parallelize — the right shape for multi-tenant SaaS. |
| `max_concurrent_tag_value` (legacy) | A single static key applied to every per-partition op. Either all serialize or no concurrency control. Use the template form instead. |

Combine with [Dagster's run-coordinator concurrency limits](https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines)
to actually throttle (the tag is the slot key; the limit is configured
on the run coordinator).

## Examples

### Last 7 days of a daily-partitioned asset, every Monday

```yaml
type: dagster_component_templates.PerPartitionBackfillJobComponent
attributes:
  job_name: weekly_sales_backfill
  schedule: "0 9 * * 1"            # Mondays 9am UTC
  target_asset_key: sales_daily
  partition_strategy: last_n_days
  partition_count: 7
  retry_max_retries: 1
```

### Multi-tenant: refresh every registered tenant's rollup, hourly

```yaml
type: dagster_component_templates.PerPartitionBackfillJobComponent
attributes:
  job_name: tenant_rollup_refresh
  schedule: "0 * * * *"
  target_asset_key: tenant_daily_rollup
  partition_strategy: dynamic_all
  dynamic_partitions_def_name: tenants    # the name= on DynamicPartitionsDefinition
  concurrency_key_template: "tenant-{partition_key}"
```

### Subset filter: only enterprise tenants

```yaml
attributes:
  job_name: enterprise_rollup
  partition_strategy: dynamic_subset
  dynamic_partitions_def_name: tenants
  dynamic_filter: "^enterprise-"           # python regex
  concurrency_key_template: "tenant-{partition_key}"
```

### Explicit list

```yaml
attributes:
  partition_strategy: explicit
  partition_keys: ["2025-04-30", "2025-05-01", "2025-05-02"]
```

### Callable (returns a list at runtime)

```yaml
attributes:
  partition_strategy: callable
  partition_callable_path: my_project.partitions:active_tenant_keys
```

## Limitations

- The target asset must live in the **same code location** as this job.
  For cross-location backfills, prefer `dg backfill` or the Dagster UI's
  backfill flow.
- Dynamic-partition strategies require the `DynamicPartitionsDefinition`
  to be already constructed somewhere in the project — this job reads
  the registered keys via `context.instance.get_dynamic_partitions(name)`.
- This job uses `dg.materialize` against the asset def loaded at runtime,
  so each per-partition run is a Dagster sub-execution (visible in the
  UI as a step within the backfill run, not a separate top-level run).
