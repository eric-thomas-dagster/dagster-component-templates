# prefect_flow_run_sensor

Sensor that watches the Prefect API for flow runs entering a terminal state (COMPLETED / FAILED / CRASHED / CANCELLED) and launches a Dagster job for each one. Use when Prefect owns some upstream work and Dagster wants to react to completions.

Filters on flow_name and/or deployment_name — combine to scope narrowly.

## Partition modes

Same shape as `filesystem_monitor` / `adls_monitor` / `gcs_monitor`:

- **`run_config`** (default): flow run info goes into the launched job's `run_config`. Legacy shape.
- **`static_partition`**: yields `RunRequest(partition_key=<from template>)` — use when the target asset has a static or dynamic partitions_def matching the derived keys.
- **`dynamic_partition`**: also registers each key on the given `DynamicPartitionsDefinition` first — so downstream assets appear in the catalog per Prefect flow run.

`partition_key_template` supports `{flow_run_id}`, `{flow_name}`, `{deployment_name}`.

## Related

- [`prefect_flow_run`](../../assets/infrastructure/prefect_flow_run) — trigger a Prefect deployment as a Dagster asset (the other direction).
- [`prefect_resource`](../../resources/prefect_resource) — optional shared connection resource.
