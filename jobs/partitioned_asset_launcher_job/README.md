# Partitioned Asset Launcher Job

Config-driven entry point for a dynamic-partitioned asset pipeline.
Reads a run-config form → computes a partition key from a template →
registers the partition → materializes the target asset selection with
that partition key set.

## When to use

- Your downstream pipeline is dynamic-partitioned (e.g. one asset run
  per GitHub issue, per tenant, per uploaded file).
- You want a single form/POST entry point where the caller provides the
  fields that identify the run (owner + repo + issue_number, tenant_id,
  file_uri) and the launcher derives the partition key from those fields.
- You want to skip declaring a `Config` class on the pipeline's asset —
  the launcher owns the form, the pipeline owns the substitution
  (`{partition_key}` / `{partition.<name>}`).

## Fields

- **`job_name`** *(required, string)* — Dagster job name for the launcher
  itself.
- **`target_asset_keys`** *(required, list[string])* — Asset keys to
  materialize when the launcher fires. Must be dynamic-partitioned on
  `dynamic_partitions_name`.
- **`dynamic_partitions_name`** *(required, string)* — Name of the
  `DynamicPartitionsDefinition` on the target assets.
- **`partition_key_template`** *(required, string)* — Format template
  composing the partition key. e.g. `"{owner}/{repo}#{issue_number}"`.
  Each `{name}` placeholder maps to a `config_schema` field.
- **`config_schema`** *(required, dict)* — Config fields exposed on the
  launchpad form. Shape:
  `{field_name: {type: str|int|float|bool, default: <value>}}`. Fields
  without `default` are required.
- **`tags`** *(optional, dict)* — Tags applied to the launcher run.

## Example

```yaml
type: dagster_community_components.PartitionedAssetLauncherJobComponent
attributes:
  job_name: launch_mir
  target_asset_keys:
    - mir_intake
    - mir_final_report
  dynamic_partitions_name: mir_investigations
  partition_key_template: "{owner}/{repo}#{issue_number}"
  config_schema:
    owner:        {type: str, default: dagster-io}
    repo:         {type: str, default: dagster}
    issue_number: {type: int}
```

Pair with an `AgenticPipelineComponent` (or any dynamic-partitioned
multi_asset) that references the same `dynamic_partitions_name` and
uses `partition_key_parser` to unpack the composite key:

```yaml
type: dagster_community_components.AgenticPipelineComponent
attributes:
  asset_name_prefix: mir
  partition_type: dynamic
  dynamic_partition_name: mir_investigations
  partition_key_parser: "{owner}/{repo}#{issue_number}"
  # ...
  steps:
    - id: mir_intake
      op: mcp_call
      server: {name: github, type: stdio, command: [npx, -y, "@modelcontextprotocol/server-github"]}
      mcp_tool_name: get_issue
      tool_args:
        owner:        "{partition.owner}"
        repo:         "{partition.repo}"
        issue_number: "{partition.issue_number}"
```

## Runtime shape

Every invocation produces two Dagster runs:

1. **The launcher run** — fast, one op (`{job_name}_op`). Reads config,
   formats the partition key, registers the dynamic partition, then
   invokes the target materialization in-process.
2. **The materialization run** — one materialization of the target
   asset selection with `partition_key` set. Tagged
   `dagster/launched_by={job_name}` so you can trace back.

Programmatic launch:

```bash
dagster job launch --job-name launch_mir --config '{"ops": {"launch_mir_op": {"config": {"owner": "dagster-io", "repo": "dagster", "issue_number": 30000}}}}'
```

Or via Dagster+ / the GraphQL API — the same shape any external system
would POST.
