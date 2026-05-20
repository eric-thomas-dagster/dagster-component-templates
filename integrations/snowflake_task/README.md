# Snowflake Task

Define a Snowflake TASK as Dagster YAML. Materialization runs CREATE OR REPLACE TASK and optionally RESUMEs + EXECUTEs it.

## Why this component

Lets you define a Snowflake `TASK` in Dagster `defs.yaml` and have Dagster manage its DDL lifecycle. Materializing the asset issues `CREATE OR REPLACE TASK` against your account and optionally runs `ALTER TASK ... RESUME` + `EXECUTE TASK` to trigger it immediately.

Pairs with `snowflake_workspace` (which discovers EXISTING tasks). Use this one when you want the task **defined in Dagster** rather than maintained separately in a Snowflake worksheet.

### Complementary to `snowflake_workspace`

| Component | Role |
|---|---|
| [`snowflake_workspace`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_workspace/README.md) | **Discovers** existing Snowflake entities in your account → imports them as Dagster assets |
| **`snowflake_task`** (this) | **Defines** a new entity in Dagster YAML → Dagster issues CREATE OR REPLACE on materialize |

Use whichever side matches the source of truth for each entity. Mix freely — both styles produce ordinary Dagster assets and can declare deps on each other.

## Typical use cases

- Define scheduled SQL routines in Dagster YAML; deploy on materialize
- Build AFTER-chains with `after_tasks: [...]` that span Dagster + Snowflake's native graph
- Switch between 'create only' / 'create + resume' / 'create + execute' via one field

## Install

```bash
uvx --from dagster-community-components-cli dagster-component add snowflake_task --auto-install
```

## Minimal config

See [`example.yaml`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_task/example.yaml) for a copy-pastable starting point.

```yaml
type: dagster_component_templates.SnowflakeTaskComponent
attributes:
  asset_name: daily_orders_rollup
  task_name: DAILY_ORDERS_ROLLUP
  account: "{{ env('SNOWFLAKE_ACCOUNT') }}"
  user:    "{{ env('SNOWFLAKE_USER') }}"
  authenticator: SNOWFLAKE_JWT
  private_key_file: "{{ env('SNOWFLAKE_PRIVATE_KEY_FILE') }}"
  warehouse: COMPUTE_WH
  database: DAGSTER_DEMO
  schema_name: STAGING
  schedule: "USING CRON 0 2 * * * UTC"
  sql: |
    INSERT INTO DAGSTER_DEMO.ANALYTICS.DAILY_REVENUE
    SELECT DATE(ORDER_DATE), REGION, COUNT(*), SUM(TOTAL)
    FROM DAGSTER_DEMO.RAW.ORDERS
    WHERE DATE(ORDER_DATE) = DATEADD('day', -1, CURRENT_DATE())
    GROUP BY 1, 2
  on_materialize: execute
  group_name: snowflake_ops
```

## Schema

Every connection field (account / user / warehouse / database / schema_name / role) is required for runtime; the auth method varies:

| Auth | What to set |
|---|---|
| Password | `password: "{{ env('SNOWFLAKE_PASSWORD') }}"` |
| SSO | `authenticator: externalbrowser` (no other auth field) |
| Keypair (recommended for headless) | `authenticator: SNOWFLAKE_JWT` + `private_key_file: "{{ env('SNOWFLAKE_PRIVATE_KEY_FILE') }}"` (+ `private_key_file_pwd` if encrypted) |
| PAT | `authenticator: PROGRAMMATIC_ACCESS_TOKEN` + `token: "{{ env('SNOWFLAKE_PAT') }}"` |

See [`schema.json`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_task/schema.json) for the full attribute list.

## Privileges required on the connecting role

| Operation | Privilege |
|---|---|
| Create the entity | `CREATE TASK` on the target schema |
| Read referenced tables | `SELECT` on source tables |
| For tasks / alerts: trigger execution | `EXECUTE TASK` (or `EXECUTE ALERT`) on account |
| For dynamic tables: run refresh | `OPERATE` on the warehouse |

## See also

- [`snowflake_workspace`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_workspace) — import existing entities
- The end-to-end walkthrough: [Snowflake Workspace → Dagster](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/snowflake_workspace.md)
