# Snowflake Alert

Define a Snowflake ALERT (scheduled conditional action) as Dagster YAML. Materialization runs CREATE OR REPLACE ALERT and RESUMEs it.

## Why this component

Defines a Snowflake `ALERT` — a scheduled conditional action that fires when a SELECT condition returns rows. Alerts pair a CRON / interval schedule with an `IF (EXISTS (...)) THEN <action>` body that runs as the schedule ticks.

`on_materialize: resume` creates the alert AND runs `ALTER ALERT ... RESUME` so it starts firing on its own cadence.

### Complementary to `snowflake_workspace`

| Component | Role |
|---|---|
| [`snowflake_workspace`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_workspace/README.md) | **Discovers** existing Snowflake entities in your account → imports them as Dagster assets |
| **`snowflake_alert`** (this) | **Defines** a new entity in Dagster YAML → Dagster issues CREATE OR REPLACE on materialize |

Use whichever side matches the source of truth for each entity. Mix freely — both styles produce ordinary Dagster assets and can declare deps on each other.

## Typical use cases

- Declare data-quality + monitoring alerts as code in YAML
- Condition any SELECT — drift, anomaly, freshness, threshold, etc.
- Action can be INSERT INTO log, CALL stored proc, etc.

## Install

```bash
uvx --from dagster-community-components-cli dagster-component add snowflake_alert --auto-install
```

## Minimal config

See [`example.yaml`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_alert/example.yaml) for a copy-pastable starting point.

```yaml
type: dagster_component_templates.SnowflakeAlertComponent
attributes:
  asset_name: high_revenue_alert
  alert_name: HIGH_REVENUE_DAY_ALERT
  account: "{{ env('SNOWFLAKE_ACCOUNT') }}"
  user:    "{{ env('SNOWFLAKE_USER') }}"
  authenticator: SNOWFLAKE_JWT
  private_key_file: "{{ env('SNOWFLAKE_PRIVATE_KEY_FILE') }}"
  warehouse: COMPUTE_WH
  database: DAGSTER_DEMO
  schema_name: STAGING
  schedule: "60 minute"
  condition_sql: |
    SELECT 1 FROM DAGSTER_DEMO.ANALYTICS.DAILY_REVENUE
    WHERE REVENUE_DATE = DATEADD('day', -1, CURRENT_DATE()) AND REVENUE > 50000
  action_sql: |
    INSERT INTO DAGSTER_DEMO.ANALYTICS.ALERT_LOG
    VALUES (CURRENT_TIMESTAMP(), 'high_revenue_day', 'Yesterday exceeded $50k')
  on_materialize: resume
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

See [`schema.json`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_alert/schema.json) for the full attribute list.

## Privileges required on the connecting role

| Operation | Privilege |
|---|---|
| Create the entity | `CREATE ALERT` on the target schema |
| Read referenced tables | `SELECT` on source tables |
| For tasks / alerts: trigger execution | `EXECUTE TASK` (or `EXECUTE ALERT`) on account |
| For dynamic tables: run refresh | `OPERATE` on the warehouse |

## See also

- [`snowflake_workspace`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_workspace) — import existing entities
- The end-to-end walkthrough: [Snowflake Workspace → Dagster](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/snowflake_workspace.md)
