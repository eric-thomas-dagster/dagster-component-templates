# Snowflake Materialized View

Define a Snowflake MATERIALIZED VIEW as Dagster YAML. Materialization runs CREATE OR REPLACE MATERIALIZED VIEW.

## Why this component

Defines a Snowflake `MATERIALIZED VIEW` as code. Materialized views are pre-computed views that Snowflake auto-refreshes when the base table changes; they trade a small write-time cost for fast aggregation reads.

**Requires Snowflake Enterprise+ edition.** Standard edition will reject `CREATE OR REPLACE MATERIALIZED VIEW` with 'Unsupported feature'.

### Complementary to `snowflake_workspace`

| Component | Role |
|---|---|
| [`snowflake_workspace`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_workspace/README.md) | **Discovers** existing Snowflake entities in your account → imports them as Dagster assets |
| **`snowflake_materialized_view`** (this) | **Defines** a new entity in Dagster YAML → Dagster issues CREATE OR REPLACE on materialize |

Use whichever side matches the source of truth for each entity. Mix freely — both styles produce ordinary Dagster assets and can declare deps on each other.

## Typical use cases

- Declare frequently-queried aggregations as code (Enterprise+ accounts only)
- Pair with snowflake_workspace to mix imported + defined MVs
- Optional CLUSTER BY for partition pruning

## Install

```bash
uvx --from dagster-community-components-cli dagster-component add snowflake_materialized_view --auto-install
```

## Minimal config

See [`example.yaml`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_materialized_view/example.yaml) for a copy-pastable starting point.

```yaml
type: dagster_component_templates.SnowflakeMaterializedViewComponent
attributes:
  asset_name: customer_ltv_mv
  mv_name: CUSTOMER_LIFETIME_VALUE_MV
  account: "{{ env('SNOWFLAKE_ACCOUNT') }}"
  user:    "{{ env('SNOWFLAKE_USER') }}"
  authenticator: SNOWFLAKE_JWT
  private_key_file: "{{ env('SNOWFLAKE_PRIVATE_KEY_FILE') }}"
  warehouse: COMPUTE_WH
  database: DAGSTER_DEMO
  schema_name: STAGING
  sql: |
    SELECT CUSTOMER_ID, SUM(TOTAL) AS LIFETIME_REVENUE
    FROM DAGSTER_DEMO.RAW.ORDERS
    WHERE STATUS IN ('paid', 'delivered')
    GROUP BY CUSTOMER_ID
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

See [`schema.json`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_materialized_view/schema.json) for the full attribute list.

## Privileges required on the connecting role

| Operation | Privilege |
|---|---|
| Create the entity | `CREATE MATERIALIZED VIEW` on the target schema |
| Read referenced tables | `SELECT` on source tables |
| For tasks / alerts: trigger execution | `EXECUTE TASK` (or `EXECUTE ALERT`) on account |
| For dynamic tables: run refresh | `OPERATE` on the warehouse |

## See also

- [`snowflake_workspace`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_workspace) — import existing entities
- The end-to-end walkthrough: [Snowflake Workspace → Dagster](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/snowflake_workspace.md)
