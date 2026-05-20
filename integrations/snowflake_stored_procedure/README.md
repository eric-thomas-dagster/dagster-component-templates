# Snowflake Stored Procedure

Define a Snowflake STORED PROCEDURE (SQL / Python / JavaScript / Scala) as Dagster YAML. Optional CALL on materialize.

## Why this component

Defines a Snowflake `STORED PROCEDURE` as code. Supports all four Snowflake procedure languages — SQL, Python (Snowpark), JavaScript, Scala — with the appropriate `RUNTIME_VERSION` / `PACKAGES` / `HANDLER` fields for Python/Scala.

`on_materialize: call` makes the materialization both CREATE the procedure AND CALL it with the args you specify — turning the procedure into a fully-managed Dagster asset.

### Complementary to `snowflake_workspace`

| Component | Role |
|---|---|
| [`snowflake_workspace`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_workspace/README.md) | **Discovers** existing Snowflake entities in your account → imports them as Dagster assets |
| **`snowflake_stored_procedure`** (this) | **Defines** a new entity in Dagster YAML → Dagster issues CREATE OR REPLACE on materialize |

Use whichever side matches the source of truth for each entity. Mix freely — both styles produce ordinary Dagster assets and can declare deps on each other.

## Typical use cases

- Define Snowpark Python UDPs / stored procs as code
- Materialization = create + call (for stateful procs like 'recompute tiers')
- Compose with snowflake_task to build proc-driven DAGs

## Install

```bash
uvx --from dagster-community-components-cli dagster-component add snowflake_stored_procedure --auto-install
```

## Minimal config

See [`example.yaml`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_stored_procedure/example.yaml) for a copy-pastable starting point.

```yaml
type: dagster_component_templates.SnowflakeStoredProcedureComponent
attributes:
  asset_name: sp_recompute_tiers
  procedure_name: SP_RECOMPUTE_TIERS
  account: "{{ env('SNOWFLAKE_ACCOUNT') }}"
  user:    "{{ env('SNOWFLAKE_USER') }}"
  authenticator: SNOWFLAKE_JWT
  private_key_file: "{{ env('SNOWFLAKE_PRIVATE_KEY_FILE') }}"
  warehouse: COMPUTE_WH
  database: DAGSTER_DEMO
  schema_name: STAGING
  language: SQL
  returns: VARCHAR
  body: |
    $$
    BEGIN
      UPDATE DAGSTER_DEMO.RAW.CUSTOMERS
      SET TIER = CASE
        WHEN LIFETIME_VALUE > 5000 THEN 'platinum'
        WHEN LIFETIME_VALUE > 2000 THEN 'gold'
        ELSE 'bronze'
      END;
      RETURN 'Tiers recomputed';
    END;
    $$
  on_materialize: call
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

See [`schema.json`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_stored_procedure/schema.json) for the full attribute list.

## Privileges required on the connecting role

| Operation | Privilege |
|---|---|
| Create the entity | `CREATE PROCEDURE` on the target schema |
| Read referenced tables | `SELECT` on source tables |
| For tasks / alerts: trigger execution | `EXECUTE TASK` (or `EXECUTE ALERT`) on account |
| For dynamic tables: run refresh | `OPERATE` on the warehouse |

## See also

- [`snowflake_workspace`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_workspace) — import existing entities
- The end-to-end walkthrough: [Snowflake Workspace → Dagster](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/snowflake_workspace.md)
