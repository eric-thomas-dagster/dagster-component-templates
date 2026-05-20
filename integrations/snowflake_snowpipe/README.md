# Snowflake Snowpipe

Define a Snowflake PIPE (Snowpipe) as Dagster YAML. Materialization runs CREATE OR REPLACE PIPE and (optionally) ALTER PIPE … REFRESH.

## Why this component

Defines a Snowflake `PIPE` (Snowpipe) as code. Snowpipes ingest files from a stage into a target table via a `COPY INTO` statement.

Supports both `AUTO_INGEST = TRUE` (where Snowflake reacts to cloud-storage events — SNS / Event Grid / GCS notifications) and `AUTO_INGEST = FALSE` (manual `ALTER PIPE ... REFRESH`). `on_materialize: refresh` triggers the manual variant immediately on materialize, useful for one-shot batch ingest.

### Complementary to `snowflake_workspace`

| Component | Role |
|---|---|
| [`snowflake_workspace`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_workspace/README.md) | **Discovers** existing Snowflake entities in your account → imports them as Dagster assets |
| **`snowflake_snowpipe`** (this) | **Defines** a new entity in Dagster YAML → Dagster issues CREATE OR REPLACE on materialize |

Use whichever side matches the source of truth for each entity. Mix freely — both styles produce ordinary Dagster assets and can declare deps on each other.

## Typical use cases

- Declare ingestion pipes as code, version with the rest of your data project
- Materialize = create + refresh — ideal for backfill / one-shot pipes
- Wire pipe completion to downstream tasks via Dagster's asset graph

## Install

```bash
uvx --from dagster-community-components-cli dagster-component add snowflake_snowpipe --auto-install
```

## Minimal config

See [`example.yaml`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_snowpipe/example.yaml) for a copy-pastable starting point.

```yaml
type: dagster_component_templates.SnowflakeSnowpipeComponent
attributes:
  asset_name: orders_pipe
  pipe_name: ORDERS_PIPE
  account: "{{ env('SNOWFLAKE_ACCOUNT') }}"
  user:    "{{ env('SNOWFLAKE_USER') }}"
  authenticator: SNOWFLAKE_JWT
  private_key_file: "{{ env('SNOWFLAKE_PRIVATE_KEY_FILE') }}"
  warehouse: COMPUTE_WH
  database: DAGSTER_DEMO
  schema_name: STAGING
  auto_ingest: false
  copy_statement: |
    COPY INTO DAGSTER_DEMO.STAGING.ORDERS_INGESTED
    FROM @DAGSTER_DEMO.STAGING.INTERNAL_STAGE
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE'
  on_materialize: refresh
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

See [`schema.json`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_snowpipe/schema.json) for the full attribute list.

## Privileges required on the connecting role

| Operation | Privilege |
|---|---|
| Create the entity | `CREATE PIPE` on the target schema |
| Read referenced tables | `SELECT` on source tables |
| For tasks / alerts: trigger execution | `EXECUTE TASK` (or `EXECUTE ALERT`) on account |
| For dynamic tables: run refresh | `OPERATE` on the warehouse |

## See also

- [`snowflake_workspace`](https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/integrations/snowflake_workspace) — import existing entities
- The end-to-end walkthrough: [Snowflake Workspace → Dagster](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/snowflake_workspace.md)
