# DagsterAuditToWarehouseJobComponent

Op-shaped job that pulls Dagster+ audit log entries (who did what, when)
via the Dagster+ GraphQL API and MERGEs them into a warehouse table for
compliance / SOC2 / retention queries.

**Dagster+ only.** For OSS deployments, audit-log equivalent doesn't
exist at the framework level — use application-level audit logging
via your identity provider (Okta / Azure AD) instead.

## Supported warehouses

Toggle via `warehouse`:
- `snowflake` — via `snowflake-connector-python`
- `bigquery` — via `google-cloud-bigquery`
- `postgres` — via `psycopg2`

## Target table DDL

```sql
CREATE TABLE dagster_audit_log (
  timestamp DOUBLE,
  timestamp_ts TIMESTAMP,
  user_email STRING,
  event_type STRING,
  target_type STRING,
  target_identifier STRING,
  raw_json STRING
);
```

## YAML example

```yaml
type: dagster_component_templates.DagsterAuditToWarehouseJobComponent
attributes:
  job_name: dagster_audit_log_to_snowflake
  schedule: "0 * * * *"                # hourly
  default_status: RUNNING
  dagster_plus_url: https://acme.dagster.cloud/prod
  dagster_plus_token_env: DAGSTER_CLOUD_API_TOKEN
  warehouse: snowflake
  snowflake_account: abc12345.us-east-1
  snowflake_user: DAGSTER_INGEST
  snowflake_password_env: SNOWFLAKE_PASSWORD
  snowflake_warehouse: LOAD_WH
  snowflake_database: SECURITY
  snowflake_schema: DAGSTER_AUDIT
  table: DAGSTER_AUDIT_LOG
```

## Required env vars

```bash
DAGSTER_CLOUD_API_TOKEN=<user-token>   # from Dagster+ → Cloud Settings → Tokens
SNOWFLAKE_PASSWORD=...                 # or your chosen warehouse's creds
```

## Behavior

- Cursor: last swept timestamp, stored as a run tag.
- Queries Dagster+'s `auditLogs` GraphQL endpoint with `sinceTimestamp` filter.
- Inserts new entries into the target table.
- Not a proper MERGE — Dagster+ audit entries are append-only immutable, so plain INSERT is fine.

## Related

- Sibling component: `dagster_plus_to_siem_job` — sends the same data to Splunk/Sentinel/Datadog/Sumo/S3 (SIEM-focused). Use one or the other based on where compliance queries live.
