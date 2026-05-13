# SAP HANA Ingestion Component

Query SAP HANA (Cloud, on-premise, or HANA-on-Azure) and materialize the result as a Dagster asset (pandas DataFrame).

This is the **ingestion** companion to [`sap_hana_resource`](../../../resources/sap_hana_resource/) — that one only registers the connection; this one runs an actual query.

## HANA SQL vs OData against S/4HANA — which to use?

There are two ways to pull SAP data into Dagster. Pick based on what your customer's security team allows AND what you need to see:

| | This component (HANA SQL) | [`odata_ingestion`](../odata_ingestion/) against S/4HANA |
|---|---|---|
| **How** | Direct HANA DB connection (port 443 / 30015) | HTTPS REST API (port 443) |
| **What you see** | Raw ABAP tables (`SAPABAP1.*`), Calculation Views (`_SYS_BIC.*`) | SAP-curated entities (BusinessPartner, SalesOrder, …) |
| **Authorization** | What the HANA user can read | SAP Communication Users — scoped per scenario |
| **Performance** | Fast — HANA columnar engine, no HTTP/pagination overhead | Slower — REST + paginated responses |
| **SQL power** | Full SQL: joins, windows, aggregations pushed to HANA | Limited: `$filter` / `$select` / `$expand` |
| **Customer governance** | Often blocked — direct DB access is sensitive | Common — it's the SAP-blessed integration path |
| **Best for** | On-prem HANA, raw analytics, Calculation Views | S/4HANA Cloud, customers with strict governance |

**Most S/4HANA Cloud deployments will be OData.** Use HANA SQL when (a) you have direct DB access, (b) you need raw table-level data the OData layer doesn't expose, or (c) you're hitting a Calculation View that's already designed for analytics.

## Why HANA gets its own component instead of `sql_to_database_asset`

`sql_to_database_asset` is source→destination DB-to-DB; this one is source→DataFrame so downstream Dagster components (`summarize`, `sort`, `dataframe_to_table`, `dataframe_to_parquet`, …) can take over. The HANA-specific URL format + TLS defaults are baked in.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_name` | `str` | yes | Dagster asset name |
| `connection_url_env_var` | `str` | no\* | Env var holding `hana://user:pwd@host:port?encrypt=true` |
| `host` / `user` / `password_env_var` | `str` | no\* | Alternative to `connection_url_env_var` |
| `port` | `int` | no | 443 (Cloud) / 30015 (on-prem default) |
| `database` | `str` | no | Tenant DB (multi-tenant only) |
| `encrypt` | `bool` | no | TLS — required for HANA Cloud (default: true) |
| `validate_certificate` | `bool` | no | Default: true |
| `query` | `str` | no\*\* | SQL with `{partition_key}` template |
| `table_name` / `schema_name` | `str` | no\*\* | Simple table read |
| `partition_type` | `str` | no | `daily` / `weekly` / `monthly` / `hourly` / `static` / `dynamic` |
| `partition_start` | `str` | no | ISO date for time-based partitions |
| Standard fields | | | `description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_*`, `deps`, `retry_policy_*` |

\* Exactly one of (`connection_url_env_var`) or (`host` + `user` + `password_env_var`) is required.
\*\* Exactly one of `query` or `table_name` is required.

## Partition templating

When `partition_type` is set, the partition key is substituted into the query via `{partition_key}`:

```yaml
query: |
  SELECT * FROM SALES_FACT WHERE BUSINESS_DATE = '{partition_key}'
partition_type: daily
partition_start: '2024-01-01'
```

For static / dynamic partitions, `{partition_key}` resolves to the partition value (string or list of strings).

## HANA Cloud vs on-prem

| | HANA Cloud | On-prem / HANA-on-Azure |
|---|---|---|
| `port` | 443 | 30015 (system DB) / 3MM15 (tenant) |
| `encrypt` | required | recommended |
| `database` | leave empty | tenant name on multi-tenant systems |

## Example — Calculation View

HANA Calculation Views show up as regular SQL views in the `_SYS_BIC` schema:

```yaml
attributes:
  asset_name: revenue_by_region
  host: myhana.hanacloud.ondemand.com
  user: DAGSTER_RO
  password_env_var: HANA_PASSWORD
  table_name: '"yourpackage.YourCV"'
  schema_name: _SYS_BIC
```

(Quoting the view name preserves case + dots.)

## Driver

This component uses `sqlalchemy-hana` (the open-source SAP-supplied SQLAlchemy dialect) on top of the official `hdbcli` Python driver. Both are PyPI packages — no SAP repository access needed.

## See also

- [`sap_hana_resource`](../../../resources/sap_hana_resource/) — connection-only sibling
- [`dataframe_to_table`](../../sinks/dataframe_to_table/) — write a DataFrame BACK to HANA
- [`sql_to_database_asset`](../sql_to_database_asset/) — direct DB-to-DB copy without a Dagster DataFrame round-trip
