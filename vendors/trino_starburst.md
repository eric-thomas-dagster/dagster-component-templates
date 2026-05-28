# Trino + Starburst

[Trino](https://trino.io/) is the open-source distributed SQL query engine (formerly PrestoSQL, renamed in 2021). **[Starburst](https://www.starburst.io/)** is the commercial distribution — same Java codebase, same wire protocol, same SQL dialect, with added enterprise features (Galaxy SaaS, Stargate cross-cluster joins, enhanced RBAC, enterprise connectors).

For Dagster integration purposes, **the two are interchangeable**. This page covers both; component implementations are shared (Starburst components subclass the Trino ones).

## Components

### Trino

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`trino_resource`](https://dagster-component-ui.vercel.app/c/trino_resource) | resource | Trino coordinator connection via `trino-python-client`. Default port 8080. | `code` |
| [`trino_io_manager`](https://dagster-component-ui.vercel.app/c/trino_io_manager) | io_manager | Pandas asset persistence in Trino-managed catalogs (Iceberg / Hive / Delta). | `code` |

### Starburst (alias)

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`starburst_resource`](https://dagster-component-ui.vercel.app/c/starburst_resource) | resource | **Subclass of `trino_resource`.** Defaults `port` to 443 (Starburst Galaxy convention) + `resource_key` to `starburst_resource`. Same field surface otherwise. | `code` |

The Starburst resource is a pure subclass — no duplicated logic. Customers running Starburst can `dagster-component add starburst_resource` for discoverability + vendor grouping without needing to know it's Trino under the hood.

## Configuration

Same fields for both:

```yaml
# Trino (on-prem / OSS install)
type: dagster_community_components.TrinoResourceComponent
attributes:
  resource_key: trino_resource
  host: trino.mycompany.com
  port: 8080
  user: dagster
  catalog: iceberg
  schema_name: warehouse
  password_env_var: TRINO_PASSWORD
```

```yaml
# Starburst Galaxy (SaaS)
type: dagster_community_components.StarburstResourceComponent
attributes:
  resource_key: starburst_resource
  host: my-cluster.galaxy.starburst.io
  port: 443
  user: my-user@example.com
  catalog: tpch
  schema_name: sf1
  password_env_var: STARBURST_PASSWORD
```

## Connection / auth — quick reference

| Aspect | Trino | Starburst |
|---|---|---|
| Default port | 8080 (HTTP) / 8443 (HTTPS) | 443 (Galaxy SaaS); 8080/8443 (Starburst Enterprise on-prem) |
| Auth | None (dev) / BasicAuth (prod) / JWT | BasicAuth / OAuth2 / JWT |
| Wire protocol | HTTP REST + JDBC | identical |
| Python client | `trino-python-client` | identical |
| SQL dialect | Trino SQL | identical |

## Trino as a federation layer — Dagster patterns

Trino's superpower is **federated query** — one SQL statement spanning multiple catalogs (Postgres + S3 Iceberg + Snowflake + Mongo + …). Useful Dagster patterns:

1. **Trino as the read-side of a cross-warehouse Pandas pipeline:**
   ```yaml
   # sql_transform reads from a Trino-federated query, writes back to S3 Iceberg
   type: dagster_community_components.SqlTransformComponent
   attributes:
     query: |
       SELECT
         pg.customers.id,
         snowflake.warehouse.orders.total,
         iceberg.lake.events.click_count
       FROM postgres.public.customers AS pg.customers
         LEFT JOIN snowflake.warehouse.orders ...
       USING (customer_id)
     connection_env_var: TRINO_URL
   ```
2. **Iceberg table writes via Trino instead of pyiceberg** — when you want server-side commit semantics + Trino-managed manifest files.

## Roadmap

- **`trino_query_asset`** — explicit single-query → DataFrame asset, mirroring `bigquery_query_asset` shape. Today: `sql_transform` against the resource's URL.
- **`starburst_io_manager` alias** — analog to `starburst_resource`. Same subclassing pattern.
- **External-catalog auto-discovery** — workspace-style import of every catalog/schema/table visible to the connected user.

## See also

- [Trino docs](https://trino.io/docs/) / [Starburst docs](https://docs.starburst.io/)
- [`vendors/snowflake.md`](snowflake.md) — common federation target
- [`vendors/databricks.md`](databricks.md) — Delta Lake catalog often federated via Trino
- [`vendors/aws.md`](aws.md) — Iceberg-on-S3 + Athena combos paired with Trino
