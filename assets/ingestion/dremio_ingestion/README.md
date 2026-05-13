# Dremio Ingestion Component

Query a Dremio cluster (OSS or Cloud) via the REST or Arrow Flight API and materialize the result as a Dagster asset (pandas DataFrame).

Dremio is SAP's lakehouse query engine (acquired 2024-25). It federates queries across Iceberg tables, Delta Lake, Snowflake, Postgres, S3, ADLS, etc. — anywhere SQL can land. This component runs a SQL query and returns the result; downstream Dagster components transform / persist as usual.

## Why a dedicated component vs `rest_api_fetcher`

Dremio's REST API requires a 3-step dance (submit → poll → page-results), with PAT vs legacy-password auth. The component handles all of that. The only thing you provide is `host` + auth + SQL.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_name` | `str` | yes | Dagster asset name |
| `host` | `str` | yes | Dremio coordinator URL incl. scheme (e.g. `http://localhost:9047`) |
| `query` | `str` | yes | SQL with `{partition_key}` template support |
| `auth_type` | `str` | no | `pat` (default — Dremio 24.0+) or `password` (legacy) |
| `auth_token_env_var` | `str` | conditional | For `pat` |
| `auth_username_env_var` / `auth_password_env_var` | `str` | conditional | For `password` |
| `transport` | `str` | no | `rest` (default) or `flight` (Apache Arrow Flight SQL — needs pyarrow) |
| `flight_port` | `int` | no | Default 32010 |
| `page_size` | `int` | no | Default 500 (Dremio REST max) |
| `poll_interval_seconds` | `float` | no | Default 1.0 |
| `poll_timeout_seconds` | `int` | no | Default 600 |
| `verify_ssl` | `bool` | no | Default true; false for self-signed |
| Standard fields | | | partition / freshness / owners / tags / deps / retry |

## REST vs Flight

| | REST | Flight |
|---|---|---|
| Performance | Slower (JSON over HTTPS, paginated 500 rows at a time) | Native columnar Arrow over gRPC — typically 5-50× faster |
| Deps | `requests` (already installed) | `pyarrow>=10` (~50MB) |
| Port | Same as web UI (9047 default) | Separate Flight port (32010 default) |
| When to use | Default. Fine up to ~100k rows | Bigger result sets, when you control the cluster |

## PAT (Personal Access Token)

Dremio 24.0+ supports PATs as the recommended auth method:

1. Web UI → username → **Account Settings** → **Personal Access Tokens** → **Create**
2. Set lifetime + scope, copy the token
3. `export DREMIO_PAT=<token>`
4. In `example.yaml`: `auth_token_env_var: DREMIO_PAT`

## Legacy password auth

Pre-24.0 OSS or environments without PAT support:

```yaml
auth_type: password
auth_username_env_var: DREMIO_USER
auth_password_env_var: DREMIO_PASSWORD
```

The component calls `/apiv2/login` to mint a session token, then uses `Authorization: _dremio<token>` per Dremio's REST convention.

## Running Dremio locally

The official OSS image starts in seconds:

```bash
docker run -p 9047:9047 -p 31010:31010 -p 32010:32010 \
  -v dremio_data:/opt/dremio/data \
  dremio/dremio-oss:latest
```

First-time setup: open http://localhost:9047, create the admin user, mint a PAT.

## Source name quoting

Dremio names spaces, folders, sources with `@-` and `.` characters that aren't valid SQL identifiers. Quote them:

```sql
SELECT * FROM "@my-space"."my-folder"."my_table"
SELECT * FROM "Samples"."samples.dremio.com"."SF_incidents2016.json"
```

## See also

- [`sap_hana_ingestion`](../sap_hana_ingestion/) — SAP HANA SQL
- [`odata_ingestion`](../odata_ingestion/) — SAP cloud APIs over OData
- Dremio walkthrough in `dagster-community-components-cli/examples/dremio_pipeline.md`
