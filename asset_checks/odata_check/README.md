# OData Asset Check

Smoke-test an OData service URL + entity set. Attaches as an `AssetCheckResult` to a Dagster asset; fails the check (with configurable severity) if:

- The service URL is unreachable or returns HTTP ≥ 400
- Fewer than `min_rows` (default 1) match the optional `$filter`
- More than `max_rows` match (if set)
- The first returned row is missing any of `expect_columns`

Useful for catching tenant misconfig, schema drift, or auth failures BEFORE the dependent pipeline runs.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_key` | `str` | yes | Slash-delimited asset key (e.g. `sap/business_partners`) |
| `check_name` | `str` | no | Defaults to `odata_reachable_<entity_set>` |
| `service_url` / `entity_set` / `odata_version` | `str` | yes/yes/no | Same as ingestion |
| `filter` | `str` | no | Optional `$filter` |
| `min_rows` / `max_rows` | `int` | no | Row count bounds |
| `expect_columns` | `list` | no | Required column names |
| `auth_type` and friends | | | Same shape as `odata_ingestion` |
| `severity` | `str` | no | `ERROR` (default) or `WARN` |

## See also

- [`odata_ingestion`](../../assets/ingestion/odata_ingestion/) — the ingestion side
- [`odata_resource`](../../resources/odata_resource/) — shared connection
- [`bigquery_dry_run_check`](../bigquery_dry_run_check/) — same shape, BigQuery
