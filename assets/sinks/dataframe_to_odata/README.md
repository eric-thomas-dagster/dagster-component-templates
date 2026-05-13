# Dataframe → OData sink

Write rows from a pandas DataFrame to an OData v2/v4 entity set via POST / PATCH / DELETE. Reverse direction of [`odata_ingestion`](../../ingestion/odata_ingestion/).

## When to use

- Push new records to S/4HANA, SuccessFactors, Dynamics 365, Datasphere, Microsoft Graph, etc.
- Update existing records via PATCH (mode=upsert).
- Soft-delete or remove records via DELETE.

## Modes

| Mode | HTTP | What it does | Requires |
|---|---|---|---|
| `insert` (default) | `POST <service>/<entity_set>` | Creates a new entity per row | — |
| `upsert` | `PATCH <service>/<entity_set>(<key>)` | Updates the entity identified by `key_column` | `key_column` |
| `delete` | `DELETE <service>/<entity_set>(<key>)` | Removes the entity identified by `key_column` | `key_column` |

## SAP CSRF

S/4HANA write APIs require a CSRF token. Set `csrf_fetch_path: $metadata` and the component will:

1. `GET <service_url>/$metadata` with `x-csrf-token: fetch`
2. Capture the returned `x-csrf-token` header
3. Include it on every subsequent write

Non-SAP OData servers (Dynamics, MS Graph) usually don't need CSRF — leave it unset.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `asset_name` | `str` | yes | |
| `upstream_asset_key` | `str` | yes | Must produce a `pandas.DataFrame` |
| `service_url` | `str` | yes | Base URL |
| `entity_set` | `str` | yes | Entity collection |
| `mode` | `str` | no | `insert` (default) / `upsert` / `delete` |
| `key_column` | `str` | conditional | Required for `upsert` and `delete` |
| `csrf_fetch_path` | `str` | no | Path to fetch CSRF token from — SAP write APIs need this |
| `auth_type` | `str` | no | `basic` / `bearer` / `none` |
| `auth_username_env_var` / `auth_password_env_var` / `auth_token_env_var` | `str` | conditional | |
| `extra_headers` | `dict` | no | sap-client, sap-language, etc. |
| `skip_columns` | `list` | no | Columns to drop before serializing |
| `rate_limit_max_retries` | `int` | no | Default 5. Honors 429 `Retry-After` |
| `request_timeout_seconds` | `int` | no | Default 120 |

## Failure handling

The component logs each failure with its HTTP status + response body, and aborts after **10+ failures**. After the run, it raises a `RuntimeError` if `rows_failed > 0` — the asset materialization fails so Dagster's retry / alerting can kick in. A sample of failures is attached to the asset metadata.

For lenient flows where some failures are OK, set a Dagster `RetryPolicy` via `retry_policy_max_retries` and handle the partial-success in a downstream alert.

## Batch mode (`$batch`)

OData defines a `$batch` protocol that bundles many writes into one HTTP request. Set `batch_mode: true` + `batch_size: <n>` to use it. Caveats:

- Not all OData servers support `$batch`. SAP S/4HANA and SuccessFactors do; some Microsoft endpoints don't.
- Per-row failures inside a batch are reported in the multipart response — the component currently treats batch responses as opaque (TBD: granular per-row reporting).

For v1, prefer per-row writes unless you're sure your backend supports `$batch` and you've benchmarked it.

## OAuth-token writes (Concur / Ariba / Datasphere)

For headless OAuth flows, use [`oauth_token_resource`](../../../resources/oauth_token_resource/) to mint access tokens and feed them in via `auth_token_env_var` (or, in the future, a `resource_key` field on this component).

## See also

- [`odata_ingestion`](../../ingestion/odata_ingestion/) — read side
- [`odata_resource`](../../../resources/odata_resource/) — shared connection
- [`odata_check`](../../../asset_checks/odata_check/) — health check
