# `GreenhouseResourceComponent`

Registers a `GreenhouseResource` (Harvest API key auth) for other components to use via `resource_key`.

Beyond the raw `.get_client()` escape hatch (an authenticated `requests.Session`), this resource provides `.update_candidate(candidate_id, tags=..., custom_fields=...)` — `PATCH /v1/candidates/{id}`.

Greenhouse's Harvest API requires an `On-Behalf-Of` header on every write (POST/PATCH/PUT), naming a real Greenhouse user ID whose permissions are checked server-side — this resource requires `on_behalf_of_user_id` explicitly rather than guessing a default.

## Pairs with

- **`greenhouse_candidate_update`** — reverse-ETL sink built on top of this resource.
- **`greenhouse_harvest_ingestion`** — the READ-side counterpart (dlt-based bulk pull; does not use this resource).

## Configuration

| Field | Required | Description |
|---|---|---|
| `resource_key` | optional (default `greenhouse`) | Key used to register this resource. |
| `api_key_env_var` | optional (default `GREENHOUSE_API_KEY`) | Env var holding a Greenhouse Harvest API key. |
| `on_behalf_of_user_id` | required | Greenhouse user ID to attribute write operations to. |

## Example
```yaml
type: dagster_component_templates.GreenhouseResourceComponent
attributes:
  resource_key: greenhouse
  api_key_env_var: GREENHOUSE_API_KEY
  on_behalf_of_user_id: "4223"
```
