# Klaviyo Resource

Registers a `KlaviyoResource` under a resource key. Holds Klaviyo auth
(private API key + `revision` date header) and exposes typed batch
operations (`upsert_profiles_bulk`) for downstream Klaviyo sinks.

Pairs with **`DataframeToKlaviyoComponent`**.

## Configuration

| Field | Required | Default | What |
|---|---|---|---|
| `resource_key` | | `klaviyo` | Dagster resource key |
| `api_key_env_var` | | `KLAVIYO_API_KEY` | Env var holding the Klaviyo private API key (starts with `pk_`) |
| `api_revision` | | `2024-10-15` | API revision date — Klaviyo pins behavior per date |
| `base_url` | | `https://a.klaviyo.com` | Klaviyo API base URL |
| `request_timeout_seconds` | | `30` | Per-request timeout |

## Example

```yaml
type: dagster_community_components.KlaviyoResourceComponent
attributes:
  resource_key: klaviyo
  api_key_env_var: KLAVIYO_API_KEY
```
