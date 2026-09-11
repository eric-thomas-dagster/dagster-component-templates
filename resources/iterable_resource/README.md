# Iterable Resource

Registers an `IterableResource` under a resource key. Holds Iterable
auth (`Api-Key` header) and exposes typed batch operations
(`bulk_update_users`, `bulk_track_events`) for downstream Iterable
sinks.

Pairs with **`DataframeToIterableComponent`**.

## Configuration

| Field | Required | Default | What |
|---|---|---|---|
| `resource_key` | | `iterable` | Dagster resource key |
| `api_key_env_var` | | `ITERABLE_API_KEY` | Env var holding the Iterable API key |
| `base_url` | | `https://api.iterable.com` | Iterable base URL. Use `https://api.eu.iterable.com` for EU workspaces |
| `request_timeout_seconds` | | `30` | Per-request timeout |

## Example

```yaml
type: dagster_community_components.IterableResourceComponent
attributes:
  resource_key: iterable
  api_key_env_var: ITERABLE_API_KEY
```
