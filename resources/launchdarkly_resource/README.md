# `LaunchDarklyResourceComponent`

Registers a `LaunchDarklyResource` (API token auth) for other components to use via `resource_key`.

Beyond the raw `.get_client()` escape hatch (an authenticated `requests.Session`), this resource provides `.add_segment_targets(project_key, env_key, segment_key, add_keys=..., remove_keys=..., context_kind=...)` -- a LaunchDarkly "semantic patch" (`PATCH /api/v2/segments/{proj}/{env}/{segment}` with a special `domain-model=launchdarkly.semanticpatch` content type) that adds/removes context keys from a segment's target list.

## Pairs with

- **`launchdarkly_segment_update`** — reverse-ETL sink built on top of this resource.
- **`launchdarkly_ingestion`** — the READ-side counterpart (dlt-based bulk pull; does not use this resource).

## Configuration

| Field | Required | Description |
|---|---|---|
| `resource_key` | optional (default `launchdarkly`) | Key used to register this resource. |
| `api_token_env_var` | optional (default `LAUNCHDARKLY_API_TOKEN`) | Env var holding a LaunchDarkly API access token. |

## Example
```yaml
type: dagster_component_templates.LaunchDarklyResourceComponent
attributes:
  resource_key: launchdarkly
  api_token_env_var: LAUNCHDARKLY_API_TOKEN
```
