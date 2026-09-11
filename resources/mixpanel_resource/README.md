# Mixpanel Resource

Registers a `MixpanelResource` under a resource key. Holds Mixpanel
auth (service-account username+secret for `/import`, project token for
`/engage`) and exposes typed batch operations
(`import_events_bulk`, `set_profiles_bulk`) for downstream Mixpanel
sinks.

Pairs with **`DataframeToMixpanelComponent`**.

## Why two auth mechanisms

Mixpanel's `/import` endpoint requires a **service account**
(username + secret via HTTP Basic) so it can enforce per-project
quotas and dedupe reliably. Profile ops via `/engage` use the
**project token** in the request body — no SA required.

## Configuration

| Field | Required | Default | What |
|---|---|---|---|
| `resource_key` | | `mixpanel` | Dagster resource key |
| `project_id` | ✔ | — | Mixpanel project ID (required by `/import`) |
| `project_token_env_var` | | `MIXPANEL_PROJECT_TOKEN` | Env var holding the project token (used for `/engage`) |
| `service_account_username_env_var` | | `MIXPANEL_SERVICE_ACCOUNT_USERNAME` | Env var holding the SA username (used for `/import`) |
| `service_account_secret_env_var` | | `MIXPANEL_SERVICE_ACCOUNT_SECRET` | Env var holding the SA secret (used for `/import`) |
| `base_url` | | `https://api.mixpanel.com` | Use `https://api-eu.mixpanel.com` for EU residency projects |
| `request_timeout_seconds` | | `30` | Per-request timeout |

## Example

```yaml
type: dagster_community_components.MixpanelResourceComponent
attributes:
  resource_key: mixpanel
  project_id: "123456"
  project_token_env_var: MIXPANEL_PROJECT_TOKEN
  service_account_username_env_var: MIXPANEL_SERVICE_ACCOUNT_USERNAME
  service_account_secret_env_var: MIXPANEL_SERVICE_ACCOUNT_SECRET
```
