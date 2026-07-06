# Vanta Resource

Register a `VantaResource` wrapping the Vanta API v1 for use by other components. Handles OAuth2 client-credentials token minting and in-memory caching + auto-refresh.

## Installation

`pip install requests>=2.28`

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"vanta_resource"` | Key used to register this resource. Other components reference it via `resource_name` / `resource_key`. |
| `client_id_env_var` | `str` | `"VANTA_CLIENT_ID"` | Env var holding Vanta OAuth client_id |
| `client_secret_env_var` | `str` | `"VANTA_CLIENT_SECRET"` | Env var holding Vanta OAuth client_secret |
| `api_base_url` | `str` | `"https://api.vanta.com"` | Vanta API base URL (override for staging / regional tenants) |

## Configuration

```yaml
type: dagster_community_components.VantaResourceComponent
attributes:
  resource_key: vanta_resource
  client_id_env_var: VANTA_CLIENT_ID
  client_secret_env_var: VANTA_CLIENT_SECRET
```

## Authentication

Vanta uses OAuth2 client-credentials flow. Provision an API application in your Vanta workspace and grab the generated client_id and client_secret:

- Vanta docs: https://developer.vanta.com/docs/authentication
- Console: https://app.vanta.com/settings/api-tokens

Then export the credentials in your Dagster environment before running:

```bash
export VANTA_CLIENT_ID="vco_..."
export VANTA_CLIENT_SECRET="vcs_..."
```

## Token caching

The resource mints a bearer token on first use and caches it in memory until `expires_in - early_refresh_seconds`. A single Dagster run typically uses one token; long-lived deployments transparently refresh.
