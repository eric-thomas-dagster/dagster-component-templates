# ServiceNowResourceComponent

Shared ServiceNow Table API connection. Centralizes the instance subdomain + auth so that `servicenow_ingestion` (reading), `servicenow_sensor` (event-driven trigger), and a future `dataframe_to_servicenow` (write-back) can all reference the same resource.

## Auth modes

| Mode | Use when | Field |
|---|---|---|
| **Basic** (username + password) | Dev instances, PDIs (Personal Developer Instances), local sandboxes | `username_env_var` + `password_env_var` |
| **Bearer token** (OAuth) | Production. Acquired via OAuth — typically `client_credentials` (headless) or `refresh_token` (delegated) | `bearer_token_env_var` |

For production OAuth, pair this resource with the community `oauth_token_resource` component — it handles client_credentials acquisition, refresh-token rotation, and writeback to AWS Secrets Manager / Azure Key Vault / Vault.

## Dependencies

- `requests>=2.28.0`

## Configuration

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `resource_key` | `str` | No | `servicenow_resource` | Resource registration key |
| `instance_env_var` | `str` | Yes | — | Env var with the instance subdomain (just `mycompany`, not `https://mycompany.service-now.com`) |
| `username_env_var` | `str` | Conditional | — | Required unless `bearer_token_env_var` is set |
| `password_env_var` | `str` | Conditional | — | Required unless `bearer_token_env_var` is set |
| `bearer_token_env_var` | `str` | Conditional | — | OAuth bearer token. When set, basic-auth fields are ignored |
| `verify_ssl` | `bool` | No | `true` | Disable only for self-signed dev instances |

## What it provides

```python
ctx.resources.servicenow_resource.base_url             # https://<instance>.service-now.com
ctx.resources.servicenow_resource.table_url("incident") # https://.../api/now/table/incident
ctx.resources.servicenow_resource.get_auth_headers()   # dict (Bearer header if OAuth)
ctx.resources.servicenow_resource.get_auth()           # (user, pass) tuple, or None
```

The shape mirrors `requests` — pass `headers=...` + `auth=...` directly.

## Setting up a ServiceNow Developer Instance (dev)

ServiceNow offers free Personal Developer Instances (PDIs):

1. Sign up at <https://developer.servicenow.com>
2. Request an instance (free, auto-named like `dev123456`)
3. Default `admin` user is provisioned with a password ServiceNow shows you
4. Export to env:
   ```bash
   export SNOW_INSTANCE=dev123456
   export SNOW_USERNAME=admin
   export SNOW_PASSWORD=...
   ```
5. Test:
   ```bash
   curl -u admin:... "https://dev123456.service-now.com/api/now/table/incident?sysparm_limit=1"
   ```

## Setting up OAuth (production)

In ServiceNow:

1. **System OAuth → Application Registry → New → "Create an OAuth API endpoint for external clients"**
2. Copy the Client ID + Client Secret
3. Use `client_credentials` grant: `POST https://<instance>.service-now.com/oauth_token.do` with `grant_type=client_credentials&client_id=...&client_secret=...`
4. Returns `access_token` (typically 30-min TTL) — pass via `bearer_token_env_var`

The `oauth_token_resource` community component automates this.

## See also

- `servicenow_ingestion` — read any ServiceNow table → DataFrame asset
- `servicenow_sensor` — trigger a Dagster job when records match a `sysparm_query`
- `oauth_token_resource` — acquire + rotate bearer tokens
- [Schema](schema.json) · [Example](example.yaml)
