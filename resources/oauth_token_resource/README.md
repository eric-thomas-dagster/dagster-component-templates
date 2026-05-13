# OAuth Token Resource Component

Headless OAuth 2.0 token manager for Dagster. Mints + caches access tokens for M2M workflows. **Zero interactive prompts** — all credentials come from env vars.

## When to use

| Vendor | Grant type | Notes |
|---|---|---|
| **Ariba** | `client_credentials` | Easiest. `client_id` + `client_secret` → access token. |
| **Microsoft Graph** (app permissions) | `client_credentials` | Azure AD tenant + app registration |
| Most SAP Cloud APIs | `client_credentials` | OAuth2 client credentials via SAP BTP UAA |
| **Concur** | `refresh_token` | Refresh tokens **rotate** — use the writeback hook |
| Salesforce | `refresh_token` (or `jwt_bearer`) | Refresh tokens rotate on some orgs |
| Gmail / Google APIs | `refresh_token` | Stable refresh tokens (no rotation) |
| Legacy SAP S/4 on-prem | `password` | Resource Owner Password — deprecated but exists |

## Three grant flows

### `client_credentials` — easiest headless
```
client_id + client_secret → POST /token → access_token
```
No user identity involved. Token is short-lived (~1hr) and auto-refreshed by the resource as needed.

### `refresh_token` — annoying but workable
```
[once on laptop] user does OAuth dance → refresh_token
[then forever]   refresh_token → POST /token → access_token + maybe new refresh_token
```

**The rotation problem**: many providers return a NEW refresh_token on each call and INVALIDATE the old one. If you don't persist the new one back to your secret store, the next run breaks.

This resource handles writeback via either:
- `refresh_writeback_file`: write the new token to a file. Useful for k8s with a PVC.
- `refresh_writeback_command_env_var`: shell-out to a user-supplied command with `{token}` substituted. The user wires this to AWS Secrets Manager / Azure Key Vault / Vault / etc.

```yaml
refresh_writeback_command_env_var: MY_REFRESH_WRITEBACK_CMD
# In your shell environment:
# export MY_REFRESH_WRITEBACK_CMD='aws secretsmanager update-secret --secret-id concur/refresh_token --secret-string {token}'
```

If the provider doesn't actually rotate (e.g. Google), leave both writeback fields unset — same token gets reused forever.

### `password` — legacy only
```
username + password → POST /token → access_token
```
Deprecated by most modern OAuth providers. Useful only for older SAP NetWeaver Gateway systems that exposed OAuth on top of HTTP basic auth.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `resource_key` | `str` | no | Default `oauth_token` |
| `token_endpoint` | `str` | yes | OAuth token URL |
| `grant_type` | `str` | no | `client_credentials` (default) / `refresh_token` / `password` |
| `client_id_env_var` / `client_secret_env_var` | `str` | conditional | Required for most flows |
| `refresh_token_env_var` | `str` | conditional | For `refresh_token` |
| `username_env_var` / `password_env_var` | `str` | conditional | For `password` |
| `refresh_writeback_file` / `refresh_writeback_command_env_var` | `str` | no | Rotation persistence |
| `scope` / `audience` | `str` | no | Standard OAuth params |
| `auth_in` | `str` | no | `form` (default) or `basic` |
| `extra_form_params` | `dict` | no | Vendor-specific extras |
| `timeout_seconds` | `int` | no | Default 30 |
| `early_refresh_seconds` | `int` | no | Refresh this many seconds before expiry. Default 60. |

## Using the resource

```python
from dagster import asset

@asset(required_resource_keys={"my_oauth_token"})
def my_data(context):
    import requests
    h = {"Authorization": context.resources.my_oauth_token.get_authorization_header()}
    r = requests.get("https://api.example.com/v1/things", headers=h)
    r.raise_for_status()
    return r.json()
```

Or wire it into `oauth_rest_ingestion`, which handles paginated GET + DataFrame conversion.

## Headless rotation patterns

Different secret-store backends, one writeback command template:

```bash
# AWS Secrets Manager
export REFRESH_WRITEBACK_CMD='aws secretsmanager update-secret --secret-id myapp/refresh_token --secret-string {token}'

# Azure Key Vault
export REFRESH_WRITEBACK_CMD='az keyvault secret set --vault-name myvault --name refresh-token --value {token}'

# GCP Secret Manager
export REFRESH_WRITEBACK_CMD='gcloud secrets versions add my-refresh-token --data-file=<(echo -n "{token}")'

# HashiCorp Vault
export REFRESH_WRITEBACK_CMD='vault kv put secret/concur refresh_token={token}'

# k8s Secret (raw API — needs ServiceAccount with patch perms)
export REFRESH_WRITEBACK_CMD='kubectl patch secret concur-token --type=json -p="[{\"op\":\"replace\",\"path\":\"/data/refresh_token\",\"value\":\"$(echo -n {token} | base64)\"}]"'
```

## Workload Identity (cleanest)

In GKE / EKS / AKS, configure the pod's ServiceAccount with workload identity and avoid stored credentials entirely:

```python
# GCP
from google.auth import default
creds, _ = default()
access_token = creds.token
```

```python
# Azure
from azure.identity import DefaultAzureCredential
token = DefaultAzureCredential().get_token("https://graph.microsoft.com/.default").token
```

```python
# AWS (for IAM-Identity-Center → external IdP)
import boto3
sts = boto3.client("sts")
# Use STS AssumeRoleWithWebIdentity to mint a federated token
```

This resource is for OAuth servers that don't speak workload identity natively (SAP cloud APIs, Concur, Ariba). For Azure-AD-backed APIs (Microsoft Graph, Dynamics 365), prefer `DefaultAzureCredential` from a workload-identity-enabled pod.

## See also

- [`oauth_rest_ingestion`](../../assets/ingestion/oauth_rest_ingestion/) — uses this resource
- [`odata_resource`](../odata_resource/) — for OData APIs, can reference this resource via `auth_type: oauth_resource`
- [`odata_ingestion`](../../assets/ingestion/odata_ingestion/) — direct bearer-token mode without this resource (`auth_type: bearer` + env var)
