# VaultAssetComponent

Read secrets from a HashiCorp Vault cluster at pipeline runtime and inject them as environment variables so that downstream processes, subcommands, or in-process code can access credentials without ever storing them in files or Dagster configuration.

## Use case

Secrets that cannot be baked into environment variables at deploy time — rotating database passwords, short-lived API tokens, per-environment credentials — can be fetched from Vault on demand as part of a Dagster asset. `VaultAssetComponent` lets you:

- **Gate downstream assets** on a successful secret-fetch via `deps`.
- **Inject secrets into the current process** with `export_to_env: true` so that any Python code running in the same worker can read them from `os.environ`.
- **Run shell commands** with secrets available as environment variables in the subprocess without exposing them to the parent shell's history.
- **Audit secret access** via Dagster asset materialization metadata — which Vault paths were accessed and which env var names were set, but never the secret values themselves.

## Quick start

```yaml
type: dagster_component_templates.VaultAssetComponent
attributes:
  asset_name: inject_pipeline_secrets
  vault_url_env_var: VAULT_URL
  role_id_env_var: VAULT_ROLE_ID
  secret_id_env_var: VAULT_SECRET_ID
  secrets:
    - path: "secret/data/data-platform/db"
      key: password
      env_var: DB_PASSWORD
    - path: "secret/data/data-platform/api"
      key: api_key
      env_var: EXTERNAL_API_KEY
  group_name: secrets
  description: Inject pipeline secrets from Vault before run
```

## Authentication

The component never accepts secret values directly in YAML. All credentials are read from environment variables at runtime.

### Token authentication

Suitable for development or when using a machine-specific token that is managed externally (e.g. by Vault Agent).

```yaml
vault_url_env_var: VAULT_URL
token_env_var: VAULT_TOKEN
```

Set `VAULT_URL` and `VAULT_TOKEN` in your Dagster deployment environment (Kubernetes secret, AWS Secrets Manager via `EnvVar`, etc.).

**Security note:** Avoid using root tokens. Create a dedicated token with a narrow policy that only allows `read` on the specific secret paths your pipeline needs. Use `token_ttl` to limit exposure.

### AppRole authentication (recommended for production)

AppRole is designed for machine-to-machine authentication. A `role_id` (non-secret, like a username) is paired with a `secret_id` (secret, like a password) to obtain a short-lived token.

```yaml
vault_url_env_var: VAULT_URL
role_id_env_var: VAULT_ROLE_ID
secret_id_env_var: VAULT_SECRET_ID
```

Set `VAULT_URL`, `VAULT_ROLE_ID`, and `VAULT_SECRET_ID` in your deployment environment. The component logs into Vault's AppRole endpoint (`/v1/auth/approle/login`) at runtime and uses the resulting token for all secret reads. The token is held in memory only and is never written to disk or logged.

**Security note:** Use `secret_id_ttl`, `secret_id_num_uses`, and `token_ttl` in your Vault AppRole configuration to limit the blast radius of a compromised secret_id.

## KV v2 path format

Vault KV version 2 stores secrets under a path that includes a `data/` segment:

| KV v2 mount | Secret name | Full path |
|---|---|---|
| `secret/` | `myapp/db` | `secret/data/myapp/db` |
| `kv/` | `team/api` | `kv/data/team/api` |

Always include `data/` in the `path` field:

```yaml
secrets:
  - path: "secret/data/myapp/db"   # correct
    key: password
    env_var: DB_PASSWORD
```

To read every key from a secret (instead of a single key), use `VaultResource.get_secret(path)` directly in your own asset code.

## Secrets as environment variables

### Inject into current process

With `export_to_env: true` (the default), the component calls `os.environ[env_var] = value` for each secret. Any Python code that runs in the same Dagster worker process after this asset materializes — including downstream assets in the same run — can read the secret via `os.environ["DB_PASSWORD"]`.

```yaml
export_to_env: true   # default
```

### Inject into subprocess commands only

Set `export_to_env: false` if you want secrets available only inside explicit subprocesses. The secrets are passed as subprocess environment variables but are never set on the parent process.

```yaml
export_to_env: false
commands:
  - "python run_etl.py"
```

### Running commands with secrets injected

```yaml
commands:
  - "python scripts/migrate_db.py"
  - "curl -H 'Authorization: Bearer $EXTERNAL_API_KEY' https://api.example.com/sync"
```

Each command is run with `shell=True` so shell variable expansion works. Stdout is streamed to the Dagster run log. A non-zero exit code raises `RuntimeError` and fails the asset.

## Vault Enterprise namespaces

```yaml
namespace: admin/team-data
```

Sets the `X-Vault-Namespace` header on all API requests including the AppRole login call.

## Asset dependencies

Express that secrets must be injected before downstream assets run:

```yaml
attributes:
  asset_name: inject_pipeline_secrets
  deps: []          # no upstream deps needed for a secrets gate
  ...
```

Then in your data asset:

```yaml
attributes:
  asset_name: run_etl
  deps:
    - inject_pipeline_secrets
```

Dagster will materialize `inject_pipeline_secrets` first and only proceed to `run_etl` once secrets are confirmed injected.

## Security best practices

**Never log secret values.** The component deliberately logs only secret paths and env var names. Do not add logging statements that print the values of secrets or `os.environ` entries that correspond to secrets.

**Use AppRole, not root tokens.** Root tokens bypass all Vault policies. Use AppRole with a narrow policy:

```hcl
# vault-policy-data-platform.hcl
path "secret/data/data-platform/*" {
  capabilities = ["read"]
}
```

**Rotate secret_ids regularly.** Configure `secret_id_ttl` (e.g. `24h`) and `secret_id_num_uses` (e.g. `10`) on your AppRole. Rotate secret_ids via your CI/CD system or a Vault Agent sidecar.

**Use short token TTLs.** Set `token_ttl` on your AppRole to match the expected maximum duration of a pipeline run (e.g. `1h`). The token is discarded after each run.

**Prefer `verify_ssl: true` (the default).** Only disable SSL verification in isolated development environments with self-signed certificates. Never disable it in production.

**Audit Vault access.** Enable Vault's audit backend. Every secret read performed by this component will appear in the audit log with the AppRole entity, path, and timestamp.

## Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset key |
| `vault_url_env_var` | `str` | required | Env var with Vault base URL |
| `token_env_var` | `str` | `None` | Env var with Vault token |
| `role_id_env_var` | `str` | `None` | Env var with AppRole role_id |
| `secret_id_env_var` | `str` | `None` | Env var with AppRole secret_id |
| `namespace` | `str` | `None` | Vault Enterprise namespace |
| `secrets` | `list[dict]` | required | Secret path/key/env_var mappings |
| `commands` | `list[str]` | `None` | Shell commands to run with secrets injected |
| `export_to_env` | `bool` | `true` | Set secrets on current process `os.environ` |
| `group_name` | `str` | `"secrets"` | Dagster asset group |
| `description` | `str` | `None` | Asset description |
| `deps` | `list[str]` | `None` | Upstream asset keys |
