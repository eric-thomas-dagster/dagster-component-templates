# CognosResourceComponent

Shared connection resource for **IBM Cognos Analytics REST API v1+**. Session-based auth using a security namespace (LDAP / CognosEx / other).

Pairs with `cognos_report_run_job`, `cognos_report_status_sensor`, `cognos_report_data_ingestion`, and `cognos_workspace`.

## Config

```yaml
type: dagster_community_components.CognosResourceComponent
attributes:
  resource_key: cognos_resource
  base_url_env_var: COGNOS_URL
  username_env_var: COGNOS_USER
  password_env_var: COGNOS_PASSWORD
  namespace_env_var: COGNOS_NAMESPACE
```

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | str | `cognos_resource` | Resource key. |
| `base_url_env_var` | str | — | Env var with Cognos base URL (no `/api` path). |
| `username_env_var` | str | — | Env var with Cognos username. |
| `password_env_var` | str | — | Env var with Cognos password. |
| `namespace_env_var` | str | — | Env var with the Cognos security namespace (LDAP, CognosEx, etc.). |
| `verify_ssl` | bool | `true` | TLS verification. |

## Cognos Analytics vs. TM1

- **Cognos Analytics** = BI dashboards + reports (this integration).
- **TM1 / Planning Analytics** = planning cubes with writeback. Use `tm1_resource`.

Both live in IBM's analytics stack; they're separate REST APIs and separate integrations.
