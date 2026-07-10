# QlikComposeResourceComponent

Shared connection resource for the **Qlik Compose REST API**. Qlik Compose is the data-warehouse automation tool in the Qlik Data Integration platform — it manages DW models, workflows, and data marts on top of the source data landed by Qlik Replicate.

Pairs with `qlik_compose_workflow_trigger_job`, `qlik_compose_workflow_status_sensor`, `qlik_compose_workflow_metrics_ingestion`, and `qlik_compose_workspace`.

## Config

```yaml
type: dagster_community_components.QlikComposeResourceComponent
attributes:
  resource_key: qlik_compose_resource
  base_url_env_var: QLIK_COMPOSE_URL
  api_token_env_var: QLIK_COMPOSE_TOKEN
```

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | str | `qlik_compose_resource` | Resource key. |
| `base_url_env_var` | str | — | Env var with Compose base URL (no `/api` path). |
| `username_env_var` / `password_env_var` | str | — | Session-based auth (dev). |
| `api_token_env_var` | str | — | API token (preferred for production). |
| `verify_ssl` | bool | `true` | TLS verification. |

## Distinct from Qlik Replicate

- **Qlik Replicate** = CDC / replication from source DBs. Use [`qlik_replicate_*` components](../qlik_replicate_resource/).
- **Qlik Compose** = DW automation on top of the replicated data. Manages DW model, workflows, and data marts.
