# QlikReplicateResourceComponent

Shared connection resource for the **Qlik Enterprise Manager REST API**, which is the control plane for Qlik Replicate. Other Qlik Replicate components — `qlik_replicate_task_trigger_job`, `qlik_replicate_task_status_sensor`, `qlik_replicate_task_metrics_ingestion` — reference this resource, so you configure credentials in one place.

## When to use

- You're running Qlik Replicate for CDC or one-shot database migrations and want Dagster to be the orchestrator (start/stop tasks, react to task completion, ingest metrics).
- You have Enterprise Manager (Qlik's control-plane console) sitting in front of one or more Replicate servers, and you want to talk to it via its REST API rather than the UI.

## Auth modes

| Mode | Where the creds come from | Best for |
|---|---|---|
| **API token** | Enterprise Manager UI → *Manage API Tokens* → *Generate* | Production, CI, headless orchestration |
| **Username / password** | Same account you'd log into EM's web UI with (Windows / SAML / LDAP-backed) | Dev, POCs, air-gapped installs |

When `api_token_env_var` is set, basic-auth fields are ignored.

## Config

```yaml
type: dagster_community_components.QlikReplicateResourceComponent
attributes:
  resource_key: qlik_replicate_resource
  base_url_env_var: QLIK_EM_URL           # e.g. https://qlikem.acme.com
  api_token_env_var: QLIK_EM_API_TOKEN
```

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | str | `qlik_replicate_resource` | Resource registration key. Other components reference it via this name. |
| `base_url_env_var` | str | — | Env var with the Enterprise Manager base URL (e.g. `https://qlikem.acme.com`). No trailing slash, no API path — the resource appends `/attunityenterprisemanager/api/v1`. |
| `username_env_var` | str | — | Env var with EM username. Required unless `api_token_env_var` is set. |
| `password_env_var` | str | — | Env var with EM password. Required unless `api_token_env_var` is set. |
| `api_token_env_var` | str | — | Env var with an EM API token. Preferred for production. |
| `verify_ssl` | bool | `true` | TLS cert verification. Set `false` for self-signed dev servers. |

## Related components

- **`qlik_replicate_task_trigger_job`** — start / stop / resume a task from Dagster on a schedule or on demand
- **`qlik_replicate_task_status_sensor`** — trigger downstream Dagster jobs when a Replicate task completes / fails
- **`qlik_replicate_task_metrics_ingestion`** — materialize per-task metrics (rows replicated, latency, error counts) as a DataFrame asset

## Distinct from

- **Fivetran / Airbyte** — SaaS ELT tools with their own connectors. Qlik Replicate is on-prem CDC that talks directly to source databases (SQL Server, Oracle, DB2, SAP HANA, mainframe VSAM, etc.). Use `dagster-fivetran` or `dagster-airbyte` for those platforms.
- **`database_replication`** — Dagster-native replication component (SQLAlchemy-based). Use that when you don't already have Replicate; use these Qlik components when Replicate is already deployed and you're wrapping it with orchestration.
