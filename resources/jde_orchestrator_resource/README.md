# JDEOrchestratorResourceComponent

Shared connection resource for the **JD Edwards EnterpriseOne Orchestrator REST API**. Orchestrator is Oracle's low-code automation tool for JDE — composes Business Functions, Data Services, and Interop Services into REST-triggerable orchestrations.

Pairs with `jde_orchestration_trigger_job`, `jde_orchestration_status_sensor`, `jde_orchestration_output_ingestion`, and `jde_orchestrator_workspace`.

## Config

```yaml
type: dagster_community_components.JDEOrchestratorResourceComponent
attributes:
  resource_key: jde_orchestrator_resource
  base_url_env_var: JDE_AIS_URL
  username_env_var: JDE_USER
  password_env_var: JDE_PASSWORD
```

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | str | `jde_orchestrator_resource` | Resource key. |
| `base_url_env_var` | str | — | Env var with AIS Server base URL (no `/jderest` path). |
| `username_env_var` | str | — | Env var with JDE username. |
| `password_env_var` | str | — | Env var with JDE password. |
| `api_path_prefix` | str | `/jderest/v3/orchestrator` | v3 for JDE Tools 9.2.7+, v2 for older. |
| `verify_ssl` | bool | `true` | TLS verification. |

## What Orchestrator gives you (that Dagster then wraps)

- **JDE Business Functions** — application logic exposed as REST endpoints
- **JDE Interoperability Services** — cross-system integration steps
- **JDE Data Services** — read/write JDE tables
- **Notifications** — email / event triggers

Dagster becomes the orchestrator of your orchestrator — scheduling, dependency tracking, alerting, replay, downstream asset lineage.
