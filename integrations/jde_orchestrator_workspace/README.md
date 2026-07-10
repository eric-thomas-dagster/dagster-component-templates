# JDEOrchestratorWorkspaceComponent

Auto-emit one Dagster asset per **JDE orchestration** by enumerating via the JDE Orchestrator REST API. `StateBackedComponent` — discovery cached to disk, refreshed on explicit trigger.

The workspace-shape peer of the `jde_orchestrator_*` low-level components.

## Example

```yaml
type: dagster_community_components.JDEOrchestratorWorkspaceComponent
attributes:
  base_url_env_var: JDE_AIS_URL
  username_env_var: JDE_USER
  password_env_var: JDE_PASSWORD
  orchestration_selector:
    by_pattern: ["JDE_*"]
    exclude_by_pattern: ["*_deprecated"]
  group_name: jde_orchestrations
  async_mode: true
  wait_for_completion: true
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Selector

Same Fivetran-shape as other vendor workspaces:

```yaml
orchestration_selector:
  by_name: [JDE_AR_Recon, JDE_AP_Payment_Run]
  by_pattern: ["JDE_*"]
  exclude_by_name: [test_orch]
  exclude_by_pattern: ["*_deprecated"]
```

## Related

- `jde_orchestrator_resource`
- `jde_orchestration_trigger_job`
- `jde_orchestration_status_sensor`
- `jde_orchestration_output_ingestion`
