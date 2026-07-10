# ServiceNowWorkspaceComponent

Auto-emit one Dagster asset per **ServiceNow table** by enumerating `sys_db_object`. `StateBackedComponent` — discovery cached to disk. Materializing an asset reads records via the Table API and returns a DataFrame.

The workspace-shape peer of `servicenow_ingestion`.

## Example

```yaml
type: dagster_community_components.ServiceNowWorkspaceComponent
attributes:
  instance_env_var: SNOW_INSTANCE
  username_env_var: SNOW_USERNAME
  password_env_var: SNOW_PASSWORD
  table_selector:
    by_name: [incident, change_request, sys_user, cmdb_ci]
  query: "active=true"
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Filtering

- `table_selector` — Fivetran-shape name/pattern filters against the enumerated table list (~1500+ tables per instance)
- `query` — a `sysparm_query` filter applied uniformly to every table asset (e.g. `active=true`, `sys_created_on>=javascript:gs.beginningOfLastMonth()`)

## Related

- `servicenow_resource`
- `servicenow_ingestion` — single-table counterpart
- `servicenow_sensor` — event-driven state trigger
