# CognosWorkspaceComponent

Auto-emit one Dagster asset per **Cognos Analytics report** by enumerating reports in the specified folders via the Cognos REST API. `StateBackedComponent` — discovery cached to disk. Materializing an asset runs the underlying report.

The workspace-shape peer of the low-level `cognos_*` components.

## Example

```yaml
type: dagster_community_components.CognosWorkspaceComponent
attributes:
  base_url_env_var: COGNOS_URL
  username_env_var: COGNOS_USER
  password_env_var: COGNOS_PASSWORD
  namespace_env_var: COGNOS_NAMESPACE
  report_selector:
    by_pattern: ["Monthly*", "Daily*"]
    exclude_by_pattern: ["*_deprecated"]
  group_name: cognos_reports
  output_format: CSV
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Selector

Same Fivetran-shape as other vendor workspaces (`by_name` / `by_pattern` / `exclude_by_name` / `exclude_by_pattern`).

## Folder walking

`folder_ids` accepts Cognos searchPath-style strings — e.g. `/content/folder[@name='Finance']`. Omit for root (`/`). If you have thousands of reports, narrow to specific folders to keep discovery fast.

## Related

- `cognos_resource`
- `cognos_report_run_job`
- `cognos_report_status_sensor`
- `cognos_report_data_ingestion` — same execution, but parses CSV/JSON into a DataFrame
