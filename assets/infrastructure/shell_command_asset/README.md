# ShellCommandAsset

Wraps `dagster-shell`'s `execute_shell_command` so a shell command (or script) is a real Dagster asset — captured stdout/stderr in the run log, exit code → asset success/failure, optional cwd + env vars.

## Example

```yaml
type: dagster_component_templates.ShellCommandAssetComponent
attributes:
  asset_name: <fill in>
  command: <fill in>
  cwd: <fill in>
  env_vars: <fill in>
  group_name: <fill in>
  description: <fill in>
  deps: <fill in>
```

## Requirements

```
dagster
dagster-shell
```
