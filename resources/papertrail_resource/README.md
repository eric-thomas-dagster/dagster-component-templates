# PapertrailResource

Wraps `dagster-papertrail`'s log handler so Dagster's run logs are forwarded to a Papertrail destination. Useful for centralized log aggregation when you don't have a heavier observability stack.

Wraps the official `dagster-papertrail` package.

## Example

```yaml
type: dagster_component_templates.PapertrailResourceComponent
attributes:
  papertrail_hostname: <fill in>
  papertrail_port: <fill in>
  resource_key: <fill in>
```

## Requirements

```
dagster
dagster-papertrail
```
