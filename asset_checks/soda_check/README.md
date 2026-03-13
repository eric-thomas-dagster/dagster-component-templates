# Soda Check

Runs a Soda suite against a Dagster asset and surfaces the results as a native `AssetCheck`.

## What this does

When the check executes:
1. Loads the Soda context
2. Runs the configured suite or ruleset against the asset's data
3. Returns an `AssetCheckResult` with pass/fail status and detailed statistics

Results appear in the **Checks** tab of the Asset Details page in the Dagster UI. Failed checks with `severity: ERROR` block downstream assets from materializing.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Asset key to attach this check to |
| `data_source_name` | `str` | `**required**` |  |
| `checks_yaml_path` | `str` | `**required**` | Path to the Soda checks YAML file |
| `soda_configuration_path` | `str` | `"./soda/configuration.yaml"` | Path to the Soda configuration YAML file |
| `severity` | `str` | `"ERROR"` | WARN or ERROR |

## Example

```yaml
type: dagster_component_templates.SodaCheckComponent
attributes:
  asset_key: my_service/asset
  data_source_name: my_data_source_name
  checks_yaml_path: ./path/to/file
  # soda_configuration_path: "./soda/configuration.yaml"  # optional
  # severity: "ERROR"  # optional
```

## Severity

| Value | Behavior |
|---|---|
| `ERROR` (default) | Blocks downstream materialization on failure |
| `WARN` | Surfaces the failure as a warning without blocking |

## Requirements

```
soda-core>=3.0.0
```
