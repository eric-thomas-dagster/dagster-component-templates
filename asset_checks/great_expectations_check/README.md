# Great Expectations Check

Runs a Great Expectations suite against a Dagster asset and surfaces the results as a native `AssetCheck`.

## What this does

When the check executes:
1. Loads the Great Expectations context
2. Runs the configured suite or ruleset against the asset's data
3. Returns an `AssetCheckResult` with pass/fail status and detailed statistics

Results appear in the **Checks** tab of the Asset Details page in the Dagster UI. Failed checks with `severity: ERROR` block downstream assets from materializing.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Asset key to attach this check to |
| `ge_context_root_dir` | `str` | `**required**` | Path to the Great Expectations project root |
| `datasource_name` | `str` | `**required**` | GE datasource name |
| `data_asset_name` | `str` | `**required**` | GE data asset name |
| `expectation_suite_name` | `str` | `**required**` | GE expectation suite name |
| `severity` | `str` | `"ERROR"` | WARN or ERROR |

## Example

```yaml
type: dagster_component_templates.GreatExpectationsCheckComponent
attributes:
  asset_key: my_service/asset
  ge_context_root_dir: my_ge_context_root_dir
  datasource_name: my_datasource_name
  data_asset_name: my_data_asset_name
  expectation_suite_name: my_expectation_suite_name
  # severity: "ERROR"  # optional
```

## Severity

| Value | Behavior |
|---|---|
| `ERROR` (default) | Blocks downstream materialization on failure |
| `WARN` | Surfaces the failure as a warning without blocking |

## Requirements

```
great-expectations>=0.18.0
```
