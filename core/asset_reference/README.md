# Asset Reference Component

Reference existing assets (dbt models, Python assets, etc.) in pipeline definitions without creating new assets.

## Overview

The `AssetReferenceComponent` allows you to include existing assets from your codebase in a YAML-based pipeline definition. This is useful when you want to mix component-generated assets with hand-written assets, or include dbt models in a pipeline.

**This component doesn't create new assets** - it only provides metadata so existing assets can be:
- Displayed in the UI graph
- Included in dependency edges
- Selected in pipeline/schedule configurations

## Quick Start

### Reference a dbt Asset

```yaml
# Existing dbt project with models
# models/staging/stg_customers.sql already exists

# Reference it in defs.yaml:
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: stg_customers
  module: my_project.dbt_assets
  description: "Staging customers from raw data"
  group_name: staging
```

### Reference a Python Asset

```python
# my_project/custom_assets.py
from dagster import asset

@asset
def complex_calculation():
    return expensive_computation()
```

```yaml
# Reference it in defs.yaml:
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: complex_calculation
  module: my_project.custom_assets
  description: "Complex calculation asset"
```

## Complete Pipeline Example

Mix existing and new assets:

```yaml
# defs.yaml

# 1. Reference existing dbt staging model
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: dbt_staging_customers
  module: my_project.dbt_assets
  description: "Customer data from dbt"

---

# 2. Fetch enrichment data from API (new asset)
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: api_enrichment
  api_url: https://api.example.com/customer-enrichment
  output_format: dataframe

---

# 3. Transform and join (new asset)
type: dagster_component_templates.DataFrameTransformerComponent
attributes:
  asset_name: enriched_customers
  # Will receive both dbt_staging_customers and api_enrichment

---

# 4. Reference existing custom Python asset
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: ml_predictions
  module: my_project.ml_assets

---

# 5. Wire everything together
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: dbt_staging_customers
      target: enriched_customers
    - source: api_enrichment
      target: enriched_customers
    - source: enriched_customers
      target: ml_predictions
```

## Use Cases

### 1. Include dbt Models in Pipelines

```yaml
# Reference multiple dbt models
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: dbt_customers
  module: my_project.dbt_assets

---

type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: dbt_orders
  module: my_project.dbt_assets

---

# Create dependency between them and new assets
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: dbt_customers
      target: api_customer_sync
    - source: dbt_orders
      target: api_order_sync
```

### 2. Migrate Gradually from Python to Components

```yaml
# Reference existing Python assets
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: legacy_etl_asset
  module: my_project.legacy_assets

---

# Add new component-based assets
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: new_api_data
  api_url: https://api.example.com/v2/data

---

# Wire old and new together
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: legacy_etl_asset
      target: new_api_data
```

### 3. Reference Sling Replication Assets

```yaml
# Reference Sling-managed assets
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: sling_postgres_to_duckdb
  module: my_project.sling_assets
  description: "Sling replication from Postgres to DuckDB"

---

# Transform the replicated data
type: dagster_component_templates.DataFrameTransformerComponent
attributes:
  asset_name: cleaned_replicated_data

---

type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: sling_postgres_to_duckdb
      target: cleaned_replicated_data
```

## How It Works

### 1. Component Doesn't Generate Code

Unlike other components, `AssetReferenceComponent` tells the code generator to **skip** code generation:

```python
# Code generator sees AssetReferenceComponent and:
# 1. Skips generating a new @asset function
# 2. Imports the asset from the specified module
# 3. Includes it in the Definitions

from my_project.dbt_assets import dbt_staging_customers  # ← Imported

# No new asset generated - uses the existing one
```

### 2. UI Integration

The UI:
- Renders AssetReferenceComponent as a node in the graph
- Shows it with special styling (e.g., "Reference" badge)
- Allows drawing connections to/from it
- Updates DependencyGraphComponent when you connect it

### 3. Module Resolution

If `module` is specified:
```python
from {module} import {asset_name}
```

If `module` is omitted, assumes the asset is available globally in your Definitions.

## Configuration

### Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
| `asset_name` | String | Yes | Name of the existing asset (must match exactly) |
| `module` | String | No | Python module where asset is defined |
| `description` | String | No | Description for documentation |
| `group_name` | String | No | Asset group for UI organization |

### Asset Name Matching

The `asset_name` **must exactly match** the asset name in your codebase:

```python
# In your code:
@asset(name="customer_data")  # ← Use this name
def process_customers():
    ...
```

```yaml
# In YAML:
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: customer_data  # ← Must match exactly
```

## Troubleshooting

### Asset Not Found Error

```
AttributeError: module 'my_project.assets' has no attribute 'my_asset'
```

**Solutions:**
1. Check asset name matches exactly
2. Verify module path is correct
3. Ensure asset is exported from module
4. Check for typos in `asset_name`

### Import Error

```
ModuleNotFoundError: No module named 'my_project.assets'
```

**Solutions:**
1. Verify module exists in your project
2. Check Python path includes your project
3. Ensure module is importable from definitions file
4. Try absolute import path

### Asset Not Appearing in UI

**Solutions:**
1. Save the component to the project
2. Refresh the project in UI
3. Check browser console for errors
4. Verify component is in project's component list

## Best Practices

### 1. Group Related References

```yaml
# Keep all asset references together
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: dbt_model_1
---
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: dbt_model_2
---
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: dbt_model_3

# Then define new assets
---
type: dagster_component_templates.RestApiFetcherComponent
# ...
```

### 2. Use Descriptive Descriptions

```yaml
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: complex_ml_model
  module: my_project.ml_assets
  description: "XGBoost model trained on customer behavior data. Updated weekly."
  group_name: ml_models
```

### 3. Match Existing Asset Groups

```python
# In your code:
@asset(group_name="staging")
def stg_customers():
    ...
```

```yaml
# In YAML - use same group:
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: stg_customers
  group_name: staging  # ← Matches existing group
```

## Limitations

1. **Asset must already exist** - This component can't create new assets
2. **Module must be importable** - Asset must be accessible at import time
3. **No code modification** - Can't modify existing asset's logic or configuration
4. **Name must match exactly** - Even small typos will cause import errors

## Requirements

- dagster >= 1.5.0
- No additional Python dependencies
- Existing assets must be defined in your codebase

## License

MIT License
