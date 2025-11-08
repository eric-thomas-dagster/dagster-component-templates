# Dependency Graph Component

Define dependencies between assets without modifying component schemas. This component enables fully YAML-based pipeline definitions where the dependency graph is explicitly declared.

## Overview

The `DependencyGraphComponent` doesn't create any assets - it only specifies how existing assets depend on each other. This allows you to:

1. **Wire up any components** - Works with all Dagster components, even those without a `deps` field
2. **Visual dependency drawing** - UI automatically updates this component when you draw connections
3. **Single source of truth** - YAML file contains both asset definitions and their relationships
4. **IO manager pattern** - Dependencies enable Dagster's automatic DataFrame passing

## Why Use This?

### Problem
Many Dagster components don't have a `deps` field in their schema. Adding dependencies to every component would:
- Pollute component schemas
- Break compatibility with existing components
- Require modifying upstream components

### Solution
Separate the "what" (components) from the "how" (dependencies). The `DependencyGraphComponent` acts as a declarative wiring diagram.

## Quick Start

### Complete YAML Pipeline

```yaml
# defs.yaml - Single file defining entire pipeline

# 1. Fetch data from API
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: sales_api
  api_url: https://api.example.com/sales
  output_format: dataframe

---

# 2. Clean and transform
type: dagster_component_templates.DataFrameTransformerComponent
attributes:
  asset_name: sales_cleaned
  drop_duplicates: true
  filter_columns: "order_id,customer_id,amount,date"

---

# 3. Aggregate daily
type: dagster_component_templates.DataFrameTransformerComponent
attributes:
  asset_name: daily_summary
  group_by: "date"
  agg_functions: '{"amount": "sum", "order_id": "count"}'

---

# 4. Wire up dependencies
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: sales_api
      target: sales_cleaned
    - source: sales_cleaned
      target: daily_summary

---

# 5. Schedule the pipeline
type: dagster.ScheduleComponent
attributes:
  name: daily_sales_pipeline
  cron_expression: "0 0 * * *"
  asset_selection: [sales_api, sales_cleaned, daily_summary]
```

### What Gets Generated

```python
@asset
def sales_api():
    # API fetching logic
    return dataframe

@asset
def sales_cleaned(sales_api):  # ← Implicit dependency via parameter name
    # Dagster's IO manager automatically loads sales_api DataFrame
    # Transformation logic
    return cleaned_dataframe

@asset
def daily_summary(sales_cleaned):  # ← Implicit dependency
    # Dagster's IO manager automatically loads sales_cleaned DataFrame
    # Aggregation logic
    return summary_dataframe
```

## How It Works

### 1. YAML Parsing
The system parses your multi-document YAML file (documents separated by `---`):
- Asset components define what to compute
- `DependencyGraphComponent` defines how they connect

### 2. Code Generation
The code generator (`template_service.py`):
- Reads edges from `DependencyGraphComponent`
- For each asset, finds its dependencies from the graph
- Adds those dependencies as function parameters
- Dagster treats matching parameter names as implicit inputs

### 3. IO Manager Pattern
Dagster's IO manager automatically:
- Serializes/persists DataFrames after each asset runs
- Loads DataFrames when needed by downstream assets
- Passes DataFrames as function arguments

### 4. Visual UI
The UI:
- Reads `DependencyGraphComponent` to draw edges in the graph
- When you draw connections, updates the `DependencyGraphComponent`
- Syncs bidirectionally: YAML ↔ UI ↔ Graph JSON

## Features

### Works with Any Component

```yaml
# Mix dbt, Sling, custom Python, and our template components
type: dagster_dbt.DbtProject
attributes:
  project_dir: dbt_project

---

type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: api_data
  api_url: https://api.example.com/data

---

type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: my_custom_python_asset
  module: my_project.assets

---

# Wire them all together
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: api_data
      target: dbt_staging_customers
    - source: dbt_staging_customers
      target: my_custom_python_asset
```

### Multiple Dependencies

```yaml
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    # Asset can depend on multiple sources
    - source: customers_api
      target: joined_data
    - source: orders_api
      target: joined_data

    # Multiple assets can depend on same source
    - source: joined_data
      target: customer_summary
    - source: joined_data
      target: order_summary
```

Generated code:
```python
@asset
def joined_data(customers_api, orders_api):
    # Both DataFrames available as parameters
    return customers_api.merge(orders_api, on='customer_id')
```

### Validation

The component validates edges:
```yaml
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: upstream  # ✅ Valid
      target: downstream

    - invalid_edge  # ❌ Error: must have 'source' and 'target'

    - source: 123  # ❌ Error: must be strings
      target: 456
```

## UI Integration

### Drawing Dependencies
1. Drag components onto canvas
2. Draw connection from source handle to target handle
3. UI automatically updates the `DependencyGraphComponent`
4. YAML file syncs with your visual changes

### Importing YAML
1. Click "Import Pipeline from YAML"
2. Select your `defs.yaml` file
3. UI parses and renders the complete graph
4. All edges from `DependencyGraphComponent` appear as connections

### Exporting YAML
1. Build your pipeline visually
2. Click "Export Pipeline to YAML"
3. Get complete pipeline definition in one file
4. Ready to commit to version control

## Best Practices

### 1. Single Dependency Graph per Project
```yaml
# ✅ Good: One DependencyGraphComponent with all edges
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: a
      target: b
    - source: b
      target: c
    - source: c
      target: d
```

```yaml
# ❌ Avoid: Multiple DependencyGraphComponents
# (While supported, makes graph harder to understand)
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: a
      target: b
---
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: b
      target: c
```

### 2. Place at End of YAML File
```yaml
# All asset definitions first
type: dagster_component_templates.RestApiFetcherComponent
# ...

---
type: dagster_component_templates.DataFrameTransformerComponent
# ...

---
# Dependency graph at end for clarity
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    # All dependencies in one place
```

### 3. Use with AssetReferenceComponent
```yaml
# Reference existing assets
type: dagster_component_templates.AssetReferenceComponent
attributes:
  asset_name: existing_dbt_model

---

# Wire new and existing assets together
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: new_api_asset
      target: existing_dbt_model
```

## Configuration

### Attributes

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
| `edges` | Array | Yes | List of dependency edges |
| `edges[].source` | String | Yes | Source asset name |
| `edges[].target` | String | Yes | Target asset name (depends on source) |

### Edge Format

Each edge must have exactly these keys:
```yaml
- source: upstream_asset_name
  target: downstream_asset_name
```

The target asset will receive the source asset's output as a function parameter.

## Troubleshooting

### Edge Not Appearing in UI?
- Check asset names match exactly
- Ensure `DependencyGraphComponent` is saved to project
- Try refreshing the UI
- Check browser console for errors

### Asset Not Receiving DataFrame?
- Verify edge is defined in `DependencyGraphComponent`
- Check source asset produces DataFrame (look for cyan handle in UI)
- Ensure both assets are in the same Definitions
- Check Dagster logs for IO manager errors

### Cyclic Dependencies?
```
dagster.core.errors.DagsterInvalidDefinitionError: Circular dependency detected
```

Solution: Check your edges for cycles:
```yaml
# ❌ Cycle
edges:
  - source: a
    target: b
  - source: b
    target: a
```

## Requirements

- dagster >= 1.5.0
- No additional Python dependencies

## License

MIT License
