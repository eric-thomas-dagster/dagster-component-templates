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

**Important**: The `DependencyGraphComponent` is a **metadata-only component** - it stores dependency information but doesn't create assets. The actual dependency injection happens at runtime in your project's `definitions.py` file.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ 1. DependencyGraphComponent (YAML)                          │
│    Stores edges as metadata                                  │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Backend writes custom_lineage.json                       │
│    Extracts edges from component → JSON file                │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. definitions.py reads JSON at runtime                     │
│    Loads edges when Dagster starts                           │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. map_asset_specs() injects dependencies                   │
│    Modifies AssetSpecs to add deps attribute                │
└─────────────────────────────────────────────────────────────┘
```

### 1. Component Storage (YAML)
The `DependencyGraphComponent` stores edges in your `defs.yaml`:
```yaml
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: api_data
      target: cleaned_data
```

### 2. JSON Export
When you save your project, the backend:
- Extracts edges from `DependencyGraphComponent`
- Merges with any UI-drawn edges
- Writes to `defs/custom_lineage.json`

### 3. Runtime Loading (definitions.py)
Your project's `definitions.py` must include this code:

```python
import json
from pathlib import Path
import dagster as dg

# Load custom lineage from JSON file
custom_lineage_file = Path(__file__).parent / "defs" / "custom_lineage.json"
custom_lineage_edges = []
if custom_lineage_file.exists():
    try:
        with open(custom_lineage_file, "r") as f:
            custom_lineage_data = json.load(f)
            custom_lineage_edges = custom_lineage_data.get("edges", [])
    except Exception as e:
        print(f"⚠️  Failed to load custom_lineage.json: {e}")

# ... create your defs ...

# Apply custom lineage by injecting dependencies into asset specs
if custom_lineage_edges:
    def inject_custom_dependencies(spec):
        """Inject custom lineage dependencies into asset specs."""
        spec_key_str = "/".join(spec.key.path)

        # Find edges where this asset is the target
        custom_deps = []
        for edge in custom_lineage_edges:
            if edge["target"] == spec_key_str:
                custom_deps.append(dg.AssetKey(edge["source"].split("/")))

        if custom_deps:
            # Merge with existing deps
            existing_deps = set(spec.deps or [])
            all_deps = existing_deps | set(custom_deps)
            return spec.replace_attributes(deps=list(all_deps))

        return spec

    defs = defs.map_asset_specs(func=inject_custom_dependencies)
```

**Why this approach?**
- Assets must exist before dependencies can be added
- Component loading order cannot be controlled
- `map_asset_specs()` runs after all assets are loaded
- Non-invasive: doesn't modify asset source code

### 4. Dependency Injection
At runtime, Dagster:
- Calls `inject_custom_dependencies` for each AssetSpec
- Adds dependencies from `custom_lineage.json` to `spec.deps`
- Creates the full DAG with all edges

### 5. Visual UI
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

## Using Outside Dagster Designer

If you're using the `DependencyGraphComponent` without the Dagster Designer tool, you **must** add the custom lineage loading logic to your `definitions.py` manually.

### Setup Steps

1. **Add the component to your YAML**:
```yaml
# defs.yaml
type: dagster_component_templates.DependencyGraphComponent
attributes:
  edges:
    - source: upstream_asset
      target: downstream_asset
```

2. **Create `custom_lineage.json`**:
```json
{
  "edges": [
    {"source": "upstream_asset", "target": "downstream_asset"}
  ]
}
```

3. **Add loading logic to `definitions.py`**:

Add this code BEFORE your `defs = Definitions(...)` line:

```python
import json
from pathlib import Path
import dagster as dg

# Load custom lineage from JSON file
custom_lineage_file = Path(__file__).parent / "defs" / "custom_lineage.json"
custom_lineage_edges = []
if custom_lineage_file.exists():
    try:
        with open(custom_lineage_file, "r") as f:
            custom_lineage_data = json.load(f)
            custom_lineage_edges = custom_lineage_data.get("edges", [])
            if custom_lineage_edges:
                print(f"✅ Loaded {len(custom_lineage_edges)} custom lineage edge(s)")
    except Exception as e:
        print(f"⚠️  Failed to load custom_lineage.json: {e}")
```

Add this code AFTER your `defs = Definitions(...)` creation:

```python
# Apply custom lineage by injecting dependencies into asset specs
if custom_lineage_edges:
    def inject_custom_dependencies(spec):
        """Inject custom lineage dependencies into asset specs."""
        spec_key_str = "/".join(spec.key.path)

        # Find edges where this asset is the target
        custom_deps = []
        for edge in custom_lineage_edges:
            if edge["target"] == spec_key_str:
                custom_deps.append(dg.AssetKey(edge["source"].split("/")))

        if custom_deps:
            # Merge with existing deps
            existing_deps = set(spec.deps or [])
            all_deps = existing_deps | set(custom_deps)
            return spec.replace_attributes(deps=list(all_deps))

        return spec

    defs = defs.map_asset_specs(func=inject_custom_dependencies)
    print(f"✅ Applied custom lineage to asset specs")
```

### Complete Example

```python
# definitions.py
import json
from pathlib import Path
import dagster as dg
from dagster import Definitions, load_from_defs_folder

# Load custom lineage
custom_lineage_file = Path(__file__).parent / "defs" / "custom_lineage.json"
custom_lineage_edges = []
if custom_lineage_file.exists():
    with open(custom_lineage_file, "r") as f:
        custom_lineage_data = json.load(f)
        custom_lineage_edges = custom_lineage_data.get("edges", [])

# Create definitions
defs = Definitions.merge(
    load_from_defs_folder(path_within_project=Path(__file__).parent),
    Definitions(resources={...})
)

# Inject custom dependencies
if custom_lineage_edges:
    def inject_custom_dependencies(spec):
        spec_key_str = "/".join(spec.key.path)
        custom_deps = []
        for edge in custom_lineage_edges:
            if edge["target"] == spec_key_str:
                custom_deps.append(dg.AssetKey(edge["source"].split("/")))

        if custom_deps:
            existing_deps = set(spec.deps or [])
            all_deps = existing_deps | set(custom_deps)
            return spec.replace_attributes(deps=list(all_deps))
        return spec

    defs = defs.map_asset_specs(func=inject_custom_dependencies)
```

### Automatic Injection (Dagster Designer)

If you import a project into Dagster Designer, this logic is **automatically injected** into your `definitions.py`. You don't need to do anything manually!

Projects created by Dagster Designer include this logic by default.

## Requirements

- dagster >= 1.5.0
- No additional Python dependencies
- Custom lineage loading code in `definitions.py` (automatically added by Dagster Designer, or manually if using standalone)

## License

MIT License
