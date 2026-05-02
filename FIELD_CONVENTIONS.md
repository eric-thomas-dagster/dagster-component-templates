# Field Naming Conventions

This document defines the canonical field names used across components in the registry. The goal: a Dagster engineer should be able to write `defs.yaml` without having to look up "is it `upstream_asset_key` or `source_asset`? `n_bins` or `n_tiles`? `random_state` or `random_seed`?"

If you're authoring a new component, use these names. If you're updating an existing component and the rename is mechanical (no logic change), prefer the canonical name and add the old name as a Pydantic `validation_alias` so existing YAML keeps working.

---

## Asset wiring

| Canonical | Type | Use when |
|---|---|---|
| `asset_name` | `str` | The asset key this component produces |
| `upstream_asset_key` | `str` | The component depends on **one** upstream Dagster asset |
| `upstream_asset_keys` | `List[str]` | The component fans **multiple** upstream assets in (e.g. unions, joins) |
| `deps` | `Optional[List[str]]` | Lineage-only dependency (no data passed) |
| `group_name` | `Optional[str]` | Dagster asset group |

**Avoid:** `source_asset`, `input_asset`, `data_asset`. Always `upstream_asset_key` for the dependency-with-data shape.

## Selecting columns

| Canonical | Type | Use when |
|---|---|---|
| `column` | `str` | The component operates on **exactly one** column (e.g. `tile_binning`, `array_exploder`) |
| `columns` | `Optional[List[str]]` | The component operates on **a set** of columns. `None` typically means "auto-detect / all numeric / all" |
| `feature_columns` | `List[str]` | ML features (separate from the prediction target) |
| `target_column` | `str` | ML target |
| `value_column` / `value_columns` | `str` / `List[str]` | The numeric column(s) being aggregated/transformed |
| `date_column` / `timestamp_field` | `str` | Datetime-typed column. Prefer `_column` suffix for new components. |
| `customer_id_field`, `order_id_field`, etc. | `Optional[str]` | When the component has heuristic auto-detection but lets the user override |
| `group_by` | `List[str]` | Group-by keys |

**Avoid:** `group_by_field` (singular, conflicts with `group_by` plural). Use `group_by: [single_col]` instead.

## Strategy vs method

Both are accepted. Prefer **`strategy`** for new components when the field selects an algorithm or approach (e.g. imputation strategy, scaling strategy, outlier clipping strategy — matches sklearn convention).

Use **`method`** when the field selects a *named operation* with no implicit configuration (e.g. join method, aggregation method).

Existing components using either name are valid; don't churn working code just to rename.

## Bin counts

| Canonical | Notes |
|---|---|
| `n_bins` | For tile_binning, lift_chart, histograms, etc. |
| `n_clusters` | For k-means, spatial clustering |

**Avoid:** `n_tiles`, `num_bins`, `bin_count`.

## ML model components

Models that produce DataFrames (predictions, importances, coefficients) should expose:

| Field | Type | Allowed values |
|---|---|---|
| `target_column` | `str` | required |
| `feature_columns` | `List[str]` | required |
| `task_type` | `str` | `classification` \| `regression` |
| `output_mode` | `str` | `predictions` \| `feature_importance` \| `coefficients` |
| `test_size` | `float` | default 0.2 |
| `random_state` | `int` | default 42 |

`output_mode` is what lets a single model component produce parallel branches (predictions + importance, etc.).

**Avoid:** `random_seed` (use `random_state` — sklearn convention).

## Filtering

| Canonical | Use when |
|---|---|
| `condition: str` | Single pandas-query expression (e.g. `"age > 18 and country == 'US'"`) |
| `conditions: List[Dict]` | Builder-UI-friendly multi-criteria (e.g. `[{column: age, operator: gt, value: 18}, ...]`) |

The two are different complexity levels. A component should pick one based on its target audience (UI builder vs. CLI/YAML author).

## Date/datetime columns

For columns that hold datetime values:

- New components: prefer the `_column` suffix (`date_column`, `timestamp_column`).
- `_field` suffix (`activity_date_field`, `first_date_field`) is allowed for **semantic differentiation** when one component takes multiple datetime columns with distinct meanings.

`first_date_field` should be auto-derivable from `min(activity_date_field)` when not provided — see `cohort_analysis` for the pattern.

## Output format / mode

These are different concepts:

| Field | Domain |
|---|---|
| `output_mode` | Which **shape** the asset emits — `predictions`, `feature_importance`, `coefficients`, etc. |
| `output_format` | Which **render** of the same data — `text`, `json`, `markdown`, `html`. Common in AI/LLM components. |

Both are valid; use whichever fits the component's intent.

## Preview / sample metadata

Every DataFrame-producing component should expose the same two fields so builder UIs (Astronomer Blueprint, Lakeflow Designer, the registry's web UI) can render an asset preview without warehouse access:

| Field | Type | Default |
|---|---|---|
| `include_preview_metadata` | `bool` | `False` |
| `preview_rows` | `int` (1-500) | `25` |

The metadata key is **`preview`** (singular). The render format is `MetadataValue.md(<df>.to_markdown(index=False))`. For `len(df) > preview_rows * 10`, use `df.sample(preview_rows)` instead of `df.head(preview_rows)` so sorted/filtered outputs are represented by their distribution rather than by the top-of-sort.

Wrap the emission in `try/except` so a markdown rendering bug doesn't kill the asset:

```python
if include_preview and len(df) > 0:
    try:
        _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
        metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
    except Exception as e:
        context.log.warning(f"preview emission failed: {e}")
```

## Catalog / governance fields

All components should accept the standard Dagster catalog fields:

| Field | Type |
|---|---|
| `description` | `Optional[str]` |
| `owners` | `Optional[List[str]]` (`team:foo` or `user@host`) |
| `asset_tags` | `Optional[Dict[str, str]]` |
| `kinds` | `Optional[List[str]]` (auto-inferred if unset) |
| `freshness_max_lag_minutes` | `Optional[int]` |
| `freshness_cron` | `Optional[str]` |
| `column_lineage` | `Optional[Dict[str, List[str]]]` (output-col → list-of-input-cols) |

## Partitioning fields

For partitioned assets, components accept this set:

| Field | Type | Notes |
|---|---|---|
| `partition_type` | `Optional[str]` | `daily` \| `weekly` \| `monthly` \| `hourly` \| `static` \| `multi` |
| `partition_start` | `Optional[str]` | ISO date string |
| `partition_date_column` | `Optional[str]` | Column to filter by current date partition |
| `partition_values` | `Optional[str]` | Comma-separated values for static / multi |
| `partition_static_dim` | `Optional[str]` | Dimension name for multi |
| `partition_static_column` | `Optional[str]` | Column to filter by static partition value |

## Retry policy

| Field | Type | Notes |
|---|---|---|
| `retry_policy_max_retries` | `Optional[int]` | Opt-in; if set, defines a `RetryPolicy` |
| `retry_policy_delay_seconds` | `Optional[int]` | Default 1 |
| `retry_policy_backoff` | `str` | `linear` \| `exponential` (default) |

## Output return shape (NOT a field, but a convention)

A DataFrame-producing asset should be **annotated** `-> pd.DataFrame` and **return a bare `df`**, not `Output(value=df, metadata=...)`. The latter raises `DagsterInvariantViolationError` against the bare annotation.

To attach metadata, use `context.add_output_metadata({...})` before the `return df`.

```python
@asset(name=asset_name)
def my_asset(context) -> pd.DataFrame:
    df = ...
    context.add_output_metadata({"row_count": MetadataValue.int(len(df))})
    return df
```

For non-DataFrame producers, return `MaterializeResult(metadata={...})`.

## Backward compatibility on rename

When renaming a field, add the old name as a Pydantic `validation_alias` so existing YAML keeps working:

```python
from pydantic import Field, AliasChoices
upstream_asset_key: str = Field(
    description="...",
    validation_alias=AliasChoices("upstream_asset_key", "source_asset"),
)
```

After two minor releases, the alias can be removed.

---

## Where this list lives

This file is the **source of truth**. It belongs at the repo root so the registry web UI, future component-author docs, and any tooling (linters, scaffolders) can sync against it.
