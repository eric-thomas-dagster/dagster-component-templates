# data_remediation_asset

Apply an agent-produced plan of data-quality remediations to an upstream DataFrame. Bounded action space — the LLM picks actions **by name**, this component executes them. Every action is auditable via asset metadata.

## Fields

| Field | Type | Description |
|---|---|---|
| `asset_name` | string | Output asset name. |
| `upstream_data_key` | string | Asset key producing the DataFrame to remediate. |
| `plan_key` | string | Asset key producing the plan DataFrame. |
| `fail_on_unknown_action` | bool (default `false`) | If `true`, raise on any unrecognized action name; if `false`, log a warning and skip. |
| `group_name` / `description` / `owners` / `tags` / `kinds` | metadata | Standard asset metadata. |

## Action space (fixed, safe)

| Action | Params | Effect |
|---|---|---|
| `drop_nulls` | `{}` | Drop rows where the target column is null (or all-null if no column given). |
| `fill_nulls` | `{value}` | Fill nulls with a constant. |
| `fill_nulls_with_median` | `{}` | Fill numeric nulls with the median. |
| `fill_nulls_with_mean` | `{}` | Fill numeric nulls with the mean. |
| `fill_nulls_with_mode` | `{}` | Fill nulls with the most common value. |
| `cast_type` | `{dtype}` | Cast the column to a pandas/numpy dtype. |
| `dedup` | `{subset?}` | Drop duplicate rows, optionally on a subset of columns. |
| `clip_outliers` | `{z_max}` | Clip values outside ±`z_max` standard deviations. |
| `filter_range` | `{min?, max?}` | Keep only rows where the column is within a numeric range. |
| `strip_whitespace` | `{}` | Strip leading/trailing whitespace from string columns. |

## Plan DataFrame shape

The plan asset (usually produced by an LLM asset like `langchain_chain_asset`) must be a DataFrame with columns:

- `column` — the column the action applies to (or `null` for row-level ops like `dedup`)
- `action` — the action name from the table above
- `params` — a dict or JSON string of action params (empty `{}` for parameterless actions)
- `reason` (optional) — why the agent picked this action; logged for the audit trail

Example plan:

| column   | action           | params               | reason                            |
|----------|------------------|----------------------|-----------------------------------|
| amount   | fill_nulls       | `{"value": 0}`       | 3% nulls, 0 is a safe default     |
| amount   | clip_outliers    | `{"z_max": 3}`       | 2 rows at z=4.7 look like typos   |
| email    | strip_whitespace | `{}`                 | 8% have trailing whitespace       |
| email    | drop_nulls       | `{}`                 | 1% nulls, can't recover           |
| (empty)  | dedup            | `{"subset": ["id"]}` | 5 duplicate ids                   |

## Example config

```yaml
type: dagster_community_components.DataRemediationAssetComponent
attributes:
  asset_name: cleaned_transactions
  upstream_data_key: raw_transactions
  plan_key: remediation_plan
  group_name: agentic_dq
```

## Metadata emitted

Every run adds asset metadata: rows before/after, actions applied/skipped, the full plan-with-status table, and a preview of the cleaned data. All auditable in the Dagster UI.

## Related

- `langchain_chain_asset` — the natural upstream that produces the plan (LLM reads a DQ report → JSON plan).
- `dataframe_describe` — the natural upstream for the DQ report the LLM diagnoses.
- `enhanced_data_quality_checks` / `pandas_dataframe_check` — non-agentic DQ enforcement (fail the run instead of remediating).
