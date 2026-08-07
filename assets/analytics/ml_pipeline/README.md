# MLPipelineComponent

**One YAML file. Whole ML pipeline.** Sibling of `polars_pipeline`, `warehouse_pipeline`, `pyspark_pipeline`, `snowpark_pipeline` — the "pipeline component" family, for ML.

Standardize what an ML pipeline looks like across your org. `source` + `steps` + `outputs`. Reviewers scan a fixed schema, CI validates against one shape, new hires learn one file and can build any ML pipeline.

## Quick example

```yaml
type: dagster_community_components.MLPipelineComponent
attributes:
  asset_name_prefix: churn
  source:
    kind: warehouse_query
    resource_key: snowflake
    sql: "SELECT * FROM analytics.customer_features WHERE snapshot_date = '{partition_key}'"
  target_column: churned
  feature_columns: [tenure_months, monthly_charges, total_charges, support_tickets]
  steps:
    - {id: imputed, op: impute, strategy: median}
    - {id: scaled,  op: scale,  method: standard}
    - {id: split,   op: split,  test_size: 0.2, stratify_column: churned, random_state: 42}
    - {id: trained, op: grid_search,
                    sklearn_class: "xgboost.XGBClassifier", task_type: classification,
                    param_grid: {n_estimators: [100, 300], max_depth: [4, 6, 8]}, cv: 5}
    - {id: preds,   op: predict, model: trained, input: scaled}
    - {id: metrics, op: evaluate, model: trained, input: preds, task_type: classification}
    - {id: imp,     op: importance, model: trained}
    - {id: saved,   op: save_model, model: trained, path: "/models/churn_{partition_key}.joblib"}
  outputs:
    assets: [preds, metrics, imp, saved]
    table_sinks:
      - {from: preds, resource_key: snowflake, table: churn_predictions,
         schema: ml_output, partition_column: prediction_date, if_exists: append}
```

## Op menu (27 total)

**Preprocessing** (9): `impute`, `scale`, `one_hot_encode`, `label_encode`, `tile_binning`, `outlier_clip`, `missing_indicator`, `quantile_transformer`, `power_transformer`

**Feature generation** (5): `date_features`, `polynomial_features`, `pca`, `tfidf`, `hashing_vectorizer`

**Feature selection** (5): `variance_threshold`, `correlation_filter`, `mutual_info_selection`, `filter`, `select`

**Time-series** (2): `lag_features`, `rolling_window`

**Split + train** (4): `split`, `train`, `grid_search`, `random_search`

**Evaluate + interpret** (7): `predict`, `predict_proba`, `evaluate`, `confusion_matrix`, `importance`, `cross_validate`, `shap_values`

**Persistence** (1): `save_model`

## Model support

**First-class enum** (`model_type: ...`):
- `decision_tree` / `random_forest` / `gradient_boosting` — sklearn tree ensembles (classification + regression)
- `logistic_regression` — sklearn LogisticRegression
- `kmeans` — sklearn KMeans (clustering)

**Escape hatch** (`sklearn_class: "..."`): any estimator with `.fit()` / `.predict()` — XGBoost, LightGBM, HistGradientBoosting, catboost, any custom class.

## Sources — where the data comes from

| kind | Config | Use case |
|---|---|---|
| `url` | `{kind: url, url: ..., delimiter: ','}` | Remote CSVs, public datasets |
| `file` | `{kind: file, path: ..., delimiter: ','}` | Local CSV files |
| `warehouse_query` | `{kind: warehouse_query, resource_key: snowflake, sql: "..."}` | **Production ML** — any Dagster resource with `.get_engine()` or `.get_connection()` |
| `upstream_asset` | `{kind: upstream_asset, upstream_asset_key: raw/events}` | Chain the pipeline downstream of any other Dagster asset |

## Sinks — where the outputs go

| kind | Config | Notes |
|---|---|---|
| `assets` (required) | `[step_id, step_id, ...]` | Step outputs become first-class Dagster assets |
| `csv_sinks` | `[{from: step_id, path: /path/to.csv}]` | Path is `{partition_key}`-aware |
| `parquet_sinks` | `[{from: step_id, path: s3://.../file.parquet}]` | Same shape as csv_sinks |
| `table_sinks` | `[{from: step_id, resource_key: snowflake, table: t, schema: s, if_exists: append, partition_column: date}]` | Write back to warehouse via same resource interface. `partition_column` enables the "single table + partition col" pattern. |

## Partitioning

`MLPipelineComponent` is fully partition-aware. `{partition_key}` templates in source SQL / URL / path get substituted at compute time; sinks get the same treatment. For warehouse table sinks, `partition_column:` writes the partition key as a column on every row (Pattern B — the analytics-friendly default).

Declare partitions via `post_processing:`:

```yaml
type: dagster_community_components.MLPipelineComponent
attributes: {...}

post_processing:
  assets:
    - target: "*"
      attributes:
        partitions_def: {type: daily, start_date: "2025-01-01"}
        automation_condition: "{{ dg.AutomationCondition.eager() }}"
        tags: {tier: gold, team: ml-platform}
        owners: ["ml-team@company.com"]
```

## Walkthrough

Full end-to-end demo + comparison against 7 alternative shapes (raw `@dg.asset`, `@op`+`@graph_multi_asset`, component-per-stage, etc.):

- [`wine_ml.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/wine_ml.md) — shape-selector index
- [`wine_ml_pipeline_component.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/wine_ml_pipeline_component.md) — this component's walkthrough
