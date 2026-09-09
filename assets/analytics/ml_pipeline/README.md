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

## Op menu (30 total)

**Preprocessing** (9): `impute`, `scale`, `one_hot_encode`, `label_encode`, `tile_binning`, `outlier_clip`, `missing_indicator`, `quantile_transformer`, `power_transformer`

**Feature generation** (5): `date_features`, `polynomial_features`, `pca`, `tfidf`, `hashing_vectorizer`

**Feature selection** (5): `variance_threshold`, `correlation_filter`, `mutual_info_selection`, `filter`, `select`

**Time-series** (2): `lag_features`, `rolling_window`

**Split + train** (5): `split`, `train`, `grid_search`, `random_search`, `bayesian_search`

**Evaluate + interpret** (7): `predict`, `predict_proba`, `evaluate`, `confusion_matrix`, `importance`, `cross_validate`, `shap_values`

**Persist + Registry** (3): `save_model` (joblib to disk), `register_model` (push a fitted model to MLflow / Snowflake Model Registry), `load_model` (load a versioned model from the Registry into a subsequent pipeline for scoring).

## MLOps: train once, predict often (via a Model Registry)

MLOps = keep training and inference on **separate schedules with a persisted model in between**, plus enough version discipline to roll back a bad model. `register_model` / `load_model` wire two Model Registry backends into the pipeline — no notebooks, no MLflow server code to maintain in your Dagster repo, no model artifacts to shuttle around.

```
  ┌──────────────────────┐        ┌─────────────────────┐        ┌───────────────────────┐
  │ Training pipeline    │        │ Model Registry      │        │ Inference pipeline    │
  │ (weekly cron)        │        │ (MLflow / Snowflake)│        │ (hourly cron)         │
  │                      │  fit + │                     │ load + │                       │
  │ features → train →   │──save─▶│ customer_churn      │◀─score─│ load_model → predict  │
  │   register_model     │        │   v_20260901_...    │        │   model_name: X       │
  │   model_name: X      │        │   v_20260908_... ◀──default  │   model_version: latest│
  │                      │        │   v_20260915_...    │        │                       │
  └──────────────────────┘        └─────────────────────┘        └───────────────────────┘
     writes new version              versioned + governed             reads default (or pin)
```

### `backend: mlflow` (default)

Requires a running MLflow tracking server. **`tracking_uri` (or `tracking_uri_env_var`) is required** — there is no silent fallback to `file:./mlruns/` because that directory vanishes on any ephemeral-disk runtime (Dagster+ Serverless, k8s pods, ECS tasks).

**Training pipeline (weekly):**

```yaml
steps:
  - id: split,   op: split, test_size: 0.2, stratify_column: churned, random_state: 42
  - id: trained, op: train, model_type: gradient_boosting, task_type: classification,
                  params: {n_estimators: 200, max_depth: 6}
  - id: eval,    op: evaluate, model: trained, input: split
  # ↓ persist to Registry
  - id: registered
    op: register_model
    model: trained
    backend: mlflow
    tracking_uri_env_var: MLFLOW_TRACKING_URI    # http://mlflow.internal:5000, databricks, sqlite:///...
    model_name: customer_churn
    # model_version: auto     # default; timestamp-based (v_20260909_123456)
    stage: Staging            # optional; MLflow stage
    description: "Weekly retrain — GBM(n=200,d=6)"
    tags: {trainer: ml_pipeline, dataset_version: v3}
outputs:
  assets: [trained, eval, registered]
```

**Inference pipeline (hourly, separate YAML):**

```yaml
steps:
  - id: model
    op: load_model
    backend: mlflow
    tracking_uri_env_var: MLFLOW_TRACKING_URI
    model_name: customer_churn
    # model_version: latest   # default; also accepts 'staging' | 'production' | literal 'v_20260909_123456'
  - id: preds
    op: predict
    model: model
    # `source: <upstream>` — this pipeline's own feature-prep steps land here
outputs:
  assets: [preds]
  csv_sinks: [{from: preds, path: /warehouse/predictions/{partition_key}.csv}]
```

### `backend: snowflake` — keep everything in the warehouse

Same op shape, different backend. Models live in the Snowflake Model Registry alongside your training data — same RBAC, audit log, replication. Requires a `connection:` dict (same shape as `snowpark_pipeline` — any field may end with `_env_var`).

```yaml
# Training pipeline (same as above, but registration hop swaps backends)
- id: registered
  op: register_model
  model: trained
  backend: snowflake
  connection:
    account_env_var:  SNOWFLAKE_ACCOUNT
    user_env_var:     SNOWFLAKE_USER
    password_env_var: SNOWFLAKE_PASSWORD
    role: TRANSFORMER
    warehouse: COMPUTE_WH
    database: ML
    schema:   MODELS
  # registry_database: ML         # optional — override session's current
  # registry_schema:   MODELS
  model_name: customer_churn
  stage: default                  # optional — promote to default version
  description: "Weekly retrain"
  tags: {trainer: ml_pipeline}
```

**Inference:**

```yaml
- id: model
  op: load_model
  backend: snowflake
  connection: {...same as above...}
  model_name: customer_churn
  # model_version: latest         # default; or literal 'v_20260909_123456'
- id: preds
  op: predict
  model: model
```

The loaded model is wrapped in a shim so downstream `predict` / `predict_proba` ops call it exactly like a freshly-trained sklearn estimator. No pipeline-side changes needed to switch backends.

### Backend picker

| Use MLflow when | Use Snowflake when |
|---|---|
| You already run an MLflow tracking server (or Databricks-managed MLflow) | You're a Snowflake shop and want the model registry inside Snowflake (RBAC, audit, replication all inherited) |
| You want the MLflow UI for browsing runs / models | You want zero extra infra to stand up |
| You need MLflow-flavored artifacts (pyfunc, ONNX, tensorflow, pytorch flavors) | Your inference runs inside Snowflake compute anyway |
| Cross-runtime portability (models scored from anywhere) | Compliance requires "models never leave Snowflake" |

Both backends use the same op names, same `model_name` / `model_version` fields, same `stage` / `description` / `tags`. Swap `backend:` to move between them.

### `bayesian_search` — Optuna TPE sampler

Reach for this instead of `random_search` when hyperparameter tuning has a real cost budget. Optuna's TPE (Tree-structured Parzen Estimator) uses each trial's result to guide the next — typically finds a near-best config in 20–40 trials vs 100+ for random search on the same distribution.

```yaml
- id: tuned
  op: bayesian_search
  task_type: classification
  model_type: gradient_boosting          # or `sklearn_class: xgboost.XGBClassifier`
  base_params: {random_state: 42}         # forwarded unchanged to every trial
  n_trials: 30
  cv: 5
  scoring: f1_weighted                    # optional; defaults to estimator's default scorer
  direction: maximize                     # or 'minimize' for regression MAE / MSE
  timeout: 3600                           # optional wall-clock cap in seconds
  random_state: 42
  param_space:
    n_estimators: {type: int,   low: 50, high: 300}
    max_depth:    {type: int,   low: 3,  high: 15}
    learning_rate: {type: float, low: 0.01, high: 0.3, log: true}
    subsample:    {type: float, low: 0.6, high: 1.0}
    booster:      {type: categorical, choices: [gbtree, dart]}
```

Returns the refit best estimator. `experiment_tracking:` (see below) logs the best params + best CV score automatically.

## Experiment tracking — MLflow / W&B declared once

Opt-in top-level `experiment_tracking:` block. Any `train`, `evaluate`, `cross_validate`, `grid_search`, `random_search`, or `bayesian_search` step in the pipeline auto-logs params + metrics through both backends.

```yaml
experiment_tracking:
  mlflow:
    tracking_uri_env_var: MLFLOW_TRACKING_URI      # http://mlflow.internal / sqlite:///... / etc.
    experiment_name: churn_v3
    run_name_template: "{prefix}_{partition_key}"  # placeholders: prefix / partition_key / run_id
    log_model: true                                 # persist the fitted best estimator as an artifact
    tags:
      env: prod
      owner: analytics-team

  wandb:
    project_env_var: WANDB_PROJECT
    api_key_env_var: WANDB_API_KEY
    run_name_template: "{prefix}_{run_id}"
    tags: [ml_pipeline]
```

**Silently no-ops when the library isn't installed** — declare both backends in the YAML, install `mlflow` in dev, add `wandb` later in prod. No code change.

**Metric prefixing** — every step's metrics are logged as `{step_id}.{metric_name}` so multiple `evaluate` steps (train + validation + test) can share a run without collisions. Example logged names: `tuned.n_estimators`, `tuned.max_depth`, `metrics.accuracy`, `metrics.f1`, `cv.cv_mean_test_score`.

## Rich metadata (emitted on every step listed in `outputs.assets`)

Every step emits a bundle of typed `MetadataValue`s so Dagster+ Insights can turn them into dashboards + alerts.

| Field | Type | Emitted from |
|---|---|---|
| `elapsed_seconds` | Float | every step |
| `rows`, `cols` | Int | any step returning a DataFrame |
| `model_class` | Text | train / *_search |
| `n_estimators`, `max_depth`, `learning_rate`, ... | Int / Float | train / *_search (hyperparameter names come from the discovered params) |
| `accuracy`, `f1`, `precision`, `recall`, `roc_auc` (classification) | Float | `evaluate` |
| `mae`, `rmse`, `r2`, `explained_variance` (regression) | Float | `evaluate` |
| `cv_mean_test_score`, `cv_std_test_score`, `cv_folds` | Float / Int | `cross_validate` |
| `top_feature`, `top_importance` | Text / Float | `importance` |

Promote any of these to a Dagster+ Insights custom metric via the UI — one click, no code.

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

## Per-step resume (`can_subset=True`)

When a downstream step fails or you only need to re-run one output, materialize a strict subset of the pipeline's asset outputs and only the steps transitively needed for those outputs execute. No re-training when you just want new predictions; no re-loading source data when only feature engineering changed.

```bash
# Only re-run `mldemo_predictions` (skips split + train if they've already run
# and only pulls their upstream from state; when they haven't run for this
# partition, they're pulled in transitively via the step-dep closure).
dg launch --assets mldemo_predictions
```

Under the hood: `MLPipelineComponent` builds a step-dep DAG at load time from each step's `source` / `input` / `model` references (plus the implicit `_last_frame_id` fallback for FRAME/APPLY ops without an explicit source). At runtime, `context.selected_output_names` triggers closure computation — only the needed subset runs; sinks whose upstream step was skipped are silently dropped from that run. Same dbt-style per-model resume without splitting the pipeline into N `@asset` decorators.

## Walkthrough

Full end-to-end demo + comparison against 7 alternative shapes (raw `@dg.asset`, `@op`+`@graph_multi_asset`, component-per-stage, etc.):

- [`wine_ml.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/wine_ml.md) — shape-selector index
- [`wine_ml_pipeline_component.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/wine_ml_pipeline_component.md) — this component's walkthrough

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name_prefix` | `str` | Prefix for emitted asset names. Each step listed in outputs.assets becomes '{prefix}_{step_id}'. |
| `source` | `Dict[str, Any]` | Data source. Shapes: {kind: url, url: '...', delimiter: ','} \| {kind: file, path: '...', delimiter: ','} \| {kind: upstream_asset, upstream_asset_key: '...'} |
| `target_column` | `str` | Column to predict. |
| `feature_columns` | `List[str]` | Feature columns for training + prediction. |
| `steps` | `List[Dict[str, Any]]` | Ordered pipeline steps. Each step: {id, op, ...op-specific args}. Steps chain by id — a step with `source: <id>` uses that step's output; omit `source:` and it defaults to the most recent DataFrame in state. |
| `outputs` | `Dict[str, Any]` | Output declaration. Shape: {assets: [<step_ids>], csv_sinks: [{from: <step_id>, path: <path>}]}. `assets:` step outputs become first-class Dagster assets; `csv_sinks:` writes side-outputs to disk without creating assets. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"ml"` | Group name for emitted assets. |
| `kinds` | `List[str]` | — | Kinds for the emitted assets (default: ['python', 'ml']). |
| `tags` | `Dict[str, str]` | — | Tags on the emitted assets. |
| `owners` | `List[str]` | — | Owners on the emitted assets. |
| `description` | `str` | — | Description on the emitted assets. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `experiment_tracking` | `Dict[str, Any]` | — | Experiment-tracker config. Auto-logged from train / evaluate / cross_validate / grid_search / random_search / bayesian_search steps. Two backends (either OR both): `mlflow:` — `{tracking_uri_env_var, experiment_name, run… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)
