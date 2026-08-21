"""MLPipelineComponent — single-asset multi-step ML pipeline.

Same "pipeline component" shape as `polars_pipeline`, `warehouse_pipeline`,
`pyspark_pipeline`, `snowpark_pipeline`: **one YAML file declares the whole
pipeline**, `steps:` list defines the DAG in reading order, `outputs:`
declares which step outputs become first-class Dagster assets and where side
CSVs get written.

Example (Wine ML pipeline in one component):

    type: dagster_community_components.MLPipelineComponent
    attributes:
      asset_name_prefix: wine_ml
      source:
        kind: url
        url: "https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"
        delimiter: ";"
      target_column: quality
      feature_columns:
        - fixed acidity
        - volatile acidity
        - citric acid
        # ... 8 more
      steps:
        - {id: scaled,  op: scale, method: standard}
        - {id: split,   op: split, test_size: 0.2, stratify_column: quality, random_state: 42}
        - {id: trained, op: train, model_type: decision_tree, task_type: classification, params: {max_depth: 6}}
        - {id: preds,   op: predict, model: trained, on: scaled}
        - {id: imp,     op: importance, model: trained}
        - {id: cv,      op: cross_validate, source: scaled, model_type: decision_tree,
                        task_type: classification, params: {max_depth: 6}, cv: 5}
      outputs:
        assets: [preds, imp, cv]
        csv_sinks:
          - {from: preds, path: /tmp/wine_predictions.csv}
          - {from: imp,   path: /tmp/wine_importance.csv}
          - {from: cv,    path: /tmp/wine_cv.csv}

Standardization — that's the point. Every ML pipeline in the org uses
the same YAML shape, the same ops, the same output conventions.
Reviewers, tests, and CI can validate against ONE schema.

Op coverage:

- Ingestion:      url | file | upstream_asset | dataframe (Python-source only)
- Feature eng:    scale, impute, one_hot_encode, label_encode, tile_binning,
                  outlier_clip, filter, select
- Split:          split
- Model train:    train (model_type enum OR sklearn_class escape hatch)
- Evaluation:    predict, predict_proba, importance, cross_validate
- Sinks:          csv_sinks (parquet_sinks + table_sinks are follow-ups)

Model type coverage:

- First-class enum (task-aware): decision_tree, random_forest,
  gradient_boosting, logistic_regression, kmeans.
- Escape hatch: `sklearn_class: "sklearn.ensemble.HistGradientBoostingClassifier"`
  — any sklearn-compatible estimator (or any estimator with .fit / .predict).
- `params:` dict is forwarded to the model constructor unchanged.
"""
import importlib
import re
from io import StringIO
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# ── Model registry — first-class enum → sklearn class path ──────────────

_MODEL_REGISTRY: Dict[tuple[str, str], str] = {
    ("decision_tree",       "classification"): "sklearn.tree.DecisionTreeClassifier",
    ("decision_tree",       "regression"):     "sklearn.tree.DecisionTreeRegressor",
    ("random_forest",       "classification"): "sklearn.ensemble.RandomForestClassifier",
    ("random_forest",       "regression"):     "sklearn.ensemble.RandomForestRegressor",
    ("gradient_boosting",   "classification"): "sklearn.ensemble.GradientBoostingClassifier",
    ("gradient_boosting",   "regression"):     "sklearn.ensemble.GradientBoostingRegressor",
    ("logistic_regression", "classification"): "sklearn.linear_model.LogisticRegression",
    ("kmeans",              "clustering"):     "sklearn.cluster.KMeans",
}


def _resolve_model_class(model_type: Optional[str], sklearn_class: Optional[str], task_type: str):
    """Map a first-class enum OR sklearn_class dotted path to an estimator class."""
    if sklearn_class:
        module_path, class_name = sklearn_class.rsplit(".", 1)
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)
    if not model_type:
        raise ValueError("either model_type or sklearn_class must be set")
    key = (model_type, task_type)
    if key not in _MODEL_REGISTRY:
        raise ValueError(
            f"unsupported model_type={model_type!r} for task_type={task_type!r}; "
            f"valid combos: {sorted(_MODEL_REGISTRY.keys())}. "
            f"Use `sklearn_class:` for any other estimator."
        )
    return _resolve_model_class(None, _MODEL_REGISTRY[key], task_type)


# ── Ingestion executors ────────────────────────────────────────────────

def _apply_partition_template(s: str, partition_key: Optional[str]) -> str:
    """Substitute `{partition_key}` and `{partition_date}` in a string template.

    Safe for non-partitioned assets (returns the string unchanged if partition_key
    is None or the template has no placeholders). Wraps in a try/format_map so
    unknown placeholders are preserved as literals rather than raising.
    """
    if not partition_key or not s or "{" not in s:
        return s
    class _SafeDict(dict):
        def __missing__(self, key):  # pragma: no cover - defensive
            return "{" + key + "}"
    try:
        return s.format_map(_SafeDict({
            "partition_key": str(partition_key),
            "partition_date": str(partition_key),
        }))
    except Exception:
        return s


def _ingest(source_config: dict, context, partition_key: Optional[str] = None) -> Any:
    """Return a pandas DataFrame from the source config.

    Partition-aware: `{partition_key}` placeholders in `url` / `path` / `sql`
    are substituted from `context.partition_key` at compute time.
    """
    import pandas as pd
    kind = source_config.get("kind", "url")
    if kind == "url":
        import requests
        url = _apply_partition_template(source_config["url"], partition_key)
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        return pd.read_csv(StringIO(resp.text), sep=source_config.get("delimiter", ","))
    if kind == "file":
        path = _apply_partition_template(source_config["path"], partition_key)
        return pd.read_csv(path, sep=source_config.get("delimiter", ","))
    if kind == "upstream_asset":
        # Loaded by Dagster via IO manager and passed as an arg to the pipeline;
        # this function is only called for kind=url|file. See build_defs wiring.
        raise RuntimeError("upstream_asset ingest is handled by Dagster IO manager")
    if kind == "dataframe":
        # In-process DataFrame supplied programmatically (used from Python instantiation).
        return source_config["dataframe"]
    if kind == "warehouse_query":
        # Execute SQL via a Dagster resource that exposes .get_engine() (SQLAlchemy)
        # OR .get_connection() (DB-API). Works out-of-the-box with the community
        # resources: postgres_resource, mysql_resource, mssql_resource,
        # snowflake_resource, bigquery_resource, and any custom resource that
        # implements the same interface.
        #
        # Partition-aware: `{partition_key}` in `sql:` gets substituted before
        # execution. Enables per-partition SQL like:
        #   sql: "SELECT * FROM events WHERE event_date = '{partition_key}'"
        resource_key = source_config["resource_key"]
        sql = _apply_partition_template(source_config["sql"], partition_key)
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            engine = resource.get_engine()
            return pd.read_sql(sql, engine)
        if hasattr(resource, "get_connection"):
            conn = resource.get_connection()
            return pd.read_sql(sql, conn)
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy) "
            f"or .get_connection() (DB-API); got {type(resource).__name__}"
        )
    raise ValueError(f"unsupported source kind: {kind!r}")


# ── Per-op executors ───────────────────────────────────────────────────
#
# Each executor takes (input_frame_or_model, step_config, target, features,
# context) and returns the produced object (DataFrame or fitted model).

def _do_impute(df, step: dict, target: str, features: list, context):
    import pandas as pd
    from sklearn.impute import SimpleImputer
    strategy = step.get("strategy", "median")
    cols = step.get("columns") or [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    fill_value = step.get("constant_value")
    kwargs = {"strategy": strategy}
    if strategy == "constant":
        kwargs["fill_value"] = fill_value
    imp = SimpleImputer(**kwargs)
    out = df.copy()
    out[cols] = imp.fit_transform(out[cols])
    return out


def _do_scale(df, step: dict, target: str, features: list, context):
    method = step.get("method", "standard")
    cols = step.get("columns") or features
    if method == "standard":
        from sklearn.preprocessing import StandardScaler as C
    elif method == "minmax":
        from sklearn.preprocessing import MinMaxScaler as C
    elif method == "robust":
        from sklearn.preprocessing import RobustScaler as C
    else:
        raise ValueError(f"unsupported scale method: {method!r}")
    out = df.copy()
    out[cols] = C().fit_transform(out[cols])
    return out


def _do_one_hot_encode(df, step: dict, target: str, features: list, context):
    import pandas as pd
    cols = step["columns"]
    drop_first = step.get("drop_first", False)
    return pd.get_dummies(df, columns=cols, drop_first=drop_first)


def _do_label_encode(df, step: dict, target: str, features: list, context):
    from sklearn.preprocessing import LabelEncoder
    cols = step["columns"]
    out = df.copy()
    for c in cols:
        out[c] = LabelEncoder().fit_transform(out[c].astype(str))
    return out


def _do_tile_binning(df, step: dict, target: str, features: list, context):
    import pandas as pd
    column = step["column"]
    n_bins = step.get("n_bins", 4)
    output_column = step.get("output_column", f"{column}_bin")
    out = df.copy()
    out[output_column] = pd.qcut(out[column], q=n_bins, labels=False, duplicates="drop")
    return out


def _do_outlier_clip(df, step: dict, target: str, features: list, context):
    cols = step.get("columns") or features
    multiplier = step.get("iqr_multiplier", 1.5)
    out = df.copy()
    for c in cols:
        q1 = out[c].quantile(0.25)
        q3 = out[c].quantile(0.75)
        iqr = q3 - q1
        low, high = q1 - multiplier * iqr, q3 + multiplier * iqr
        out[c] = out[c].clip(low, high)
    return out


def _do_filter(df, step: dict, target: str, features: list, context):
    predicate = step["predicate"]  # pandas.DataFrame.query syntax
    return df.query(predicate).reset_index(drop=True)


def _do_select(df, step: dict, target: str, features: list, context):
    cols = step["columns"]
    return df[cols].copy()


def _do_date_features(df, step: dict, target: str, features: list, context):
    """Extract year/month/day/weekday/hour from a datetime column."""
    import pandas as pd
    column = step["column"]
    parts = step.get("parts") or ["year", "month", "day", "weekday"]
    out = df.copy()
    dt = pd.to_datetime(out[column], errors="coerce")
    for part in parts:
        out[f"{column}_{part}"] = getattr(dt.dt, part) if part != "weekday" else dt.dt.weekday
    if step.get("drop_original", False):
        out = out.drop(columns=[column])
    return out


def _do_polynomial_features(df, step: dict, target: str, features: list, context):
    """sklearn PolynomialFeatures on named columns."""
    import pandas as pd
    from sklearn.preprocessing import PolynomialFeatures
    cols = step.get("columns") or features
    degree = step.get("degree", 2)
    interaction_only = step.get("interaction_only", False)
    include_bias = step.get("include_bias", False)
    poly = PolynomialFeatures(
        degree=degree, interaction_only=interaction_only, include_bias=include_bias,
    )
    out = df.copy()
    poly_arr = poly.fit_transform(out[cols])
    names = poly.get_feature_names_out(cols)
    poly_df = pd.DataFrame(poly_arr, columns=names, index=out.index)
    # Preserve original columns; the new poly cols get an underscored prefix.
    return out.join(poly_df.drop(columns=[c for c in cols if c in poly_df.columns], errors="ignore"))


def _do_pca(df, step: dict, target: str, features: list, context):
    """PCA dimensionality reduction — replaces features with PC1..PCn."""
    import pandas as pd
    from sklearn.decomposition import PCA
    cols = step.get("columns") or features
    n_components = step.get("n_components", 2)
    pca = PCA(n_components=n_components, random_state=step.get("random_state", 42))
    arr = pca.fit_transform(df[cols])
    pc_names = [f"pc{i+1}" for i in range(n_components)]
    out = df.copy()
    for name, values in zip(pc_names, arr.T):
        out[name] = values
    if step.get("drop_original", False):
        out = out.drop(columns=cols)
    context.log.info(f"PCA {len(cols)}→{n_components} — explained variance: {pca.explained_variance_ratio_}")
    return out


def _do_split(df, step: dict, target: str, features: list, context):
    """Adds a 'split' column with values 'train' / 'test' (and optionally 'val')."""
    import pandas as pd
    from sklearn.model_selection import train_test_split
    test_size = step.get("test_size", 0.2)
    val_size = step.get("validation_size", 0.0)
    stratify_col = step.get("stratify_column")
    random_state = step.get("random_state", 42)
    output_col = step.get("output_split_column", "split")

    strat = df[stratify_col] if stratify_col else None
    train, test = train_test_split(
        df, test_size=test_size, random_state=random_state, stratify=strat,
    )
    if val_size > 0.0:
        # Peel a val set from train.
        rel_val = val_size / (1.0 - test_size)
        strat2 = train[stratify_col] if stratify_col else None
        train, val = train_test_split(
            train, test_size=rel_val, random_state=random_state, stratify=strat2,
        )
        parts = [
            train.assign(**{output_col: "train"}),
            val.assign(**{output_col: "val"}),
            test.assign(**{output_col: "test"}),
        ]
    else:
        parts = [
            train.assign(**{output_col: "train"}),
            test.assign(**{output_col: "test"}),
        ]
    return pd.concat(parts, ignore_index=True)


# ── Experiment tracker (MLflow + Weights & Biases) ─────────────────────
#
# Wraps both backends behind a small interface. Init once in build_defs,
# stash in state["__tracker__"], and every train / evaluate / *_search /
# cross_validate op logs params/metrics/artifacts through it. Silently
# no-ops when the library isn't installed OR when experiment_tracking:
# is unset.


class _ExperimentTracker:
    """Thin wrapper over mlflow + wandb. Either OR both may be active."""

    def __init__(self, cfg: Optional[Dict[str, Any]], run_context: Dict[str, str], log):
        self.cfg = cfg or {}
        self.run_context = run_context
        self.log = log
        self.mlflow = None
        self.wandb = None
        self._active = False
        self._init_mlflow()
        self._init_wandb()

    def _init_mlflow(self):
        c = self.cfg.get("mlflow")
        if not c:
            return
        try:
            import mlflow
        except ImportError:
            self.log.warning("experiment_tracking.mlflow declared but `mlflow` not installed — skipping.")
            return
        import os as _os
        uri_env = c.get("tracking_uri_env_var")
        if uri_env and _os.environ.get(uri_env):
            mlflow.set_tracking_uri(_os.environ[uri_env])
        exp = c.get("experiment_name")
        if exp:
            mlflow.set_experiment(exp)
        run_name = _render_run_name(c.get("run_name_template"), self.run_context)
        active = mlflow.start_run(run_name=run_name)
        for k, v in (c.get("tags") or {}).items():
            mlflow.set_tag(k, v)
        self.mlflow = mlflow
        self._active = True
        self.log.info(f"mlflow run started: {active.info.run_id} (experiment={exp!r})")

    def _init_wandb(self):
        c = self.cfg.get("wandb")
        if not c:
            return
        try:
            import wandb
        except ImportError:
            self.log.warning("experiment_tracking.wandb declared but `wandb` not installed — skipping.")
            return
        import os as _os
        api_key_env = c.get("api_key_env_var") or "WANDB_API_KEY"
        api_key = _os.environ.get(api_key_env)
        if api_key:
            _os.environ["WANDB_API_KEY"] = api_key
        project_env = c.get("project_env_var")
        project = _os.environ.get(project_env) if project_env else c.get("project")
        entity_env = c.get("entity_env_var")
        entity = _os.environ.get(entity_env) if entity_env else c.get("entity")
        run_name = _render_run_name(c.get("run_name_template"), self.run_context)
        run = wandb.init(
            project=project, entity=entity, name=run_name,
            tags=c.get("tags") or None, reinit=True,
        )
        self.wandb = wandb
        self._active = True
        self.log.info(f"wandb run started: {run.name!r} (project={project!r})")

    def log_params(self, step_id: str, params: Dict[str, Any]):
        if not params or not self._active:
            return
        prefixed = {f"{step_id}.{k}": v for k, v in params.items()}
        if self.mlflow:
            try:
                self.mlflow.log_params({k: str(v) for k, v in prefixed.items()})
            except Exception as e:  # noqa: BLE001
                self.log.warning(f"mlflow.log_params failed: {e}")
        if self.wandb:
            try:
                self.wandb.config.update(prefixed, allow_val_change=True)
            except Exception as e:  # noqa: BLE001
                self.log.warning(f"wandb.config.update failed: {e}")

    def log_metrics(self, step_id: str, metrics: Dict[str, float]):
        if not metrics or not self._active:
            return
        prefixed = {f"{step_id}.{k}": float(v) for k, v in metrics.items() if isinstance(v, (int, float))}
        if self.mlflow:
            try:
                self.mlflow.log_metrics(prefixed)
            except Exception as e:  # noqa: BLE001
                self.log.warning(f"mlflow.log_metrics failed: {e}")
        if self.wandb:
            try:
                self.wandb.log(prefixed)
            except Exception as e:  # noqa: BLE001
                self.log.warning(f"wandb.log failed: {e}")

    def log_model(self, step_id: str, model, features: list):
        if not self._active:
            return
        c = (self.cfg.get("mlflow") or {})
        if self.mlflow and c.get("log_model"):
            try:
                self.mlflow.sklearn.log_model(model, artifact_path=step_id)
            except Exception as e:  # noqa: BLE001
                self.log.warning(f"mlflow.log_model failed: {e}")
        # W&B model logging is left to the user's own artifact code — auto-uploading
        # can hit W&B storage quotas surprisingly quickly.

    def end(self):
        if self.mlflow:
            try:
                self.mlflow.end_run()
            except Exception:
                pass
        if self.wandb:
            try:
                self.wandb.finish()
            except Exception:
                pass


def _render_run_name(template: Optional[str], ctx: Dict[str, str]) -> Optional[str]:
    if not template:
        return None
    out = template
    for k, v in ctx.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def _tracker(state: Dict[str, Any]) -> Optional[_ExperimentTracker]:
    """Fetch the tracker stashed in state (may be None)."""
    return state.get("__tracker__") if isinstance(state, dict) else None


def _step_meta(state: Dict[str, Any], step_id: str, **fields):
    """Append MetadataValue-compatible fields for a step's output.
    Consumed after _run_step completes to call context.add_output_metadata."""
    if not isinstance(state.get("__step_metadata__"), dict):
        return
    state["__step_metadata__"].setdefault(step_id, {}).update(fields)


def _do_train(df, step: dict, target: str, features: list, context):
    """Fit an estimator on the 'train' subset (or the whole df if no split column)."""
    output_col = step.get("split_column", "split")
    train_df = df[df[output_col] == "train"] if output_col in df.columns else df

    cls = _resolve_model_class(
        step.get("model_type"), step.get("sklearn_class"), step.get("task_type", "classification"),
    )
    params = step.get("params", {}) or {}
    model = cls(**params)
    model.fit(train_df[features], train_df[target])
    context.log.info(
        f"trained {cls.__name__} on {len(train_df)} rows, {len(features)} features, "
        f"params={params}"
    )
    return model


def _do_predict(model, df, step: dict, target: str, features: list, context):
    out = df.copy()
    out["predicted"] = model.predict(df[features])
    return out


def _do_predict_proba(model, df, step: dict, target: str, features: list, context):
    import pandas as pd
    proba = model.predict_proba(df[features])
    classes = list(getattr(model, "classes_", range(proba.shape[1])))
    proba_df = pd.DataFrame(proba, columns=[f"proba_{c}" for c in classes], index=df.index)
    return df.join(proba_df)


def _do_importance(model, step: dict, target: str, features: list, context):
    import pandas as pd
    if hasattr(model, "feature_importances_"):
        vals = model.feature_importances_
    elif hasattr(model, "coef_"):
        # Absolute coefficient magnitude as a proxy for importance
        coef = model.coef_
        vals = abs(coef).mean(axis=0) if coef.ndim > 1 else abs(coef)
    else:
        raise ValueError(
            f"model {type(model).__name__} has neither feature_importances_ nor coef_"
        )
    return pd.DataFrame({"feature": features, "importance": vals}).sort_values(
        "importance", ascending=False,
    )


# ── Feature engineering — 6 more ops ──────────────────────────────────

def _do_missing_indicator(df, step: dict, target: str, features: list, context):
    """Add boolean columns flagging null presence for each named column."""
    cols = step.get("columns") or features
    suffix = step.get("suffix", "_is_missing")
    out = df.copy()
    for c in cols:
        out[f"{c}{suffix}"] = out[c].isna()
    return out


def _do_quantile_transformer(df, step: dict, target: str, features: list, context):
    """sklearn QuantileTransformer — reshape features to a uniform/normal distribution."""
    from sklearn.preprocessing import QuantileTransformer
    cols = step.get("columns") or features
    output_distribution = step.get("output_distribution", "uniform")
    n_quantiles = step.get("n_quantiles", 1000)
    qt = QuantileTransformer(output_distribution=output_distribution,
                             n_quantiles=min(n_quantiles, len(df)))
    out = df.copy()
    out[cols] = qt.fit_transform(out[cols])
    return out


def _do_power_transformer(df, step: dict, target: str, features: list, context):
    """sklearn PowerTransformer — Yeo-Johnson (default) or Box-Cox to make more Gaussian."""
    from sklearn.preprocessing import PowerTransformer
    cols = step.get("columns") or features
    method = step.get("method", "yeo-johnson")
    standardize = step.get("standardize", True)
    pt = PowerTransformer(method=method, standardize=standardize)
    out = df.copy()
    out[cols] = pt.fit_transform(out[cols])
    return out


def _do_tfidf(df, step: dict, target: str, features: list, context):
    """sklearn TfidfVectorizer on a text column → adds tfidf_* columns."""
    import pandas as pd
    from sklearn.feature_extraction.text import TfidfVectorizer
    text_column = step["text_column"]
    max_features = step.get("max_features", 100)
    ngram_range = tuple(step.get("ngram_range", [1, 1]))
    stop_words = step.get("stop_words")
    vec = TfidfVectorizer(max_features=max_features, ngram_range=ngram_range, stop_words=stop_words)
    mat = vec.fit_transform(df[text_column].fillna("").astype(str))
    names = [f"tfidf_{term}" for term in vec.get_feature_names_out()]
    tfidf_df = pd.DataFrame(mat.toarray(), columns=names, index=df.index)
    context.log.info(f"TF-IDF on {text_column!r} → {len(names)} features")
    return df.join(tfidf_df)


def _do_hashing_vectorizer(df, step: dict, target: str, features: list, context):
    """sklearn HashingVectorizer on a text column → adds hash_0..hash_N columns.
    Stateless (no vocabulary fit) — good for streaming / very large corpora."""
    import pandas as pd
    from sklearn.feature_extraction.text import HashingVectorizer
    text_column = step["text_column"]
    n_features = step.get("n_features", 128)
    ngram_range = tuple(step.get("ngram_range", [1, 1]))
    vec = HashingVectorizer(n_features=n_features, ngram_range=ngram_range, alternate_sign=False)
    mat = vec.fit_transform(df[text_column].fillna("").astype(str))
    names = [f"hash_{i}" for i in range(n_features)]
    hash_df = pd.DataFrame(mat.toarray(), columns=names, index=df.index)
    return df.join(hash_df)


def _do_lag_features(df, step: dict, target: str, features: list, context):
    """Add lag columns from a value column — for time-series feature engineering."""
    value_column = step["value_column"]
    lags = step.get("lags", [1, 7, 30])
    group_by = step.get("group_by")   # optional: compute lags within groups
    out = df.copy()
    if group_by:
        for lag in lags:
            out[f"{value_column}_lag_{lag}"] = out.groupby(group_by)[value_column].shift(lag)
    else:
        for lag in lags:
            out[f"{value_column}_lag_{lag}"] = out[value_column].shift(lag)
    return out


def _do_rolling_window(df, step: dict, target: str, features: list, context):
    """Add rolling-window aggregates from a value column — mean / std / min / max / sum."""
    value_column = step["value_column"]
    windows = step.get("windows", [7, 30])
    aggs = step.get("aggregations", ["mean", "std"])
    group_by = step.get("group_by")
    out = df.copy()
    for w in windows:
        for agg in aggs:
            col = f"{value_column}_roll_{w}_{agg}"
            if group_by:
                out[col] = out.groupby(group_by)[value_column].transform(lambda x: getattr(x.rolling(w, min_periods=1), agg)())
            else:
                out[col] = getattr(out[value_column].rolling(w, min_periods=1), agg)()
    return out


# ── Feature selection — 3 ops ──────────────────────────────────────────

def _do_variance_threshold(df, step: dict, target: str, features: list, context):
    """Drop features with variance below a threshold."""
    from sklearn.feature_selection import VarianceThreshold
    cols = step.get("columns") or features
    threshold = step.get("threshold", 0.0)
    vt = VarianceThreshold(threshold=threshold)
    vt.fit(df[cols])
    kept = [c for c, keep in zip(cols, vt.get_support()) if keep]
    dropped = set(cols) - set(kept)
    if dropped:
        context.log.info(f"variance_threshold dropped {len(dropped)} low-variance features: {sorted(dropped)}")
    return df[[c for c in df.columns if c not in dropped]].copy()


def _do_correlation_filter(df, step: dict, target: str, features: list, context):
    """Drop features that are highly correlated with each other (upper-triangle drop)."""
    cols = step.get("columns") or features
    threshold = step.get("threshold", 0.95)
    method = step.get("method", "pearson")   # pearson | spearman | kendall
    corr = df[cols].corr(method=method).abs()
    upper = corr.where(_upper_triangle_mask(corr))
    to_drop = [c for c in upper.columns if (upper[c] > threshold).any()]
    if to_drop:
        context.log.info(f"correlation_filter dropped {len(to_drop)} correlated features (>{threshold}, {method}): {to_drop}")
    return df.drop(columns=to_drop)


def _upper_triangle_mask(corr):
    import numpy as np
    return np.triu(np.ones(corr.shape, dtype=bool), k=1)


def _do_mutual_info_selection(df, step: dict, target: str, features: list, context):
    """Keep top-K features by mutual information with the target."""
    cols = step.get("columns") or features
    k = step.get("k", 20)
    task = step.get("task_type", "classification")
    if task == "classification":
        from sklearn.feature_selection import mutual_info_classif as mi_func
    else:
        from sklearn.feature_selection import mutual_info_regression as mi_func
    scores = mi_func(df[cols], df[target], random_state=step.get("random_state", 42))
    ranked = sorted(zip(cols, scores), key=lambda p: p[1], reverse=True)
    keep = [c for c, _ in ranked[:k]]
    drop = [c for c in cols if c not in keep]
    if drop:
        context.log.info(f"mutual_info_selection kept top {k} features; dropped {len(drop)}")
    return df.drop(columns=drop)


# ── Hyperparameter tuning — 2 ops (produce a model) ────────────────────

def _do_grid_search(df, step: dict, target: str, features: list, context):
    """sklearn GridSearchCV — returns the best fitted estimator."""
    from sklearn.model_selection import GridSearchCV
    output_col = step.get("split_column", "split")
    train_df = df[df[output_col] == "train"] if output_col in df.columns else df

    cls = _resolve_model_class(step.get("model_type"), step.get("sklearn_class"), step.get("task_type", "classification"))
    base_params = step.get("base_params", {}) or {}
    param_grid = step["param_grid"]
    cv = step.get("cv", 5)
    scoring = step.get("scoring")
    n_jobs = step.get("n_jobs", -1)
    gs = GridSearchCV(cls(**base_params), param_grid=param_grid, cv=cv, scoring=scoring, n_jobs=n_jobs, refit=True)
    gs.fit(train_df[features], train_df[target])
    context.log.info(f"grid_search: best_params={gs.best_params_}, best_score={gs.best_score_:.4f}")
    return gs.best_estimator_


def _do_random_search(df, step: dict, target: str, features: list, context):
    """sklearn RandomizedSearchCV — returns the best fitted estimator."""
    from sklearn.model_selection import RandomizedSearchCV
    output_col = step.get("split_column", "split")
    train_df = df[df[output_col] == "train"] if output_col in df.columns else df

    cls = _resolve_model_class(step.get("model_type"), step.get("sklearn_class"), step.get("task_type", "classification"))
    base_params = step.get("base_params", {}) or {}
    param_distributions = step["param_distributions"]
    n_iter = step.get("n_iter", 20)
    cv = step.get("cv", 5)
    scoring = step.get("scoring")
    n_jobs = step.get("n_jobs", -1)
    random_state = step.get("random_state", 42)
    rs = RandomizedSearchCV(
        cls(**base_params), param_distributions=param_distributions, n_iter=n_iter,
        cv=cv, scoring=scoring, n_jobs=n_jobs, random_state=random_state, refit=True,
    )
    rs.fit(train_df[features], train_df[target])
    context.log.info(f"random_search: best_params={rs.best_params_}, best_score={rs.best_score_:.4f}")
    return rs.best_estimator_


def _do_bayesian_search(df, step: dict, target: str, features: list, context):
    """Optuna-driven Bayesian hyperparameter search.

    Config:
      model_type / sklearn_class + task_type — same as train.
      base_params: dict — fixed params passed to the model constructor.
      param_space: dict[name, {type, low, high, [log], [step], [choices]}]
                   type ∈ {int, float, categorical}
      n_trials: int (default 30)
      cv: int (default 5) — CV folds per trial.
      scoring: str — sklearn scorer name (default None → estimator's default).
      direction: 'maximize' | 'minimize' (default 'maximize').
      timeout: int — seconds to cap the full search (default None = no cap).
      random_state: int (default 42).

    Returns the best fitted estimator, refit on the full training data.
    """
    try:
        import optuna
    except ImportError:
        raise ImportError("bayesian_search requires optuna: pip install optuna")
    from sklearn.model_selection import cross_val_score

    output_col = step.get("split_column", "split")
    train_df = df[df[output_col] == "train"] if output_col in df.columns else df

    cls = _resolve_model_class(step.get("model_type"), step.get("sklearn_class"), step.get("task_type", "classification"))
    base_params = step.get("base_params", {}) or {}
    param_space = step.get("param_space") or {}
    if not param_space:
        raise ValueError("bayesian_search: `param_space:` is required (see README).")
    n_trials = int(step.get("n_trials", 30))
    cv = int(step.get("cv", 5))
    scoring = step.get("scoring")
    direction = step.get("direction", "maximize")
    timeout = step.get("timeout")
    random_state = int(step.get("random_state", 42))

    def _suggest(trial, name: str, spec: dict):
        t = spec.get("type", "float")
        if t == "int":
            return trial.suggest_int(name, spec["low"], spec["high"], step=spec.get("step", 1))
        if t == "float":
            return trial.suggest_float(name, spec["low"], spec["high"], log=bool(spec.get("log")))
        if t == "categorical":
            return trial.suggest_categorical(name, spec["choices"])
        raise ValueError(f"bayesian_search param_space[{name}]: unknown type {t!r}")

    def _objective(trial):
        trial_params = {**base_params}
        for name, spec in param_space.items():
            trial_params[name] = _suggest(trial, name, spec)
        model = cls(**trial_params)
        scores = cross_val_score(model, train_df[features], train_df[target], cv=cv, scoring=scoring)
        return float(scores.mean())

    sampler = optuna.samplers.TPESampler(seed=random_state)
    study = optuna.create_study(direction=direction, sampler=sampler)
    study.optimize(_objective, n_trials=n_trials, timeout=timeout, show_progress_bar=False)

    context.log.info(
        f"bayesian_search: best_params={study.best_params} best_value={study.best_value:.4f} "
        f"(trials={len(study.trials)})"
    )
    # Refit on the full train set with the best params.
    best = cls(**{**base_params, **study.best_params})
    best.fit(train_df[features], train_df[target])
    return best


# ── Model apply — 3 more (evaluate + confusion + shap) ─────────────────

def _do_evaluate(model, df, step: dict, target: str, features: list, context):
    """Emit a standard metrics DataFrame for the given task_type.

    Classification: accuracy, precision, recall, f1, roc_auc (if binary).
    Regression: mae, rmse, r2, explained_variance.
    """
    import pandas as pd
    task = step.get("task_type", "classification")
    y_true = df[target]
    y_pred = df["predicted"] if "predicted" in df.columns else model.predict(df[features])
    metrics = {}
    if task == "classification":
        from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
        metrics["accuracy"] = accuracy_score(y_true, y_pred)
        avg = step.get("average", "weighted")
        for name, fn in [("precision", precision_score), ("recall", recall_score), ("f1", f1_score)]:
            metrics[name] = fn(y_true, y_pred, average=avg, zero_division=0)
        # ROC-AUC only for binary problems where the model exposes predict_proba
        try:
            from sklearn.metrics import roc_auc_score
            if hasattr(model, "predict_proba"):
                proba = model.predict_proba(df[features])
                if proba.shape[1] == 2:
                    metrics["roc_auc"] = roc_auc_score(y_true, proba[:, 1])
                else:
                    metrics["roc_auc_ovr"] = roc_auc_score(y_true, proba, multi_class="ovr", average=avg)
        except Exception:
            pass
    else:
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, explained_variance_score
        metrics["mae"] = mean_absolute_error(y_true, y_pred)
        mse = mean_squared_error(y_true, y_pred)
        metrics["mse"] = mse
        metrics["rmse"] = mse ** 0.5
        metrics["r2"] = r2_score(y_true, y_pred)
        metrics["explained_variance"] = explained_variance_score(y_true, y_pred)
    return pd.DataFrame({"metric": list(metrics.keys()), "value": list(metrics.values())})


def _do_confusion_matrix(model, df, step: dict, target: str, features: list, context):
    """Emit the confusion matrix as a DataFrame (rows=true, cols=predicted)."""
    import pandas as pd
    from sklearn.metrics import confusion_matrix
    y_true = df[target]
    y_pred = df["predicted"] if "predicted" in df.columns else model.predict(df[features])
    labels = sorted(set(y_true) | set(y_pred))
    matrix = confusion_matrix(y_true, y_pred, labels=labels)
    return pd.DataFrame(matrix, index=[f"true_{l}" for l in labels], columns=[f"pred_{l}" for l in labels]).reset_index().rename(columns={"index": "label"})


def _do_shap_values(model, df, step: dict, target: str, features: list, context):
    """Per-row SHAP values as a DataFrame with columns shap_<feature>."""
    import pandas as pd
    try:
        import shap
    except ImportError:
        raise ImportError("shap_values op requires `shap`: pip install shap")
    n_sample = step.get("sample_size", min(len(df), 500))
    df_sample = df.sample(n=n_sample, random_state=step.get("random_state", 42)) if len(df) > n_sample else df
    explainer_type = step.get("explainer", "tree")   # tree | kernel | linear
    if explainer_type == "tree":
        explainer = shap.TreeExplainer(model)
    elif explainer_type == "linear":
        explainer = shap.LinearExplainer(model, df_sample[features])
    else:
        explainer = shap.KernelExplainer(model.predict, df_sample[features])
    shap_vals = explainer.shap_values(df_sample[features])
    if isinstance(shap_vals, list):
        # Multi-class case — average absolute SHAP across classes.
        import numpy as np
        shap_vals = np.mean([abs(sv) for sv in shap_vals], axis=0)
    return pd.DataFrame(shap_vals, columns=[f"shap_{f}" for f in features], index=df_sample.index).reset_index(drop=True)


# ── Model persistence — 1 op (side effect + returns metadata DF) ───────

def _do_save_model(model, step: dict, target: str, features: list, context):
    """Persist a fitted model to disk via joblib. Returns a DataFrame with
    save metadata so it can be used as an asset output if desired."""
    import pandas as pd
    import joblib
    import time
    from pathlib import Path
    path = step["path"]
    joblib.dump(model, path)
    size = Path(path).stat().st_size
    context.log.info(f"save_model: {type(model).__name__} → {path} ({size:,} bytes)")
    return pd.DataFrame([{
        "path": path,
        "size_bytes": size,
        "model_class": f"{type(model).__module__}.{type(model).__name__}",
        "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }])


def _do_cross_validate(df, step: dict, target: str, features: list, context):
    import pandas as pd
    from sklearn.model_selection import cross_validate
    cls = _resolve_model_class(
        step.get("model_type"), step.get("sklearn_class"), step.get("task_type", "classification"),
    )
    params = step.get("params", {}) or {}
    model = cls(**params)
    cv = step.get("cv", 5)
    scoring = step.get("scoring")
    kwargs = {"cv": cv, "return_train_score": True}
    if scoring:
        kwargs["scoring"] = scoring
    scores = cross_validate(model, df[features], df[target], **kwargs)
    return pd.DataFrame({
        "fold":         range(1, cv + 1),
        "train_score":  scores["train_score"],
        "test_score":   scores["test_score"],
        "fit_time":     scores["fit_time"],
    })


# ── Step dispatcher ────────────────────────────────────────────────────

# Ops that produce a DataFrame given a DataFrame input.
_FRAME_OPS = {
    # Preprocessing
    "impute":                 _do_impute,
    "scale":                  _do_scale,
    "one_hot_encode":         _do_one_hot_encode,
    "label_encode":           _do_label_encode,
    "tile_binning":           _do_tile_binning,
    "outlier_clip":           _do_outlier_clip,
    "missing_indicator":      _do_missing_indicator,
    "quantile_transformer":   _do_quantile_transformer,
    "power_transformer":      _do_power_transformer,
    # Selection + generation
    "filter":                 _do_filter,
    "select":                 _do_select,
    "date_features":          _do_date_features,
    "polynomial_features":    _do_polynomial_features,
    "pca":                    _do_pca,
    # Text features
    "tfidf":                  _do_tfidf,
    "hashing_vectorizer":     _do_hashing_vectorizer,
    # Feature selection
    "variance_threshold":     _do_variance_threshold,
    "correlation_filter":     _do_correlation_filter,
    "mutual_info_selection":  _do_mutual_info_selection,
    # Time-series
    "lag_features":           _do_lag_features,
    "rolling_window":         _do_rolling_window,
    # Split + evaluate
    "split":                  _do_split,
    "cross_validate":         _do_cross_validate,
}
# Ops that produce a model given a DataFrame input.
_MODEL_TRAIN_OPS = {
    "train":            _do_train,
    "grid_search":      _do_grid_search,
    "random_search":    _do_random_search,
    "bayesian_search":  _do_bayesian_search,
}
# Ops that take a model + DataFrame and produce a DataFrame.
_MODEL_APPLY_OPS = {
    "predict":          _do_predict,
    "predict_proba":    _do_predict_proba,
    "evaluate":         _do_evaluate,
    "confusion_matrix": _do_confusion_matrix,
    "shap_values":      _do_shap_values,
}
# Ops that take a model alone.
_MODEL_ONLY_OPS = {
    "importance":    _do_importance,
    "save_model":    _do_save_model,
}


def _last_frame_id(state: Dict[str, Any]) -> str:
    """Return the id of the most recently added DataFrame in state.
    Used when a step omits `source:`."""
    import pandas as pd
    for key in reversed(list(state.keys())):
        if isinstance(state[key], pd.DataFrame):
            return key
    raise ValueError("no DataFrame in state — first frame step must have a source")


def _run_step(step: dict, state: Dict[str, Any], target: str, features: list, context) -> None:
    import time as _time
    op = step["op"]
    step_id = step["id"]
    tracker = _tracker(state)
    t0 = _time.time()

    if op in _FRAME_OPS:
        source_id = step.get("source") or _last_frame_id(state)
        df = _FRAME_OPS[op](state[source_id], step, target, features, context)
        state[step_id] = df
        context.log.info(
            f"step {step_id!r} ({op}) → DataFrame ({len(df) if hasattr(df, '__len__') else '?'} rows)"
        )
    elif op in _MODEL_TRAIN_OPS:
        source_id = step.get("source") or _last_frame_id(state)
        model = _MODEL_TRAIN_OPS[op](state[source_id], step, target, features, context)
        state[step_id] = model
        context.log.info(f"step {step_id!r} ({op}) → model {type(model).__name__}")
    elif op in _MODEL_APPLY_OPS:
        model_id = step["model"]
        on_id = step.get("input") or _last_frame_id(state)
        df = _MODEL_APPLY_OPS[op](state[model_id], state[on_id], step, target, features, context)
        state[step_id] = df
        context.log.info(f"step {step_id!r} ({op}) → DataFrame ({len(df)} rows)")
    elif op in _MODEL_ONLY_OPS:
        model_id = step["model"]
        df = _MODEL_ONLY_OPS[op](state[model_id], step, target, features, context)
        state[step_id] = df
        context.log.info(f"step {step_id!r} ({op}) → DataFrame ({len(df)} rows)")
    else:
        raise ValueError(
            f"unknown op: {op!r}. valid: {sorted(set(_FRAME_OPS) | _MODEL_TRAIN_OPS | set(_MODEL_APPLY_OPS) | set(_MODEL_ONLY_OPS))}"
        )

    elapsed = _time.time() - t0

    # Rich per-step metadata — Dagster MetadataValues + tracker log_metrics.
    import pandas as pd
    step_meta: Dict[str, Any] = {"op": op, "elapsed_seconds": round(elapsed, 3)}
    result = state[step_id]

    if op in _MODEL_TRAIN_OPS:
        # Log model params + attach model size estimate.
        params = _extract_model_params(step, result)
        step_meta["model_class"] = type(result).__name__
        step_meta.update(params)
        if tracker:
            tracker.log_params(step_id, params)
            tracker.log_metrics(step_id, {"fit_seconds": elapsed})
            tracker.log_model(step_id, result, features)
    elif op == "evaluate":
        # `evaluate` returns a DataFrame with (metric, value) columns.
        if isinstance(result, pd.DataFrame) and {"metric", "value"} <= set(result.columns):
            metrics = dict(zip(result["metric"], result["value"]))
            step_meta.update({k: float(v) for k, v in metrics.items() if isinstance(v, (int, float))})
            if tracker:
                tracker.log_metrics(step_id, metrics)
    elif op == "cross_validate":
        # Fold-by-fold DataFrame; log mean train/test score.
        if isinstance(result, pd.DataFrame) and "test_score" in result.columns:
            step_meta["cv_mean_test_score"] = float(result["test_score"].mean())
            step_meta["cv_std_test_score"] = float(result["test_score"].std())
            step_meta["cv_folds"] = int(len(result))
            if tracker:
                tracker.log_metrics(step_id, {
                    "cv_mean_test_score": step_meta["cv_mean_test_score"],
                    "cv_std_test_score": step_meta["cv_std_test_score"],
                })
    elif op == "importance":
        if isinstance(result, pd.DataFrame) and "importance" in result.columns:
            step_meta["top_feature"] = str(result.iloc[0]["feature"])
            step_meta["top_importance"] = float(result.iloc[0]["importance"])
    elif isinstance(result, pd.DataFrame):
        step_meta["rows"] = int(len(result))
        step_meta["cols"] = int(len(result.columns))

    _step_meta(state, step_id, **step_meta)


def _extract_model_params(step: dict, model) -> Dict[str, Any]:
    """Extract loggable model hyperparameters from the step config + the
    fitted model. Prefers the step's declared `params`; falls back to
    the fitted model's `.get_params()` for searches that discovered them."""
    step_params = step.get("params") or step.get("base_params") or {}
    if step["op"] in ("grid_search", "random_search", "bayesian_search") and hasattr(model, "get_params"):
        # Search ops return the fitted best_estimator_ — merge its params.
        try:
            fitted = model.get_params()
            # Keep only the keys that were in the search space
            search_keys = set(
                (step.get("param_grid") or step.get("param_distributions") or step.get("param_space") or {}).keys()
            )
            step_params = {**step_params, **{k: fitted[k] for k in search_keys if k in fitted}}
        except Exception:  # noqa: BLE001
            pass
    return step_params


# ── The component ──────────────────────────────────────────────────────


class MLPipelineComponent(dg.Component, dg.Model, dg.Resolvable):
    """Standardized ML pipeline — one YAML, one asset (with named outputs).

    Same "single component, `steps:` list, multiple outputs" shape as the
    other pipeline components (polars_pipeline, warehouse_pipeline,
    pyspark_pipeline, snowpark_pipeline). Reimplements the ops internally —
    no delegation to other components.
    """

    asset_name_prefix: str = Field(
        description="Prefix for emitted asset names. Each step listed in outputs.assets becomes '{prefix}_{step_id}'."
    )
    source: Dict[str, Any] = Field(
        description=(
            "Data source. Shapes: "
            "{kind: url, url: '...', delimiter: ','} | "
            "{kind: file, path: '...', delimiter: ','} | "
            "{kind: upstream_asset, upstream_asset_key: '...'}"
        ),
    )
    target_column: str = Field(description="Column to predict.")
    feature_columns: List[str] = Field(description="Feature columns for training + prediction.")
    steps: List[Dict[str, Any]] = Field(
        description=(
            "Ordered pipeline steps. Each step: {id, op, ...op-specific args}. "
            "Steps chain by id — a step with `source: <id>` uses that step's output; "
            "omit `source:` and it defaults to the most recent DataFrame in state."
        ),
    )
    outputs: Dict[str, Any] = Field(
        description=(
            "Output declaration. Shape: "
            "{assets: [<step_ids>], csv_sinks: [{from: <step_id>, path: <path>}]}. "
            "`assets:` step outputs become first-class Dagster assets; `csv_sinks:` "
            "writes side-outputs to disk without creating assets."
        ),
    )
    experiment_tracking: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Experiment-tracker config. Auto-logged from train / evaluate / "
            "cross_validate / grid_search / random_search / bayesian_search "
            "steps. Two backends (either OR both):\n"
            "  `mlflow:` — `{tracking_uri_env_var, experiment_name, "
            "run_name_template, log_params, log_metrics, log_model, "
            "log_artifacts, tags}`\n"
            "  `wandb:` — `{project_env_var, api_key_env_var, entity_env_var, "
            "run_name_template, tags}`\n"
            "`run_name_template` supports `{prefix}` / `{partition_key}` / "
            "`{run_id}` substitutions. Silently no-ops if the tracker library "
            "isn't installed."
        ),
    )
    group_name: Optional[str] = Field(default="ml", description="Group name for emitted assets.")
    kinds: Optional[List[str]] = Field(default=None, description="Kinds for the emitted assets (default: ['python', 'ml']).")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Tags on the emitted assets.")
    owners: Optional[List[str]] = Field(default=None, description="Owners on the emitted assets.")
    description: Optional[str] = Field(default=None, description="Description on the emitted assets.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        prefix = self.asset_name_prefix
        target = self.target_column
        features = list(self.feature_columns)
        steps = list(self.steps)
        source_config = dict(self.source)
        outputs = dict(self.outputs)
        asset_ids: List[str] = list(outputs.get("assets", []))
        csv_sinks: List[Dict[str, Any]] = list(outputs.get("csv_sinks", []) or [])
        parquet_sinks: List[Dict[str, Any]] = list(outputs.get("parquet_sinks", []) or [])
        table_sinks: List[Dict[str, Any]] = list(outputs.get("table_sinks", []) or [])

        # Auto-detect required Dagster resource keys from source + table_sinks —
        # customer never has to list them explicitly.
        required_resource_keys = set()
        if source_config.get("kind") == "warehouse_query":
            required_resource_keys.add(source_config["resource_key"])
        for sink in table_sinks:
            required_resource_keys.add(sink["resource_key"])

        group_name = self.group_name or "ml"
        kinds = list(self.kinds or ["python", "ml"])
        tags = dict(self.tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        if not asset_ids:
            raise ValueError("outputs.assets must list at least one step id.")

        # Build the outs dict for @multi_asset — one entry per asset id.
        # tags/kinds go on each AssetOut (multi_asset doesn't accept them at the decorator level).
        outs = {
            f"{prefix}_{aid}": dg.AssetOut(
                group_name=group_name,
                description=self.description,
                owners=self.owners or None,
                tags=tags,
            )
            for aid in asset_ids
        }

        # If the source is an upstream asset, we accept it as a compute input.
        ins: Dict[str, dg.AssetIn] = {}
        if source_config.get("kind") == "upstream_asset":
            ins["source"] = dg.AssetIn(key=dg.AssetKey.from_user_string(source_config["upstream_asset_key"]))

        @dg.multi_asset(
            outs=outs,
            name=f"{prefix}_pipeline",
            ins=ins or None,
            required_resource_keys=required_resource_keys or None,
        )
        def _pipeline(context: dg.AssetExecutionContext, **kwargs):
            # Partition-aware compute: read context.partition_key once + thread
            # into ingest + sink templates. Safe on unpartitioned assets
            # (partition_key is None; substitutions become no-ops).
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key:
                context.log.info(f"partition-aware materialization: partition_key={partition_key!r}")

            # Ingest — either from an upstream asset input OR by fetching a URL / file / SQL.
            if source_config.get("kind") == "upstream_asset":
                initial_frame = kwargs["source"]
                context.log.info(f"ingested {len(initial_frame)} rows from upstream asset")
            else:
                initial_frame = _ingest(source_config, context, partition_key=partition_key)
                context.log.info(
                    f"ingested {len(initial_frame)} rows via {source_config.get('kind', 'url')}"
                )

            # Init experiment tracker (mlflow / wandb / both / neither).
            tracker = _ExperimentTracker(
                cfg=_self.experiment_tracking,
                run_context={
                    "prefix": prefix,
                    "partition_key": str(partition_key or ""),
                    "run_id": context.run_id,
                },
                log=context.log,
            )

            state: Dict[str, Any] = {
                "source": initial_frame,
                "__tracker__": tracker,
                "__step_metadata__": {},
            }

            # Run each step in order.
            try:
                for step in steps:
                    _run_step(step, state, target, features, context)
            finally:
                tracker.end()

            # Write CSV sinks — side effects, not first-class assets.
            # Path is partition-aware via `{partition_key}` templating.
            for sink in csv_sinks:
                from_id = sink["from"]
                path = _apply_partition_template(sink["path"], partition_key)
                if from_id not in state:
                    raise ValueError(f"csv_sinks: unknown step id {from_id!r}")
                state[from_id].to_csv(path, index=False)
                context.log.info(f"csv_sink {from_id!r} → {path}")

            # Write Parquet sinks — same shape as csv_sinks.
            for sink in parquet_sinks:
                from_id = sink["from"]
                path = _apply_partition_template(sink["path"], partition_key)
                if from_id not in state:
                    raise ValueError(f"parquet_sinks: unknown step id {from_id!r}")
                state[from_id].to_parquet(path, index=False)
                context.log.info(f"parquet_sink {from_id!r} → {path}")

            # Write Table sinks — via a Dagster resource. Two partition patterns:
            #   Pattern A (per-partition-table): put `{partition_key}` in `table:`
            #     table: "predictions_{partition_key}"  → predictions_2025_01_15
            #   Pattern B (single table + partition column): set `partition_column:`
            #     partition_column: partition_date  → appends this column to every
            #     row with the partition_key value. Analytics queries stay clean
            #     (WHERE partition_date = ...).
            # Both patterns can combine with `if_exists: append` for streaming or
            # `if_exists: replace` for idempotent per-partition writes.
            for sink in table_sinks:
                from_id = sink["from"]
                resource_key = sink["resource_key"]
                table = _apply_partition_template(sink["table"], partition_key)
                schema = sink.get("schema")
                if_exists = sink.get("if_exists", "append")
                partition_col = sink.get("partition_column")
                if from_id not in state:
                    raise ValueError(f"table_sinks: unknown step id {from_id!r}")

                df_to_write = state[from_id]
                if partition_key and partition_col:
                    if partition_col in df_to_write.columns:
                        context.log.warning(
                            f"partition_column={partition_col!r} already in {from_id!r} — overwriting"
                        )
                    df_to_write = df_to_write.assign(**{partition_col: str(partition_key)})
                    context.log.info(
                        f"table_sink Pattern B: added column {partition_col!r}={partition_key!r} to {len(df_to_write)} rows"
                    )

                resource = getattr(context.resources, resource_key)
                if hasattr(resource, "get_engine"):
                    engine = resource.get_engine()
                elif hasattr(resource, "get_connection"):
                    engine = resource.get_connection()
                else:
                    raise ValueError(
                        f"resource {resource_key!r} must expose .get_engine() or .get_connection()"
                    )
                df_to_write.to_sql(
                    table, engine, schema=schema, if_exists=if_exists, index=False,
                )
                context.log.info(f"table_sink {from_id!r} → {schema+'.' if schema else ''}{table} (via {resource_key}, {if_exists})")

            # Emit per-step MetadataValue for every step that maps to an output asset.
            step_meta = state.get("__step_metadata__") or {}
            for aid in asset_ids:
                meta = step_meta.get(aid) or {}
                if not meta:
                    continue
                mv: Dict[str, Any] = {}
                for k, v in meta.items():
                    if isinstance(v, bool):
                        mv[k] = dg.MetadataValue.bool(v)
                    elif isinstance(v, int):
                        mv[k] = dg.MetadataValue.int(v)
                    elif isinstance(v, float):
                        mv[k] = dg.MetadataValue.float(v)
                    elif isinstance(v, (dict, list)):
                        mv[k] = dg.MetadataValue.json(v)
                    else:
                        mv[k] = dg.MetadataValue.text(str(v))
                context.add_output_metadata(output_name=f"{prefix}_{aid}", metadata=mv)

            # Return the values of the assets in the order of outs.
            missing = [aid for aid in asset_ids if aid not in state]
            if missing:
                raise ValueError(f"outputs.assets references unknown step ids: {missing}")
            return tuple(state[aid] for aid in asset_ids)

        return dg.Definitions(assets=[_pipeline])
