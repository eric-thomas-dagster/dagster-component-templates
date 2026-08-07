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
    "impute":              _do_impute,
    "scale":               _do_scale,
    "one_hot_encode":      _do_one_hot_encode,
    "label_encode":        _do_label_encode,
    "tile_binning":        _do_tile_binning,
    "outlier_clip":        _do_outlier_clip,
    "filter":              _do_filter,
    "select":              _do_select,
    "date_features":       _do_date_features,
    "polynomial_features": _do_polynomial_features,
    "pca":                 _do_pca,
    "split":               _do_split,
    "cross_validate":      _do_cross_validate,
}
# Ops that produce a model given a DataFrame input.
_MODEL_TRAIN_OPS = {"train"}
# Ops that take a model + DataFrame and produce a DataFrame.
_MODEL_APPLY_OPS = {
    "predict":       _do_predict,
    "predict_proba": _do_predict_proba,
}
# Ops that take a model alone.
_MODEL_ONLY_OPS = {
    "importance":    _do_importance,
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
    op = step["op"]
    step_id = step["id"]

    if op in _FRAME_OPS:
        source_id = step.get("source") or _last_frame_id(state)
        df = _FRAME_OPS[op](state[source_id], step, target, features, context)
        state[step_id] = df
        context.log.info(
            f"step {step_id!r} ({op}) → DataFrame ({len(df) if hasattr(df, '__len__') else '?'} rows)"
        )
    elif op in _MODEL_TRAIN_OPS:
        source_id = step.get("source") or _last_frame_id(state)
        model = _do_train(state[source_id], step, target, features, context)
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

            state: Dict[str, Any] = {"source": initial_frame}

            # Run each step in order.
            for step in steps:
                _run_step(step, state, target, features, context)

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

            # Return the values of the assets in the order of outs.
            missing = [aid for aid in asset_ids if aid not in state]
            if missing:
                raise ValueError(f"outputs.assets references unknown step ids: {missing}")
            return tuple(state[aid] for aid in asset_ids)

        return dg.Definitions(assets=[_pipeline])
