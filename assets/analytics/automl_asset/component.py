"""AutoMLAssetComponent — real AutoML: search across model families, not
hyperparameter tuning for one you already picked.

`ml_pipeline`'s `grid_search`/`random_search`/`bayesian_search` ops tune
hyperparameters for a `model_type`/`sklearn_class` YOU specify -- that's
the same thing `GridSearchCV`/`RandomizedSearchCV` already do, just
YAML-wrapped. This component is the actual missing piece: give it a
DataFrame + target column, and it searches ACROSS model families
(LightGBM, XGBoost, random forest, extra trees, linear models, ...) via
FLAML and picks the winner -- almost no config required.

Caching, not "always search": a full search is expensive (many model
fits across a time budget) and wasteful to repeat on every materialize
when the data hasn't meaningfully changed. After the first full search,
the winning recipe (which estimator, which hyperparameters) is persisted
to `state_path` (any fsspec path -- local or cloud). Every subsequent
run reads that recipe back and does a single cheap refit against it
(confirmed live: ~0.08s vs ~8s for a full search, same result) instead
of re-searching from scratch. Set `refresh_search: true` to force a
fresh search (e.g. on a schedule, or after a known data/schema change),
or `max_state_age_hours` to auto-expire the cache after a given age.

Non-numeric feature columns are auto-label-encoded (a mechanical
necessity for several of FLAML's estimator families, not a modeling
decision) -- deeper preprocessing (scaling, imputation, custom
encoding) is `ml_pipeline`'s job; feed its output into this component
if you need that first.

Requires `flaml[automl]` (`pip install "flaml[automl]"` -- the bare
`flaml` package does NOT include AutoML functionality, only FLAML's
other tuning library). On macOS, also requires the OpenMP runtime
(`brew install libomp`) for the xgboost/lightgbm backends FLAML uses
internally -- confirmed live, `flaml.automl` fails to import without it.
"""
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import ConfigDict, Field


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
):
    """Construct a Dagster partitions_def from the canonical partition fields.
    Canonical implementation — copied as-is per FIELD_CONVENTIONS.md."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _load_state(state_path: str) -> Optional[Dict[str, Any]]:
    """Read the cached AutoML recipe (fsspec path -- local or cloud). None
    if it doesn't exist yet (first run)."""
    import fsspec
    try:
        with fsspec.open(state_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def _save_state(state_path: str, state: Dict[str, Any]) -> None:
    import fsspec
    with fsspec.open(state_path, "w") as f:
        json.dump(state, f, indent=2)


def _state_age_hours(state: Dict[str, Any]) -> Optional[float]:
    created_at = state.get("created_at")
    if not created_at:
        return None
    try:
        created = datetime.fromisoformat(created_at)
    except ValueError:
        return None
    return (datetime.now(timezone.utc) - created).total_seconds() / 3600.0


def _encode_non_numeric_features(df: "pd.DataFrame", feature_columns: List[str], context) -> "pd.DataFrame":
    """Label-encode any feature column FLAML's estimators can't take raw
    (object/category dtype) -- mechanical necessity for linear-model
    estimators in FLAML's registry (tree estimators handle it natively,
    but the encoding must be uniform across the whole feature set FLAML
    receives). Not a substitute for real preprocessing -- route through
    ml_pipeline first if you need scaling/imputation/custom encoding."""
    from sklearn.preprocessing import LabelEncoder
    out = df.copy()
    encoded_cols = []
    for col in feature_columns:
        if not pd.api.types.is_numeric_dtype(out[col]):
            out[col] = LabelEncoder().fit_transform(out[col].astype(str))
            encoded_cols.append(col)
    if encoded_cols:
        context.log.info(f"automl_asset: label-encoded non-numeric feature columns: {encoded_cols}")
    return out


def _evaluate_holdout(model, test_df: "pd.DataFrame", target: str, features: List[str], task_type: str) -> Dict[str, float]:
    """Same metric shape as ml_pipeline's evaluate op, computed against a
    genuinely held-out split (never seen during the AutoML search)."""
    y_true = test_df[target]
    y_pred = model.predict(test_df[features])
    metrics: Dict[str, float] = {}
    if task_type == "classification":
        from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
        metrics["accuracy"] = float(accuracy_score(y_true, y_pred))
        metrics["precision"] = float(precision_score(y_true, y_pred, average="weighted", zero_division=0))
        metrics["recall"] = float(recall_score(y_true, y_pred, average="weighted", zero_division=0))
        metrics["f1"] = float(f1_score(y_true, y_pred, average="weighted", zero_division=0))
    else:
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
        metrics["mae"] = float(mean_absolute_error(y_true, y_pred))
        mse = float(mean_squared_error(y_true, y_pred))
        metrics["mse"] = mse
        metrics["rmse"] = mse ** 0.5
        metrics["r2"] = float(r2_score(y_true, y_pred))
    return metrics


class AutoMLAssetComponent(Component, Model, Resolvable):
    """Real AutoML for tabular data: searches across model families via
    FLAML, caches the winning recipe, and does a cheap refit against that
    cached recipe on every subsequent run instead of re-searching.

    Example:

        ```yaml
        type: dagster_component_templates.AutoMLAssetComponent
        attributes:
          asset_name: wine_quality_automl
          upstream_asset_key: wine_features
          target_column: quality
          task_type: classification
          state_path: /tmp/automl_state/wine_quality.json
          time_budget_seconds: 60
        ```

    First materialize: full FLAML search (up to `time_budget_seconds`),
    winning recipe cached to `state_path`. Every materialize after that:
    reads the cached recipe, refits in a fraction of a second, no new
    search. Set `refresh_search: true` (one-off) or `max_state_age_hours`
    (automatic) to force a fresh search.
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")

    target_column: str = Field(description="Column to predict")
    feature_columns: Optional[List[str]] = Field(
        default=None,
        description="Feature columns to train on. Defaults to every column except target_column. Non-numeric columns are auto-label-encoded (see module docstring).",
    )
    task_type: str = Field(description="'classification' or 'regression'")

    state_path: str = Field(
        description="fsspec path (local or cloud -- s3://, gs://, etc.) where the winning AutoML recipe is cached between runs. Required -- this is what makes repeated materializes cheap instead of always re-searching."
    )
    refresh_search: bool = Field(
        default=False,
        description="Force a full fresh search even if a cached recipe exists at state_path (and overwrite it with the new result). Use for a one-off re-tune; for automatic periodic re-tuning use max_state_age_hours instead.",
    )
    max_state_age_hours: Optional[float] = Field(
        default=None,
        description="If the cached recipe is older than this, treat it as stale and run a fresh full search instead of a cheap refit. None (default) never auto-expires the cache.",
    )

    time_budget_seconds: int = Field(
        default=60,
        ge=1,
        description="Wall-clock budget for a full search (ignored for a cached cheap refit, which always takes well under a second regardless of this value).",
    )
    estimator_list: Optional[List[str]] = Field(
        default=None,
        description="Restrict the search to these FLAML estimator names (e.g. ['lgbm', 'rf', 'xgboost']). None (default) lets FLAML choose from its full built-in set for the task type.",
    )
    metric: Optional[str] = Field(
        default=None,
        description="FLAML search metric (e.g. 'accuracy', 'f1', 'r2', 'rmse'). None (default) uses FLAML's own default for task_type.",
    )

    test_size: float = Field(default=0.2, gt=0.0, lt=1.0, description="Held-out fraction for the honest evaluation metrics in output metadata (never used for the search itself).")
    random_state: int = Field(default=42, description="Random seed for the train/test split and FLAML's own internal splits.")

    output_column: str = Field(default="predicted", description="Column to write predictions into (on ALL input rows, using the final fitted model).")
    model_output_path: Optional[str] = Field(
        default=None,
        description="Optional fsspec path to also persist the final fitted model via joblib (e.g. for use outside this asset). None (default) skips this.",
    )

    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime).")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags to apply to the asset.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners — list of team names or email addresses.")

    include_preview_metadata: bool = Field(default=False, description="Include a preview of the output data in metadata.")
    preview_rows: int = Field(default=25, ge=1, le=500, description="Rows to include in the preview metadata when include_preview_metadata is True.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily'/'weekly'/'monthly'/'hourly'/'static'/'dynamic'/None.")
    partition_start: Optional[str] = Field(default=None, description="Partition start date (ISO), required for time-based types.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if self.task_type not in ("classification", "regression"):
            raise ValueError(f"AutoMLAssetComponent: task_type must be 'classification' or 'regression', got {self.task_type!r}.")

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name,
        )

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            freshness_policy = FreshnessPolicy(maximum_lag_minutes=self.freshness_max_lag_minutes, cron_schedule=self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        target_column = self.target_column
        feature_columns = self.feature_columns
        task_type = self.task_type
        state_path = self.state_path
        refresh_search = self.refresh_search
        max_state_age_hours = self.max_state_age_hours
        time_budget_seconds = self.time_budget_seconds
        estimator_list = self.estimator_list
        metric = self.metric
        test_size = self.test_size
        random_state = self.random_state
        output_column = self.output_column
        model_output_path = self.model_output_path
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"AutoML ({task_type}) predicting {target_column} via FLAML, with cached-recipe cheap refits.",
            group_name=self.group_name,
            kinds={"sklearn", "ml", "automl"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # Defensive Output/MaterializeResult unwrap -- same convention
            # as every other component in this repo.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

            if target_column not in upstream.columns:
                raise ValueError(f"target_column={target_column!r} not in upstream: {list(upstream.columns)}")
            _features = feature_columns or [c for c in upstream.columns if c != target_column]
            missing = [c for c in _features if c not in upstream.columns]
            if missing:
                raise ValueError(f"feature_columns not in upstream: {missing}")

            try:
                from flaml import AutoML
            except ImportError:
                raise ImportError(
                    "AutoML requires flaml[automl]: pip install \"flaml[automl]\" "
                    "(the bare flaml package does NOT include AutoML functionality)"
                )
            from sklearn.model_selection import train_test_split

            df = upstream.copy().reset_index(drop=True)
            df = _encode_non_numeric_features(df, _features, context)

            stratify = df[target_column] if task_type == "classification" else None
            train_df, test_df = train_test_split(
                df, test_size=test_size, random_state=random_state, stratify=stratify,
            )

            cached_state = None if refresh_search else _load_state(state_path)
            is_stale = False
            if cached_state is not None and max_state_age_hours is not None:
                age = _state_age_hours(cached_state)
                is_stale = age is None or age > max_state_age_hours

            cached_estimator_available = (
                cached_state is not None
                and not is_stale
                and (estimator_list is None or cached_state.get("best_estimator") in estimator_list)
            )

            automl = AutoML()
            fit_kwargs: Dict[str, Any] = dict(
                X_train=train_df[_features],
                y_train=train_df[target_column],
                task=task_type,
                metric=metric,
                verbose=0,
            )

            t0 = time.time()
            if cached_estimator_available:
                context.log.info(f"automl_asset: cached recipe found at {state_path!r} -- cheap refit, no search.")
                fit_kwargs.update(
                    time_budget=3600,  # generous ceiling; max_iter=1 stops it almost immediately
                    estimator_list=[cached_state["best_estimator"]],
                    starting_points={cached_state["best_estimator"]: [cached_state["best_config"]]},
                    max_iter=1,
                )
                search_mode = "cached_refit"
            else:
                if cached_state is not None and not cached_estimator_available:
                    reason = "stale (max_state_age_hours exceeded)" if is_stale else "cached estimator excluded by estimator_list"
                    context.log.info(f"automl_asset: cached recipe at {state_path!r} ignored ({reason}) -- running a full search.")
                else:
                    context.log.info(f"automl_asset: no cached recipe at {state_path!r} -- running a full search (time_budget_seconds={time_budget_seconds}).")
                fit_kwargs.update(time_budget=time_budget_seconds, estimator_list=estimator_list)
                search_mode = "full_search"

            automl.fit(**fit_kwargs)
            elapsed = time.time() - t0

            if search_mode == "full_search":
                new_state = {
                    "best_estimator": automl.best_estimator,
                    "best_config": automl.best_config,
                    "task_type": task_type,
                    "metric": metric,
                    "best_loss": float(automl.best_loss),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                _save_state(state_path, new_state)
                context.log.info(f"automl_asset: search complete in {elapsed:.1f}s -- best_estimator={automl.best_estimator!r}, cached to {state_path!r}.")
            else:
                context.log.info(f"automl_asset: cached refit complete in {elapsed:.2f}s -- estimator={automl.best_estimator!r}.")

            holdout_metrics = _evaluate_holdout(automl, test_df, target_column, _features, task_type)

            df[output_column] = automl.predict(df[_features])

            if model_output_path:
                import joblib
                import fsspec
                with fsspec.open(model_output_path, "wb") as f:
                    joblib.dump(automl.model.estimator, f)
                context.log.info(f"automl_asset: fitted model persisted to {model_output_path!r}.")

            metadata: Dict[str, Any] = {
                "rows": MetadataValue.int(len(df)),
                "search_mode": MetadataValue.text(search_mode),
                "best_estimator": MetadataValue.text(automl.best_estimator or ""),
                "best_config": MetadataValue.json(automl.best_config or {}),
                "search_time_seconds": MetadataValue.float(round(elapsed, 3)),
                "state_path": MetadataValue.path(state_path),
            }
            for k, v in holdout_metrics.items():
                metadata[f"holdout_{k}"] = MetadataValue.float(round(v, 4))
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False) or "")
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[_asset])
