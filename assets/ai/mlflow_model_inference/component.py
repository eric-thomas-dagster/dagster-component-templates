"""MLflow Model Inference Component.

Materializable asset that loads a registered MLflow model version and scores
an upstream DataFrame. Bounded selection via version-pin or stage; auditable
per-run metadata (model URI, version, row_count, latency).

Right for teams that treat "score today's batch" as a first-class Dagster
asset in the graph — not a shell script that runs somewhere on a cron.
"""

import os
import time
from typing import List, Optional

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class MLflowModelInferenceComponent(Component, Model, Resolvable):
    """Load a registered MLflow model + score an upstream DataFrame.

    Uses `mlflow.pyfunc.load_model()` so the same component works for
    sklearn / xgboost / lightgbm / pytorch / anything with a pyfunc flavor.

    Selection modes (pick one):
      1. `model_stage: Production` → loads latest Production version
      2. `model_version: "7"` → pinned version

    Example (score upstream DataFrame with latest Production model):

        ```yaml
        type: dagster_community_components.MLflowModelInferenceComponent
        attributes:
          asset_name: daily_churn_scores
          upstream_asset_key: customer_features
          tracking_uri_env_var: MLFLOW_TRACKING_URI
          model_name: churn_model
          model_stage: Production
          output_column: churn_score
          group_name: ml_scoring
        ```

    Example (pin a version, drop input columns from output):

        ```yaml
        attributes:
          asset_name: daily_churn_scores
          upstream_asset_key: customer_features
          model_name: churn_model
          model_version: "7"
          output_column: churn_score
          keep_input_columns: false
        ```
    """

    asset_name: str = Field(description="Dagster asset name for the scored output.")

    upstream_asset_key: str = Field(
        description="Asset key of the upstream DataFrame to score."
    )

    tracking_uri_env_var: str = Field(
        description="Env var holding the MLflow Tracking URI."
    )

    model_name: str = Field(description="Registered model name to load from MLflow.")

    model_stage: Optional[str] = Field(
        default="Production",
        description=(
            "Load the latest version at this stage. Set to null and provide "
            "model_version instead to pin a specific version."
        ),
    )

    model_version: Optional[str] = Field(
        default=None,
        description=(
            "Pin a specific model version to load. Overrides model_stage if both "
            "are provided."
        ),
    )

    output_column: str = Field(
        default="prediction",
        description="Column name for the model's output in the returned DataFrame.",
    )

    keep_input_columns: bool = Field(
        default=True,
        description=(
            "If true, output DataFrame is upstream + prediction column. If false, "
            "output contains only the prediction column + any explicit id columns."
        ),
    )

    id_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Columns to keep in the output even when keep_input_columns=false "
            "(e.g. customer_id, order_id). Useful for downstream joins."
        ),
    )

    feature_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Columns to pass to the model. If None, uses all upstream columns "
            "except id_columns."
        ),
    )

    group_name: Optional[str] = Field(
        default="mlflow_scoring", description="Asset group name."
    )

    asset_key_prefix: Optional[List[str]] = Field(
        default=None,
        description="Prefix for the asset key.",
    )

    username_env_var: Optional[str] = Field(
        default=None, description="Env var for HTTP-basic MLflow auth username (optional)."
    )
    password_env_var: Optional[str] = Field(
        default=None, description="Env var for HTTP-basic MLflow auth password (optional)."
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        tracking_uri_env_var = self.tracking_uri_env_var
        model_name = self.model_name
        model_stage = self.model_stage
        model_version = self.model_version
        output_column = self.output_column
        keep_input_columns = self.keep_input_columns
        id_columns = self.id_columns or []
        feature_columns = self.feature_columns
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var

        prefix = self.asset_key_prefix or []
        target_key = AssetKey([*prefix, asset_name])
        upstream_key = AssetKey(upstream_asset_key.split("/")) if "/" in upstream_asset_key else AssetKey(upstream_asset_key)

        model_uri = (
            f"models:/{model_name}/{model_version}" if model_version
            else f"models:/{model_name}/{model_stage}"
        )

        @asset(
            key=target_key,
            group_name=self.group_name,
            compute_kind="mlflow",
            ins={"upstream": AssetIn(key=upstream_key)},
            description=f"Loads {model_uri} and scores {upstream_asset_key!r}.",
        )
        def _inference(context: AssetExecutionContext, upstream) -> MaterializeResult:
            try:
                import mlflow.pyfunc  # type: ignore[import-not-found]
                import pandas as pd
            except ImportError as e:
                raise RuntimeError("mlflow and pandas required") from e

            tracking_uri = os.environ.get(tracking_uri_env_var)
            if not tracking_uri:
                raise RuntimeError(f"Env var {tracking_uri_env_var!r} is not set")

            if username_env_var and password_env_var:
                os.environ["MLFLOW_TRACKING_USERNAME"] = os.environ.get(username_env_var, "")
                os.environ["MLFLOW_TRACKING_PASSWORD"] = os.environ.get(password_env_var, "")

            os.environ["MLFLOW_TRACKING_URI"] = tracking_uri

            # Normalize upstream to a DataFrame.
            df = upstream if isinstance(upstream, pd.DataFrame) else pd.DataFrame(upstream)
            context.log.info(f"Loading model {model_uri}")

            t0 = time.time()
            model = mlflow.pyfunc.load_model(model_uri)
            load_secs = time.time() - t0

            # Select feature columns.
            if feature_columns:
                missing = [c for c in feature_columns if c not in df.columns]
                if missing:
                    raise RuntimeError(f"upstream missing feature columns: {missing}")
                feat_df = df[feature_columns]
            else:
                feat_df = df.drop(columns=[c for c in id_columns if c in df.columns], errors="ignore")

            t0 = time.time()
            preds = model.predict(feat_df)
            score_secs = time.time() - t0

            if keep_input_columns:
                out = df.copy()
            else:
                keep = [c for c in id_columns if c in df.columns]
                out = df[keep].copy() if keep else pd.DataFrame(index=df.index)

            # preds might be a Series / ndarray / list of arrays — normalize.
            try:
                out[output_column] = preds
            except Exception:
                # For models that return multi-column outputs
                if hasattr(preds, "columns"):
                    for col in preds.columns:
                        out[f"{output_column}_{col}"] = preds[col].values
                else:
                    out[output_column] = list(preds)

            return MaterializeResult(
                value=out,
                metadata={
                    "model_uri": MetadataValue.text(model_uri),
                    "model_name": model_name,
                    "row_count": len(out),
                    "load_seconds": round(load_secs, 3),
                    "score_seconds": round(score_secs, 3),
                    "rows_per_second": round(len(out) / max(score_secs, 0.001), 1),
                    "input_columns": len(feat_df.columns),
                    "output_columns": len(out.columns),
                },
            )

        return Definitions(assets=[_inference])
