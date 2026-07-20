"""MLflow Model Promotion Component.

Materializable asset that transitions a registered model version from one
stage to another (e.g. Staging → Production). Every materialization is a
Dagster event with the full transition captured in metadata — auditable ML CD.

Right for teams that want the "promote model to Production" step to be a
first-class Dagster asset with lineage, retries, on-call alerts, and RBAC —
instead of a manual click in the MLflow UI or a hand-rolled script.
"""

import os
from typing import Optional

from dagster import (
    AssetExecutionContext,
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


class MLflowModelPromotionComponent(Component, Model, Resolvable):
    """Emits one Dagster asset that promotes an MLflow model version.

    Two selection modes:
      1. Explicit version — `model_version: "7"`
      2. Latest at source stage — `source_stage: Staging` (promote the newest one)

    On materialize:
      1. Resolves the source model version
      2. Calls MLflow's transition_model_version_stage(target_stage, archive_existing_versions=...)
      3. Emits a MaterializeResult with before/after stage, run_id, source URI

    Example (promote latest Staging → Production, archive existing Production):

        ```yaml
        type: dagster_community_components.MLflowModelPromotionComponent
        attributes:
          asset_name: promote_churn_to_production
          tracking_uri_env_var: MLFLOW_TRACKING_URI
          model_name: churn_model
          source_stage: Staging
          target_stage: Production
          archive_existing_target: true
          group_name: mlflow_cd
        ```

    Example (pin a specific version to Production):

        ```yaml
        attributes:
          asset_name: force_promote_churn_v7
          model_version: "7"
          target_stage: Production
        ```
    """

    asset_name: str = Field(description="Dagster asset name (single key).")

    tracking_uri_env_var: str = Field(
        description="Env var holding the MLflow Tracking URI."
    )

    model_name: str = Field(description="Registered model name to promote.")

    target_stage: str = Field(
        default="Production",
        description="Stage to promote to (Production / Staging / Archived / None).",
    )

    source_stage: Optional[str] = Field(
        default=None,
        description=(
            "If set, promote the LATEST version at this stage. Mutually exclusive "
            "with `model_version`."
        ),
    )

    model_version: Optional[str] = Field(
        default=None,
        description=(
            "If set, promote this specific version. Mutually exclusive with "
            "`source_stage`."
        ),
    )

    archive_existing_target: bool = Field(
        default=True,
        description=(
            "Whether to archive any existing model versions at `target_stage` before "
            "promoting the new one. Matches MLflow's `archive_existing_versions` flag."
        ),
    )

    description_prefix: Optional[str] = Field(
        default=None,
        description="Prepended to the promotion's Dagster asset description.",
    )

    group_name: Optional[str] = Field(
        default="mlflow", description="Asset group name for the promotion asset."
    )

    asset_key_prefix: Optional[list] = Field(
        default=None,
        description="Prefix for the asset key (list of strings for hierarchical keys).",
    )

    username_env_var: Optional[str] = Field(
        default=None, description="Env var for HTTP-basic MLflow auth username (optional)."
    )
    password_env_var: Optional[str] = Field(
        default=None, description="Env var for HTTP-basic MLflow auth password (optional)."
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        tracking_uri_env_var = self.tracking_uri_env_var
        model_name = self.model_name
        target_stage = self.target_stage
        source_stage = self.source_stage
        pinned_version = self.model_version
        archive_existing_target = self.archive_existing_target
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var

        if not source_stage and not pinned_version:
            raise ValueError(
                "MLflowModelPromotionComponent needs either `source_stage` or `model_version`."
            )
        if source_stage and pinned_version:
            raise ValueError(
                "MLflowModelPromotionComponent: set `source_stage` OR `model_version`, not both."
            )

        prefix = self.asset_key_prefix or []
        target_key = AssetKey([*prefix, asset_name])

        @asset(
            key=target_key,
            group_name=self.group_name,
            compute_kind="mlflow",
            description=(
                (self.description_prefix + " " if self.description_prefix else "")
                + f"Promotes {model_name!r} to {target_stage!r} in MLflow Registry."
            ),
        )
        def _promote(context: AssetExecutionContext) -> MaterializeResult:
            try:
                import mlflow
            except ImportError as e:
                raise RuntimeError("mlflow package not installed") from e

            tracking_uri = os.environ.get(tracking_uri_env_var)
            if not tracking_uri:
                raise RuntimeError(f"Env var {tracking_uri_env_var!r} is not set")

            if username_env_var and password_env_var:
                os.environ["MLFLOW_TRACKING_USERNAME"] = os.environ.get(username_env_var, "")
                os.environ["MLFLOW_TRACKING_PASSWORD"] = os.environ.get(password_env_var, "")

            client = mlflow.tracking.MlflowClient(tracking_uri=tracking_uri)

            # Resolve the source version.
            if pinned_version:
                resolved_version = pinned_version
                source_mv = client.get_model_version(model_name, pinned_version)
                prior_stage = source_mv.current_stage
            else:
                latest = client.get_latest_versions(model_name, stages=[source_stage])
                if not latest:
                    raise RuntimeError(
                        f"No versions of {model_name!r} at source stage {source_stage!r}"
                    )
                source_mv = max(latest, key=lambda v: int(v.version))
                resolved_version = source_mv.version
                prior_stage = source_mv.current_stage

            # Idempotency: if already at target, log + emit without re-transitioning.
            if source_mv.current_stage == target_stage:
                context.log.info(
                    f"{model_name} v{resolved_version} already at {target_stage} — no-op"
                )
                return MaterializeResult(
                    metadata={
                        "model_name": model_name,
                        "version": resolved_version,
                        "prior_stage": prior_stage,
                        "new_stage": target_stage,
                        "no_op": True,
                        "run_id": MetadataValue.text(source_mv.run_id or ""),
                    }
                )

            client.transition_model_version_stage(
                name=model_name,
                version=resolved_version,
                stage=target_stage,
                archive_existing_versions=archive_existing_target,
            )
            context.log.info(
                f"Transitioned {model_name} v{resolved_version}: "
                f"{prior_stage} → {target_stage}"
            )
            return MaterializeResult(
                metadata={
                    "model_name": model_name,
                    "version": resolved_version,
                    "prior_stage": prior_stage,
                    "new_stage": target_stage,
                    "archived_existing_at_target": archive_existing_target,
                    "run_id": MetadataValue.text(source_mv.run_id or ""),
                    "source_uri": MetadataValue.text(source_mv.source or ""),
                }
            )

        return Definitions(assets=[_promote])
