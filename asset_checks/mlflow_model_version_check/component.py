"""MLflow Model Version Asset Check Component.

Emits a Dagster @asset_check attached to an existing asset. The check queries
the MLflow Model Registry and validates:

  - The model name exists
  - (optional) A specific version exists
  - (optional) The model is at a required stage (Production / Staging / Archived)
  - (optional) Model has been updated within a freshness window

Right for gating downstream inference / promotion jobs on model-registry state:
"don't materialize the daily prediction asset unless a Production model exists."
"""

from datetime import datetime, timedelta, timezone
import os
from typing import List, Optional

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset_check,
)
from pydantic import Field


class MLflowModelVersionCheckComponent(Component, Model, Resolvable):
    """Asset check verifying an MLflow registered model's state.

    Attach to any existing Dagster asset by setting `asset_key`. The check
    calls MLflow Registry APIs at evaluation time; passes if all configured
    conditions are met.

    Example (production model must exist):

        ```yaml
        type: dagster_community_components.MLflowModelVersionCheckComponent
        attributes:
          asset_key: daily_churn_predictions
          check_name: churn_model_prod_exists
          tracking_uri_env_var: MLFLOW_TRACKING_URI
          model_name: churn_model
          required_stage: Production
        ```

    Example (pin version + freshness):

        ```yaml
        attributes:
          asset_key: daily_churn_predictions
          check_name: churn_model_v7_recent
          tracking_uri_env_var: MLFLOW_TRACKING_URI
          model_name: churn_model
          required_version: "7"
          max_age_days: 30
          severity: WARN
        ```
    """

    asset_key: str = Field(
        description="Asset key this check attaches to (dot-separated for hierarchical keys)."
    )

    check_name: str = Field(description="Unique name for this asset check.")

    tracking_uri_env_var: str = Field(
        description="Env var holding the MLflow Tracking URI."
    )

    model_name: str = Field(description="Registered model name to check.")

    required_stage: Optional[str] = Field(
        default=None,
        description="If set, the model must have at least one version at this stage.",
    )

    required_version: Optional[str] = Field(
        default=None,
        description="If set, this specific version must exist in the registry.",
    )

    max_age_days: Optional[int] = Field(
        default=None,
        description=(
            "If set, the latest version at required_stage (or overall) must have been "
            "created within this many days. Freshness gate — useful to catch stale "
            "production models."
        ),
    )

    severity: str = Field(
        default="ERROR",
        description="ERROR blocks downstream materialization; WARN just flags in the UI.",
    )

    username_env_var: Optional[str] = Field(
        default=None, description="Env var for HTTP-basic MLflow auth username (optional)."
    )
    password_env_var: Optional[str] = Field(
        default=None, description="Env var for HTTP-basic MLflow auth password (optional)."
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Close over field values for the check body.
        asset_key_str = self.asset_key
        check_name = self.check_name
        tracking_uri_env_var = self.tracking_uri_env_var
        model_name = self.model_name
        required_stage = self.required_stage
        required_version = self.required_version
        max_age_days = self.max_age_days
        severity_str = self.severity.upper()
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var

        parts = asset_key_str.split("/") if "/" in asset_key_str else asset_key_str.split(".")
        target_key = AssetKey(parts) if len(parts) > 1 else AssetKey(asset_key_str)

        severity_enum = (
            AssetCheckSeverity.WARN if severity_str == "WARN" else AssetCheckSeverity.ERROR
        )

        @asset_check(
            name=check_name,
            asset=target_key,
            description=(
                f"Verifies MLflow model {model_name!r} state "
                f"({'stage=' + required_stage if required_stage else 'exists'})."
            ),
        )
        def _check(context: AssetCheckExecutionContext) -> AssetCheckResult:
            try:
                import mlflow
            except ImportError:
                return AssetCheckResult(
                    passed=False,
                    severity=severity_enum,
                    description="mlflow package not installed",
                )

            tracking_uri = os.environ.get(tracking_uri_env_var)
            if not tracking_uri:
                return AssetCheckResult(
                    passed=False,
                    severity=severity_enum,
                    description=f"Env var {tracking_uri_env_var!r} is not set",
                )

            if username_env_var and password_env_var:
                os.environ["MLFLOW_TRACKING_USERNAME"] = os.environ.get(username_env_var, "")
                os.environ["MLFLOW_TRACKING_PASSWORD"] = os.environ.get(password_env_var, "")

            client = mlflow.tracking.MlflowClient(tracking_uri=tracking_uri)

            # 1. Model exists?
            try:
                registered = client.get_registered_model(model_name)
            except Exception as e:  # noqa: BLE001
                return AssetCheckResult(
                    passed=False,
                    severity=severity_enum,
                    description=f"Model {model_name!r} not found: {e}",
                    metadata={"model_name": model_name},
                )

            # 2. Required version?
            if required_version:
                try:
                    mv = client.get_model_version(model_name, required_version)
                except Exception as e:  # noqa: BLE001
                    return AssetCheckResult(
                        passed=False,
                        severity=severity_enum,
                        description=f"Version {required_version} of {model_name!r} not found: {e}",
                        metadata={"model_name": model_name, "required_version": required_version},
                    )
                versions_at_stage: List = [mv]
            elif required_stage:
                versions_at_stage = client.get_latest_versions(model_name, stages=[required_stage])
                if not versions_at_stage:
                    return AssetCheckResult(
                        passed=False,
                        severity=severity_enum,
                        description=f"No versions of {model_name!r} at stage {required_stage!r}",
                        metadata={"model_name": model_name, "required_stage": required_stage},
                    )
            else:
                # No stage / version pin — just check model exists.
                return AssetCheckResult(
                    passed=True,
                    description=f"Model {model_name!r} exists in registry.",
                    metadata={
                        "model_name": model_name,
                        "latest_versions": len(registered.latest_versions),
                    },
                )

            # 3. Freshness?
            if max_age_days is not None and versions_at_stage:
                latest = max(versions_at_stage, key=lambda v: v.creation_timestamp)
                created = datetime.fromtimestamp(latest.creation_timestamp / 1000, tz=timezone.utc)
                age = datetime.now(timezone.utc) - created
                if age > timedelta(days=max_age_days):
                    return AssetCheckResult(
                        passed=False,
                        severity=severity_enum,
                        description=(
                            f"Latest {model_name!r} version {latest.version} at stage "
                            f"{latest.current_stage!r} is {age.days}d old (> max_age_days={max_age_days})"
                        ),
                        metadata={
                            "model_name": model_name,
                            "latest_version": latest.version,
                            "age_days": age.days,
                            "max_age_days": max_age_days,
                        },
                    )

            latest = versions_at_stage[0]
            return AssetCheckResult(
                passed=True,
                description=(
                    f"Model {model_name!r} check passed: version {latest.version} "
                    f"at stage {latest.current_stage!r}."
                ),
                metadata={
                    "model_name": model_name,
                    "version": latest.version,
                    "stage": latest.current_stage,
                    "run_id": MetadataValue.text(latest.run_id or ""),
                    "source": MetadataValue.text(latest.source or ""),
                },
            )

        return Definitions(asset_checks=[_check])
