"""Weights & Biases Asset Component.

Logs metrics, artifacts, and model checkpoints to Weights & Biases.

Supports two modes:
- ``log_metrics``: accepts a DataFrame from an upstream Dagster asset and logs each
  row as a W&B run step.
- ``log_artifact``: uploads a local file or directory as a W&B artifact.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
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
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class WandbAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Log metrics or upload artifacts to Weights & Biases.

    Set ``mode`` to ``"log_metrics"`` to stream rows from an upstream Dagster asset
    DataFrame into a W&B run, or to ``"log_artifact"`` to upload a file/directory
    as a W&B artifact.

    Example:
        ```yaml
        type: dagster_component_templates.WandbAssetComponent
        attributes:
          asset_name: model_training_run
          project: churn-prediction
          entity: my-team
          mode: log_metrics
          upstream_asset_key: marts/training_data
          wandb_api_key_env_var: WANDB_API_KEY
          metrics_columns:
            - train_loss
            - val_loss
            - accuracy
          step_column: epoch
          tags:
            - production
            - v2
          group_name: ml_tracking
        ```
    """

    asset_name: str = Field(description="Dagster asset key name.")
    upstream_asset_key: str = Field(
        description=(
            "Asset key of the upstream asset providing a pandas DataFrame. "
            "In ``log_metrics`` mode, each row is logged as a W&B run step. "
            "In ``log_artifact`` mode, this field is used for lineage only."
        )
    )
    wandb_api_key_env_var: str = Field(
        default="WANDB_API_KEY",
        description="Environment variable name holding the W&B API key.",
    )
    project: str = Field(description="W&B project name to log runs into.")
    entity: Optional[str] = Field(
        default=None,
        description="W&B entity (team or username). Defaults to the API-key owner.",
    )
    run_name: Optional[str] = Field(
        default=None,
        description="W&B run name. Auto-generated by W&B when None.",
    )
    mode: str = Field(
        default="log_metrics",
        description='Operation mode: "log_metrics" or "log_artifact".',
    )
    metrics_columns: Optional[list] = Field(
        default=None,
        description="Specific columns to log. None logs all numeric columns.",
    )
    step_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column to use as the W&B step/epoch number.",
    )
    artifact_path: Optional[str] = Field(
        default=None,
        description="Local file or directory path to upload. Required for log_artifact mode.",
    )
    artifact_name: Optional[str] = Field(
        default=None,
        description="Artifact name in W&B. Defaults to the basename of artifact_path.",
    )
    artifact_type: str = Field(
        default="dataset",
        description='W&B artifact type, e.g. "dataset", "model", "evaluation".',
    )
    tags: Optional[list] = Field(
        default=None,
        description="List of string tags to attach to the W&B run.",
    )
    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output DataFrame in metadata (for builder UIs).",
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview when include_preview_metadata=True.",
    )

    group_name: Optional[str] = Field(
        default="ml_tracking",
        description="Dagster asset group name shown in the UI.",
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Standard catalog fields — phase 2 wiring
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        component = self


        # Build partition definition (auto-generated; supports daily, weekly, monthly,

        # hourly partitions out of the box).
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @dg.asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=component.asset_name,
            group_name=component.group_name or "ml_tracking",
            kinds={"wandb", "ml"},
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(component.upstream_asset_key))},
            description=(
                f"W&B {component.mode} asset — project: {component.project}"
            ),
            freshness_policy=_freshness_policy,
            owners=self.owners or [],
            tags=_all_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _wandb_asset(
            context: dg.AssetExecutionContext,
            upstream,
        ) -> dg.MaterializeResult:
            import os
            import wandb

            api_key = os.environ[component.wandb_api_key_env_var]
            wandb.login(key=api_key)

            run = wandb.init(
                project=component.project,
                entity=component.entity,
                name=component.run_name,
                tags=component.tags or [],
                group=component.group_name,
            )

            try:
                if component.mode == "log_metrics":
                    import pandas as pd  # type: ignore[import]

                    df = upstream
                    if not isinstance(df, pd.DataFrame):
                        raise ValueError("Upstream asset did not provide a pandas DataFrame.")

                    context.log.info(f"[W&B] Logging {len(df)} rows to run {run.id}")

                    columns = list(df.columns)
                    if component.metrics_columns:
                        col_set = set(component.metrics_columns)
                        if component.step_column:
                            col_set.add(component.step_column)
                        columns_to_log = [c for c in columns if c in col_set]
                    else:
                        columns_to_log = columns

                    rows_logged = 0
                    for _, row in df.iterrows():
                        row_dict = row.to_dict()
                        step = None
                        if component.step_column and component.step_column in row_dict:
                            step = int(row_dict[component.step_column])

                        metrics = {
                            k: v
                            for k, v in row_dict.items()
                            if k in columns_to_log
                            and k != component.step_column
                            and isinstance(v, (int, float))
                        }
                        run.log(metrics, step=step)
                        rows_logged += 1

                    context.log.info(f"[W&B] Logged {rows_logged} rows to run {run.id}")

                elif component.mode == "log_artifact":
                    import os as _os

                    if not component.artifact_path:
                        raise ValueError("artifact_path is required for log_artifact mode.")

                    name = component.artifact_name or _os.path.basename(
                        component.artifact_path.rstrip("/")
                    )
                    artifact = wandb.Artifact(name=name, type=component.artifact_type)

                    if _os.path.isdir(component.artifact_path):
                        artifact.add_dir(component.artifact_path)
                    else:
                        artifact.add_file(component.artifact_path)

                    run.log_artifact(artifact)
                    context.log.info(f"[W&B] Uploaded artifact '{name}' to run {run.id}")

                else:
                    raise ValueError(
                        f"Unknown mode '{component.mode}'. Must be 'log_metrics' or 'log_artifact'."
                    )

            finally:
                run.finish()

            return dg.MaterializeResult(
                metadata={
                    "run_id": dg.MetadataValue.text(run.id),
                    "project": dg.MetadataValue.text(component.project),
                    "run_url": dg.MetadataValue.url(run.get_url() or ""),
                    "mode": dg.MetadataValue.text(component.mode),
                }
            )

        return dg.Definitions(assets=[_wandb_asset])
