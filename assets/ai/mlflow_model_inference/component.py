"""MLflow Model Inference Component.

Materializable asset that loads a registered MLflow model version and scores
an upstream DataFrame. Bounded selection via version-pin or stage; auditable
per-run metadata (model URI, version, row_count, latency).

Right for teams that treat "score today's batch" as a first-class Dagster
asset in the graph — not a shell script that runs somewhere on a cron.
"""

import os
import time
from typing import Any, Dict, List, Optional, Union

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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


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

    upstream_asset_key: Union[str, List[str]] = Field(
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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

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

        @asset(partitions_def=partitions_def, 
            key=target_key,
            group_name=self.group_name,
            compute_kind="mlflow",
            ins={"upstream": AssetIn(key=upstream_key)},
            description=f"Loads {model_uri} and scores {upstream_asset_key!r}.",
        )
        def _inference(context: AssetExecutionContext, upstream) -> MaterializeResult:
            # Filter to current partition if partitioned
            if hasattr(upstream, "columns") and (context.has_partition_key or context.has_partition_key_range):
                import pandas as pd
                # One filter pass per partition key: a normal tick gives exactly one,
                # a single_run backfill gives the whole range. Rows are selected by
                # position so keys that match overlapping rows never duplicate them.
                _pk_base = upstream
                _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                _pk_keep = set()
                _pk_filtered = False
                for _pk_iter in context.partition_keys:
                    upstream = _pk_pos
                    _pk = _pk_iter
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                    if partition_date_column and partition_date_column in upstream.columns and _date_key:
                        _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                        upstream = upstream[_col_dates == _date_key]
                    if partition_static_column and partition_static_column in upstream.columns and _static_key:
                        upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                        upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                    if upstream is not _pk_pos:
                        _pk_filtered = True
                        _pk_keep.update(upstream.index)
                upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
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


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
# END scaffold partitions-def builder
