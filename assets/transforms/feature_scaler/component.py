"""Feature Scaler Asset Component.

Rescale numeric DataFrame columns using one of: standard (z-score), min-max,
robust (median/IQR), or max-abs scaling. Each strategy mirrors the
corresponding scikit-learn scaler but is implemented directly in pandas/numpy
so the component has no extra runtime dependency.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
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
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class FeatureScalerComponent(Component, Model, Resolvable):
    """Rescale numeric columns using standard, min-max, robust, or max-abs scaling.

    - `standard`: (x − mean) / std  (z-score)
    - `minmax`:   (x − min) / (max − min) → [0, 1] (or to feature_range)
    - `robust`:   (x − median) / IQR  (resistant to outliers)
    - `maxabs`:   x / max(|x|)  → [-1, 1] (preserves sparsity)
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    strategy: str = Field(
        default="standard",
        description="Scaling strategy: 'standard', 'minmax', 'robust', or 'maxabs'.",
    )
    columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to scale. None = all numeric columns in the upstream DataFrame.",
    )
    feature_range_min: float = Field(
        default=0.0,
        description="Lower bound for 'minmax' (default 0.0). Ignored for other strategies.",
    )
    feature_range_max: float = Field(
        default=1.0,
        description="Upper bound for 'minmax' (default 1.0). Ignored for other strategies.",
    )
    suffix: Optional[str] = Field(
        default=None,
        description="If set, scaled values go into new columns named '<col><suffix>' (e.g. '_scaled'). Otherwise originals are overwritten.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format")
    partition_date_column: Optional[str] = Field(default=None, description="Column used to filter to the current date partition key.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static or multi partitioning.")
    partition_static_dim: Optional[str] = Field(default=None, description="Dimension name for the static axis in multi-partitioning.")
    partition_static_column: Optional[str] = Field(default=None, description="Column used to filter to the current static partition dimension.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners — team names or email addresses.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags applied to the asset.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the Dagster catalog. Auto-inferred from asset name if empty.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping. Auto-inferred when empty.",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Rescale numeric columns using standard, min-max, robust, or max-abs scaling."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        strategy = self.strategy
        columns = self.columns
        fr_min = self.feature_range_min
        fr_max = self.feature_range_max
        suffix = self.suffix
        group_name = self.group_name

        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            description=FeatureScalerComponent.get_description(),
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]

            df = upstream.copy()

            target_cols = columns if columns else df.select_dtypes(include="number").columns.tolist()
            stats: Dict[str, Dict[str, float]] = {}

            for col in target_cols:
                if col not in df.columns:
                    context.log.warning(f"Column '{col}' not found, skipping.")
                    continue
                s = pd.to_numeric(df[col], errors="coerce")
                out_col = f"{col}{suffix}" if suffix else col

                if strategy == "standard":
                    mu, sd = float(s.mean()), float(s.std(ddof=0))
                    sd = sd if sd != 0 else 1.0
                    df[out_col] = (s - mu) / sd
                    stats[col] = {"mean": mu, "std": sd}
                elif strategy == "minmax":
                    lo, hi = float(s.min()), float(s.max())
                    rng = (hi - lo) if hi != lo else 1.0
                    scaled = (s - lo) / rng
                    df[out_col] = scaled * (fr_max - fr_min) + fr_min
                    stats[col] = {"min": lo, "max": hi}
                elif strategy == "robust":
                    med = float(s.median())
                    q1, q3 = float(s.quantile(0.25)), float(s.quantile(0.75))
                    iqr = (q3 - q1) if (q3 - q1) != 0 else 1.0
                    df[out_col] = (s - med) / iqr
                    stats[col] = {"median": med, "iqr": iqr}
                elif strategy == "maxabs":
                    m = float(s.abs().max())
                    m = m if m != 0 else 1.0
                    df[out_col] = s / m
                    stats[col] = {"max_abs": m}
                else:
                    raise ValueError(f"Unknown scaling strategy: {strategy!r}")

                context.log.info(f"Column '{col}' → '{out_col}' scaled with '{strategy}': {stats[col]}")

            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(c), type=str(df.dtypes[c])) for c in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "scaling_strategy": MetadataValue.text(strategy),
                "scaling_stats": MetadataValue.json(stats),
            }

            _effective_lineage = column_lineage
            if not _effective_lineage:
                _effective_lineage = {}
                _upstream_cols = set(upstream.columns)
                for c in df.columns:
                    if c in _upstream_cols:
                        _effective_lineage[c] = [c]
                if suffix:
                    for col in target_cols:
                        if col in _upstream_cols:
                            _effective_lineage[f"{col}{suffix}"] = [col]
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {
                        out_col: [TableColumnDep(asset_key=_upstream_key, column_name=ic) for ic in in_cols]
                        for out_col, in_cols in _effective_lineage.items()
                    }
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=[_asset])
        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
