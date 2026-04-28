"""Label Encoder Asset Component.

Encode categorical columns into integer codes. Useful for tree-based models
(which don't need one-hot expansion) and for compressing high-cardinality
categoricals into compact integer features.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

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
class LabelEncoderComponent(Component, Model, Resolvable):
    """Encode categorical columns into integer codes.

    Each unique value becomes a non-negative integer. Ordering can be:
    - `frequency` (default): most frequent value gets code 0, next gets 1, etc.
    - `alphabetical`: codes assigned by sorted value order.
    - `appearance`: codes assigned in first-seen order.

    NaN values map to -1 by default (configurable via `na_code`).
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    columns: List[str] = Field(description="Categorical columns to label-encode.")
    ordering: str = Field(
        default="frequency",
        description="How codes are assigned: 'frequency', 'alphabetical', or 'appearance'.",
    )
    na_code: int = Field(
        default=-1,
        description="Integer code assigned to NaN values. Use -1 to flag them, 0 to merge with the first category.",
    )
    suffix: Optional[str] = Field(
        default=None,
        description="If set, encoded values go into '<col><suffix>' (e.g. '_code'). Empty = overwrite original.",
    )
    keep_original: bool = Field(
        default=False,
        description="If True, retain the original categorical columns alongside the encoded ones. Implies a suffix.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(default=None, description="Partition type")
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format")
    partition_date_column: Optional[str] = Field(default=None, description="Column used to filter to current date partition.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static/multi partitioning.")
    partition_static_dim: Optional[str] = Field(default=None, description="Static dimension name for multi-partitioning.")
    partition_static_column: Optional[str] = Field(default=None, description="Column used to filter to the static partition value.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Max acceptable lag minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule for the freshness policy.")
    column_lineage: Optional[Dict[str, List[str]]] = Field(default=None, description="Column-level lineage.")

    @classmethod
    def get_description(cls) -> str:
        return "Encode categorical columns into integer codes (frequency, alphabetical, or appearance order)."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        columns = self.columns
        ordering = self.ordering
        na_code = self.na_code
        suffix = self.suffix
        keep_original = self.keep_original
        group_name = self.group_name

        if keep_original and not suffix:
            suffix = "_code"

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
            description=LabelEncoderComponent.get_description(),
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
            mappings: Dict[str, Dict[str, int]] = {}

            for col in columns:
                if col not in df.columns:
                    context.log.warning(f"Column '{col}' not found, skipping.")
                    continue
                s = df[col]
                if ordering == "frequency":
                    ordered = s.value_counts(dropna=True).index.tolist()
                elif ordering == "alphabetical":
                    ordered = sorted(s.dropna().unique().tolist(), key=lambda x: str(x))
                elif ordering == "appearance":
                    seen, ordered = set(), []
                    for v in s.dropna():
                        if v not in seen:
                            seen.add(v)
                            ordered.append(v)
                else:
                    raise ValueError(f"Unknown ordering: {ordering!r}")

                code_map = {v: i for i, v in enumerate(ordered)}
                out_col = f"{col}{suffix}" if suffix else col
                df[out_col] = s.map(code_map).fillna(na_code).astype("int64")
                mappings[col] = {str(k): v for k, v in code_map.items()}
                context.log.info(f"Column '{col}' → '{out_col}': encoded {len(code_map)} unique values ({ordering} order).")

            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(c), type=str(df.dtypes[c])) for c in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "encoding_ordering": MetadataValue.text(ordering),
                "encoding_mappings": MetadataValue.json(mappings),
            }

            _effective_lineage = column_lineage
            if not _effective_lineage:
                _effective_lineage = {}
                _upstream_cols = set(upstream.columns)
                for c in df.columns:
                    if c in _upstream_cols:
                        _effective_lineage[c] = [c]
                if suffix:
                    for col in columns:
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
