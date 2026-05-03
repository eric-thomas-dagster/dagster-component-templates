"""One-Hot Encoding Asset Component.

Convert categorical columns into binary indicator columns using pandas
`get_dummies`, with options for collinearity handling, NaN indicators,
rare-category bucketing, and configurable output dtype.
"""
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


class OneHotEncodingComponent(Component, Model, Resolvable):
    """Convert categorical columns into binary indicator (dummy) columns.

    Uses pandas `get_dummies` to expand each value of a categorical column
    into its own 0/1 column. Supports dropping the first level (to avoid
    multicollinearity in linear models), encoding NaN as its own indicator,
    capping rare categories into an `__other__` bucket, and configurable
    output dtype.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    columns: List[str] = Field(
        description="Categorical columns to one-hot encode. Each becomes N (or N-1) indicator columns.",
    )
    drop_first: bool = Field(
        default=False,
        description="Drop the first dummy level per column to avoid multicollinearity (recommended for linear models).",
    )
    dummy_na: bool = Field(
        default=False,
        description="Add an indicator column for NaN values. If False, NaN rows get all-zero dummies.",
    )
    prefix_sep: str = Field(
        default="_",
        description="Separator between original column name and category value, e.g. 'color_red'.",
    )
    prefix: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional per-column prefix override, e.g. {'state': 'st'} → 'st_CA'. Defaults to the column name.",
    )
    max_categories: Optional[int] = Field(
        default=None,
        description=(
            "Cap the number of categories per column. The most frequent N values keep their "
            "own dummy; the rest collapse into a single '__other__' indicator. None = unlimited."
        ),
    )
    keep_original: bool = Field(
        default=False,
        description="If True, keep the original categorical columns in the output alongside the dummy columns.",
    )
    dtype: str = Field(
        default="int",
        description="Output dtype for the indicator columns: 'int', 'bool', or 'float'.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from. Auto-inferred for passthrough and dummy columns when not set.",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 25 "
            "rows or a sample) for builder UIs."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata. For long DataFrames "
            "(>10x preview_rows), a random sample is used; otherwise head()."
        ),
    )


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Expand categorical columns into binary indicator columns with optional rare-category bucketing."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        columns = self.columns
        drop_first = self.drop_first
        dummy_na = self.dummy_na
        prefix_sep = self.prefix_sep
        prefix_map = self.prefix or {}
        max_categories = self.max_categories
        keep_original = self.keep_original
        dtype_name = self.dtype
        group_name = self.group_name

        _dtype_map = {"int": "int8", "bool": "bool", "float": "float32"}
        _out_dtype = _dtype_map.get(dtype_name, "int8")

        # Build partition definition
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

        # Infer kinds from asset name if not explicitly set
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
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
            description=OneHotEncodingComponent.get_description(),
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
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

            missing = [c for c in columns if c not in df.columns]
            for c in missing:
                context.log.warning(f"Column '{c}' not found in upstream DataFrame, skipping.")
            target_cols = [c for c in columns if c in df.columns]

            # Cap rare categories per column
            if max_categories is not None and max_categories > 0:
                for col in target_cols:
                    top = df[col].value_counts(dropna=True).head(max_categories).index
                    mask = ~df[col].isin(top) & df[col].notna()
                    n_other = int(mask.sum())
                    if n_other > 0:
                        df.loc[mask, col] = "__other__"
                        context.log.info(
                            f"Column '{col}': bucketed {n_other} rows into '__other__' "
                            f"(kept top {max_categories} categories)."
                        )

            # Apply pandas get_dummies, encoding only target_cols
            dummies = pd.get_dummies(
                df[target_cols],
                columns=target_cols,
                prefix={c: prefix_map.get(c, c) for c in target_cols},
                prefix_sep=prefix_sep,
                drop_first=drop_first,
                dummy_na=dummy_na,
                dtype=_out_dtype,
            )

            # Build the output frame: passthrough non-target columns, optionally
            # keep originals, append dummies.
            passthrough = df.drop(columns=target_cols) if not keep_original else df
            out = pd.concat([passthrough.reset_index(drop=True), dummies.reset_index(drop=True)], axis=1)

            for col in target_cols:
                n_dummies = sum(1 for c in dummies.columns if c.startswith(f"{prefix_map.get(col, col)}{prefix_sep}"))
                context.log.info(f"Column '{col}': produced {n_dummies} indicator columns.")

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(c), type=str(out.dtypes[c]))
                for c in out.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(out)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "encoded_columns": MetadataValue.json({
                    col: [c for c in dummies.columns if c.startswith(f"{prefix_map.get(col, col)}{prefix_sep}")]
                    for col in target_cols
                }),
            }

            # Auto-derive lineage if not explicitly provided: each dummy column
            # depends on its source categorical column; passthrough columns map 1:1.
            _effective_lineage = column_lineage
            if not _effective_lineage:
                _effective_lineage = {}
                _upstream_cols = set(upstream.columns)
                for c in passthrough.columns:
                    if c in _upstream_cols:
                        _effective_lineage[c] = [c]
                for col in target_cols:
                    _pfx = f"{prefix_map.get(col, col)}{prefix_sep}"
                    for d in dummies.columns:
                        if d.startswith(_pfx):
                            _effective_lineage[d] = [col]

            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {
                        out_col: [TableColumnDep(asset_key=_upstream_key, column_name=ic) for ic in in_cols]
                        for out_col, in_cols in _effective_lineage.items()
                    }
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(out) > 0:
                try:
                    _prev = out.sample(min(preview_rows, len(out))) if len(out) > preview_rows * 10 else out.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)

            return out

        from dagster import build_column_schema_change_checks

        _schema_checks = build_column_schema_change_checks(assets=[_asset])

        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
