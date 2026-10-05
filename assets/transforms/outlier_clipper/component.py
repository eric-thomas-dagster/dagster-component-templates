"""Outlier Clipper Asset Component.

Detect and handle outliers in numeric columns using IQR, z-score, or
quantile thresholds. Outliers can be clipped (winsorized) to the boundary
or dropped from the DataFrame.
"""
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
    Resolvable,
    asset,
)
from pydantic import Field



def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine()
    (SQLAlchemy), .get_connection() (DBAPI), or .get_client() (vendor
    client -- dispatched by the client's own shape since "get_client" means
    something different per vendor: BigQuery's .query(sql).to_dataframe(),
    Redshift's .execute_query(sql, fetch_results=True, cursor_factory=
    RealDictCursor)), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no Dagster resource is registered. Same
    helper, same contract, as every other dual-ingestion component in this
    repo (e.g. automl_asset, logistic_regression_model, churn_prediction)."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        if hasattr(resource, "get_client"):
            client = resource.get_client()
            if hasattr(client, "query"):
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(sql, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this helper doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found). Add a dispatch branch for it."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        import os
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


class OutlierClipperComponent(Component, Model, Resolvable):
    """Clip or drop outliers in numeric columns.

    Strategies:
    - `iqr`: outlier if x < Q1 − k·IQR or x > Q3 + k·IQR (k from `iqr_multiplier`, default 1.5).
    - `zscore`: outlier if |z| > `zscore_threshold` (default 3.0).
    - `quantile`: outlier if x < quantile(`lower_quantile`) or x > quantile(`upper_quantile`).

    Action:
    - `clip` (default): replace outliers with the boundary value (winsorize).
    - `drop`: remove rows where any target column is an outlier.
    - `flag`: keep rows; add a boolean column `<col>_is_outlier`.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )
    strategy: str = Field(
        default="iqr",
        description="Detection strategy: 'iqr', 'zscore', or 'quantile'.",
    )
    action: str = Field(
        default="clip",
        description="What to do with outliers: 'clip' (winsorize), 'drop' (remove rows), or 'flag' (add boolean column).",
    )
    columns: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Columns to check for outliers. None = all numeric columns.",
    )
    iqr_multiplier: float = Field(
        default=1.5,
        description="IQR fence multiplier. 1.5 = standard Tukey fences; 3.0 = extreme outliers only.",
    )
    zscore_threshold: float = Field(
        default=3.0,
        description="Absolute z-score threshold for the 'zscore' strategy.",
    )
    lower_quantile: float = Field(
        default=0.01,
        description="Lower quantile for the 'quantile' strategy (default 0.01 = 1st percentile).",
    )
    upper_quantile: float = Field(
        default=0.99,
        description="Upper quantile for the 'quantile' strategy (default 0.99 = 99th percentile).",
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

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(default=None, description="Partition type")
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format")
    partition_date_column: Optional[Union[str, int]] = Field(default=None, description="Column used to filter to current date partition.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static/multi partitioning.")
    partition_static_dim: Optional[str] = Field(default=None, description="Static dimension name for multi-partitioning.")
    partition_static_column: Optional[Union[str, int]] = Field(default=None, description="Column used to filter to the static partition value.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Max acceptable lag minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule for the freshness policy.")
    column_lineage: Optional[Dict[str, List[str]]] = Field(default=None, description="Column-level lineage.")


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
        return "Detect and clip, drop, or flag outliers in numeric columns using IQR, z-score, or quantile thresholds."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("OutlierClipperComponent: set exactly one of `upstream_asset_key` or `source`.")
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
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        strategy = self.strategy
        action = self.action
        columns = self.columns
        iqr_multiplier = self.iqr_multiplier
        zscore_threshold = self.zscore_threshold
        lower_quantile = self.lower_quantile
        upper_quantile = self.upper_quantile
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
            key=AssetKey.from_user_string(asset_name),
            ins=({"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))} if upstream_asset_key else None),
            required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
            partitions_def=partitions_def,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            description=OutlierClipperComponent.get_description(),
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            upstream = kwargs.get("upstream")
            if upstream is None:
                upstream = _ingest_warehouse_query(self.source, context)
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
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
            bounds: Dict[str, Dict[str, float]] = {}
            outlier_counts: Dict[str, int] = {}
            n_in = len(df)

            outlier_mask_any = pd.Series(False, index=df.index)

            for col in target_cols:
                if col not in df.columns:
                    context.log.warning(f"Column '{col}' not found, skipping.")
                    continue
                s = pd.to_numeric(df[col], errors="coerce")

                if strategy == "iqr":
                    q1, q3 = float(s.quantile(0.25)), float(s.quantile(0.75))
                    iqr = q3 - q1
                    lo, hi = q1 - iqr_multiplier * iqr, q3 + iqr_multiplier * iqr
                elif strategy == "zscore":
                    mu, sd = float(s.mean()), float(s.std(ddof=0))
                    if sd == 0:
                        lo, hi = float("-inf"), float("inf")
                    else:
                        lo, hi = mu - zscore_threshold * sd, mu + zscore_threshold * sd
                elif strategy == "quantile":
                    lo, hi = float(s.quantile(lower_quantile)), float(s.quantile(upper_quantile))
                else:
                    raise ValueError(f"Unknown outlier strategy: {strategy!r}")

                col_mask = (s < lo) | (s > hi)
                col_mask = col_mask.fillna(False)
                n_out = int(col_mask.sum())
                outlier_counts[col] = n_out
                bounds[col] = {"lower": lo, "upper": hi}

                if action == "clip":
                    df[col] = s.clip(lower=lo, upper=hi)
                elif action == "drop":
                    outlier_mask_any = outlier_mask_any | col_mask
                elif action == "flag":
                    df[f"{col}_is_outlier"] = col_mask.astype("bool")
                else:
                    raise ValueError(f"Unknown action: {action!r}")

                context.log.info(f"Column '{col}': {n_out} outliers detected (bounds [{lo:.4g}, {hi:.4g}]); action='{action}'.")

            if action == "drop":
                df = df.loc[~outlier_mask_any]
                context.log.info(f"Dropped {int(outlier_mask_any.sum())} of {n_in} rows containing outliers.")

            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(c), type=str(df.dtypes[c])) for c in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "outlier_strategy": MetadataValue.text(strategy),
                "outlier_action": MetadataValue.text(action),
                "outlier_bounds": MetadataValue.json(bounds),
                "outlier_counts": MetadataValue.json(outlier_counts),
                "rows_in": MetadataValue.int(n_in),
                "rows_out": MetadataValue.int(len(df)),
            }

            _effective_lineage = column_lineage
            if not _effective_lineage:
                _effective_lineage = {}
                _upstream_cols = set(upstream.columns)
                for c in df.columns:
                    base = c[:-len("_is_outlier")] if c.endswith("_is_outlier") else c
                    if base in _upstream_cols:
                        _effective_lineage[c] = [base]
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
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=[_asset])
        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
