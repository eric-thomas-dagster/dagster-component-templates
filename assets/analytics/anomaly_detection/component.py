"""Anomaly Detection Component.

Detect anomalies in customer behavior, metrics, and time-series data using statistical methods
including Z-score, IQR, and moving average approaches.
"""

from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
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
    Output,
)
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


class AnomalyDetectionComponent(Component, Model, Resolvable):
    """Component for detecting anomalies in data.

    Detects unusual patterns using statistical methods:
    - **Z-Score**: Identifies values far from mean (standard deviations)
    - **IQR (Interquartile Range)**: Detects outliers using quartiles
    - **Moving Average**: Finds deviations from trend
    - **Threshold**: Simple threshold-based detection

    Use cases:
    - Unusual purchase amounts
    - Sudden activity spikes/drops
    - Fraudulent behavior patterns
    - System performance issues
    - Business metric anomalies

    Example:
        ```yaml
        type: dagster_component_templates.AnomalyDetectionComponent
        attributes:
          asset_name: transaction_anomalies
          upstream_asset_key: transactions
          detection_method: z_score
          metric_column: amount
          threshold: 3.0
          description: "Transaction anomaly detection"
          group_name: fraud_detection
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame to analyze for anomalies"
    )

    detection_method: str = Field(
        default="z_score",
        description="Method: z_score, iqr, moving_average, threshold"
    )

    metric_column: Optional[str] = Field(
        default=None,
        description="Column containing metric to analyze for anomalies"
    )

    threshold: float = Field(
        default=3.0,
        description="Detection threshold (Z-score stdevs, IQR multiplier, or absolute threshold)"
    )

    moving_average_window: int = Field(
        default=7,
        description="Window size for moving average method (days/records)"
    )

    group_by: Optional[str] = Field(
        default=None,
        description="Group by field (e.g., customer_id) for per-group anomaly detection",
    )

    timestamp_field: Optional[str] = Field(
        default=None,
        description="Timestamp field for time-series anomaly detection (optional)"
    )

    id_field: Optional[str] = Field(
        default=None,
        description="ID field for tracking individual records (auto-detected)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="monitoring",
        description="Asset group for organization"
    )
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
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
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



    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        detection_method = self.detection_method
        metric_column = self.metric_column
        threshold = self.threshold
        ma_window = self.moving_average_window
        group_by_field = self.group_by
        timestamp_field = self.timestamp_field
        id_field = self.id_field
        description = self.description or f"Anomaly detection ({detection_method})"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "anomaly_detection"  # component directory name
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

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def anomaly_detection_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that detects anomalies in data."""

            df = upstream
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} records for anomaly detection")

            # Auto-detect columns
            def find_column(possible_names, custom_name=None):
                if custom_name and custom_name in df.columns:
                    return custom_name
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            # Find metric column
            if not metric_column:
                # Try to find a numeric column
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    metric_col = numeric_cols[0]
                    context.log.info(f"Auto-detected metric column: {metric_col}")
                else:
                    context.log.error("No numeric columns found for anomaly detection")
                    return pd.DataFrame()
            else:
                metric_col = metric_column

            if metric_col not in df.columns:
                context.log.error(f"Metric column '{metric_col}' not found in data")
                return pd.DataFrame()

            # Optional fields
            id_col = find_column(
                ['id', 'record_id', 'transaction_id', 'event_id'],
                id_field
            )
            timestamp_col = find_column(
                ['timestamp', 'date', 'created_at', 'event_time'],
                timestamp_field
            )

            # Prepare data
            anomaly_df = df.copy()

            # Convert metric to numeric
            anomaly_df[metric_col] = pd.to_numeric(anomaly_df[metric_col], errors='coerce')
            anomaly_df = anomaly_df.dropna(subset=[metric_col])

            if len(anomaly_df) == 0:
                context.log.warning("No valid numeric data to analyze")
                return pd.DataFrame()

            context.log.info(f"Detecting anomalies using {detection_method} method on column '{metric_col}'")

            # Initialize anomaly flags
            anomaly_df['is_anomaly'] = False
            anomaly_df['anomaly_score'] = 0.0
            anomaly_df['anomaly_reason'] = ''

            # Apply detection method
            if detection_method == 'z_score':
                # Z-score method: Flag points > threshold standard deviations from mean
                if group_by_field and group_by_field in anomaly_df.columns:
                    # Per-group anomaly detection
                    for group, group_data in anomaly_df.groupby(group_by_field):
                        mean = group_data[metric_col].mean()
                        std = group_data[metric_col].std()

                        if std > 0:
                            z_scores = np.abs((group_data[metric_col] - mean) / std)
                            anomaly_mask = z_scores > threshold

                            anomaly_df.loc[group_data.index, 'anomaly_score'] = z_scores
                            anomaly_df.loc[group_data.index[anomaly_mask], 'is_anomaly'] = True
                            anomaly_df.loc[group_data.index[anomaly_mask], 'anomaly_reason'] = f'Z-score > {threshold}'
                else:
                    # Global anomaly detection
                    mean = anomaly_df[metric_col].mean()
                    std = anomaly_df[metric_col].std()

                    if std > 0:
                        z_scores = np.abs((anomaly_df[metric_col] - mean) / std)
                        anomaly_df['anomaly_score'] = z_scores
                        anomaly_df['is_anomaly'] = z_scores > threshold
                        anomaly_df.loc[anomaly_df['is_anomaly'], 'anomaly_reason'] = f'Z-score > {threshold}'

            elif detection_method == 'iqr':
                # IQR method: Flag points outside threshold * IQR from quartiles
                if group_by_field and group_by_field in anomaly_df.columns:
                    for group, group_data in anomaly_df.groupby(group_by_field):
                        q1 = group_data[metric_col].quantile(0.25)
                        q3 = group_data[metric_col].quantile(0.75)
                        iqr = q3 - q1

                        lower_bound = q1 - threshold * iqr
                        upper_bound = q3 + threshold * iqr

                        anomaly_mask = (group_data[metric_col] < lower_bound) | (group_data[metric_col] > upper_bound)

                        # Score based on distance from bounds
                        scores = np.maximum(
                            (lower_bound - group_data[metric_col]) / iqr,
                            (group_data[metric_col] - upper_bound) / iqr
                        ).clip(0)

                        anomaly_df.loc[group_data.index, 'anomaly_score'] = scores
                        anomaly_df.loc[group_data.index[anomaly_mask], 'is_anomaly'] = True
                        anomaly_df.loc[group_data.index[anomaly_mask], 'anomaly_reason'] = f'Outside {threshold}x IQR'
                else:
                    q1 = anomaly_df[metric_col].quantile(0.25)
                    q3 = anomaly_df[metric_col].quantile(0.75)
                    iqr = q3 - q1

                    lower_bound = q1 - threshold * iqr
                    upper_bound = q3 + threshold * iqr

                    anomaly_mask = (anomaly_df[metric_col] < lower_bound) | (anomaly_df[metric_col] > upper_bound)

                    # Score based on distance from bounds
                    scores = np.maximum(
                        (lower_bound - anomaly_df[metric_col]) / iqr,
                        (anomaly_df[metric_col] - upper_bound) / iqr
                    ).clip(0)

                    anomaly_df['anomaly_score'] = scores
                    anomaly_df['is_anomaly'] = anomaly_mask
                    anomaly_df.loc[anomaly_mask, 'anomaly_reason'] = f'Outside {threshold}x IQR'

            elif detection_method == 'moving_average':
                # Moving average: Flag points that deviate from trend
                if timestamp_col and timestamp_col in anomaly_df.columns:
                    anomaly_df[timestamp_col] = pd.to_datetime(anomaly_df[timestamp_col], errors='coerce')
                    anomaly_df = anomaly_df.sort_values(timestamp_col)

                    # Calculate moving average
                    anomaly_df['moving_avg'] = anomaly_df[metric_col].rolling(window=ma_window, min_periods=1).mean()
                    anomaly_df['moving_std'] = anomaly_df[metric_col].rolling(window=ma_window, min_periods=1).std()

                    # Detect deviations
                    anomaly_df['deviation'] = np.abs(anomaly_df[metric_col] - anomaly_df['moving_avg'])
                    anomaly_df['anomaly_score'] = anomaly_df['deviation'] / (anomaly_df['moving_std'] + 0.0001)

                    anomaly_df['is_anomaly'] = anomaly_df['anomaly_score'] > threshold
                    anomaly_df.loc[anomaly_df['is_anomaly'], 'anomaly_reason'] = f'Deviation > {threshold} × std from MA'
                else:
                    context.log.warning("Moving average requires timestamp column, using global moving average")
                    anomaly_df['moving_avg'] = anomaly_df[metric_col].rolling(window=ma_window, min_periods=1).mean()
                    anomaly_df['deviation'] = np.abs(anomaly_df[metric_col] - anomaly_df['moving_avg'])
                    anomaly_df['anomaly_score'] = anomaly_df['deviation']

                    # Use threshold as absolute deviation
                    threshold_value = anomaly_df[metric_col].std() * threshold
                    anomaly_df['is_anomaly'] = anomaly_df['anomaly_score'] > threshold_value
                    anomaly_df.loc[anomaly_df['is_anomaly'], 'anomaly_reason'] = f'Deviation > {threshold_value:.2f} from MA'

            elif detection_method == 'threshold':
                # Simple threshold: Flag values above threshold
                anomaly_df['is_anomaly'] = anomaly_df[metric_col] > threshold
                anomaly_df['anomaly_score'] = anomaly_df[metric_col] / threshold
                anomaly_df.loc[anomaly_df['is_anomaly'], 'anomaly_reason'] = f'Value > {threshold}'

            else:
                context.log.error(f"Unknown detection method: {detection_method}")
                return pd.DataFrame()

            # Round anomaly scores
            anomaly_df['anomaly_score'] = anomaly_df['anomaly_score'].round(3)

            # Filter to anomalies only (or keep all with flag)
            # For now, keep all records with anomaly flag
            context.log.info(f"Anomaly detection complete: {anomaly_df['is_anomaly'].sum()} anomalies found out of {len(anomaly_df)} records")

            # Log summary
            if anomaly_df['is_anomaly'].sum() > 0:
                anomaly_rate = (anomaly_df['is_anomaly'].sum() / len(anomaly_df) * 100).round(2)
                context.log.info(f"  Anomaly rate: {anomaly_rate}%")

                # Log top anomalies
                top_anomalies = anomaly_df[anomaly_df['is_anomaly']].nlargest(5, 'anomaly_score')
                context.log.info(f"\n  Top 5 Anomalies:")
                for idx, row in top_anomalies.iterrows():
                    context.log.info(f"    Score: {row['anomaly_score']:.3f}, Value: {row[metric_col]}, Reason: {row['anomaly_reason']}")

            # Add metadata (always; sample preview is appended below if requested).
            # Cast aggregations to Python natives — numpy.float64/int64 from
            # pandas operations don't serialize cleanly into Dagster's event log.
            _n_anomalies = int(anomaly_df['is_anomaly'].sum())
            _n_total = int(len(anomaly_df))
            metadata = {
                "row_count": _n_total,
                "total_records": _n_total,
                "anomaly_count": _n_anomalies,
                "anomaly_rate": round(_n_anomalies / _n_total * 100, 2) if _n_total else 0.0,
                "detection_method": detection_method,
                "metric_column": metric_col,
                "threshold": float(threshold),
            }

            if include_preview and len(anomaly_df) > 0:
                # Anomalies first, then a few normals — markdown preview
                anomalies_first = pd.concat([
                    anomaly_df[anomaly_df['is_anomaly']].head(10),
                    anomaly_df[~anomaly_df['is_anomaly']].head(5),
                ])
                metadata["preview"] = MetadataValue.md(anomalies_first.to_markdown(index=False))

            context.add_output_metadata(metadata)

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(anomaly_df.dtypes[col]))
                for col in anomaly_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(anomaly_df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return anomaly_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[anomaly_detection_asset])


        return Definitions(assets=[anomaly_detection_asset], asset_checks=list(_schema_checks))
