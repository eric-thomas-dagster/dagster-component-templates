"""Anomaly Detection Component.

Detect anomalies in customer behavior, metrics, and time-series data using statistical methods
including Z-score, IQR, and moving average approaches.
"""

from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field


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
          source_asset: transactions
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

    source_asset: Optional[str] = Field(
        default=None,
        description="Source asset with data to analyze (set via lineage)"
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

    group_by_field: Optional[str] = Field(
        default=None,
        description="Group by field (e.g., customer_id) for per-group anomaly detection"
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

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_asset = self.source_asset
        detection_method = self.detection_method
        metric_column = self.metric_column
        threshold = self.threshold
        ma_window = self.moving_average_window
        group_by_field = self.group_by_field
        timestamp_field = self.timestamp_field
        id_field = self.id_field
        description = self.description or f"Anomaly detection ({detection_method})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Set up dependencies
        upstream_keys = []
        if source_asset:
            upstream_keys.append(source_asset)

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def anomaly_detection_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that detects anomalies in data."""

            # Load upstream data
            upstream_data = {}
            if upstream_keys and hasattr(context, 'load_asset_value'):
                for key in upstream_keys:
                    try:
                        value = context.load_asset_value(AssetKey(key))
                        upstream_data[key] = value
                        context.log.info(f"Loaded {len(value)} rows from {key}")
                    except Exception as e:
                        context.log.warning(f"Could not load {key}: {e}")
            else:
                upstream_data = kwargs

            if not upstream_data:
                context.log.warning("No upstream data available")
                return pd.DataFrame()

            # Get the source DataFrame
            df = list(upstream_data.values())[0]
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

            # Add metadata
            metadata = {
                "row_count": len(anomaly_df),
                "total_records": len(anomaly_df),
                "anomaly_count": int(anomaly_df['is_anomaly'].sum()),
                "anomaly_rate": round(anomaly_df['is_anomaly'].sum() / len(anomaly_df) * 100, 2),
                "detection_method": detection_method,
                "metric_column": metric_col,
                "threshold": threshold,
            }

            # Return with metadata
            if include_sample and len(anomaly_df) > 0:
                # Show anomalies first
                anomalies_first = pd.concat([
                    anomaly_df[anomaly_df['is_anomaly']].head(10),
                    anomaly_df[~anomaly_df['is_anomaly']].head(5)
                ])

                return Output(
                    value=anomaly_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(anomalies_first.to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(anomalies_first)
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return anomaly_df

        return Definitions(assets=[anomaly_detection_asset])
