"""ETS Forecast.

Fit an ETS (Exponential Smoothing) model and generate forecasts using Holt-Winters method.
"""
from dataclasses import dataclass
from typing import Optional

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
class EtsForecastComponent(Component, Model, Resolvable):
    """Fit an ETS (Exponential Smoothing) model and generate forecasts."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a time series DataFrame")
    date_column: str = Field(description="Column name containing datetime values")
    value_column: str = Field(description="Column name containing the numeric values to forecast")
    forecast_periods: int = Field(default=12, description="Number of periods to forecast ahead")
    trend: Optional[str] = Field(default="add", description="Trend component: None, 'add', or 'mul'")
    seasonal: Optional[str] = Field(default="add", description="Seasonal component: None, 'add', or 'mul'")
    seasonal_periods: Optional[int] = Field(default=12, description="Number of seasonal periods (e.g. 12 for monthly, 7 for weekly)")
    damped_trend: bool = Field(default=False, description="Whether to dampen the trend component")
    output_mode: str = Field(default="forecast", description="Output mode: 'forecast' or 'append'")
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

    @classmethod
    def get_description(cls) -> str:
        return "Fit an ETS (Exponential Smoothing / Holt-Winters) model and generate forecasts."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        date_column = self.date_column
        value_column = self.value_column
        forecast_periods = self.forecast_periods
        trend = self.trend
        seasonal = self.seasonal
        seasonal_periods = self.seasonal_periods
        damped_trend = self.damped_trend
        output_mode = self.output_mode
        group_name = self.group_name

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
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
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
            try:
                from statsmodels.tsa.holtwinters import ExponentialSmoothing
            except ImportError as e:
                raise ImportError("statsmodels is required: pip install statsmodels") from e

            df = upstream.copy()
            df[date_column] = pd.to_datetime(df[date_column])
            series = df.set_index(date_column)[value_column].sort_index()

            model = ExponentialSmoothing(
                series,
                trend=trend,
                seasonal=seasonal,
                seasonal_periods=seasonal_periods,
                damped_trend=damped_trend,
            )
            fitted = model.fit()

            context.add_output_metadata({
                "aic": MetadataValue.float(float(fitted.aic)),
                "bic": MetadataValue.float(float(fitted.bic)),
                "sse": MetadataValue.float(float(fitted.sse)),
                "observations": MetadataValue.int(len(series)),
                "trend": MetadataValue.text(str(trend)),
                "seasonal": MetadataValue.text(str(seasonal)),
            })

            forecast_vals = fitted.forecast(forecast_periods)
            last_date = series.index[-1]
            freq = pd.infer_freq(series.index)
            if freq is None:
                freq = "ME"
            future_dates = pd.date_range(start=last_date, periods=forecast_periods + 1, freq=freq)[1:]
            forecast_df = pd.DataFrame({
                date_column: future_dates,
                value_column: forecast_vals.values,
            })

            if output_mode == "forecast":
                return forecast_df
            elif output_mode == "append":
                original = df[[date_column, value_column]].copy()
                return pd.concat([original, forecast_df], ignore_index=True)
            else:
                raise ValueError(f"Unknown output_mode: {output_mode}. Use 'forecast' or 'append'.")

        return Definitions(assets=[_asset])
