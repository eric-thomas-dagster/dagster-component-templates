"""ARIMA Forecast.

Fit an ARIMA time series model and generate forecasts with optional confidence intervals.
"""
from dataclasses import dataclass, field
from typing import List, Optional

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
class ArimaForecastComponent(Component, Model, Resolvable):
    """Fit an ARIMA time series model and generate forecasts."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a time series DataFrame")
    date_column: str = Field(description="Column name containing datetime values")
    value_column: str = Field(description="Column name containing the numeric values to forecast")
    forecast_periods: int = Field(default=12, description="Number of periods to forecast ahead")
    order: List[int] = Field(default_factory=lambda: [1, 1, 1], description="ARIMA (p, d, q) order")
    seasonal_order: Optional[List[int]] = Field(default=None, description="Seasonal (P, D, Q, S) order")
    output_mode: str = Field(
        default="forecast",
        description="Output mode: 'forecast' (only future rows), 'append' (original + forecast), 'fitted' (original + in-sample fitted values)",
    )
    confidence_level: float = Field(default=0.95, description="Confidence level for prediction intervals")
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
        return "Fit an ARIMA time series model and generate forecasts with confidence intervals."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        date_column = self.date_column
        value_column = self.value_column
        forecast_periods = self.forecast_periods
        order = self.order
        seasonal_order = self.seasonal_order
        output_mode = self.output_mode
        confidence_level = self.confidence_level
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
                from statsmodels.tsa.arima.model import ARIMA
            except ImportError as e:
                raise ImportError("statsmodels is required: pip install statsmodels") from e

            df = upstream.copy()
            df[date_column] = pd.to_datetime(df[date_column])
            series = df.set_index(date_column)[value_column].sort_index()

            arima_order = tuple(order)
            sarima_order = tuple(seasonal_order) if seasonal_order else None

            model = ARIMA(series, order=arima_order, seasonal_order=sarima_order)
            fitted = model.fit()

            context.add_output_metadata({
                "aic": MetadataValue.float(float(fitted.aic)),
                "bic": MetadataValue.float(float(fitted.bic)),
                "order": MetadataValue.text(str(arima_order)),
                "observations": MetadataValue.int(len(series)),
            })

            if output_mode == "fitted":
                result = df.copy()
                result["fitted_value"] = fitted.fittedvalues.values
                result["residual"] = fitted.resid.values
                return result

            forecast = fitted.get_forecast(steps=forecast_periods)
            forecast_df = forecast.summary_frame(alpha=1 - confidence_level)
            forecast_df = forecast_df.rename(columns={
                "mean": value_column,
                "mean_ci_lower": f"{value_column}_lower_{int(confidence_level*100)}",
                "mean_ci_upper": f"{value_column}_upper_{int(confidence_level*100)}",
            })
            forecast_df.index.name = date_column
            forecast_df = forecast_df.reset_index()

            if output_mode == "forecast":
                return forecast_df[[date_column, value_column,
                                    f"{value_column}_lower_{int(confidence_level*100)}",
                                    f"{value_column}_upper_{int(confidence_level*100)}"]]
            elif output_mode == "append":
                original = df[[date_column, value_column]].copy()
                forecast_out = forecast_df[[date_column, value_column,
                                            f"{value_column}_lower_{int(confidence_level*100)}",
                                            f"{value_column}_upper_{int(confidence_level*100)}"]]
                return pd.concat([original, forecast_out], ignore_index=True)
            else:
                raise ValueError(f"Unknown output_mode: {output_mode}. Use 'forecast', 'append', or 'fitted'.")

        return Definitions(assets=[_asset])
