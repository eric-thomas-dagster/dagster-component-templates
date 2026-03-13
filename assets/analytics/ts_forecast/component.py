"""Time Series Forecast Component.

Generate forecasts from time series data using ARIMA, ETS, or automatic model selection.
"""

from dataclasses import dataclass
from typing import Optional, List
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
class TsForecastComponent(Component, Model, Resolvable):
    """Component for generating time series forecasts using ARIMA or ETS models.

    Takes a historical time series DataFrame and produces forecast values for
    a configurable number of future periods. Supports automatic model selection
    by comparing AIC scores.

    Features:
    - ARIMA and ETS (Holt-Winters Exponential Smoothing) models
    - Automatic model selection via AIC comparison
    - Append mode to combine historical and forecast data
    - Configurable seasonal periods

    Use Cases:
    - Sales forecasting
    - Demand planning
    - Capacity planning
    - Financial projections
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with date + value columns"
    )
    date_column: str = Field(description="Column containing dates or timestamps")
    value_column: str = Field(description="Column containing the numeric time series values")
    forecast_periods: int = Field(default=12, description="Number of future periods to forecast")
    model: str = Field(
        default="auto",
        description="Model to use: 'auto' (tries ARIMA and ETS, picks best AIC), 'arima', 'ets'",
    )
    arima_order: List[int] = Field(
        default=[1, 1, 1], description="ARIMA (p, d, q) order as a list of 3 integers"
    )
    ets_trend: Optional[str] = Field(
        default="add", description="ETS trend component: None, 'add', 'mul'"
    )
    ets_seasonal: Optional[str] = Field(
        default="add", description="ETS seasonal component: None, 'add', 'mul'"
    )
    ets_seasonal_periods: int = Field(
        default=12, description="Number of periods in one seasonal cycle"
    )
    output_mode: str = Field(
        default="forecast_only",
        description="Output mode: 'forecast_only' (only forecasted rows) or 'append' (history + forecast)",
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

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        date_column = self.date_column
        value_column = self.value_column
        forecast_periods = self.forecast_periods
        model = self.model
        arima_order = self.arima_order
        ets_trend = self.ets_trend
        ets_seasonal = self.ets_seasonal
        ets_seasonal_periods = self.ets_seasonal_periods
        output_mode = self.output_mode

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
            group_name=self.group_name,
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
                from statsmodels.tsa.holtwinters import ExponentialSmoothing
            except ImportError:
                raise ImportError("statsmodels required: pip install statsmodels")

            series = upstream.set_index(date_column)[value_column].sort_index()
            context.log.info(
                f"Fitting model='{model}' on {len(series)} observations, forecasting {forecast_periods} periods"
            )

            arima_fit = None
            ets_fit = None
            aic_arima = float("inf")
            aic_ets = float("inf")

            if model in ("auto", "arima"):
                arima_fit = ARIMA(series, order=tuple(arima_order)).fit()
                aic_arima = arima_fit.aic
                context.log.info(f"ARIMA AIC: {aic_arima:.4f}")

            if model in ("auto", "ets"):
                ets_fit = ExponentialSmoothing(
                    series,
                    trend=ets_trend,
                    seasonal=ets_seasonal,
                    seasonal_periods=ets_seasonal_periods,
                ).fit()
                aic_ets = ets_fit.aic
                context.log.info(f"ETS AIC: {aic_ets:.4f}")

            if model == "auto":
                best_fit = arima_fit if aic_arima <= aic_ets else ets_fit
                chosen = "ARIMA" if aic_arima <= aic_ets else "ETS"
                context.log.info(f"Auto selected: {chosen}")
            elif model == "arima":
                best_fit = arima_fit
            else:
                best_fit = ets_fit

            forecast_vals = best_fit.forecast(forecast_periods)
            forecast_df = pd.DataFrame(
                {
                    date_column: forecast_vals.index,
                    value_column: forecast_vals.values,
                    "is_forecast": True,
                }
            )

            if output_mode == "append":
                hist_df = upstream.copy()
                hist_df["is_forecast"] = False
                result = pd.concat([hist_df, forecast_df], ignore_index=True)
            else:
                result = forecast_df

            context.add_output_metadata(
                {
                    "forecast_periods": forecast_periods,
                    "model": model,
                    "output_mode": output_mode,
                    "output_rows": len(result),
                    "preview": MetadataValue.md(result.head(10).to_markdown()),
                }
            )
            return result

        return Definitions(assets=[_asset])
