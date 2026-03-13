"""Time Series Compare Component.

Compare ARIMA vs ETS models on a hold-out test set using AIC, MAE, RMSE, and MAPE.
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
class TsCompareComponent(Component, Model, Resolvable):
    """Component for comparing ARIMA and ETS model performance on a time series.

    Splits the series into train and hold-out test sets, fits both models,
    and evaluates each against the test set using standard error metrics.

    Features:
    - Train/test split evaluation
    - AIC, MAE, RMSE, MAPE metrics
    - Error handling per model (won't fail if one model errors)
    - Clear comparison DataFrame output

    Use Cases:
    - Model selection before production forecasting
    - Automated model auditing
    - Forecast accuracy reporting
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    date_column: str = Field(description="Column containing dates or timestamps")
    value_column: str = Field(description="Column containing numeric time series values")
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
    test_periods: int = Field(
        default=6, description="Number of hold-out periods for evaluation"
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
        arima_order = self.arima_order
        ets_trend = self.ets_trend
        ets_seasonal = self.ets_seasonal
        ets_seasonal_periods = self.ets_seasonal_periods
        test_periods = self.test_periods

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
                import numpy as np
            except ImportError:
                raise ImportError("statsmodels and numpy required: pip install statsmodels numpy")

            series = upstream.set_index(date_column)[value_column].sort_index()
            if len(series) <= test_periods:
                raise ValueError(
                    f"Series length ({len(series)}) must be greater than test_periods ({test_periods})"
                )

            train = series[:-test_periods]
            test = series[-test_periods:]
            context.log.info(
                f"Train: {len(train)} obs, Test: {len(test)} obs"
            )

            results = []

            model_specs = [
                (
                    "ARIMA",
                    lambda: ARIMA(train, order=tuple(arima_order)).fit(),
                ),
                (
                    "ETS",
                    lambda: ExponentialSmoothing(
                        train,
                        trend=ets_trend,
                        seasonal=ets_seasonal,
                        seasonal_periods=ets_seasonal_periods,
                    ).fit(),
                ),
            ]

            for model_name, fit_fn in model_specs:
                try:
                    fit = fit_fn()
                    forecast = fit.forecast(test_periods)
                    mae = float(np.mean(np.abs(forecast.values - test.values)))
                    rmse = float(np.sqrt(np.mean((forecast.values - test.values) ** 2)))
                    # Avoid division by zero in MAPE
                    nonzero = test.values != 0
                    if nonzero.any():
                        mape = float(
                            np.mean(
                                np.abs(
                                    (forecast.values[nonzero] - test.values[nonzero])
                                    / test.values[nonzero]
                                )
                            )
                            * 100
                        )
                    else:
                        mape = float("nan")
                    results.append(
                        {
                            "model": model_name,
                            "aic": float(fit.aic),
                            "mae": mae,
                            "rmse": rmse,
                            "mape": mape,
                        }
                    )
                    context.log.info(
                        f"{model_name} — AIC={fit.aic:.2f}, MAE={mae:.4f}, RMSE={rmse:.4f}, MAPE={mape:.2f}%"
                    )
                except Exception as e:
                    context.log.warning(f"{model_name} failed: {e}")
                    results.append({"model": model_name, "error": str(e)})

            result = pd.DataFrame(results)

            context.add_output_metadata(
                {
                    "train_periods": len(train),
                    "test_periods": test_periods,
                    "preview": MetadataValue.md(result.to_markdown()),
                }
            )
            return result

        return Definitions(assets=[_asset])
