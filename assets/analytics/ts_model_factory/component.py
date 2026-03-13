"""Time Series Model Factory Component.

Fit and forecast a separate time series model per group (e.g. product, store, region).
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
class TsModelFactoryComponent(Component, Model, Resolvable):
    """Component for fitting individual time series models per group.

    Iterates over each unique value in a group column, fits a separate ARIMA
    or ETS model for each group, and returns all forecasts in a single DataFrame.
    Groups that fail to fit are skipped with a warning.

    Features:
    - Per-group model fitting (e.g. per product, store, region)
    - ARIMA and ETS model support
    - Graceful failure handling per group
    - Single output DataFrame with all forecasts

    Use Cases:
    - Multi-SKU demand forecasting
    - Store-level revenue forecasting
    - Regional metric projections
    - Per-customer churn forecasting
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    date_column: str = Field(description="Column containing dates or timestamps")
    value_column: str = Field(description="Column containing numeric time series values")
    group_column: str = Field(
        description="Column identifying each group/entity (e.g. product_id, store_id)"
    )
    forecast_periods: int = Field(default=12, description="Number of future periods per group")
    model: str = Field(default="ets", description="Model: 'ets' or 'arima'")
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
        group_column = self.group_column
        forecast_periods = self.forecast_periods
        model = self.model
        arima_order = self.arima_order
        ets_trend = self.ets_trend
        ets_seasonal = self.ets_seasonal
        ets_seasonal_periods = self.ets_seasonal_periods

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

            groups = upstream[group_column].unique()
            context.log.info(
                f"Fitting {model.upper()} for {len(groups)} groups, {forecast_periods} periods each"
            )

            results = []
            failed = []

            for grp, gdf in upstream.groupby(group_column):
                series = gdf.set_index(date_column)[value_column].sort_index()
                try:
                    if model == "arima":
                        fit = ARIMA(series, order=tuple(arima_order)).fit()
                    else:
                        fit = ExponentialSmoothing(
                            series,
                            trend=ets_trend,
                            seasonal=ets_seasonal,
                            seasonal_periods=ets_seasonal_periods,
                        ).fit()
                    forecast = fit.forecast(forecast_periods)
                    fdf = pd.DataFrame(
                        {
                            date_column: forecast.index,
                            value_column: forecast.values,
                            group_column: grp,
                            "is_forecast": True,
                        }
                    )
                    results.append(fdf)
                except Exception as e:
                    context.log.warning(f"Failed to fit model for group {grp}: {e}")
                    failed.append(grp)

            if failed:
                context.log.warning(f"Skipped {len(failed)} groups: {failed}")

            result = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

            context.add_output_metadata(
                {
                    "total_groups": len(groups),
                    "successful_groups": len(results),
                    "failed_groups": len(failed),
                    "forecast_periods": forecast_periods,
                    "model": model,
                    "output_rows": len(result),
                    "preview": MetadataValue.md(result.head(10).to_markdown()) if len(result) > 0 else "empty",
                }
            )
            return result

        return Definitions(assets=[_asset])
