"""TSCovariateForecastComponent.

Fits a SARIMAX model (statsmodels) with optional exogenous covariates. Forecasts `n_periods` ahead and emits per-step predicted values + 95% CI. Use when you have known external drivers (price, marketing spend, holiday flag) that influence the series.
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


class TSCovariateForecastComponent(Component, Model, Resolvable):
    """Forecast a time series with exogenous regressors via SARIMAX."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    date_column: str = Field(description="Datetime column.")
    value_column: str = Field(description="Numeric series to forecast.")
    covariate_columns: List[str] = Field(description="Exogenous regressor columns (must extend through the forecast window in the input).")
    n_periods: int = Field(default=12, description="Forecast horizon (number of future periods to predict).")
    order: List[int] = Field(default=[1, 1, 1], description="SARIMAX order (p,d,q).")
    seasonal_order: Optional[List[int]] = Field(default=None, description="Seasonal order (P,D,Q,m). None = non-seasonal.")

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
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")


    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        group_name = self.group_name
        _self = self

        ins = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}
        tags_dict = dict(self.asset_tags or {})
        for k in (self.kinds or ["python"]):
            tags_dict[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            ins=ins,
            group_name=group_name,
            description=self.description or "Forecast a time series with exogenous regressors via SARIMAX.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            from statsmodels.tsa.statespace.sarimax import SARIMAX
            df = df.copy().sort_values(_self.date_column)
            history = df.dropna(subset=[_self.value_column])
            future = df[df[_self.value_column].isna()].head(_self.n_periods)
            if len(future) < _self.n_periods:
                last_dates = df[_self.date_column].iloc[-1:].tolist()
                # Caller should provide future rows; if missing, just forecast on history's tail
                pass
            y = history[_self.value_column].astype(float).to_numpy()
            X = history[_self.covariate_columns].astype(float).to_numpy()
            order = tuple(_self.order)
            sorder = tuple(_self.seasonal_order) if _self.seasonal_order else (0, 0, 0, 0)
            model = SARIMAX(y, exog=X, order=order, seasonal_order=sorder, enforce_stationarity=False, enforce_invertibility=False)
            res = model.fit(disp=False)
            X_future = (future[_self.covariate_columns].astype(float).to_numpy() if len(future) >= _self.n_periods
                        else history[_self.covariate_columns].tail(_self.n_periods).astype(float).to_numpy())
            fcast = res.get_forecast(steps=_self.n_periods, exog=X_future)
            mean = fcast.predicted_mean
            ci = fcast.conf_int(alpha=0.05)
            future_dates = future[_self.date_column].head(_self.n_periods).tolist() if len(future) >= _self.n_periods else None
            out_df = pd.DataFrame({
                "step": range(1, _self.n_periods + 1),
                "predicted": mean,
                "lower_95": ci[:, 0] if hasattr(ci, "__getitem__") else ci.iloc[:, 0].values,
                "upper_95": ci[:, 1] if hasattr(ci, "__getitem__") else ci.iloc[:, 1].values,
            })
            if future_dates:
                out_df.insert(0, _self.date_column, future_dates)

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
