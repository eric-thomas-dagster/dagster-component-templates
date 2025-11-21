"""Time Series Generator Asset Component."""

from typing import Optional, Literal
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetSpec,
    AssetExecutionContext,
    ComponentLoadContext,
    multi_asset,
)
from pydantic import Field


class TimeSeriesGeneratorComponent(Component, Model, Resolvable):
    """
    Component for generating synthetic time-series data with various patterns.

    This component creates realistic time-series data with configurable patterns,
    frequencies, and noise. Perfect for demonstrating analytics, forecasting,
    anomaly detection, and time-series transformations.

    Supports multiple patterns:
    - trend: Linear upward or downward trend
    - seasonal: Recurring daily/weekly/monthly patterns
    - random_walk: Random incremental changes
    - sine_wave: Smooth periodic oscillation
    - step_function: Sudden level changes
    - spike: Occasional random spikes
    - complex: Combination of trend + seasonal + noise
    """

    asset_name: str = Field(description="Name of the asset")

    pattern_type: Literal[
        "trend",
        "seasonal",
        "random_walk",
        "sine_wave",
        "step_function",
        "spike",
        "complex"
    ] = Field(
        default="complex",
        description="Pattern to generate in the time series"
    )

    start_date: str = Field(
        description="Start date (YYYY-MM-DD format)",
        default=""
    )

    end_date: str = Field(
        description="End date (YYYY-MM-DD format)",
        default=""
    )

    frequency: Literal["1min", "5min", "15min", "30min", "1h", "1d"] = Field(
        default="1h",
        description="Data point frequency"
    )

    base_value: float = Field(
        default=100.0,
        description="Starting/baseline value for the series"
    )

    noise_level: float = Field(
        default=0.1,
        description="Amount of random noise to add (0.0 = none, 1.0 = high)",
        ge=0.0,
        le=1.0
    )

    random_seed: Optional[int] = Field(
        default=None,
        description="Random seed for reproducible data generation (leave empty for random)"
    )

    metric_name: str = Field(
        default="value",
        description="Name of the metric/value column"
    )

    description: Optional[str] = Field(
        default="",
        description="Description of the asset"
    )

    group_name: Optional[str] = Field(
        default="",
        description="Asset group name for organization"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the time series generator."""

        # Capture fields for closure
        asset_name = self.asset_name
        pattern_type = self.pattern_type
        start_date = self.start_date
        end_date = self.end_date
        frequency = self.frequency
        base_value = self.base_value
        noise_level = self.noise_level
        random_seed = self.random_seed
        metric_name = self.metric_name
        description = self.description or f"Time series with {pattern_type} pattern"
        group_name = self.group_name or None

        @multi_asset(
            name=f"{asset_name}_generator",
            specs=[
                AssetSpec(
                    key=asset_name,
                    description=description,
                    group_name=group_name,
                )
            ],
        )
        def time_series_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Generate time series data based on pattern type."""

            # Set random seed if provided
            if random_seed is not None:
                np.random.seed(random_seed)

            # Check if running in partitioned mode
            if context.has_partition_key:
                # Parse partition key as date (format: YYYY-MM-DD)
                try:
                    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
                    # For partitioned mode, generate data for just this day
                    start = partition_date
                    end = partition_date + timedelta(days=1) - timedelta(seconds=1)
                    context.log.info(
                        f"Generating {pattern_type} time series for partition {context.partition_key} "
                        f"at {frequency} frequency"
                    )
                except ValueError:
                    context.log.warning(
                        f"Could not parse partition key '{context.partition_key}' as date, "
                        f"falling back to configured dates"
                    )
                    # Fall back to configured dates
                    if start_date:
                        start = pd.to_datetime(start_date)
                    else:
                        start = datetime.now() - timedelta(days=30)

                    if end_date:
                        end = pd.to_datetime(end_date)
                    else:
                        end = datetime.now()
            else:
                # Non-partitioned mode: use configured dates or defaults
                context.log.info(f"Generating {pattern_type} time series (non-partitioned)")

                if start_date:
                    start = pd.to_datetime(start_date)
                else:
                    start = datetime.now() - timedelta(days=30)

                if end_date:
                    end = pd.to_datetime(end_date)
                else:
                    end = datetime.now()

                context.log.info(
                    f"Generating {pattern_type} time series from {start} to {end} "
                    f"at {frequency} frequency"
                )

            # Generate datetime index
            date_range = pd.date_range(start=start, end=end, freq=frequency)
            n_points = len(date_range)

            context.log.info(f"Generating {n_points} data points")

            # Generate base pattern
            if pattern_type == "trend":
                values = _generate_trend(n_points, base_value, slope=0.5)
            elif pattern_type == "seasonal":
                values = _generate_seasonal(n_points, base_value, date_range)
            elif pattern_type == "random_walk":
                values = _generate_random_walk(n_points, base_value)
            elif pattern_type == "sine_wave":
                values = _generate_sine_wave(n_points, base_value)
            elif pattern_type == "step_function":
                values = _generate_step_function(n_points, base_value)
            elif pattern_type == "spike":
                values = _generate_spike(n_points, base_value)
            elif pattern_type == "complex":
                values = _generate_complex(n_points, base_value, date_range)
            else:
                raise ValueError(f"Unknown pattern type: {pattern_type}")

            # Add noise
            if noise_level > 0:
                noise = np.random.normal(0, base_value * noise_level, n_points)
                values = values + noise

            # Create DataFrame
            df = pd.DataFrame({
                "timestamp": date_range,
                metric_name: values
            })

            context.log.info(
                f"Generated time series with {len(df)} points, "
                f"min={df[metric_name].min():.2f}, "
                f"max={df[metric_name].max():.2f}, "
                f"mean={df[metric_name].mean():.2f}"
            )

            return df

        return Definitions(assets=[time_series_asset])


def _generate_trend(n: int, base: float, slope: float = 0.5) -> np.ndarray:
    """Generate linear trend."""
    return base + slope * np.arange(n)


def _generate_seasonal(n: int, base: float, date_range: pd.DatetimeIndex) -> np.ndarray:
    """Generate seasonal pattern based on hour of day."""
    # Daily seasonality - higher during business hours
    hours = date_range.hour
    # Peak at hour 14 (2 PM), low at hour 2 (2 AM)
    seasonal = base * (1 + 0.3 * np.sin((hours - 2) * 2 * np.pi / 24))
    return seasonal


def _generate_random_walk(n: int, base: float) -> np.ndarray:
    """Generate random walk."""
    steps = np.random.normal(0, base * 0.02, n)
    return base + np.cumsum(steps)


def _generate_sine_wave(n: int, base: float) -> np.ndarray:
    """Generate sine wave."""
    x = np.linspace(0, 4 * np.pi, n)
    return base + base * 0.2 * np.sin(x)


def _generate_step_function(n: int, base: float) -> np.ndarray:
    """Generate step function with level changes."""
    values = np.ones(n) * base
    # Add 3 random steps
    num_steps = min(3, n // 10)
    step_positions = sorted(np.random.choice(range(n // 4, 3 * n // 4), num_steps, replace=False))

    current_level = base
    prev_pos = 0
    for pos in step_positions:
        step_change = np.random.choice([-1, 1]) * base * np.random.uniform(0.2, 0.4)
        current_level += step_change
        values[pos:] = current_level

    return values


def _generate_spike(n: int, base: float) -> np.ndarray:
    """Generate mostly flat with occasional spikes."""
    values = np.ones(n) * base
    # Add 5-10 random spikes
    num_spikes = min(np.random.randint(5, 11), n // 10)
    spike_positions = np.random.choice(n, num_spikes, replace=False)

    for pos in spike_positions:
        spike_magnitude = base * np.random.uniform(2, 5)
        values[pos] = spike_magnitude

    return values


def _generate_complex(n: int, base: float, date_range: pd.DatetimeIndex) -> np.ndarray:
    """Generate complex pattern: trend + seasonal + noise."""
    # Upward trend
    trend = base + 0.1 * np.arange(n)

    # Daily seasonality
    hours = date_range.hour
    seasonal = base * 0.2 * np.sin((hours - 2) * 2 * np.pi / 24)

    # Combine
    return trend + seasonal
