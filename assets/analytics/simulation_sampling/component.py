"""Simulation Sampling Component.

Run Monte Carlo simulations by sampling from statistical distributions defined
in an upstream DataFrame. Supports normal, uniform, triangular, lognormal,
Poisson, exponential, and beta distributions.
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


class SimulationSamplingComponent(Component, Model, Resolvable):
    """Component for Monte Carlo simulation sampling.

    Reads a distribution definition DataFrame (one row per variable) and
    generates N simulation samples. Supports multiple distribution types.

    Example:
        ```yaml
        type: dagster_component_templates.SimulationSamplingComponent
        attributes:
          asset_name: demand_simulation
          upstream_asset_key: demand_distributions
          variable_column: variable
          distribution_column: distribution
          param1_column: param1
          param2_column: param2
          n_simulations: 10000
          output_mode: statistics
          group_name: analytics
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame defining distributions (one row per variable)"
    )

    variable_column: str = Field(
        description="Column with variable names"
    )

    distribution_column: str = Field(
        description="Column with distribution type: normal, uniform, triangular, lognormal, poisson, exponential, beta"
    )

    param1_column: str = Field(
        description="Column with first distribution parameter (mean for normal, low for uniform, etc.)"
    )

    param2_column: str = Field(
        description="Column with second distribution parameter (std for normal, high for uniform, etc.)"
    )

    n_simulations: int = Field(
        default=1000,
        description="Number of Monte Carlo iterations"
    )

    random_state: int = Field(
        default=42,
        description="Random seed for reproducibility"
    )

    output_mode: str = Field(
        default="samples",
        description="Output mode: 'samples' (N rows x M variable columns) or 'statistics' (summary stats per variable)"
    )

    group_name: Optional[str] = Field(
        default=None,
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        variable_column = self.variable_column
        distribution_column = self.distribution_column
        param1_column = self.param1_column
        param2_column = self.param2_column
        n_simulations = self.n_simulations
        random_state = self.random_state
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
            description=f"Monte Carlo simulation ({n_simulations} iterations)",
            partitions_def=partitions_def,
            group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def simulation_sampling_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Run Monte Carlo simulation from distribution definitions."""
            try:
                import numpy as np
            except ImportError:
                raise ImportError("numpy is required: pip install numpy")

            rng = np.random.default_rng(random_state)
            samples = {}

            context.log.info(f"Running {n_simulations} simulations for {len(upstream)} variables")

            for _, row in upstream.iterrows():
                var = row[variable_column]
                dist = row[distribution_column]
                p1 = float(row[param1_column])
                p2 = float(row[param2_column])

                if dist == "normal":
                    samples[var] = rng.normal(p1, p2, n_simulations)
                elif dist == "uniform":
                    samples[var] = rng.uniform(p1, p2, n_simulations)
                elif dist == "triangular":
                    samples[var] = rng.triangular(p1, p2, (p1 + p2) / 2, n_simulations)
                elif dist == "lognormal":
                    samples[var] = rng.lognormal(p1, p2, n_simulations)
                elif dist == "poisson":
                    samples[var] = rng.poisson(p1, n_simulations)
                elif dist == "exponential":
                    samples[var] = rng.exponential(p1, n_simulations)
                elif dist == "beta":
                    samples[var] = rng.beta(p1, p2, n_simulations)
                else:
                    context.log.warning(f"Unknown distribution '{dist}' for variable '{var}', skipping")

            if output_mode == "samples":
                result_df = pd.DataFrame(samples)
            else:
                stats = []
                for var, vals in samples.items():
                    stats.append({
                        "variable": var,
                        "mean": float(np.mean(vals)),
                        "std": float(np.std(vals)),
                        "p5": float(np.percentile(vals, 5)),
                        "p25": float(np.percentile(vals, 25)),
                        "p50": float(np.percentile(vals, 50)),
                        "p75": float(np.percentile(vals, 75)),
                        "p95": float(np.percentile(vals, 95)),
                        "min": float(np.min(vals)),
                        "max": float(np.max(vals)),
                    })
                result_df = pd.DataFrame(stats)

            context.log.info(f"Simulation complete: {result_df.shape[0]} rows x {result_df.shape[1]} columns")

            context.add_output_metadata({
                "n_simulations": MetadataValue.int(n_simulations),
                "n_variables": MetadataValue.int(len(samples)),
                "output_mode": MetadataValue.text(output_mode),
                "row_count": MetadataValue.int(len(result_df)),
            })
            return result_df

        return Definitions(assets=[simulation_sampling_asset])
