"""Optimization Component.

Solve linear programming and mixed-integer optimization problems using SciPy,
reading problem definitions (coefficients, bounds, constraints) from a DataFrame.
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


class OptimizationComponent(Component, Model, Resolvable):
    """Component for solving linear programming optimization problems.

    Reads a problem definition DataFrame (coefficients, bounds, constraints) from
    an upstream asset and solves it using SciPy's linprog (HiGHS solver).

    Example:
        ```yaml
        type: dagster_component_templates.OptimizationComponent
        attributes:
          asset_name: portfolio_optimization
          upstream_asset_key: portfolio_problem
          objective_column: expected_return
          bounds_lower_column: min_weight
          bounds_upper_column: max_weight
          maximize: true
          group_name: analytics
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame defining the optimization problem"
    )

    objective_column: str = Field(
        description="Column with objective function coefficients"
    )

    constraint_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns for constraint coefficients (each column is one constraint row)"
    )

    constraint_rhs_column: Optional[str] = Field(
        default=None,
        description="Column with constraint right-hand side values"
    )

    bounds_lower_column: Optional[str] = Field(
        default=None,
        description="Column with lower bounds per variable"
    )

    bounds_upper_column: Optional[str] = Field(
        default=None,
        description="Column with upper bounds per variable"
    )

    method: str = Field(
        default="linprog",
        description="Solver method: 'linprog' (linear), 'milp' (mixed integer)"
    )

    maximize: bool = Field(
        default=False,
        description="If True, maximize the objective (default is minimize)"
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
        objective_column = self.objective_column
        constraint_columns = self.constraint_columns
        constraint_rhs_column = self.constraint_rhs_column
        bounds_lower_column = self.bounds_lower_column
        bounds_upper_column = self.bounds_upper_column
        method = self.method
        maximize = self.maximize
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
            description=f"Optimization results ({method})",
            partitions_def=partitions_def,
            group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def optimization_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Solve optimization problem defined in the upstream DataFrame."""
            try:
                from scipy.optimize import linprog
                import numpy as np
            except ImportError:
                raise ImportError("scipy and numpy are required: pip install scipy numpy")

            df = upstream.copy()
            context.log.info(f"Solving optimization problem with {len(df)} variables")

            c = df[objective_column].values.astype(float)
            if maximize:
                c = -c

            bounds = None
            if bounds_lower_column and bounds_upper_column:
                bounds = list(zip(
                    df[bounds_lower_column].values.astype(float),
                    df[bounds_upper_column].values.astype(float)
                ))

            A_ub = None
            b_ub = None
            if constraint_columns and constraint_rhs_column:
                A_ub = df[constraint_columns].values.astype(float).T
                b_ub = df[constraint_rhs_column].values.astype(float)

            result = linprog(c, A_ub=A_ub, b_ub=b_ub, bounds=bounds, method="highs")

            result_df = df.copy()
            result_df["optimal_value"] = result.x if result.success else None
            result_df["status"] = result.message

            obj_val = float(-result.fun if maximize else result.fun) if result.success else 0.0
            context.log.info(f"Optimization status: {result.message}")
            context.log.info(f"Objective value: {obj_val}")

            context.add_output_metadata({
                "objective_value": MetadataValue.float(obj_val),
                "status": MetadataValue.text(result.message),
                "n_variables": MetadataValue.int(len(df)),
                "success": MetadataValue.bool(bool(result.success)),
            })
            return result_df

        return Definitions(assets=[optimization_asset])
