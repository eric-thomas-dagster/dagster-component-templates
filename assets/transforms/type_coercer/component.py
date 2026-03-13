from dataclasses import dataclass
from typing import Optional, Dict
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


VALID_TYPES = ("int", "float", "str", "bool", "datetime", "date", "json")


@dataclass
class TypeCoercerComponent(Component, Model, Resolvable):
    """Cast DataFrame columns to specified types; handles mixed types common in document DB output."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
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
    type_map: Dict[str, str] = Field(
        description=(
            "Mapping of column_name to target type. "
            "Valid values: 'int', 'float', 'str', 'bool', 'datetime', 'date', 'json'."
        )
    )
    errors: str = Field(
        default="coerce",
        description="What to do when conversion fails: 'raise', 'coerce' (NaN/NaT), or 'ignore'.",
    )
    datetime_format: Optional[str] = Field(
        default=None,
        description="Optional strptime format string for datetime parsing, e.g. '%Y-%m-%d'.",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        type_map = self.type_map
        errors = self.errors
        datetime_format = self.datetime_format

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
            import json as _json

            if errors not in ("raise", "coerce", "ignore"):
                raise ValueError(f"errors must be 'raise', 'coerce', or 'ignore', got: {errors}")

            df = upstream.copy()

            for col, target_type in type_map.items():
                if col not in df.columns:
                    context.log.warning(f"Column '{col}' not found, skipping.")
                    continue
                if target_type not in VALID_TYPES:
                    raise ValueError(f"Invalid type '{target_type}' for column '{col}'. Valid: {VALID_TYPES}")

                try:
                    if target_type == "int":
                        df[col] = pd.to_numeric(df[col], errors=errors).astype(
                            "Int64" if errors != "raise" else int
                        )
                    elif target_type == "float":
                        df[col] = pd.to_numeric(df[col], errors=errors)
                    elif target_type == "str":
                        if errors == "ignore":
                            pass
                        else:
                            df[col] = df[col].astype(str)
                    elif target_type == "bool":
                        if errors == "ignore":
                            df[col] = df[col].map(
                                lambda v: bool(v) if v is not None else None
                            )
                        else:
                            df[col] = df[col].astype(bool)
                    elif target_type == "datetime":
                        df[col] = pd.to_datetime(
                            df[col], format=datetime_format, errors=errors
                        )
                    elif target_type == "date":
                        df[col] = pd.to_datetime(
                            df[col], format=datetime_format, errors=errors
                        ).dt.date
                    elif target_type == "json":
                        df[col] = df[col].apply(
                            lambda v: _json.loads(v) if isinstance(v, str) else v
                        )
                except Exception as e:
                    if errors == "raise":
                        raise
                    context.log.warning(f"Failed to coerce column '{col}' to {target_type}: {e}")

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "columns": MetadataValue.int(len(df.columns)),
            })
            return df

        return Definitions(assets=[_asset])
