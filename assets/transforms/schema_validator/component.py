from dataclasses import dataclass
from typing import Optional, List, Dict, Any
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
class SchemaValidatorComponent(Component, Model, Resolvable):
    """Validate each row against a JSON Schema; drop, tag, or raise on failures."""

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
    json_schema: Dict[str, Any] = Field(
        description="The JSON Schema to validate each row against."
    )
    on_invalid: str = Field(
        default="tag",
        description="Action on invalid rows: 'drop', 'tag', or 'raise'.",
    )
    tag_column: str = Field(
        default="_validation_errors",
        description="Column added when on_invalid='tag', containing a list of error messages.",
    )
    subset_columns: Optional[List[str]] = Field(
        default=None,
        description="Only validate these columns. If omitted, validate all columns.",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        json_schema = self.json_schema
        on_invalid = self.on_invalid
        tag_column = self.tag_column
        subset_columns = self.subset_columns

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
                import jsonschema
                from jsonschema import validate, ValidationError
            except ImportError:
                raise ImportError("Install jsonschema: pip install jsonschema")

            if on_invalid not in ("drop", "tag", "raise"):
                raise ValueError(f"on_invalid must be 'drop', 'tag', or 'raise', got: {on_invalid}")

            df = upstream.copy()

            def validate_row(row: pd.Series) -> List[str]:
                data = row.to_dict() if subset_columns is None else {k: row[k] for k in subset_columns if k in row}
                errors = []
                try:
                    validate(instance=data, schema=json_schema)
                except ValidationError as e:
                    errors.append(e.message)
                except jsonschema.exceptions.SchemaError as e:
                    raise ValueError(f"Invalid JSON Schema: {e.message}")
                # Collect all errors, not just first
                validator = jsonschema.Draft7Validator(json_schema)
                errors = [err.message for err in sorted(validator.iter_errors(data), key=str)]
                return errors

            error_series = df.apply(validate_row, axis=1)

            if on_invalid == "raise":
                bad = error_series[error_series.map(len) > 0]
                if not bad.empty:
                    first_errors = bad.iloc[0]
                    raise ValueError(f"Row {bad.index[0]} failed validation: {first_errors}")
            elif on_invalid == "drop":
                valid_mask = error_series.map(len) == 0
                df = df[valid_mask].reset_index(drop=True)
                context.log.info(f"Dropped {(~valid_mask).sum()} invalid rows.")
            elif on_invalid == "tag":
                df[tag_column] = error_series.apply(lambda errs: errs if errs else None)

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "columns": MetadataValue.int(len(df.columns)),
                "invalid_rows": MetadataValue.int(int((error_series.map(len) > 0).sum())),
            })
            return df

        return Definitions(assets=[_asset])
