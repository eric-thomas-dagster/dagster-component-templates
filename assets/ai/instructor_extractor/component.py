"""Instructor Extractor Component.

Extract structured Pydantic models from text using the Instructor library.
Works with any OpenAI-compatible LLM and dynamically creates extraction schemas.
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any
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
class InstructorExtractorComponent(Component, Model, Resolvable):
    """Extract structured Pydantic models from text using the Instructor library.

    Dynamically creates a Pydantic model from extraction_schema, then uses
    instructor.from_openai() to reliably extract typed data from each row.
    Extracted fields are expanded as new columns.

    Example:
        ```yaml
        type: dagster_component_templates.InstructorExtractorComponent
        attributes:
          asset_name: extracted_invoice_fields
          upstream_asset_key: raw_invoices
          text_column: invoice_text
          extraction_schema:
            vendor_name: {type: str, description: "Name of the vendor"}
            total_amount: {type: float, description: "Total invoice amount"}
            invoice_date: {type: str, description: "Invoice date in YYYY-MM-DD format"}
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    text_column: str = Field(description="Column containing input text")
    model: str = Field(default="gpt-4o-mini", description="LLM model string")
    extraction_schema: Dict[str, Any] = Field(description='Dict describing fields to extract: {"field_name": {"type": "str|int|float|bool|list", "description": "..."}}')
    output_prefix: str = Field(default="", description="Prefix for extracted column names")
    max_retries: int = Field(default=3, description="Max retries for structured extraction")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var name for API key")
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
        text_column = self.text_column
        model = self.model
        extraction_schema = self.extraction_schema
        output_prefix = self.output_prefix
        max_retries = self.max_retries
        api_key_env_var = self.api_key_env_var
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
            kinds={"ai", "llm"},
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
                import instructor
            except ImportError:
                raise ImportError("pip install instructor>=0.4.0")
            try:
                import openai
            except ImportError:
                raise ImportError("pip install openai>=1.0.0")

            import os
            from pydantic import BaseModel, create_model
            from typing import Optional as Opt

            df = upstream.copy()
            context.log.info(f"Extracting structured data from {len(df)} rows with model={model}")

            # Map schema type strings to Python types
            type_map = {
                "str": str,
                "string": str,
                "int": int,
                "integer": int,
                "float": float,
                "number": float,
                "bool": bool,
                "boolean": bool,
                "list": list,
                "array": list,
            }

            # Build dynamic Pydantic model fields
            model_fields = {}
            for field_name, field_def in extraction_schema.items():
                field_type_str = field_def.get("type", "str") if isinstance(field_def, dict) else "str"
                field_type = type_map.get(field_type_str, str)
                field_desc = field_def.get("description", field_name) if isinstance(field_def, dict) else field_name
                model_fields[field_name] = (Opt[field_type], Field(default=None, description=field_desc))

            DynamicModel = create_model("ExtractionModel", **model_fields)

            openai_kwargs: dict = {}
            if api_key_env_var:
                openai_kwargs["api_key"] = os.environ[api_key_env_var]

            client = instructor.from_openai(openai.OpenAI(**openai_kwargs))

            field_names = list(extraction_schema.keys())
            column_names = [f"{output_prefix}{f}" for f in field_names]
            extracted_rows = []

            for i, row in df.iterrows():
                text = str(row[text_column])
                extraction = client.chat.completions.create(
                    model=model,
                    response_model=DynamicModel,
                    messages=[{"role": "user", "content": text}],
                    max_retries=max_retries,
                )
                extracted_rows.append(
                    {f"{output_prefix}{f}": getattr(extraction, f, None) for f in field_names}
                )

                if (i + 1) % 20 == 0:
                    context.log.info(f"Extracted {i + 1}/{len(df)} rows")

            extracted_df = pd.DataFrame(extracted_rows, index=df.index)
            result = pd.concat([df, extracted_df], axis=1)

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(result)),
                "extracted_columns": MetadataValue.text(", ".join(column_names)),
                "model": MetadataValue.text(model),
            })
            return result

        return Definitions(assets=[_asset])
