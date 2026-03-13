"""LiteLLM Structured Output Component.

Extract structured JSON data from text using LiteLLM's JSON mode.
Expands extracted fields as new DataFrame columns.
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
class LitellmStructuredOutputComponent(Component, Model, Resolvable):
    """Extract structured JSON data from text using LiteLLM's JSON mode.

    Uses response_format={"type": "json_object"} to ensure structured extraction.
    Each key from the JSON response is expanded as a new DataFrame column.

    Example:
        ```yaml
        type: dagster_component_templates.LitellmStructuredOutputComponent
        attributes:
          asset_name: extracted_entities
          upstream_asset_key: raw_documents
          text_column: content
          schema_definition:
            name: {type: string}
            age: {type: integer}
            email: {type: string}
          model: gpt-4o-mini
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    text_column: str = Field(description="Column containing input text")
    schema_definition: Dict[str, Any] = Field(description='JSON Schema describing the fields to extract e.g. {"name": {"type": "string"}, "age": {"type": "integer"}}')
    model: str = Field(default="gpt-4o-mini", description="LiteLLM model string")
    prompt_prefix: Optional[str] = Field(default=None, description="Instruction prepended to each extraction request")
    output_prefix: str = Field(default="", description="Prefix for extracted column names")
    on_error: str = Field(default="null", description='Error handling strategy: "skip" (drop row), "null" (fill with nulls), "raise" (raise exception)')
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
        schema_definition = self.schema_definition
        model = self.model
        prompt_prefix = self.prompt_prefix
        output_prefix = self.output_prefix
        on_error = self.on_error
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
                import litellm
            except ImportError:
                raise ImportError("pip install litellm>=1.0.0")

            import os
            import json

            df = upstream.copy()
            context.log.info(f"Extracting structured output from {len(df)} rows with model={model}")

            schema_str = json.dumps(schema_definition, indent=2)
            field_names = list(schema_definition.keys())
            column_names = [f"{output_prefix}{f}" for f in field_names]

            kwargs: dict = {
                "model": model,
                "response_format": {"type": "json_object"},
            }
            if api_key_env_var:
                kwargs["api_key"] = os.environ[api_key_env_var]

            rows_to_drop = []
            extracted_rows = []

            for i, row in df.iterrows():
                text = str(row[text_column])
                if prompt_prefix:
                    user_content = f"{prompt_prefix}\n\n{text}"
                else:
                    user_content = (
                        f"Extract the following fields from the text below as a JSON object.\n"
                        f"Schema: {schema_str}\n\n"
                        f"Text: {text}"
                    )

                messages = [{"role": "user", "content": user_content}]

                try:
                    response = litellm.completion(messages=messages, **kwargs                api_key=api_key,
            )
                    content = response.choices[0].message.content or "{}"
                    parsed = json.loads(content)
                    extracted_rows.append({f"{output_prefix}{k}": parsed.get(k) for k in field_names})
                except Exception as e:
                    if on_error == "raise":
                        raise
                    elif on_error == "skip":
                        rows_to_drop.append(i)
                        extracted_rows.append(None)
                        context.log.warning(f"Row {i} extraction failed, skipping: {e}")
                    else:  # null
                        extracted_rows.append({col: None for col in column_names})
                        context.log.warning(f"Row {i} extraction failed, filling with nulls: {e}")

            # Build extracted DataFrame
            valid_extracted = [r for r in extracted_rows if r is not None]
            extracted_df = pd.DataFrame(
                [r if r is not None else {col: None for col in column_names} for r in extracted_rows],
                index=df.index,
            )

            result = pd.concat([df, extracted_df], axis=1)

            if rows_to_drop:
                result = result.drop(index=rows_to_drop).reset_index(drop=True)

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(result)),
                "extracted_columns": MetadataValue.text(", ".join(column_names)),
                "model": MetadataValue.text(model),
            })
            return result

        return Definitions(assets=[_asset])
