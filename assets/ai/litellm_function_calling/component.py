"""LiteLLM Function Calling Component.

Use LiteLLM function/tool calling to invoke structured tool definitions against each row.
"""
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
class LitellmFunctionCallingComponent(Component, Model, Resolvable):
    """Use LiteLLM function/tool calling to invoke structured tool definitions against each row.

    Calls litellm.completion() with tools= parameter, parses tool_calls from the response,
    and writes them as JSON to the output column.

    Example:
        ```yaml
        type: dagster_component_templates.LitellmFunctionCallingComponent
        attributes:
          asset_name: tool_call_results
          upstream_asset_key: raw_queries
          text_column: query
          tools:
            - type: function
              function:
                name: search_database
                description: Search the product database
                parameters:
                  type: object
                  properties:
                    query: {type: string}
                    limit: {type: integer}
                  required: [query]
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    text_column: str = Field(description="Column containing input text")
    tools: List[Dict[str, Any]] = Field(description="List of tool definitions in OpenAI format")
    model: str = Field(default="gpt-4o-mini", description="LiteLLM model string")
    output_column: str = Field(default="tool_calls", description="Column to write tool call results as JSON string")
    system_prompt: Optional[str] = Field(default=None, description="System message prepended to each request")
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
        tools = self.tools
        model = self.model
        output_column = self.output_column
        system_prompt = self.system_prompt
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
            context.log.info(f"Running function calling on {len(df)} rows with model={model}")

            kwargs: dict = {
                "model": model,
                "tools": tools,
            }
            if api_key_env_var:
                kwargs["api_key"] = os.environ[api_key_env_var]

            results = []
            tool_call_count = 0

            for _, row in df.iterrows():
                text = str(row[text_column])
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": text})

                response = litellm.completion(messages=messages, **kwargs                api_key=api_key,
            )
                message = response.choices[0].message

                if message.tool_calls:
                    tool_calls_data = [
                        {
                            "id": tc.id,
                            "type": tc.type,
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments,
                            },
                        }
                        for tc in message.tool_calls
                    ]
                    results.append(json.dumps(tool_calls_data))
                    tool_call_count += len(message.tool_calls)
                else:
                    results.append(json.dumps([]))

            df[output_column] = results

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "total_tool_calls": MetadataValue.int(tool_call_count),
                "model": MetadataValue.text(model),
                "output_column": MetadataValue.text(output_column),
            })
            return df

        return Definitions(assets=[_asset])
