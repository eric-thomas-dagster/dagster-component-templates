"""LLM Chain Executor Asset Component.

Execute chains of LLM calls row-by-row over an upstream DataFrame,
passing context between steps and returning the enriched DataFrame.
"""

import os
import json
from typing import Optional, List, Dict, Any
import pandas as pd

from dagster import (
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
)
from pydantic import Field


class LLMChainExecutorComponent(Component, Model, Resolvable):
    """Component for executing LLM chains row-by-row over an upstream DataFrame.

    Accepts a DataFrame via ins=. For each row, runs a chain of LLM steps where
    each step's output feeds into the next. The final context dict for each row
    is serialized to output_column as JSON.

    Example:
        ```yaml
        type: dagster_component_templates.LLMChainExecutorComponent
        attributes:
          asset_name: chain_output
          upstream_asset_key: raw_texts
          input_column: text
          provider: openai
          model: gpt-4
          api_key: ${OPENAI_API_KEY}
          chain_steps: '[{"prompt": "Summarize: {text}", "output_key": "summary"}, {"prompt": "Generate title for: {summary}", "output_key": "title"}]'
          output_column: chain_result
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    input_column: str = Field(default="text", description="Column name containing the initial input text for the chain")
    output_column: str = Field(default="chain_result", description="Column name to store chain result (JSON string)")
    provider: str = Field(description="LLM provider")
    model: str = Field(description="Model name")
    chain_steps: str = Field(description="JSON array of chain steps, each with 'prompt' and 'output_key'")
    temperature: float = Field(default=0.7, description="Temperature")
    api_key: Optional[str] = Field(default=None, description="API key (use ${VAR_NAME} for environment variable)")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")
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
        input_column = self.input_column
        output_column = self.output_column
        provider = self.provider
        model = self.model
        chain_steps_str = self.chain_steps
        temperature = self.temperature
        api_key = self.api_key
        description = self.description or f"Execute LLM chain with {provider}/{model}"
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
            description=description,
            partitions_def=partitions_def,
            group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def llm_chain_executor_asset(ctx: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Execute LLM chain row-by-row over an upstream DataFrame."""

            df = upstream.copy()

            if input_column not in df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(df.columns)}")

            chain_steps = json.loads(chain_steps_str)

            # Expand environment variables in API key (supports ${VAR_NAME} pattern)
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    raise ValueError(f"Environment variable not set: {var_name}")

            ctx.log.info(f"Executing {len(chain_steps)}-step chain over {len(df)} rows")

            results = []
            for idx, row in df.iterrows():
                # Start context from this row
                context_data: Dict[str, Any] = row.to_dict()

                for i, step in enumerate(chain_steps):
                    prompt_template = step['prompt']
                    output_key = step.get('output_key', f'output_{i}')

                    # Format prompt with context
                    prompt = prompt_template.format(**context_data)

                    # Call LLM
                    if provider == "openai":
                        import openai
                        client = openai.OpenAI(api_key=expanded_api_key)
                        response = client.chat.completions.create(
                            model=model,
                            messages=[{"role": "user", "content": prompt}],
                            temperature=temperature
                        )
                        output = response.choices[0].message.content
                    elif provider == "anthropic":
                        import anthropic
                        client = anthropic.Anthropic(api_key=expanded_api_key)
                        message = client.messages.create(
                            model=model,
                            max_tokens=4096,
                            temperature=temperature,
                            messages=[{"role": "user", "content": prompt}]
                        )
                        output = message.content[0].text
                    else:
                        raise ValueError(f"Unsupported provider: {provider}")

                    context_data[output_key] = output

                results.append(json.dumps(context_data))

                if idx % 10 == 0:
                    ctx.log.info(f"Processed {idx + 1}/{len(df)}")

            df[output_column] = results
            ctx.log.info(f"Completed chain execution on {len(df)} rows")
            ctx.add_output_metadata({"num_steps": len(chain_steps), "rows_processed": len(df)})

            return df

        return Definitions(assets=[llm_chain_executor_asset])
