"""LLM Prompt Executor Asset Component.

Execute prompts against LLM APIs (OpenAI, Anthropic, etc.) row-by-row over an upstream
DataFrame, and return the enriched DataFrame with response column added.
Supports multiple LLM providers, temperature control, and various response formats.
"""

import json
import os
from typing import Optional
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


class LLMPromptExecutorComponent(Component, Model, Resolvable):
    """Component for executing prompts against LLM APIs row-by-row over an upstream DataFrame.

    Accepts a DataFrame via ins= and applies the LLM to each row using input_column as the
    prompt text (or user_prompt_template for templated prompts). Returns an enriched DataFrame
    with the LLM response added as output_column.

    Example:
        ```yaml
        type: dagster_component_templates.LLMPromptExecutorComponent
        attributes:
          asset_name: product_description
          upstream_asset_key: raw_products
          input_column: product_name
          provider: openai
          model: gpt-4
          temperature: 0.7
          output_column: llm_response
        ```
    """

    asset_name: str = Field(
        description="Name of the asset"
    )

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with text to process"
    )

    input_column: str = Field(
        default="text",
        description="Column name containing input text to send to LLM as the prompt"
    )

    output_column: str = Field(
        default="llm_response",
        description="Column name for LLM responses"
    )

    provider: str = Field(
        description="LLM provider: 'openai', 'anthropic', 'cohere', or 'huggingface'"
    )

    model: str = Field(
        description="Model name (e.g., 'gpt-4', 'claude-3-5-sonnet-20241022', 'command-r-plus')"
    )

    system_prompt: Optional[str] = Field(
        default=None,
        description="System prompt to set context for the LLM"
    )

    user_prompt_template: Optional[str] = Field(
        default=None,
        description="User prompt template with {column_name} placeholders for DataFrame columns. "
                    "If not set, input_column value is used directly as the prompt."
    )

    temperature: float = Field(
        default=0.7,
        description="Temperature for response randomness (0.0-2.0)"
    )

    max_tokens: Optional[int] = Field(
        default=None,
        description="Maximum tokens in the response"
    )

    api_key: Optional[str] = Field(
        default=None,
        description="API key (use ${VAR_NAME} for environment variable, e.g., ${OPENAI_API_KEY})"
    )

    response_format: str = Field(
        default="text",
        description="Response format: 'text', 'json', or 'markdown'"
    )

    json_schema: Optional[str] = Field(
        default=None,
        description="JSON schema for structured output (JSON string)"
    )

    streaming: bool = Field(
        default=False,
        description="Whether to use streaming responses"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
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
        input_column = self.input_column
        output_column = self.output_column
        provider = self.provider
        model = self.model
        system_prompt = self.system_prompt
        user_prompt_template = self.user_prompt_template
        temperature = self.temperature
        max_tokens = self.max_tokens
        api_key = self.api_key
        response_format = self.response_format
        json_schema_str = self.json_schema
        streaming = self.streaming
        description = self.description or f"Execute LLM prompt using {provider}/{model}"
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
        def llm_prompt_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that executes an LLM prompt row-by-row over an upstream DataFrame."""

            df = upstream.copy()

            if input_column not in df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(df.columns)}")

            context.log.info(f"Executing LLM prompt with {provider}/{model} on {len(df)} rows")

            # Expand environment variables in API key (supports ${VAR_NAME} pattern)
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    raise ValueError(f"Environment variable not set: {var_name}")

            # Parse JSON schema if provided
            schema_dict = None
            if json_schema_str:
                try:
                    schema_dict = json.loads(json_schema_str)
                except json.JSONDecodeError as e:
                    context.log.error(f"Invalid JSON schema: {e}")
                    raise

            responses = []

            for idx, row in df.iterrows():
                # Build prompt for this row
                if user_prompt_template:
                    prompt = user_prompt_template.format(**row.to_dict())
                else:
                    prompt = str(row[input_column])

                response_text = None

                # Execute based on provider
                if provider == "openai":
                    try:
                        import openai
                        client = openai.OpenAI(api_key=expanded_api_key)

                        messages = []
                        if system_prompt:
                            messages.append({"role": "system", "content": system_prompt})
                        messages.append({"role": "user", "content": prompt})

                        call_kwargs = {
                            "model": model,
                            "messages": messages,
                            "temperature": temperature,
                        }

                        if max_tokens:
                            call_kwargs["max_tokens"] = max_tokens

                        if response_format == "json" and schema_dict:
                            call_kwargs["response_format"] = {
                                "type": "json_schema",
                                "json_schema": schema_dict
                            }
                        elif response_format == "json":
                            call_kwargs["response_format"] = {"type": "json_object"}

                        if streaming:
                            stream = client.chat.completions.create(stream=True, **call_kwargs)
                            chunks = []
                            for chunk in stream:
                                if chunk.choices[0].delta.content:
                                    chunks.append(chunk.choices[0].delta.content)
                            response_text = "".join(chunks)
                        else:
                            response = client.chat.completions.create(**call_kwargs)
                            response_text = response.choices[0].message.content

                    except ImportError:
                        raise ImportError("OpenAI package not installed. Install with: pip install openai")

                elif provider == "anthropic":
                    try:
                        import anthropic
                        client = anthropic.Anthropic(api_key=expanded_api_key)

                        call_kwargs = {
                            "model": model,
                            "max_tokens": max_tokens or 4096,
                            "temperature": temperature,
                            "messages": [{"role": "user", "content": prompt}]
                        }

                        if system_prompt:
                            call_kwargs["system"] = system_prompt

                        if streaming:
                            chunks = []
                            with client.messages.stream(**call_kwargs) as stream:
                                for text in stream.text_stream:
                                    chunks.append(text)
                            response_text = "".join(chunks)
                        else:
                            message = client.messages.create(**call_kwargs)
                            response_text = message.content[0].text

                    except ImportError:
                        raise ImportError("Anthropic package not installed. Install with: pip install anthropic")

                elif provider == "cohere":
                    try:
                        import cohere
                        client = cohere.Client(api_key=expanded_api_key)

                        call_kwargs = {
                            "model": model,
                            "message": prompt,
                            "temperature": temperature,
                        }

                        if max_tokens:
                            call_kwargs["max_tokens"] = max_tokens

                        if system_prompt:
                            call_kwargs["preamble"] = system_prompt

                        if streaming:
                            chunks = []
                            for event in client.chat_stream(**call_kwargs):
                                if hasattr(event, 'text'):
                                    chunks.append(event.text)
                            response_text = "".join(chunks)
                        else:
                            response = client.chat(**call_kwargs)
                            response_text = response.text

                    except ImportError:
                        raise ImportError("Cohere package not installed. Install with: pip install cohere")

                elif provider == "huggingface":
                    try:
                        from huggingface_hub import InferenceClient
                        client = InferenceClient(token=expanded_api_key)

                        messages = []
                        if system_prompt:
                            messages.append({"role": "system", "content": system_prompt})
                        messages.append({"role": "user", "content": prompt})

                        call_kwargs = {
                            "model": model,
                            "messages": messages,
                            "temperature": temperature,
                        }

                        if max_tokens:
                            call_kwargs["max_tokens"] = max_tokens

                        if streaming:
                            chunks = []
                            for message in client.chat_completion(stream=True, **call_kwargs):
                                if message.choices[0].delta.content:
                                    chunks.append(message.choices[0].delta.content)
                            response_text = "".join(chunks)
                        else:
                            response = client.chat_completion(**call_kwargs)
                            response_text = response.choices[0].message.content

                    except ImportError:
                        raise ImportError("Hugging Face package not installed. Install with: pip install huggingface_hub")

                else:
                    raise ValueError(f"Unsupported provider: {provider}")

                # Format response
                if response_format == "json":
                    try:
                        result = json.loads(response_text)
                        response_text = json.dumps(result)
                    except json.JSONDecodeError:
                        context.log.warning(f"Row {idx}: response is not valid JSON, storing as text")

                responses.append(response_text)

                if idx % 10 == 0:
                    context.log.info(f"Processed {idx + 1}/{len(df)}")

            df[output_column] = responses
            context.log.info(f"Completed {len(df)} LLM calls")

            context.add_output_metadata({
                "provider": provider,
                "model": model,
                "rows_processed": len(df),
                "output_column": output_column,
            })

            return df

        return Definitions(assets=[llm_prompt_asset])
