"""Text Classifier Asset Component.

Classify text using LLMs with predefined categories or custom classification tasks.
Accepts a DataFrame with a text column and returns the DataFrame enriched with classification results.
"""

import os
import json
from typing import Optional, List
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


class TextClassifierComponent(Component, Model, Resolvable):
    """Component for classifying text with LLMs.

    Accepts a DataFrame via ins= and applies LLM classification to each row's
    input_column, adding the classification result as new column(s).

    Example:
        ```yaml
        type: dagster_component_templates.TextClassifierComponent
        attributes:
          asset_name: text_classification
          upstream_asset_key: documents
          input_column: document_text
          output_column: category
          provider: openai
          model: gpt-4
          categories: '["positive", "negative", "neutral"]'
          classification_task: "sentiment analysis"
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with text to classify")
    input_column: str = Field(default="text", description="Column name containing text to classify")
    output_column: str = Field(default="category", description="Column name for classification result")
    confidence_column: Optional[str] = Field(default="confidence", description="Column name for confidence score (None to skip)")
    provider: str = Field(description="LLM provider")
    model: str = Field(description="Model name")
    categories: str = Field(description="JSON array of categories")
    classification_task: str = Field(default="classification", description="Task description")
    include_confidence: bool = Field(default=True, description="Include confidence scores")
    include_reasoning: bool = Field(default=False, description="Include reasoning")
    api_key: Optional[str] = Field(default=None, description="API key with ${VAR_NAME} syntax")
    temperature: float = Field(default=0.1, description="Temperature (low for consistency)")
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
        confidence_column = self.confidence_column
        provider = self.provider
        model = self.model
        categories_str = self.categories
        classification_task = self.classification_task
        include_confidence = self.include_confidence
        include_reasoning = self.include_reasoning
        api_key = self.api_key
        temperature = self.temperature
        description = self.description or f"Classify text using {provider}"
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
        def text_classifier_asset(ctx: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Classify text column in upstream DataFrame."""

            df = upstream.copy()

            if input_column not in df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(df.columns)}")

            categories = json.loads(categories_str)

            # Expand environment variables in API key
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and '${' in api_key:
                    raise ValueError(f"Environment variable in api_key '{api_key}' is not set")

            ctx.log.info(f"Classifying {len(df)} rows into {len(categories)} categories using {provider}/{model}")

            def build_prompt(text: str) -> str:
                p = f"Perform {classification_task} on the following text.\n\n"
                p += f"Categories: {', '.join(categories)}\n\n"
                p += f"Text: {text}\n\n"
                p += "Respond with a JSON object containing:\n"
                p += "- category: the selected category\n"
                if include_confidence:
                    p += "- confidence: confidence score between 0 and 1\n"
                if include_reasoning:
                    p += "- reasoning: brief explanation of the classification\n"
                return p

            def call_llm(prompt: str) -> dict:
                if provider == "openai":
                    import openai
                    client = openai.OpenAI(api_key=expanded_api_key)
                    response = client.chat.completions.create(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=temperature,
                        response_format={"type": "json_object"}
                    )
                    return json.loads(response.choices[0].message.content)
                elif provider == "anthropic":
                    import anthropic
                    client = anthropic.Anthropic(api_key=expanded_api_key)
                    message = client.messages.create(
                        model=model,
                        max_tokens=1024,
                        temperature=temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    return json.loads(message.content[0].text)
                else:
                    raise ValueError(f"Unsupported provider: {provider}")

            categories_out = []
            confidences_out = []

            for idx, row in df.iterrows():
                text = str(row[input_column])
                prompt = build_prompt(text)
                result = call_llm(prompt)
                categories_out.append(result.get("category"))
                confidences_out.append(result.get("confidence"))
                if idx % 10 == 0:
                    ctx.log.info(f"Classified {idx + 1}/{len(df)}")

            df[output_column] = categories_out
            if include_confidence and confidence_column:
                df[confidence_column] = confidences_out

            ctx.log.info(f"Classification complete: {len(df)} rows processed")
            ctx.add_output_metadata({
                "rows_processed": len(df),
                "provider": provider,
                "model": model,
                "categories": categories,
            })

            return df

        return Definitions(assets=[text_classifier_asset])
