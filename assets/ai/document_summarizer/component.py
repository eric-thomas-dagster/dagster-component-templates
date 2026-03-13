"""Document Summarizer Asset Component.

Summarize documents using LLMs with support for long documents via chunking and map-reduce.
Accepts a DataFrame with a text column and returns the DataFrame enriched with summaries.
"""

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


class DocumentSummarizerComponent(Component, Model, Resolvable):
    """Component for summarizing documents with LLMs.

    Accepts a DataFrame via ins= and applies LLM summarization to each row's
    text_column, adding the summary as summary_column.

    Example:
        ```yaml
        type: dagster_component_templates.DocumentSummarizerComponent
        attributes:
          asset_name: document_summary
          upstream_asset_key: extracted_documents
          text_column: document_text
          summary_column: summary
          provider: openai
          model: gpt-4
          summary_type: concise
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with documents to summarize")
    text_column: str = Field(default="text", description="Column name containing document text to summarize")
    summary_column: str = Field(default="summary", description="Column name for generated summaries")
    provider: str = Field(description="LLM provider")
    model: str = Field(description="Model name")
    summary_type: str = Field(default="concise", description="Type: 'concise', 'detailed', 'bullet_points', 'executive'")
    max_length: Optional[int] = Field(default=None, description="Max summary length in words")
    chunk_size: int = Field(default=3000, description="Chunk size for long documents")
    use_map_reduce: bool = Field(default=True, description="Use map-reduce for long documents")
    api_key: Optional[str] = Field(default=None, description="API key with ${VAR_NAME} syntax")
    temperature: float = Field(default=0.3, description="Temperature")
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
        text_column = self.text_column
        summary_column = self.summary_column
        provider = self.provider
        model = self.model
        summary_type = self.summary_type
        max_length = self.max_length
        chunk_size = self.chunk_size
        use_map_reduce = self.use_map_reduce
        api_key = self.api_key
        temperature = self.temperature
        description = self.description or f"Summarize document with {provider}"
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
        def document_summarizer_asset(ctx: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Summarize text column in an upstream DataFrame."""

            df = upstream.copy()

            if text_column not in df.columns:
                raise ValueError(f"Text column '{text_column}' not found. Available: {list(df.columns)}")

            ctx.log.info(f"Summarizing {len(df)} documents using {provider}/{model}")

            # Expand environment variables in API key
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and '${' in api_key:
                    raise ValueError(f"Environment variable in api_key '{api_key}' is not set")

            # Build prompt based on summary type
            prompts = {
                "concise": "Provide a concise summary of the following text:\n\n{text}",
                "detailed": "Provide a detailed summary covering all key points:\n\n{text}",
                "bullet_points": "Summarize the following text as bullet points:\n\n{text}",
                "executive": "Provide an executive summary suitable for leadership:\n\n{text}"
            }
            base_prompt = prompts.get(summary_type, prompts["concise"])
            if max_length:
                base_prompt += f"\n\nLimit the summary to approximately {max_length} words."

            def summarize_text(text: str) -> str:
                """Summarize a single piece of text."""
                # Handle long documents with map-reduce
                if len(text) > chunk_size and use_map_reduce:
                    # Split into chunks
                    chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

                    # Map: Summarize each chunk
                    chunk_summaries = []
                    for i, chunk in enumerate(chunks):
                        ctx.log.info(f"Summarizing chunk {i+1}/{len(chunks)}")
                        chunk_prompt = f"Summarize this section:\n\n{chunk}"
                        chunk_summaries.append(_call_llm(chunk_prompt))

                    # Reduce: Combine summaries
                    combined = "\n\n".join(chunk_summaries)
                    final_prompt = base_prompt.format(text=combined)
                else:
                    final_prompt = base_prompt.format(text=text)

                return _call_llm(final_prompt)

            def _call_llm(prompt: str) -> str:
                if provider == "openai":
                    import openai
                    client = openai.OpenAI(api_key=expanded_api_key)
                    response = client.chat.completions.create(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=temperature
                    )
                    return response.choices[0].message.content
                elif provider == "anthropic":
                    import anthropic
                    client = anthropic.Anthropic(api_key=expanded_api_key)
                    message = client.messages.create(
                        model=model,
                        max_tokens=4096,
                        temperature=temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    return message.content[0].text
                else:
                    raise ValueError(f"Unsupported provider: {provider}")

            summaries = []
            for idx, row in df.iterrows():
                text = str(row[text_column])
                ctx.log.info(f"Summarizing document {idx + 1}/{len(df)} ({len(text)} characters)")
                summaries.append(summarize_text(text))

            df[summary_column] = summaries

            ctx.log.info(f"Summarization complete: {len(df)} documents processed")
            ctx.add_output_metadata({
                "rows_processed": len(df),
                "provider": provider,
                "model": model,
                "summary_type": summary_type,
            })

            return df

        return Definitions(assets=[document_summarizer_asset])
