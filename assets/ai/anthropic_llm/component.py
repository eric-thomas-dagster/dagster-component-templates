"""Anthropic LLM Component.

Process text data using Anthropic's Claude models (Claude 3.5 Sonnet, Claude 3 Opus, Claude 3 Haiku).
Supports batch processing, streaming, tool use, prompt caching, and cost tracking.
"""

import os
import json
import time
import hashlib
from typing import Optional, Dict, Any, List
from pathlib import Path
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
    Output,
    MetadataValue,
)
from pydantic import Field


class AnthropicLLMComponent(Component, Model, Resolvable):
    """Component for processing text with Anthropic's Claude models.

    This component processes text using Anthropic's Claude API (Sonnet, Opus, Haiku).
    It supports both single prompt execution and batch processing of DataFrame columns.

    Features:
    - Claude 3.5 Sonnet, Claude 3 Opus, Claude 3 Haiku models
    - Streaming responses
    - Tool use (function calling)
    - Token counting and cost tracking
    - Prompt caching for repeated contexts (reduces costs by 90%)
    - Batch processing with rate limiting
    - Response caching to reduce costs
    - Retry logic with exponential backoff
    - Extended context windows (up to 200K tokens)

    Use Cases:
    - Long document analysis and summarization
    - Complex reasoning and analysis
    - Code generation and review
    - Research and fact-checking
    - Creative writing
    - Data extraction from documents

    Example:
        ```yaml
        type: dagster_component_templates.AnthropicLLMComponent
        attributes:
          asset_name: document_summaries
          api_key: "${ANTHROPIC_API_KEY}"
          model: claude-3-5-sonnet-20241022
          system_prompt: "You are a document summarization expert."
          input_column: document_text
          output_column: summary
          max_tokens: 1000
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the enriched data"
    )

    api_key: str = Field(
        description="Anthropic API key. Use ${ANTHROPIC_API_KEY} for environment variables."
    )

    model: str = Field(
        default="claude-3-5-sonnet-20241022",
        description="Claude model: claude-3-5-sonnet-20241022, claude-3-opus-20240229, claude-3-sonnet-20240229, claude-3-haiku-20240307"
    )

    system_prompt: Optional[str] = Field(
        default=None,
        description="System prompt to set context and behavior for Claude"
    )

    user_prompt_template: Optional[str] = Field(
        default=None,
        description="User prompt template with {column_name} placeholders for DataFrame columns"
    )

    input_column: Optional[str] = Field(
        default=None,
        description="Column name containing input text (for batch processing)"
    )

    output_column: str = Field(
        default="claude_response",
        description="Column name for Claude responses"
    )

    max_tokens: int = Field(
        default=1024,
        description="Maximum tokens in response (required by Anthropic API)"
    )

    temperature: float = Field(
        default=1.0,
        description="Temperature for response randomness (0.0-1.0). Lower = more deterministic."
    )

    top_p: Optional[float] = Field(
        default=None,
        description="Nucleus sampling parameter (0.0-1.0). Alternative to temperature."
    )

    top_k: Optional[int] = Field(
        default=None,
        description="Top-K sampling parameter. Only consider top K tokens."
    )

    stream: bool = Field(
        default=False,
        description="Use streaming responses (for single prompts only)"
    )

    tools: Optional[str] = Field(
        default=None,
        description="JSON array of tool definitions for tool use"
    )

    tool_choice: Optional[str] = Field(
        default=None,
        description="Control tool use: 'auto', 'any', 'tool_name', or {'type': 'tool', 'name': 'tool_name'}"
    )

    enable_prompt_caching: bool = Field(
        default=False,
        description="Enable prompt caching to reduce costs for repeated contexts (saves 90% on cached tokens)"
    )

    batch_size: int = Field(
        default=10,
        description="Number of rows to process in parallel for batch operations"
    )

    rate_limit_delay: float = Field(
        default=0.2,
        description="Delay in seconds between API calls to respect rate limits"
    )

    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for failed API calls"
    )

    enable_caching: bool = Field(
        default=True,
        description="Cache responses to avoid redundant API calls"
    )

    cache_dir: Optional[str] = Field(
        default=None,
        description="Directory for cache files. Default: /tmp/anthropic_llm_cache"
    )

    track_costs: bool = Field(
        default=True,
        description="Track token usage and estimated costs"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="anthropic_llm",
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
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with text to process")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        api_key = self.api_key
        model = self.model
        system_prompt = self.system_prompt
        user_prompt_template = self.user_prompt_template
        input_column = self.input_column
        output_column = self.output_column
        max_tokens = self.max_tokens
        temperature = self.temperature
        top_p = self.top_p
        top_k = self.top_k
        stream = self.stream
        tools_str = self.tools
        tool_choice_str = self.tool_choice
        enable_prompt_caching = self.enable_prompt_caching
        batch_size = self.batch_size
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries
        enable_caching = self.enable_caching
        cache_dir = self.cache_dir or "/tmp/anthropic_llm_cache"
        track_costs = self.track_costs
        description = self.description or f"Anthropic Claude processing with {model}"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Cost per 1M tokens (approximate, as of 2024)
        COST_PER_1M_INPUT = {
            "claude-3-5-sonnet-20241022": 3.0,
            "claude-3-opus-20240229": 15.0,
            "claude-3-sonnet-20240229": 3.0,
            "claude-3-haiku-20240307": 0.25,
        }
        COST_PER_1M_OUTPUT = {
            "claude-3-5-sonnet-20241022": 15.0,
            "claude-3-opus-20240229": 75.0,
            "claude-3-sonnet-20240229": 15.0,
            "claude-3-haiku-20240307": 1.25,
        }
        # Cached input costs (90% discount)
        COST_PER_1M_CACHED_INPUT = {
            "claude-3-5-sonnet-20241022": 0.3,
            "claude-3-opus-20240229": 1.5,
            "claude-3-sonnet-20240229": 0.3,
            "claude-3-haiku-20240307": 0.03,
        }

        upstream_asset_key = self.upstream_asset_key

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

        # Infer kinds from component name if not explicitly set
        _comp_name = "anthropic_llm"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        @asset(
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def anthropic_llm_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that processes text using Anthropic's Claude models."""

            context.log.info(f"Starting Anthropic Claude processing with model: {model}")

            # Expand environment variables in API key
            expanded_api_key = os.path.expandvars(api_key)
            if expanded_api_key == api_key and api_key.startswith('${'):
                var_name = api_key.strip('${}')
                raise ValueError(f"Environment variable not set: {var_name}")

            # Import Anthropic
            try:
                import anthropic
                client = anthropic.Anthropic(api_key=expanded_api_key)
            except ImportError:
                raise ImportError("Anthropic package not installed. Install with: pip install anthropic")

            # Setup cache directory
            if enable_caching:
                Path(cache_dir).mkdir(parents=True, exist_ok=True)
                context.log.info(f"Using cache directory: {cache_dir}")

            # Parse tools if provided
            tools = None
            if tools_str:
                try:
                    tools = json.loads(tools_str)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid tools JSON: {e}")

            # Parse tool_choice if provided
            tool_choice = None
            if tool_choice_str:
                if tool_choice_str in ["auto", "any"]:
                    tool_choice = {"type": tool_choice_str}
                else:
                    try:
                        tool_choice = json.loads(tool_choice_str)
                    except json.JSONDecodeError:
                        tool_choice = {"type": "tool", "name": tool_choice_str}

            input_df = upstream
            context.log.info(f"Received DataFrame: {len(input_df)} rows, {len(input_df.columns)} columns")

            # Validate input column exists
            if input_column and input_column not in input_df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(input_df.columns)}")

            # Token tracking
            total_input_tokens = 0
            total_output_tokens = 0
            total_cache_creation_tokens = 0
            total_cache_read_tokens = 0

            def get_cache_key(prompt: str) -> str:
                """Generate cache key from prompt and parameters."""
                cache_params = f"{model}_{temperature}_{max_tokens}_{system_prompt}_{prompt}"
                return hashlib.md5(cache_params.encode()).hexdigest()

            def get_cached_response(cache_key: str) -> Optional[str]:
                """Retrieve cached response if available."""
                if not enable_caching:
                    return None
                cache_file = Path(cache_dir) / f"{cache_key}.json"
                if cache_file.exists():
                    try:
                        with open(cache_file, 'r') as f:
                            cached = json.load(f)
                            return cached.get('response')
                    except:
                        return None
                return None

            def save_to_cache(cache_key: str, response: str, tokens_in: int, tokens_out: int):
                """Save response to cache."""
                if not enable_caching:
                    return
                cache_file = Path(cache_dir) / f"{cache_key}.json"
                with open(cache_file, 'w') as f:
                    json.dump({
                        'response': response,
                        'input_tokens': tokens_in,
                        'output_tokens': tokens_out,
                        'timestamp': time.time()
                    }, f)

            def call_anthropic_with_retry(messages: List[Dict], attempt: int = 0) -> tuple[str, int, int, int, int]:
                """Call Anthropic API with retry logic.

                Returns: (response_text, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens)
                """
                try:
                    # Build API call parameters
                    api_params = {
                        "model": model,
                        "max_tokens": max_tokens,
                        "messages": messages,
                        "temperature": temperature,
                    }

                    if system_prompt:
                        if enable_prompt_caching:
                            # Mark system prompt for caching
                            api_params["system"] = [
                                {
                                    "type": "text",
                                    "text": system_prompt,
                                    "cache_control": {"type": "ephemeral"}
                                }
                            ]
                        else:
                            api_params["system"] = system_prompt

                    if top_p is not None:
                        api_params["top_p"] = top_p
                    if top_k is not None:
                        api_params["top_k"] = top_k
                    if tools:
                        api_params["tools"] = tools
                    if tool_choice:
                        api_params["tool_choice"] = tool_choice

                    if stream and not input_column:
                        # Streaming mode for single prompts
                        response_chunks = []
                        with client.messages.stream(**api_params) as stream:
                            for text in stream.text_stream:
                                response_chunks.append(text)
                        response_text = "".join(response_chunks)
                        # Note: streaming doesn't return detailed token counts
                        return response_text, 0, 0, 0, 0
                    else:
                        # Standard mode
                        response = client.messages.create(**api_params)
                        response_text = response.content[0].text
                        tokens_in = response.usage.input_tokens
                        tokens_out = response.usage.output_tokens

                        # Prompt caching tokens
                        cache_creation = getattr(response.usage, 'cache_creation_input_tokens', 0)
                        cache_read = getattr(response.usage, 'cache_read_input_tokens', 0)

                        return response_text, tokens_in, tokens_out, cache_creation, cache_read

                except anthropic.RateLimitError as e:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * rate_limit_delay
                        context.log.warning(f"Rate limit hit, waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        return call_anthropic_with_retry(messages, attempt + 1)
                    raise
                except Exception as e:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * rate_limit_delay
                        context.log.warning(f"API error: {e}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        return call_anthropic_with_retry(messages, attempt + 1)
                    raise

            # Process DataFrame
            responses = []
            cache_hits = 0

            for idx, row in input_df.iterrows():
                # Build prompt
                if input_column:
                    if user_prompt_template:
                        # Template with column substitution
                        user_prompt = user_prompt_template.format(**row.to_dict())
                    else:
                        # Use input column directly
                        user_prompt = str(row[input_column])
                else:
                    # Single prompt mode
                    user_prompt = user_prompt_template

                # Build messages
                messages = [{"role": "user", "content": user_prompt}]

                # Check cache
                cache_key = get_cache_key(user_prompt)
                cached_response = get_cached_response(cache_key)

                if cached_response:
                    responses.append(cached_response)
                    cache_hits += 1
                    if idx % 10 == 0:
                        context.log.info(f"Processed {idx + 1}/{len(input_df)} (cache hit)")
                else:
                    # Call API
                    response_text, tokens_in, tokens_out, cache_creation, cache_read = call_anthropic_with_retry(messages)
                    responses.append(response_text)
                    total_input_tokens += tokens_in
                    total_output_tokens += tokens_out
                    total_cache_creation_tokens += cache_creation
                    total_cache_read_tokens += cache_read

                    # Save to cache
                    save_to_cache(cache_key, response_text, tokens_in, tokens_out)

                    if idx % 10 == 0:
                        context.log.info(f"Processed {idx + 1}/{len(input_df)}")

                    # Rate limiting
                    if rate_limit_delay > 0:
                        time.sleep(rate_limit_delay)

            # Add responses to DataFrame
            result_df = input_df.copy()
            result_df[output_column] = responses

            # Remove single prompt placeholder if used
            if "_single_prompt" in result_df.columns:
                result_df = result_df.drop(columns=["_single_prompt"])

            context.log.info(f"Completed processing: {len(result_df)} rows")
            context.log.info(f"Cache hits: {cache_hits}/{len(result_df)}")
            if enable_prompt_caching and total_cache_read_tokens > 0:
                context.log.info(f"Prompt cache reads: {total_cache_read_tokens} tokens (90% savings)")

            # Calculate costs
            cost_input = 0.0
            cost_output = 0.0
            cost_cached = 0.0

            if track_costs and total_input_tokens > 0:
                if model in COST_PER_1M_INPUT:
                    cost_input = (total_input_tokens / 1_000_000) * COST_PER_1M_INPUT[model]
                    cost_output = (total_output_tokens / 1_000_000) * COST_PER_1M_OUTPUT[model]
                    if total_cache_read_tokens > 0:
                        cost_cached = (total_cache_read_tokens / 1_000_000) * COST_PER_1M_CACHED_INPUT[model]

            total_cost = cost_input + cost_output + cost_cached

            # Metadata
            metadata = {
                "model": model,
                "rows_processed": len(result_df),
                "cache_hits": cache_hits,
                "cache_hit_rate": f"{cache_hits / len(result_df) * 100:.1f}%",
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "estimated_cost_usd": f"${total_cost:.4f}",
            }

            if enable_prompt_caching:
                metadata["cache_creation_tokens"] = total_cache_creation_tokens
                metadata["cache_read_tokens"] = total_cache_read_tokens
                if total_cache_read_tokens > 0:
                    savings = (total_cache_read_tokens / 1_000_000) * (COST_PER_1M_INPUT[model] - COST_PER_1M_CACHED_INPUT[model])
                    metadata["prompt_cache_savings_usd"] = f"${savings:.4f}"

            if include_sample and len(result_df) > 0:
                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(result_df.head(10))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(result_df.dtypes[col]))
                for col in result_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(result_df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
                return result_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[anthropic_llm_asset])


        return Definitions(assets=[anthropic_llm_asset], asset_checks=list(_schema_checks))
