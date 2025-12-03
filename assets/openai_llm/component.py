"""OpenAI LLM Component.

Process text data using OpenAI's GPT models (GPT-4, GPT-3.5-turbo, GPT-4-turbo).
Supports batch processing of DataFrame columns, streaming, function calling, token counting,
response caching, and cost tracking.
"""

import os
import json
import time
import hashlib
from typing import Optional, Dict, Any, List
from pathlib import Path
import pandas as pd

from dagster import (
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


class OpenAILLMComponent(Component, Model, Resolvable):
    """Component for processing text with OpenAI's GPT models.

    This component processes text using OpenAI's API (GPT-4, GPT-3.5-turbo, etc.).
    It supports both single prompt execution and batch processing of DataFrame columns.

    Features:
    - Multiple GPT models (gpt-4, gpt-3.5-turbo, gpt-4-turbo, etc.)
    - Streaming responses
    - Function calling / tool use
    - Token counting and cost tracking
    - Batch processing with rate limiting
    - Response caching to reduce costs
    - Retry logic with exponential backoff
    - Temperature and max_tokens control

    Use Cases:
    - Text generation and completion
    - Classification and categorization
    - Entity extraction
    - Summarization
    - Translation
    - Question answering
    - Data enrichment

    Example:
        ```yaml
        type: dagster_component_templates.OpenAILLMComponent
        attributes:
          asset_name: product_descriptions_enriched
          api_key: "${OPENAI_API_KEY}"
          model: gpt-4-turbo
          system_prompt: "You are a product description expert."
          input_column: product_name
          output_column: description
          temperature: 0.7
          max_tokens: 500
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the enriched data"
    )

    api_key: str = Field(
        description="OpenAI API key. Use ${OPENAI_API_KEY} for environment variables."
    )

    model: str = Field(
        default="gpt-3.5-turbo",
        description="OpenAI model: gpt-4, gpt-4-turbo, gpt-3.5-turbo, gpt-4o, gpt-4o-mini"
    )

    system_prompt: Optional[str] = Field(
        default=None,
        description="System prompt to set context and behavior for the LLM"
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
        default="llm_response",
        description="Column name for LLM responses"
    )

    temperature: float = Field(
        default=0.7,
        description="Temperature for response randomness (0.0-2.0). Lower = more deterministic."
    )

    max_tokens: Optional[int] = Field(
        default=None,
        description="Maximum tokens in response. Default uses model's max."
    )

    top_p: Optional[float] = Field(
        default=None,
        description="Nucleus sampling parameter (0.0-1.0). Alternative to temperature."
    )

    frequency_penalty: float = Field(
        default=0.0,
        description="Reduce repetition of token sequences (-2.0 to 2.0)"
    )

    presence_penalty: float = Field(
        default=0.0,
        description="Increase likelihood of new topics (-2.0 to 2.0)"
    )

    stream: bool = Field(
        default=False,
        description="Use streaming responses (for single prompts only)"
    )

    functions: Optional[str] = Field(
        default=None,
        description="JSON array of function definitions for function calling"
    )

    function_call: Optional[str] = Field(
        default=None,
        description="Control function calling: 'auto', 'none', or {'name': 'function_name'}"
    )

    response_format: Optional[str] = Field(
        default=None,
        description="Response format: 'text' or 'json_object' for structured outputs"
    )

    batch_size: int = Field(
        default=10,
        description="Number of rows to process in parallel for batch operations"
    )

    rate_limit_delay: float = Field(
        default=0.1,
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
        description="Directory for cache files. Default: /tmp/openai_llm_cache"
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
        default="openai_llm",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        api_key = self.api_key
        model = self.model
        system_prompt = self.system_prompt
        user_prompt_template = self.user_prompt_template
        input_column = self.input_column
        output_column = self.output_column
        temperature = self.temperature
        max_tokens = self.max_tokens
        top_p = self.top_p
        frequency_penalty = self.frequency_penalty
        presence_penalty = self.presence_penalty
        stream = self.stream
        functions_str = self.functions
        function_call_str = self.function_call
        response_format = self.response_format
        batch_size = self.batch_size
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries
        enable_caching = self.enable_caching
        cache_dir = self.cache_dir or "/tmp/openai_llm_cache"
        track_costs = self.track_costs
        description = self.description or f"OpenAI LLM processing with {model}"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Cost per 1M tokens (approximate, as of 2024)
        COST_PER_1M_INPUT = {
            "gpt-4": 30.0,
            "gpt-4-turbo": 10.0,
            "gpt-4o": 5.0,
            "gpt-4o-mini": 0.15,
            "gpt-3.5-turbo": 0.5,
        }
        COST_PER_1M_OUTPUT = {
            "gpt-4": 60.0,
            "gpt-4-turbo": 30.0,
            "gpt-4o": 15.0,
            "gpt-4o-mini": 0.6,
            "gpt-3.5-turbo": 1.5,
        }

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def openai_llm_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that processes text using OpenAI's GPT models."""

            context.log.info(f"Starting OpenAI LLM processing with model: {model}")

            # Expand environment variables in API key
            expanded_api_key = os.path.expandvars(api_key)
            if expanded_api_key == api_key and api_key.startswith('${'):
                var_name = api_key.strip('${}')
                raise ValueError(f"Environment variable not set: {var_name}")

            # Import OpenAI
            try:
                import openai
                client = openai.OpenAI(api_key=expanded_api_key)
            except ImportError:
                raise ImportError("OpenAI package not installed. Install with: pip install openai")

            # Setup cache directory
            if enable_caching:
                Path(cache_dir).mkdir(parents=True, exist_ok=True)
                context.log.info(f"Using cache directory: {cache_dir}")

            # Parse functions if provided
            functions = None
            if functions_str:
                try:
                    functions = json.loads(functions_str)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid functions JSON: {e}")

            # Parse function_call if provided
            function_call = None
            if function_call_str:
                if function_call_str in ["auto", "none"]:
                    function_call = function_call_str
                else:
                    try:
                        function_call = json.loads(function_call_str)
                    except json.JSONDecodeError:
                        function_call = {"name": function_call_str}

            # Get input DataFrame from upstream assets
            input_df = None
            for key, value in kwargs.items():
                if isinstance(value, pd.DataFrame):
                    input_df = value
                    context.log.info(f"Received DataFrame from '{key}': {len(value)} rows, {len(value.columns)} columns")
                    break

            if input_df is None:
                context.log.info("No upstream DataFrame found. Using single prompt mode.")
                if not user_prompt_template:
                    raise ValueError("Either provide upstream DataFrame or user_prompt_template")
                # Single prompt mode
                input_df = pd.DataFrame({"_single_prompt": [True]})

            # Validate input column exists
            if input_column and input_column not in input_df.columns:
                raise ValueError(f"Input column '{input_column}' not found. Available: {list(input_df.columns)}")

            # Token tracking
            total_input_tokens = 0
            total_output_tokens = 0

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

            def call_openai_with_retry(messages: List[Dict], attempt: int = 0) -> tuple[str, int, int]:
                """Call OpenAI API with retry logic."""
                try:
                    # Build API call parameters
                    api_params = {
                        "model": model,
                        "messages": messages,
                        "temperature": temperature,
                        "frequency_penalty": frequency_penalty,
                        "presence_penalty": presence_penalty,
                    }

                    if max_tokens:
                        api_params["max_tokens"] = max_tokens
                    if top_p is not None:
                        api_params["top_p"] = top_p
                    if response_format:
                        api_params["response_format"] = {"type": response_format}
                    if functions:
                        api_params["functions"] = functions
                    if function_call:
                        api_params["function_call"] = function_call

                    if stream and not input_column:
                        # Streaming mode for single prompts
                        api_params["stream"] = True
                        response_chunks = []
                        response_stream = client.chat.completions.create(**api_params)
                        for chunk in response_stream:
                            if chunk.choices[0].delta.content:
                                response_chunks.append(chunk.choices[0].delta.content)
                        response_text = "".join(response_chunks)
                        # Note: streaming doesn't return token counts directly
                        return response_text, 0, 0
                    else:
                        # Standard mode
                        response = client.chat.completions.create(**api_params)
                        response_text = response.choices[0].message.content
                        tokens_in = response.usage.prompt_tokens
                        tokens_out = response.usage.completion_tokens
                        return response_text, tokens_in, tokens_out

                except openai.RateLimitError as e:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * rate_limit_delay
                        context.log.warning(f"Rate limit hit, waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        return call_openai_with_retry(messages, attempt + 1)
                    raise
                except Exception as e:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * rate_limit_delay
                        context.log.warning(f"API error: {e}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        return call_openai_with_retry(messages, attempt + 1)
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
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": user_prompt})

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
                    response_text, tokens_in, tokens_out = call_openai_with_retry(messages)
                    responses.append(response_text)
                    total_input_tokens += tokens_in
                    total_output_tokens += tokens_out

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

            # Calculate costs
            cost_input = 0.0
            cost_output = 0.0
            if track_costs and total_input_tokens > 0:
                model_base = model.split("-")[0] + "-" + model.split("-")[1] if "-" in model else model
                for key in COST_PER_1M_INPUT.keys():
                    if key in model:
                        cost_input = (total_input_tokens / 1_000_000) * COST_PER_1M_INPUT[key]
                        cost_output = (total_output_tokens / 1_000_000) * COST_PER_1M_OUTPUT[key]
                        break

            total_cost = cost_input + cost_output

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
                return result_df

        return Definitions(assets=[openai_llm_asset])
