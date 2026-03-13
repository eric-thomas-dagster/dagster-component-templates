"""LiteLLM Inference Asset Component.

Reads rows from an upstream asset DataFrame, runs each row through
a configurable LLM prompt via LiteLLM, and returns the enriched DataFrame.

LiteLLM supports 100+ providers: OpenAI, Anthropic, Azure, Bedrock, Gemini,
Mistral, Cohere, and more — all through a unified API.
"""
from typing import Optional
import pandas as pd
import dagster as dg
from dagster import AssetExecutionContext, AssetIn, AssetKey
from pydantic import Field


class LiteLLMResource(dg.ConfigurableResource):
    """Shared LiteLLM configuration for use across multiple components.

    Example:
        ```python
        resources = {
            "litellm": LiteLLMResource(
                model="gpt-4o",
                api_key_env_var="OPENAI_API_KEY",
            )
        }
        ```
    """
    model: str = Field(description="LiteLLM model string (e.g. gpt-4o, claude-3-5-sonnet, gemini/gemini-pro)")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var with provider API key")
    api_base_env_var: Optional[str] = Field(default=None, description="Env var with custom API base URL (for proxies)")
    temperature: float = Field(default=0.0, description="Sampling temperature")
    max_tokens: int = Field(default=1024, description="Max tokens per completion")
    timeout_seconds: int = Field(default=60, description="Request timeout in seconds")
    max_retries: int = Field(default=3, description="Max retries on failure")

    def complete(self, messages: list[dict]) -> str:
        """Run a chat completion and return the response content."""
        import os
        import litellm

        kwargs: dict = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "timeout": self.timeout_seconds,
            "num_retries": self.max_retries,
        }
        if self.api_key_env_var:
            kwargs["api_key"] = os.environ[self.api_key_env_var]
        if self.api_base_env_var:
            kwargs["api_base"] = os.environ[self.api_base_env_var]

        response = litellm.completion(**kwargs                api_key=api_key,
            )
        return response.choices[0].message.content


class LiteLLMInferenceAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read rows from an upstream asset DataFrame, enrich via LiteLLM, return enriched DataFrame.

    Each row is formatted using prompt_template (a Python format string with column names
    as variables), sent to the configured LLM, and the response is added as response_column.

    Example:
        ```yaml
        type: dagster_component_templates.LiteLLMInferenceAssetComponent
        attributes:
          asset_name: enriched_support_tickets
          upstream_asset_key: raw_support_tickets
          model: claude-3-5-sonnet-20241022
          api_key_env_var: ANTHROPIC_API_KEY
          prompt_template: "Classify the sentiment and urgency of this support ticket: {body}"
          response_column: ai_classification
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame (e.g. 'raw_orders' or 'schema/table')")
    model: Optional[str] = Field(default=None, description="LiteLLM model string — overrides resource if set")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var with provider API key — overrides resource if set")
    api_base_env_var: Optional[str] = Field(default=None, description="Env var with custom API base URL")
    litellm_resource_key: Optional[str] = Field(default=None, description="Key of a LiteLLMResource in resources dict")
    prompt_template: str = Field(description="Python format string using column names: 'Classify: {text_column}'")
    system_prompt: Optional[str] = Field(default=None, description="System prompt for chat completions")
    response_column: str = Field(default="llm_response", description="Column name to store LLM responses")
    temperature: float = Field(default=0.0, description="Sampling temperature")
    max_tokens: int = Field(default=1024, description="Max tokens per completion")
    batch_size: int = Field(default=10, description="Rows to process per batch (for progress logging)")
    max_rows: Optional[int] = Field(default=None, description="Limit rows processed (useful for testing)")
    group_name: Optional[str] = Field(default="ai", description="Asset group name")
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
    description: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"LiteLLM inference: {_self.prompt_template[:60]}...",
            group_name=_self.group_name,
            kinds={"ai", "llm"},
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(_self.upstream_asset_key))},
        )
        def litellm_inference_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            import os
            import litellm

            df = upstream.copy()

            if _self.max_rows:
                df = df.head(_self.max_rows)

            context.log.info(f"Processing {len(df)} rows with LiteLLM model")

            # Configure LiteLLM kwargs
            litellm_kwargs: dict = {
                "temperature": _self.temperature,
                "max_tokens": _self.max_tokens,
            }
            if _self.api_key_env_var:
                litellm_kwargs["api_key"] = os.environ[_self.api_key_env_var]
            if _self.api_base_env_var:
                litellm_kwargs["api_base"] = os.environ[_self.api_base_env_var]

            model = _self.model
            if not model:
                raise ValueError("model must be set on the component or via litellm_resource_key")

            responses = []
            for i, row in enumerate(df.itertuples(index=False)):
                row_dict = row._asdict()
                try:
                    prompt = _self.prompt_template.format(**row_dict)
                except KeyError as e:
                    raise ValueError(f"prompt_template references missing column {e}. Available: {list(row_dict.keys())}")

                messages = []
                if _self.system_prompt:
                    messages.append({"role": "system", "content": _self.system_prompt})
                messages.append({"role": "user", "content": prompt})

                response = litellm.completion(model=model, messages=messages, **litellm_kwargs                api_key=api_key,
            )
                responses.append(response.choices[0].message.content)

                if (i + 1) % _self.batch_size == 0:
                    context.log.info(f"Processed {i + 1}/{len(df)} rows")

            df[_self.response_column] = responses
            context.log.info(f"Completed {len(df)} LLM calls")
            return df

        return dg.Definitions(assets=[litellm_inference_asset])
