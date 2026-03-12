"""LiteLLM Inference Asset Component.

Reads rows from an upstream asset (or a database table), runs each row through
a configurable LLM prompt via LiteLLM, and writes the enriched output to a
destination database table.

LiteLLM supports 100+ providers: OpenAI, Anthropic, Azure, Bedrock, Gemini,
Mistral, Cohere, and more — all through a unified API.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext, Config
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

        response = litellm.completion(**kwargs)
        return response.choices[0].message.content


class LiteLLMInferenceAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read rows from an upstream asset or database table, enrich via LiteLLM, write to destination.

    Each row is formatted using prompt_template (a Python format string with column names
    as variables), sent to the configured LLM, and the response is written to response_column.

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
          database_url_env_var: DATABASE_URL
          table_name: enriched_support_tickets
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: Optional[str] = Field(default=None, description="Upstream asset key to load data from (e.g. 'raw_orders' or 'schema/table')")
    source_database_url_env_var: Optional[str] = Field(default=None, description="Env var with source SQLAlchemy URL (alternative to upstream_asset_key)")
    source_table: Optional[str] = Field(default=None, description="Source table to read when using source_database_url_env_var")
    source_query: Optional[str] = Field(default=None, description="Custom SQL query for source data (overrides source_table)")
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
    database_url_env_var: str = Field(description="Env var with destination SQLAlchemy database URL")
    table_name: str = Field(description="Destination table name")
    schema_name: Optional[str] = Field(default=None, description="Destination schema name")
    if_exists: str = Field(default="replace", description="fail, replace, or append")
    group_name: Optional[str] = Field(default="ai", description="Asset group name")
    description: Optional[str] = Field(default=None)
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        upstream_keys = []
        if _self.upstream_asset_key:
            upstream_keys = [dg.AssetKey.from_user_string(_self.upstream_asset_key)]
        for k in (_self.deps or []):
            key = dg.AssetKey.from_user_string(k)
            if key not in upstream_keys:
                upstream_keys.append(key)

        @dg.asset(
            name=_self.asset_name,
            description=_self.description or f"LiteLLM inference: {_self.prompt_template[:60]}...",
            group_name=_self.group_name,
            kinds={"ai", "llm", "sql"},
            deps=upstream_keys if upstream_keys else None,
        )
        def litellm_inference_asset(context: AssetExecutionContext):
            import os, json
            import litellm
            import pandas as pd
            from sqlalchemy import create_engine, text

            dst_url = os.environ[_self.database_url_env_var]

            # Load source data
            if _self.upstream_asset_key:
                df = context.load_asset_value(dg.AssetKey.from_user_string(_self.upstream_asset_key))
                if not isinstance(df, pd.DataFrame):
                    raise ValueError(f"Upstream asset {_self.upstream_asset_key} must return a pandas DataFrame")
            elif _self.source_database_url_env_var:
                src_url = os.environ[_self.source_database_url_env_var]
                src_engine = create_engine(src_url)
                query = _self.source_query or f"SELECT * FROM {_self.source_table}"
                with src_engine.connect() as conn:
                    df = pd.read_sql(text(query), conn)
            else:
                raise ValueError("Must set upstream_asset_key or source_database_url_env_var")

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

                response = litellm.completion(model=model, messages=messages, **litellm_kwargs)
                responses.append(response.choices[0].message.content)

                if (i + 1) % _self.batch_size == 0:
                    context.log.info(f"Processed {i + 1}/{len(df)} rows")

            df[_self.response_column] = responses
            context.log.info(f"Completed {len(df)} LLM calls, writing to {_self.table_name}")

            dst_engine = create_engine(dst_url)
            df.to_sql(_self.table_name, con=dst_engine, schema=_self.schema_name,
                      if_exists=_self.if_exists, index=False, method="multi", chunksize=500)

            context.log.info(f"Wrote {len(df)} enriched rows to {_self.schema_name + '.' if _self.schema_name else ''}{_self.table_name}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "model": model,
                "response_column": _self.response_column,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{_self.table_name}",
            })

        return dg.Definitions(assets=[litellm_inference_asset])
