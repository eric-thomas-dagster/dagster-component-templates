"""LangChain Chain Asset Component.

Reads rows from an upstream asset DataFrame, runs them through a
configurable LangChain chain (prompt template + LLM + optional output parser),
and returns the enriched DataFrame.

Supports any LangChain-compatible LLM: OpenAI, Anthropic, Azure, Ollama, etc.
"""
from typing import Optional
import pandas as pd
import dagster as dg
from dagster import AssetExecutionContext, AssetIn, AssetKey
from pydantic import Field


class LangChainChainAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a LangChain chain over rows from an upstream asset DataFrame and return enriched DataFrame.

    Configures a LangChain prompt + LLM + optional output parser chain. Each row
    is formatted using the prompt_template and passed to the chain. The response
    is written to response_column.

    Supports OpenAI, Anthropic, Azure OpenAI, Google, Ollama, and any other
    LangChain-compatible LLM provider.

    Example:
        ```yaml
        type: dagster_component_templates.LangChainChainAssetComponent
        attributes:
          asset_name: summarized_articles
          upstream_asset_key: raw_articles
          llm_provider: openai
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          prompt_template: |
            Summarize the following article in 2-3 sentences:
            {content}
          response_column: summary
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame (must return a pandas DataFrame)")

    # LLM configuration
    llm_provider: str = Field(default="openai", description="LLM provider: openai, anthropic, azure_openai, google, ollama")
    model: str = Field(default="gpt-4o-mini", description="Model name (provider-specific)")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var with provider API key")
    api_base_env_var: Optional[str] = Field(default=None, description="Env var with custom API base URL")
    temperature: float = Field(default=0.0, description="Sampling temperature")
    max_tokens: int = Field(default=1024, description="Max tokens per completion")

    # Chain configuration
    prompt_template: str = Field(description="LangChain prompt template string with {column_name} placeholders")
    system_message: Optional[str] = Field(default=None, description="System message for chat models")
    response_column: str = Field(default="chain_output", description="Column name to store chain output")
    parse_json: bool = Field(default=False, description="Attempt to parse LLM response as JSON and expand into columns")

    # Processing
    batch_size: int = Field(default=10, description="Rows per batch for progress logging")
    max_rows: Optional[int] = Field(default=None, description="Limit rows processed (for testing)")
    max_concurrency: int = Field(default=1, description="Concurrent LLM calls (use >1 carefully for rate limits)")

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
            description=_self.description or f"LangChain ({_self.llm_provider}/{_self.model}): {_self.prompt_template[:50]}...",
            group_name=_self.group_name,
            kinds={"ai", "langchain"},
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(_self.upstream_asset_key))},
        )
        def langchain_chain_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            import os, json

            df = upstream.copy()

            if _self.max_rows:
                df = df.head(_self.max_rows)

            # Build LangChain LLM
            from langchain_core.prompts import ChatPromptTemplate, PromptTemplate
            from langchain_core.output_parsers import StrOutputParser

            provider = _self.llm_provider.lower()
            api_key = os.environ[_self.api_key_env_var] if _self.api_key_env_var else None
            api_base = os.environ.get(_self.api_base_env_var or "", None) if _self.api_base_env_var else None

            if provider == "openai":
                from langchain_openai import ChatOpenAI
                llm_kwargs: dict = {"model": _self.model, "temperature": _self.temperature, "max_tokens": _self.max_tokens}
                if api_key:
                    llm_kwargs["api_key"] = api_key
                if api_base:
                    llm_kwargs["base_url"] = api_base
                llm = ChatOpenAI(**llm_kwargs)

            elif provider == "anthropic":
                from langchain_anthropic import ChatAnthropic
                llm_kwargs = {"model": _self.model, "temperature": _self.temperature, "max_tokens": _self.max_tokens}
                if api_key:
                    llm_kwargs["api_key"] = api_key
                llm = ChatAnthropic(**llm_kwargs)

            elif provider == "azure_openai":
                from langchain_openai import AzureChatOpenAI
                llm_kwargs = {"azure_deployment": _self.model, "temperature": _self.temperature, "max_tokens": _self.max_tokens}
                if api_key:
                    llm_kwargs["api_key"] = api_key
                if api_base:
                    llm_kwargs["azure_endpoint"] = api_base
                llm = AzureChatOpenAI(**llm_kwargs)

            elif provider == "google":
                from langchain_google_genai import ChatGoogleGenerativeAI
                llm_kwargs = {"model": _self.model, "temperature": _self.temperature}
                if api_key:
                    llm_kwargs["google_api_key"] = api_key
                llm = ChatGoogleGenerativeAI(**llm_kwargs)

            elif provider == "ollama":
                from langchain_ollama import ChatOllama
                llm_kwargs = {"model": _self.model, "temperature": _self.temperature}
                if api_base:
                    llm_kwargs["base_url"] = api_base
                llm = ChatOllama(**llm_kwargs)

            else:
                raise ValueError(f"Unsupported llm_provider: {provider}. Use: openai, anthropic, azure_openai, google, ollama")

            # Build prompt
            if _self.system_message:
                prompt = ChatPromptTemplate.from_messages([
                    ("system", _self.system_message),
                    ("human", _self.prompt_template),
                ])
            else:
                prompt = ChatPromptTemplate.from_template(_self.prompt_template)

            chain = prompt | llm | StrOutputParser()

            context.log.info(f"Running LangChain chain ({provider}/{_self.model}) over {len(df)} rows")

            responses = []
            for i, row in enumerate(df.itertuples(index=False)):
                row_dict = row._asdict()
                try:
                    response = chain.invoke(row_dict)
                except Exception as e:
                    context.log.warning(f"Row {i} failed: {e}")
                    response = None
                responses.append(response)

                if (i + 1) % _self.batch_size == 0:
                    context.log.info(f"Processed {i + 1}/{len(df)} rows")

            if _self.parse_json:
                parsed_rows = []
                for r in responses:
                    try:
                        parsed_rows.append(json.loads(r) if r else {})
                    except Exception:
                        parsed_rows.append({_self.response_column: r})
                if parsed_rows:
                    parsed_df = pd.DataFrame(parsed_rows)
                    for col in parsed_df.columns:
                        df[col] = parsed_df[col].values
            else:
                df[_self.response_column] = responses

            context.log.info(f"Completed chain over {len(df)} rows")
            return df

        return dg.Definitions(assets=[langchain_chain_asset])
