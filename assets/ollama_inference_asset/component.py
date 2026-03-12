"""Ollama Inference Asset Component.

Reads rows from an upstream asset or database table, runs each row through
a locally-running Ollama model, and writes the enriched output to a destination
database table.

Ollama runs open-source models locally: Llama 3, Mistral, Gemma, Phi-3, etc.
No API key required — just a running Ollama server.
"""
from typing import Optional
import dagster as dg
from dagster import AssetExecutionContext
from pydantic import Field


class OllamaResource(dg.ConfigurableResource):
    """Shared Ollama connection config for use across multiple components.

    Example:
        ```python
        resources = {
            "ollama": OllamaResource(host="http://localhost:11434", model="llama3.2")
        }
        ```
    """
    host: str = Field(default="http://localhost:11434", description="Ollama server URL")
    model: str = Field(description="Ollama model name (e.g. llama3.2, mistral, gemma2)")
    temperature: float = Field(default=0.0, description="Sampling temperature")
    timeout_seconds: int = Field(default=120, description="Request timeout in seconds")

    def generate(self, prompt: str, system: Optional[str] = None) -> str:
        """Run a generation and return the response text."""
        import requests

        payload: dict = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": self.temperature},
        }
        if system:
            payload["system"] = system

        resp = requests.post(
            f"{self.host}/api/generate",
            json=payload,
            timeout=self.timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()["response"]


class OllamaInferenceAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read rows from an upstream asset or table, enrich via local Ollama, write to destination.

    Requires a running Ollama server. No API key or internet connection needed.
    Ideal for private data, air-gapped environments, or cost-sensitive workloads.

    Example:
        ```yaml
        type: dagster_component_templates.OllamaInferenceAssetComponent
        attributes:
          asset_name: categorized_feedback
          upstream_asset_key: raw_feedback
          ollama_host_env_var: OLLAMA_HOST
          model: llama3.2
          prompt_template: "Categorize this customer feedback into one of [bug, feature, praise, question]: {feedback_text}"
          response_column: category
          database_url_env_var: DATABASE_URL
          table_name: categorized_feedback
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: Optional[str] = Field(default=None, description="Upstream asset key to load data from")
    source_database_url_env_var: Optional[str] = Field(default=None, description="Env var with source SQLAlchemy URL (alternative to upstream_asset_key)")
    source_table: Optional[str] = Field(default=None, description="Source table name")
    source_query: Optional[str] = Field(default=None, description="Custom SQL query for source data")
    ollama_host_env_var: Optional[str] = Field(default=None, description="Env var with Ollama server URL (default: http://localhost:11434)")
    ollama_resource_key: Optional[str] = Field(default=None, description="Key of an OllamaResource in resources dict")
    model: str = Field(default="llama3.2", description="Ollama model name (e.g. llama3.2, mistral, gemma2, phi3)")
    prompt_template: str = Field(description="Python format string using column names: 'Classify: {text_column}'")
    system_prompt: Optional[str] = Field(default=None, description="System prompt for the model")
    response_column: str = Field(default="llm_response", description="Column name to store model responses")
    temperature: float = Field(default=0.0, description="Sampling temperature")
    timeout_seconds: int = Field(default=120, description="Request timeout per row in seconds")
    batch_size: int = Field(default=10, description="Rows per batch for progress logging")
    max_rows: Optional[int] = Field(default=None, description="Limit rows processed (for testing)")
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
            description=_self.description or f"Ollama ({_self.model}) inference: {_self.prompt_template[:50]}...",
            group_name=_self.group_name,
            kinds={"ai", "ollama", "sql"},
            deps=upstream_keys if upstream_keys else None,
        )
        def ollama_inference_asset(context: AssetExecutionContext):
            import os
            import requests
            import pandas as pd
            from sqlalchemy import create_engine, text

            dst_url = os.environ[_self.database_url_env_var]
            ollama_host = os.environ.get(_self.ollama_host_env_var or "", "http://localhost:11434") if _self.ollama_host_env_var else "http://localhost:11434"

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

            context.log.info(f"Processing {len(df)} rows with Ollama model {_self.model} at {ollama_host}")

            responses = []
            for i, row in enumerate(df.itertuples(index=False)):
                row_dict = row._asdict()
                try:
                    prompt = _self.prompt_template.format(**row_dict)
                except KeyError as e:
                    raise ValueError(f"prompt_template references missing column {e}. Available: {list(row_dict.keys())}")

                payload: dict = {
                    "model": _self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": _self.temperature},
                }
                if _self.system_prompt:
                    payload["system"] = _self.system_prompt

                resp = requests.post(
                    f"{ollama_host}/api/generate",
                    json=payload,
                    timeout=_self.timeout_seconds,
                )
                resp.raise_for_status()
                responses.append(resp.json()["response"])

                if (i + 1) % _self.batch_size == 0:
                    context.log.info(f"Processed {i + 1}/{len(df)} rows")

            df[_self.response_column] = responses
            context.log.info(f"Completed {len(df)} Ollama calls, writing to {_self.table_name}")

            dst_engine = create_engine(dst_url)
            df.to_sql(_self.table_name, con=dst_engine, schema=_self.schema_name,
                      if_exists=_self.if_exists, index=False, method="multi", chunksize=500)

            context.log.info(f"Wrote {len(df)} enriched rows to {_self.schema_name + '.' if _self.schema_name else ''}{_self.table_name}")
            return dg.MaterializeResult(metadata={
                "num_rows": len(df),
                "model": _self.model,
                "ollama_host": ollama_host,
                "response_column": _self.response_column,
                "table": f"{_self.schema_name + '.' if _self.schema_name else ''}{_self.table_name}",
            })

        return dg.Definitions(assets=[ollama_inference_asset])
