"""Ollama Inference Asset Component.

Reads rows from an upstream asset DataFrame, runs each row through
a locally-running Ollama model, and returns the enriched DataFrame.

Ollama runs open-source models locally: Llama 3, Mistral, Gemma, Phi-3, etc.
No API key required — just a running Ollama server.
"""
from typing import Optional
import pandas as pd
import dagster as dg
from dagster import AssetExecutionContext, AssetIn, AssetKey
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
    """Read rows from an upstream asset DataFrame, enrich via local Ollama, return enriched DataFrame.

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
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
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
            description=_self.description or f"Ollama ({_self.model}) inference: {_self.prompt_template[:50]}...",
            group_name=_self.group_name,
            kinds={"ai", "ollama"},
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(_self.upstream_asset_key))},
        )
        def ollama_inference_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            import os
            import requests

            ollama_host = (
                os.environ.get(_self.ollama_host_env_var, "http://localhost:11434")
                if _self.ollama_host_env_var
                else "http://localhost:11434"
            )

            df = upstream.copy()

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
            context.log.info(f"Completed {len(df)} Ollama calls")
            return df

        return dg.Definitions(assets=[ollama_inference_asset])
