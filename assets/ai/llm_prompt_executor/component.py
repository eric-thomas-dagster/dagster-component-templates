"""LLM Prompt Executor Asset Component.

Execute prompts against LLM APIs (OpenAI, Anthropic, etc.) row-by-row over an upstream
DataFrame, and return the enriched DataFrame with response column added.
Supports multiple LLM providers, temperature control, and various response formats.
"""

import json
import os
from typing import Any, Dict, List, Optional
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
    MetadataValue,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
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

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 25 "
            "rows or a sample) for builder UIs."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata. For long DataFrames "
            "(>10x preview_rows), a random sample is used; otherwise head()."
        ),
    )

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
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

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "llm_prompt_executor"  # component directory name
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


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, 
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
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
                # Build prompt for this row (alias input_column → 'input' for templates)
                if user_prompt_template:
                    row_data = row.to_dict()
                    row_data["input"] = row[input_column]
                    prompt = user_prompt_template.format(**row_data)
                else:
                    prompt = str(row[input_column])

                response_text = None

                # Execute based on provider
                if provider == "openai":
                    try:
                        import openai
                        client = _make_openai_client(expanded_api_key)

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


            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)

            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[llm_prompt_asset])


        return Definitions(assets=[llm_prompt_asset], asset_checks=list(_schema_checks))



def _make_openai_client(api_key):
    """Build an OpenAI or AzureOpenAI client based on env vars.

    Set OPENAI_AZURE_ENDPOINT to route through Azure OpenAI Service. Optional:
    OPENAI_AZURE_API_VERSION (default 2024-10-21). For Entra OAuth, set
    OPENAI_AZURE_USE_ENTRA=1 and the standard AZURE_TENANT_ID/CLIENT_ID/
    CLIENT_SECRET env vars (or rely on managed identity in Azure compute).
    """
    import openai as _openai
    azure_endpoint = os.environ.get("OPENAI_AZURE_ENDPOINT")
    if not azure_endpoint:
        return _openai.OpenAI(api_key=api_key)
    api_version = os.environ.get("OPENAI_AZURE_API_VERSION", "2024-10-21")
    if os.environ.get("OPENAI_AZURE_USE_ENTRA") == "1":
        from azure.identity import DefaultAzureCredential, get_bearer_token_provider
        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(),
            "https://cognitiveservices.azure.com/.default",
        )
        return _openai.AzureOpenAI(
            azure_ad_token_provider=token_provider,
            azure_endpoint=azure_endpoint,
            api_version=api_version,
        )
    return _openai.AzureOpenAI(
        api_key=api_key,
        azure_endpoint=azure_endpoint,
        api_version=api_version,
    )
