"""OpenAI LLM Component.

Process text data using OpenAI's GPT models (GPT-4, GPT-3.5-turbo, GPT-4-turbo).
Supports batch processing of DataFrame columns, streaming, function calling, token counting,
response caching, and cost tracking.
"""

import os
import json
import time
import hashlib
from typing import Any, Dict, List, Optional, Union, Literal
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
from pydantic import ConfigDict, Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
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
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


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

    model_config = ConfigDict(populate_by_name=True)
    asset_name: str = Field(
        description="Name of the asset that will hold the enriched data"
    )

    api_key: str = Field(
        description="OpenAI API key. Use ${OPENAI_API_KEY} for environment variables."
    )

    model_id: str = Field(
        alias="model",
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

    input_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column name containing input text (for batch processing)"
    )

    output_column: Union[str, int] = Field(
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
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
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

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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
        default=True,
        description="Include sample data preview in metadata"
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )

    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame with text to process")

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' materializes an entire backfill "
            'range in ONE run: the component processes each partition key in the range in turn (filtering '
            'upstream rows by partition_date_column / partition_static_column per key) and concatenates the '
            "results into one DataFrame. 'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's default (one run per partition, "
            'uncapped). Ignored when no partitioning is configured. COST: total paid-API/LLM calls are the same '
            'as running the partitions separately, but note that when no '
            'partition_date_column/partition_static_column matches, every partition key processes ALL upstream '
            "rows, so a range multiplies calls by the number of partitions. 'single_run' requires a "
            'partition-range-aware IO manager (e.g. a DB-table IO manager with partition_expr set); the default '
            'filesystem IO manager cannot persist one output spanning multiple partitions and will raise a clear '
            'error at materialization time. Per-partition output metadata is overwritten by the last partition in '
            'the range.'
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        api_key = self.api_key
        model = self.model_id
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
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key

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

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "openai_llm"  # component directory name
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
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

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



        from dagster import BackfillPolicy as _BackfillPolicy
        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == 'single_run':
            _backfill_policy_obj = _BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == 'multi_run':
            _backfill_policy_obj = _BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        _asset_deco = asset(retry_policy=_retry_policy, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _openai_llm_asset_impl(context: AssetExecutionContext, upstream: pd.DataFrame, _pk=None) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if _pk is not None:
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                    upstream = upstream[_col_dates == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
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
                client = _make_openai_client(expanded_api_key)
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

            input_df = upstream
            context.log.info(f"Received DataFrame: {len(input_df)} rows, {len(input_df.columns)} columns")

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
                    except (OSError, ValueError):
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

            if include_preview and len(result_df) > 0:
                context.add_output_metadata({
                        **metadata,
                        "preview": MetadataValue.md(result_df.head(10).to_markdown())
                    })
                return result_df
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
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col.name: [col.name] for col in _col_schema.columns
                        if col.name in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return result_df

        @_asset_deco
        def openai_llm_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            _keys = list(context.partition_keys) if (context.has_partition_key or context.has_partition_key_range) else [None]
            if len(_keys) == 1:
                return _openai_llm_asset_impl(context, upstream, _keys[0])
            context.log.info(f'Processing {len(_keys)} partition keys in one run (backfill range)')
            return pd.concat([_openai_llm_asset_impl(context, upstream, _k) for _k in _keys], ignore_index=True)

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[openai_llm_asset])


        return Definitions(assets=[openai_llm_asset], asset_checks=list(_schema_checks))



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


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
