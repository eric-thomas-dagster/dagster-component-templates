"""LiteLLM Inference Asset Component.

Reads rows from an upstream asset DataFrame, runs each row through
a configurable LLM prompt via LiteLLM, and returns the enriched DataFrame.

LiteLLM supports 100+ providers: OpenAI, Anthropic, Azure, Bedrock, Gemini,
Mistral, Cohere, and more — all through a unified API.
"""
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import dagster as dg
from dagster import AssetExecutionContext, AssetIn, AssetKey
from pydantic import ConfigDict, Field


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
    model_id: str = Field(
        alias="model",
        description="LiteLLM model string (e.g. gpt-4o, claude-3-5-sonnet, gemini/gemini-pro)")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var with provider API key")
    api_base_env_var: Optional[str] = Field(default=None, description="Env var with custom API base URL (for proxies)")
    temperature: float = Field(default=0.0, description="Sampling temperature")
    max_tokens: int = Field(default=1024, description="Max tokens per completion")
    timeout_seconds: int = Field(default=60, description="Request timeout in seconds")
    max_retries: int = Field(default=3, description="Max retries on failure")

    def complete(self, messages: list[dict]) -> str:
        """Run a chat completion and return the response content."""
        import litellm

        kwargs: dict = {
            "model": self.model_id,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "timeout": self.timeout_seconds,
            "num_retries": self.max_retries,
        }
        if self.api_key_env_var:
            kwargs["api_key"] = dg.EnvVar(self.api_key_env_var).get_value()
        if self.api_base_env_var:
            kwargs["api_base"] = dg.EnvVar(self.api_base_env_var).get_value()

        response = litellm.completion(**kwargs)
        return response.choices[0].message.content


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

    model_config = ConfigDict(populate_by_name=True)
    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame (e.g. 'raw_orders' or 'schema/table')")
    model_id: Optional[str] = Field(
        alias="model",
        default=None, description="LiteLLM model string — overrides resource if set",
    )
    api_key_env_var: Optional[str] = Field(default=None, description="Env var with provider API key — overrides resource if set")
    api_base_env_var: Optional[str] = Field(default=None, description="Env var with custom API base URL")
    litellm_resource_key: Optional[str] = Field(default=None, description="Key of a LiteLLMResource in resources dict")
    prompt_template: str = Field(description="Python format string using column names: 'Classify: {text_column}'")
    system_prompt: Optional[str] = Field(default=None, description="System prompt for chat completions")
    response_column: Union[str, int] = Field(default="llm_response", description="Column name to store LLM responses")
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
    description: Optional[str] = Field(default=None)

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



    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self


        # Build partition definition (auto-generated; supports daily, weekly, monthly,

        # hourly partitions out of the box).
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @dg.asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            key=dg.AssetKey.from_user_string(_self.asset_name),
            description=_self.description or f"LiteLLM inference: {_self.prompt_template[:60]}...",
            group_name=_self.group_name,
            kinds={"ai", "llm"},
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(_self.upstream_asset_key))},
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def litellm_inference_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
                litellm_kwargs["api_key"] = dg.EnvVar(_self.api_key_env_var).get_value()
            if _self.api_base_env_var:
                litellm_kwargs["api_base"] = dg.EnvVar(_self.api_base_env_var).get_value()

            model = _self.model_id
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
            context.log.info(f"Completed {len(df)} LLM calls")
            return df

        return dg.Definitions(assets=[litellm_inference_asset])
