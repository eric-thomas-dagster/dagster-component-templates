"""GroqLLMComponent — native Groq LLM inference (ultra-fast, no LiteLLM).

Per-row LLM inference via Groq's OpenAI-compatible API. Groq is uniquely
fast (500+ tokens/sec on Llama-3.x / Mixtral / Gemma), has a generous
free tier, and is widely used for low-latency LLM workloads.

Drop-in shape parallel to openai_llm / anthropic_llm / gemini_llm —
single-vendor native component. For multi-vendor routing, see
litellm_inference_asset.
"""

import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    """Canonical partition factory shared across the registry."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dim type={t!r} requires 'start'")
        if t == "daily":   return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":  return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":  return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dim type='static' requires 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dim type='dynamic' requires a name")
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
        raise ValueError(f"partition_type={partition_type!r} requires partition_start")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_start + partition_values")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class GroqLLMComponent(Component, Model, Resolvable):
    """Per-row LLM inference via Groq's OpenAI-compatible API. Drop-in peer
    of openai_llm / anthropic_llm / gemini_llm — native single-vendor, no
    LiteLLM. Groq is uniquely fast (500+ tok/s on supported models).
    """

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    api_key_env_var: str = Field(default="GROQ_API_KEY", description="Env var holding the Groq API key.")
    base_url: str = Field(
        default="https://api.groq.com/openai/v1",
        description="Groq OpenAI-compatible base URL. Override only for proxies.",
    )

    text_model: str = Field(
        default="llama-3.3-70b-versatile",
        description=(
            "Groq model id. Common: llama-3.3-70b-versatile, llama-3.1-8b-instant, "
            "llama-3.3-70b-versatile, mixtral-8x7b-32768, gemma2-9b-it, deepseek-r1-distill-llama-70b."
        ),
    )

    system_prompt: Optional[str] = Field(default=None)
    user_prompt_template: Optional[str] = Field(
        default=None,
        description="Template with {column} placeholders. Mutually exclusive with input_column.",
    )
    input_column: Optional[str] = Field(
        default=None,
        description="Column whose value becomes the per-row prompt. Mutually exclusive with user_prompt_template.",
    )
    output_column: str = Field(default="groq_response")

    max_tokens: int = Field(default=1024)
    temperature: float = Field(default=0.0)
    top_p: Optional[float] = Field(default=None)
    response_format: Optional[str] = Field(
        default=None,
        description="Set to 'json_object' to force JSON-only responses (supported by most Groq models).",
    )

    rate_limit_delay: float = Field(default=0.0, description="Groq's free tier is generous — 0 is fine for moderate workloads.")
    max_retries: int = Field(default=3)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        if not self.input_column and not self.user_prompt_template:
            raise ValueError("GroqLLMComponent: set input_column or user_prompt_template.")
        if self.input_column and self.user_prompt_template:
            raise ValueError("GroqLLMComponent: set input_column OR user_prompt_template, not both.")

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        api_key_env = self.api_key_env_var
        base_url = self.base_url
        model = self.text_model
        system_prompt = self.system_prompt
        user_template = self.user_prompt_template
        input_column = self.input_column
        output_column = self.output_column
        max_tokens = self.max_tokens
        temperature = self.temperature
        top_p = self.top_p
        response_format = self.response_format
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries
        deps_keys = [AssetKey.from_user_string(k) for k in (self.deps or [])]

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Groq inference per row from {self.upstream_asset_key} ({model}).",
            group_name=self.group_name,
            kinds={"groq", "llm"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=deps_keys or None,
            partitions_def=partitions_def,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            try:
                from openai import OpenAI
            except ImportError:
                raise ImportError("Groq uses the OpenAI client. Install: pip install openai>=1.0")

            api_key = os.environ.get(api_key_env)
            if not api_key:
                raise ValueError(
                    f"{api_key_env} not set. Get a free key at https://console.groq.com/keys"
                )

            client = OpenAI(api_key=api_key, base_url=base_url)

            df = upstream.copy().reset_index(drop=True)
            if df.empty:
                df[output_column] = []
                return df

            if input_column and input_column not in df.columns:
                raise ValueError(
                    f"input_column={input_column!r} not in upstream columns: {list(df.columns)}"
                )

            responses: List[Optional[str]] = []
            errors: List[Optional[str]] = []
            success = 0

            for idx, row in df.iterrows():
                if input_column:
                    user_msg = str(row[input_column])
                else:
                    template = user_template or ""
                    try:
                        user_msg = template.format(**row.to_dict())
                    except KeyError as e:
                        raise ValueError(
                            f"user_prompt_template references missing column {e}; "
                            f"row columns: {list(df.columns)}"
                        )

                messages: List[Dict[str, Any]] = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": user_msg})

                kwargs: Dict[str, Any] = {
                    "model": model,
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                }
                if top_p is not None:
                    kwargs["top_p"] = top_p
                if response_format == "json_object":
                    kwargs["response_format"] = {"type": "json_object"}

                attempt = 0
                last_err: Optional[Exception] = None
                resp = None
                while attempt <= max_retries:
                    try:
                        resp = client.chat.completions.create(**kwargs)
                        last_err = None
                        break
                    except Exception as e:
                        err_str = str(e)
                        is_not_found = "404" in err_str or "model_not_found" in err_str.lower()
                        last_err = e
                        attempt += 1
                        if is_not_found or attempt > max_retries:
                            break
                        wait = (2 ** attempt) * 0.5
                        context.log.warning(
                            f"row {idx}: groq call failed ({e!r}), retrying in {wait}s "
                            f"(attempt {attempt}/{max_retries})"
                        )
                        time.sleep(wait)

                if last_err is not None or resp is None:
                    err_str = str(last_err) if last_err else "no response"
                    responses.append(None)
                    errors.append(err_str)
                    if "404" in err_str or "model_not_found" in err_str.lower():
                        context.log.error(
                            f"row {idx}: model {model!r} not found on Groq. "
                            f"Common ids: llama-3.3-70b-versatile, llama-3.1-8b-instant, "
                            f"mixtral-8x7b-32768, gemma2-9b-it, deepseek-r1-distill-llama-70b. "
                            f"Full list: https://console.groq.com/docs/models"
                        )
                    elif "401" in err_str or "invalid_api_key" in err_str.lower():
                        context.log.error(
                            f"row {idx}: invalid Groq API key. Get a free key at "
                            f"https://console.groq.com/keys"
                        )
                    elif "429" in err_str.lower() or "rate" in err_str.lower():
                        context.log.error(
                            f"row {idx}: Groq rate limit hit. Free tier has per-model limits. "
                            f"Set rate_limit_delay > 0 to throttle, or upgrade at https://console.groq.com/settings/billing"
                        )
                    else:
                        context.log.error(f"row {idx}: groq call ultimately failed: {last_err}")
                    if rate_limit_delay > 0:
                        time.sleep(rate_limit_delay)
                    continue

                choice = resp.choices[0] if resp.choices else None
                text_out = (choice.message.content if choice and choice.message else None) or None
                responses.append(text_out)
                errors.append(None)
                if text_out is not None:
                    success += 1
                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)

            df[output_column] = responses
            if any(errors):
                df[f"{output_column}_error"] = errors

            preview_md = df.head(5).to_markdown(index=False) or ""
            context.add_output_metadata({
                "rows":      MetadataValue.int(len(df)),
                "responses": MetadataValue.int(success),
                "model":     MetadataValue.text(model),
                "provider":  MetadataValue.text("Groq"),
                "preview":   MetadataValue.md(preview_md),
            })
            return df

        return Definitions(assets=[_asset])
