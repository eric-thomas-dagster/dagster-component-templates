"""OpenRouterLLMComponent — OpenRouter LLM router (one key, 100+ models).

OpenRouter is a hosted multi-vendor LLM gateway — one API key gives you
access to 100+ models (Anthropic, OpenAI, Google, Meta/Llama, Mistral,
DeepSeek, xAI, Cohere, ...) with automatic fallback, caching, and
unified billing.

Drop-in shape parallel to openai_llm / anthropic_llm / gemini_llm /
groq_llm — single integration, but multi-vendor backend. For pure
local-routing without the gateway, see litellm_inference_asset.
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
    """Canonical partition factory."""
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


class OpenRouterLLMComponent(Component, Model, Resolvable):
    """Per-row LLM inference via OpenRouter — one key, 100+ models from
    Anthropic / OpenAI / Google / Meta / Mistral / DeepSeek / xAI / Cohere.
    Drop-in peer of openai_llm / anthropic_llm / gemini_llm / groq_llm.
    """

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    api_key_env_var: str = Field(default="OPENROUTER_API_KEY")
    base_url: str = Field(default="https://openrouter.ai/api/v1")

    text_model: str = Field(
        default="anthropic/claude-haiku-4-5",
        description=(
            "OpenRouter model id ('<provider>/<model>'). Examples: "
            "anthropic/claude-haiku-4-5, openai/gpt-4o-mini, "
            "google/gemini-2.5-flash, meta-llama/llama-3.3-70b-instruct, "
            "deepseek/deepseek-chat-v3, mistralai/mistral-large-latest, "
            "x-ai/grok-2-1212. Full list: https://openrouter.ai/models"
        ),
    )

    # Optional OpenRouter-specific routing hints
    provider_preferences: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional `provider` object passed to OpenRouter for routing "
            "control: e.g. {'order': ['anthropic'], 'allow_fallbacks': true}. "
            "See https://openrouter.ai/docs/provider-routing for the full schema."
        ),
    )
    transforms: Optional[List[str]] = Field(
        default=None,
        description="Optional list of OpenRouter transforms (e.g. ['middle-out']).",
    )

    # App attribution (shown on the OpenRouter leaderboard if you set it)
    referer: Optional[str] = Field(
        default=None,
        description="HTTP-Referer header — your app's URL. Optional, shown on https://openrouter.ai/rankings.",
    )
    x_title: Optional[str] = Field(
        default=None,
        description="X-Title header — your app's name. Optional, shown on the leaderboard.",
    )

    system_prompt: Optional[str] = Field(default=None)
    user_prompt_template: Optional[str] = Field(default=None)
    input_column: Optional[str] = Field(default=None)
    output_column: str = Field(default="openrouter_response")

    max_tokens: int = Field(default=1024)
    temperature: float = Field(default=0.0)
    top_p: Optional[float] = Field(default=None)
    response_format: Optional[str] = Field(default=None, description="'json_object' for JSON-only output (supported by most underlying models).")

    rate_limit_delay: float = Field(default=0.0)
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
            raise ValueError("OpenRouterLLMComponent: set input_column or user_prompt_template.")
        if self.input_column and self.user_prompt_template:
            raise ValueError("OpenRouterLLMComponent: set input_column OR user_prompt_template, not both.")

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        api_key_env = self.api_key_env_var
        base_url = self.base_url
        model = self.text_model
        provider_preferences = self.provider_preferences
        transforms = list(self.transforms or [])
        referer = self.referer
        x_title = self.x_title
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
            name=asset_name,
            description=self.description or f"OpenRouter inference per row from {self.upstream_asset_key} ({model}).",
            group_name=self.group_name,
            kinds={"openrouter", "llm"},
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
                raise ImportError("OpenRouter uses the OpenAI client. Install: pip install openai>=1.0")

            api_key = os.environ.get(api_key_env)
            if not api_key:
                raise ValueError(
                    f"{api_key_env} not set. Get a key at https://openrouter.ai/keys"
                )

            extra_headers: Dict[str, str] = {}
            if referer:
                extra_headers["HTTP-Referer"] = referer
            if x_title:
                extra_headers["X-Title"] = x_title

            client = OpenAI(api_key=api_key, base_url=base_url, default_headers=extra_headers or None)

            df = upstream.copy().reset_index(drop=True)
            if df.empty:
                df[output_column] = []
                return df

            if input_column and input_column not in df.columns:
                raise ValueError(f"input_column={input_column!r} not in upstream: {list(df.columns)}")

            responses: List[Optional[str]] = []
            errors: List[Optional[str]] = []
            success = 0

            for idx, row in df.iterrows():
                user_msg = str(row[input_column]) if input_column else (user_template or "").format(**row.to_dict())

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

                extra_body: Dict[str, Any] = {}
                if provider_preferences:
                    extra_body["provider"] = provider_preferences
                if transforms:
                    extra_body["transforms"] = list(transforms)
                if extra_body:
                    kwargs["extra_body"] = extra_body

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
                        is_not_found = "404" in err_str
                        last_err = e
                        attempt += 1
                        if is_not_found or attempt > max_retries:
                            break
                        wait = (2 ** attempt) * 0.5
                        context.log.warning(
                            f"row {idx}: openrouter call failed ({e!r}), retrying in {wait}s"
                        )
                        time.sleep(wait)

                if last_err is not None or resp is None:
                    err_str = str(last_err) if last_err else "no response"
                    responses.append(None)
                    errors.append(err_str)
                    if "404" in err_str:
                        context.log.error(
                            f"row {idx}: model {model!r} not found on OpenRouter. "
                            f"Browse models at https://openrouter.ai/models"
                        )
                    elif "401" in err_str:
                        context.log.error(
                            f"row {idx}: invalid OpenRouter API key. Get one at https://openrouter.ai/keys"
                        )
                    elif "402" in err_str or "credit" in err_str.lower():
                        context.log.error(
                            f"row {idx}: OpenRouter credit exhausted. Top up at https://openrouter.ai/settings/credits"
                        )
                    else:
                        context.log.error(f"row {idx}: openrouter call ultimately failed: {last_err}")
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
                "provider":  MetadataValue.text("OpenRouter"),
                "preview":   MetadataValue.md(preview_md),
            })
            return df

        return Definitions(assets=[_asset])
