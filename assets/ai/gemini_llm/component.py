"""GeminiLLMComponent — native (no-LiteLLM) Google Gemini text LLM.

Per-row text generation with Gemini models (gemini-2.5-flash, gemini-2.5-pro,
gemini-2.0-flash-lite, etc.) via the `google-genai` SDK directly.

Sister of `openai_llm` / `anthropic_llm`. Drop-in field shape so you can
swap providers by changing the `type:` line and the api_key env var.

For multi-vendor / model-switching workflows, see `litellm_inference_asset`.
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
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Canonical partition factory shared across the registry."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

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
            f"partition_type={partition_type!r} requires partition_start (ISO date)."
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
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values.")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class GeminiLLMComponent(Component, Model, Resolvable):
    """Per-row text generation with Google's Gemini text models, native SDK.

    Drop-in peer of `openai_llm` and `anthropic_llm`. Field shape matches
    so you can swap providers by changing the `type:` and api_key env var.

    For multi-vendor / model-switching workflows, see `litellm_inference_asset`.
    """

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(
        description="Upstream DataFrame asset key — one row produces one Gemini call."
    )

    api_key_env_var: str = Field(
        default="GEMINI_API_KEY",
        description="Env var holding the Google AI Studio API key. Falls back to GOOGLE_API_KEY.",
    )

    text_model: str = Field(
        default="gemini-2.5-flash",
        description=(
            "Gemini text model. Common ids: gemini-2.5-flash (fast, cheap), "
            "gemini-2.5-pro (most capable), gemini-2.0-flash-lite (cheapest), "
            "gemini-flash-latest, gemini-pro-latest. "
            "Run client.models.list() against your key for the live set."
        ),
    )

    system_prompt: Optional[str] = Field(
        default=None,
        description="System instruction sent on every call (Gemini 'system_instruction').",
    )
    user_prompt_template: Optional[str] = Field(
        default=None,
        description="Static template with {column_name} placeholders. Mutually exclusive with input_column.",
    )
    input_column: Optional[str] = Field(
        default=None,
        description="Column whose value becomes the per-row prompt. Mutually exclusive with user_prompt_template.",
    )
    output_column: str = Field(
        default="gemini_response",
        description="Column for the model's text response.",
    )

    max_output_tokens: int = Field(default=1024, description="Max output tokens per response.")
    temperature: float = Field(default=0.0, description="Temperature (0.0–2.0). 0 = deterministic.")
    top_p: Optional[float] = Field(default=None, description="Optional nucleus sampling parameter.")
    top_k: Optional[int] = Field(default=None, description="Optional top-k sampling parameter.")
    thinking_budget: Optional[int] = Field(
        default=None,
        description=(
            "Tokens reserved for internal reasoning on Gemini 2.5+ thinking models. "
            "max_output_tokens is the budget for thinking + visible output combined; "
            "if thinking burns most of it, the visible response gets truncated. Set to 0 "
            "to disable thinking entirely (recommended for short, structured outputs "
            "like one-sentence summaries or labels). Leave unset to let the model decide."
        ),
    )

    rate_limit_delay: float = Field(default=0.2, description="Seconds to sleep between API calls.")
    max_retries: int = Field(default=3, description="Retries on transient errors (5xx, 429).")

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
            raise ValueError(
                "GeminiLLMComponent: set either input_column or user_prompt_template."
            )
        if self.input_column and self.user_prompt_template:
            raise ValueError(
                "GeminiLLMComponent: set input_column OR user_prompt_template, not both."
            )

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )

        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        deps_keys = [AssetKey.from_user_string(k) for k in (self.deps or [])]

        api_key_env_var_local = self.api_key_env_var
        text_model_local = self.text_model
        system_prompt_local = self.system_prompt
        user_prompt_template_local = self.user_prompt_template
        input_column_local = self.input_column
        output_column_local = self.output_column
        max_output_tokens_local = self.max_output_tokens
        temperature_local = self.temperature
        top_p_local = self.top_p
        top_k_local = self.top_k
        rate_limit_delay_local = self.rate_limit_delay
        max_retries_local = self.max_retries
        thinking_budget_local = self.thinking_budget

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            description=self.description or f"Gemini text generation per row from {self.upstream_asset_key}.",
            group_name=self.group_name,
            kinds={"gemini", "llm"},
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
                from google import genai
                from google.genai import types as genai_types
            except ImportError as e:
                raise ImportError("google-genai required: pip install google-genai>=0.3.0") from e

            api_key = os.environ.get(api_key_env_var_local) or os.environ.get("GOOGLE_API_KEY")
            if not api_key:
                raise ValueError(
                    f"{api_key_env_var_local} not set in environment "
                    "(also tried GOOGLE_API_KEY as fallback)."
                )

            client = genai.Client(api_key=api_key)

            df = upstream.copy().reset_index(drop=True)
            if df.empty:
                df[output_column_local] = []
                return df

            if input_column_local and input_column_local not in df.columns:
                raise ValueError(
                    f"input_column={input_column_local!r} not in upstream columns: {list(df.columns)}"
                )

            responses: List[Optional[str]] = []
            errors: List[Optional[str]] = []
            success_count = 0

            for idx, row in df.iterrows():
                if input_column_local:
                    prompt = str(row[input_column_local])
                else:
                    template = user_prompt_template_local or ""
                    try:
                        prompt = template.format(**row.to_dict())
                    except KeyError as e:
                        raise ValueError(
                            f"user_prompt_template references missing column {e}; "
                            f"row columns: {list(df.columns)}"
                        )

                config_kwargs: Dict[str, Any] = {
                    "temperature": temperature_local,
                    "max_output_tokens": max_output_tokens_local,
                }
                if top_p_local is not None:
                    config_kwargs["top_p"] = top_p_local
                if top_k_local is not None:
                    config_kwargs["top_k"] = top_k_local
                if system_prompt_local:
                    config_kwargs["system_instruction"] = system_prompt_local
                if thinking_budget_local is not None:
                    # ThinkingConfig requires the typed object on Gemini 2.5+; it
                    # silently is a no-op on older models that don't support thinking.
                    try:
                        config_kwargs["thinking_config"] = genai_types.ThinkingConfig(
                            thinking_budget=thinking_budget_local,
                        )
                    except AttributeError:
                        # Older google-genai versions without ThinkingConfig — fall back
                        # to a plain dict, which the SDK accepts.
                        config_kwargs["thinking_config"] = {
                            "thinking_budget": thinking_budget_local,
                        }

                attempt = 0
                last_err: Optional[Exception] = None
                resp = None
                while attempt <= max_retries_local:
                    try:
                        resp = client.models.generate_content(
                            model=text_model_local,
                            contents=prompt,
                            config=genai_types.GenerateContentConfig(**config_kwargs),
                        )
                        last_err = None
                        break
                    except Exception as e:
                        err_str = str(e)
                        is_not_found = "404" in err_str or "NOT_FOUND" in err_str
                        last_err = e
                        attempt += 1
                        if is_not_found or attempt > max_retries_local:
                            break
                        wait = (2 ** attempt) * 0.5
                        context.log.warning(
                            f"row {idx}: gemini call failed ({e!r}), retrying in {wait}s "
                            f"(attempt {attempt}/{max_retries_local})"
                        )
                        time.sleep(wait)

                if last_err is not None or resp is None:
                    err_str = str(last_err) if last_err else "no response"
                    responses.append(None)
                    errors.append(err_str)
                    if "404" in err_str or "NOT_FOUND" in err_str:
                        context.log.error(
                            f"row {idx}: model {text_model_local!r} returned 404. "
                            f"Set `text_model:` to a current id (e.g. gemini-2.5-flash, "
                            f"gemini-2.5-pro, gemini-2.0-flash-lite, gemini-flash-latest)."
                        )
                    elif "429" in err_str or "RESOURCE_EXHAUSTED" in err_str or "quota" in err_str.lower():
                        context.log.error(
                            f"row {idx}: quota exhausted. Check rate limits at "
                            f"https://ai.dev/rate-limit or enable billing at "
                            f"console.cloud.google.com."
                        )
                    else:
                        context.log.error(f"row {idx}: gemini call ultimately failed: {last_err}")
                    time.sleep(rate_limit_delay_local)
                    continue

                # Pull text out of the response. Most responses have .text directly.
                text_out = getattr(resp, "text", None)
                if text_out is None:
                    pieces = []
                    for cand in (resp.candidates or []):
                        for part in (getattr(cand.content, "parts", None) or []):
                            t = getattr(part, "text", None)
                            if t:
                                pieces.append(t)
                    text_out = "".join(pieces) if pieces else None

                if text_out is None:
                    responses.append(None)
                    errors.append("no text in response (possibly safety-blocked)")
                    context.log.warning(f"row {idx}: response had no text")
                    time.sleep(rate_limit_delay_local)
                    continue

                responses.append(text_out)
                errors.append(None)
                success_count += 1
                time.sleep(rate_limit_delay_local)

            df[output_column_local] = responses
            if any(errors):
                df[f"{output_column_local}_error"] = errors

            n_404 = sum(1 for e in errors if e and ("404" in e or "NOT_FOUND" in e))
            n_429 = sum(1 for e in errors if e and ("429" in e or "RESOURCE_EXHAUSTED" in e or "quota" in e.lower()))

            preview_md = df.head(5).to_markdown(index=False) or ""
            md_metadata: Dict[str, Any] = {
                "rows":           MetadataValue.int(len(df)),
                "responses":      MetadataValue.int(success_count),
                "model":          MetadataValue.text(text_model_local),
                "preview":        MetadataValue.md(preview_md),
            }
            if n_404:
                md_metadata["model_not_found_count"] = MetadataValue.int(n_404)
                md_metadata["hint"] = MetadataValue.text(
                    f"Model {text_model_local!r} returned 404. Set `text_model:` to a current id."
                )
            if n_429:
                md_metadata["quota_exhausted_count"] = MetadataValue.int(n_429)
                md_metadata["hint"] = MetadataValue.text(
                    "Quota exhausted — check https://ai.dev/rate-limit or enable billing."
                )
            context.add_output_metadata(md_metadata)
            return df

        return Definitions(assets=[_asset])
