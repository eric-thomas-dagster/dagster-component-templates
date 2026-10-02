"""PerplexityLLMComponent — native Perplexity (Sonar) web-grounded LLM inference.

Per-row LLM inference via Perplexity's OpenAI-compatible Chat Completions API
(POST https://api.perplexity.ai/chat/completions, env var PERPLEXITY_API_KEY).
Perplexity's wire format follows the standard `choices[].message.content` /
`usage` shape, so the stock OpenAI client works unchanged with just a base
URL + API key swap.

Perplexity's actual differentiator vs every other LLM provider in this repo
is its built-in web-grounded search (the "Sonar" model family): responses
come back with real-time web citations. Unlike a bare OpenAI-wire proxy,
this component surfaces those citations -- both as an extra output column
(a list of source URLs per row) and in materialization metadata -- since
that grounding is the whole reason to reach for Perplexity over
Mistral/Groq/OpenAI here.

Drop-in shape parallel to openai_llm / anthropic_llm / gemini_llm / groq_llm /
mistral_llm -- single-vendor native component. For multi-vendor routing, see
litellm_inference_asset.
"""

import os
import time
from typing import Any, Dict, List, Optional, Union

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


def _call_chat_completion(client: Any, **kwargs: Any) -> Any:
    """The one external network call, isolated into its own module-level
    function so tests can monkeypatch `perplexity_llm_component._call_chat_completion`
    instead of hitting the real (paid) Perplexity API."""
    return client.chat.completions.create(**kwargs)


def _extract_citations(resp: Any, choice: Any) -> List[str]:
    """Perplexity returns web citations as a top-level `citations` field on
    the chat completion response (a list of source URL strings) -- confirmed
    against Perplexity's own API reference (docs.perplexity.ai/api-reference/
    chat-completions-post), which is not part of the standard OpenAI wire
    schema. The openai-python client's response models allow unrecognized
    fields through, so `resp.citations` round-trips even though OpenAI's own
    ChatCompletion schema has no such field.

    Some third-party write-ups instead show citations nested under
    `choice.message.citations`; we check both locations defensively so this
    keeps working regardless of exactly which shape a given account/model
    returns, without ever raising on a missing field."""
    citations = getattr(resp, "citations", None)
    if not citations and choice is not None:
        msg = getattr(choice, "message", None)
        citations = getattr(msg, "citations", None) if msg is not None else None
    if not citations:
        return []
    return list(citations)


def _extract_search_results(resp: Any) -> List[Dict[str, Any]]:
    """Perplexity's richer companion to `citations`: a top-level
    `search_results` field with title/url/date/snippet per source."""
    results = getattr(resp, "search_results", None)
    if not results:
        return []
    out = []
    for r in results:
        if isinstance(r, dict):
            out.append(r)
        else:
            # pydantic-model-like object (openai client wraps unknown
            # fields in its own BaseModel); fall back to a dict via
            # whichever accessor is available.
            to_dict = getattr(r, "model_dump", None) or getattr(r, "dict", None)
            out.append(to_dict() if callable(to_dict) else {"url": getattr(r, "url", None)})
    return out


class PerplexityLLMComponent(Component, Model, Resolvable):
    """Per-row LLM inference via Perplexity's OpenAI-compatible Sonar API.
    Drop-in peer of openai_llm / anthropic_llm / gemini_llm / groq_llm /
    mistral_llm -- native single-vendor, no LiteLLM. Unlike those peers,
    Perplexity's models are web-grounded: every response can carry real-time
    citations, which this component surfaces as its own output column
    (and in materialization metadata) rather than discarding them.
    """

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    api_key_env_var: str = Field(default="PERPLEXITY_API_KEY", description="Env var holding the Perplexity API key.")
    base_url: str = Field(
        default="https://api.perplexity.ai",
        description="Perplexity OpenAI-compatible base URL. Override only for proxies.",
    )

    text_model: str = Field(
        default="sonar",
        description=(
            "Perplexity model id. Common: sonar (fast, web-grounded), sonar-pro "
            "(deeper search, more citations), sonar-reasoning, sonar-reasoning-pro "
            "(chain-of-thought + search), sonar-deep-research (exhaustive multi-step research)."
        ),
    )

    system_prompt: Optional[str] = Field(default=None)
    user_prompt_template: Optional[str] = Field(
        default=None,
        description="Template with {column} placeholders. Mutually exclusive with input_column.",
    )
    input_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column whose value becomes the per-row prompt. Mutually exclusive with user_prompt_template.",
    )
    output_column: Union[str, int] = Field(default="perplexity_response")
    citations_column: Union[str, int] = Field(
        default="perplexity_citations",
        description="Column holding the list of web source URLs Perplexity cited for that row's answer.",
    )
    include_search_results: bool = Field(
        default=False,
        description=(
            "When true, also add a column (named '<citations_column>_detail') with the fuller "
            "search_results payload (title/url/date/snippet per source) instead of just bare URLs."
        ),
    )

    max_tokens: int = Field(default=1024)
    temperature: float = Field(default=0.0)
    top_p: Optional[float] = Field(default=None)
    response_format: Optional[str] = Field(
        default=None,
        description="Set to 'json_object' to force JSON-only responses.",
    )

    search_domain_filter: Optional[List[str]] = Field(
        default=None,
        description=(
            "Limit/exclude web sources by domain (max 20). Plain entries allowlist a domain; "
            "prefix with '-' to denylist it, e.g. ['nytimes.com', '-reddit.com']."
        ),
    )
    search_recency_filter: Optional[str] = Field(
        default=None,
        description="Restrict search results to a recency window: 'day', 'week', 'month', or 'year'.",
    )
    return_related_questions: Optional[bool] = Field(
        default=None,
        description="Ask Perplexity to also return suggested follow-up questions (surfaced in metadata, not per row).",
    )

    rate_limit_delay: float = Field(default=0.0, description="Seconds to sleep between rows. Raise this if you hit Perplexity's rate limits.")
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
            raise ValueError("PerplexityLLMComponent: set input_column or user_prompt_template.")
        if self.input_column and self.user_prompt_template:
            raise ValueError("PerplexityLLMComponent: set input_column OR user_prompt_template, not both.")

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
        citations_column = self.citations_column
        include_search_results = self.include_search_results
        max_tokens = self.max_tokens
        temperature = self.temperature
        top_p = self.top_p
        response_format = self.response_format
        search_domain_filter = self.search_domain_filter
        search_recency_filter = self.search_recency_filter
        return_related_questions = self.return_related_questions
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries
        deps_keys = [AssetKey.from_user_string(k) for k in (self.deps or [])]

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Perplexity web-grounded inference per row from {self.upstream_asset_key} ({model}).",
            group_name=self.group_name,
            kinds={"perplexity", "llm"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=deps_keys or None,
            partitions_def=partitions_def,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            try:
                from openai import OpenAI
            except ImportError:
                raise ImportError("Perplexity uses the OpenAI client. Install: pip install openai>=1.0")

            api_key = os.environ.get(api_key_env)
            if not api_key:
                raise ValueError(
                    f"{api_key_env} not set. Get a key at https://www.perplexity.ai/account/api/keys"
                )

            client = OpenAI(api_key=api_key, base_url=base_url)

            df = upstream.copy().reset_index(drop=True)
            if df.empty:
                df[output_column] = []
                df[citations_column] = []
                return df

            if input_column and input_column not in df.columns:
                raise ValueError(
                    f"input_column={input_column!r} not in upstream columns: {list(df.columns)}"
                )

            responses: List[Optional[str]] = []
            errors: List[Optional[str]] = []
            all_citations: List[List[str]] = []
            all_search_results: List[List[Dict[str, Any]]] = []
            related_questions_seen: List[str] = []
            success = 0
            total_citations = 0

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
                if search_domain_filter:
                    kwargs["search_domain_filter"] = search_domain_filter
                if search_recency_filter:
                    kwargs["search_recency_filter"] = search_recency_filter
                if return_related_questions is not None:
                    kwargs["return_related_questions"] = return_related_questions

                attempt = 0
                last_err: Optional[Exception] = None
                resp = None
                while attempt <= max_retries:
                    try:
                        resp = _call_chat_completion(client, **kwargs)
                        last_err = None
                        break
                    except Exception as e:
                        err_str = str(e)
                        is_not_found = "404" in err_str or "model_not_found" in err_str.lower() or "invalid_model" in err_str.lower()
                        last_err = e
                        attempt += 1
                        if is_not_found or attempt > max_retries:
                            break
                        wait = (2 ** attempt) * 0.5
                        context.log.warning(
                            f"row {idx}: perplexity call failed ({e!r}), retrying in {wait}s "
                            f"(attempt {attempt}/{max_retries})"
                        )
                        time.sleep(wait)

                if last_err is not None or resp is None:
                    err_str = str(last_err) if last_err else "no response"
                    responses.append(None)
                    errors.append(err_str)
                    all_citations.append([])
                    all_search_results.append([])
                    if "404" in err_str or "model_not_found" in err_str.lower() or "invalid_model" in err_str.lower():
                        context.log.error(
                            f"row {idx}: model {model!r} not found on Perplexity. "
                            f"Common ids: sonar, sonar-pro, sonar-reasoning, sonar-reasoning-pro, "
                            f"sonar-deep-research. Full list: https://docs.perplexity.ai/models/model-cards"
                        )
                    elif "401" in err_str or "invalid_api_key" in err_str.lower() or "unauthorized" in err_str.lower():
                        context.log.error(
                            f"row {idx}: invalid Perplexity API key. Get a key at "
                            f"https://www.perplexity.ai/account/api/keys"
                        )
                    elif "429" in err_str.lower() or "rate" in err_str.lower():
                        context.log.error(
                            f"row {idx}: Perplexity rate limit hit. Set rate_limit_delay > 0 to "
                            f"throttle, or check your tier at https://www.perplexity.ai/account/api/billing"
                        )
                    else:
                        context.log.error(f"row {idx}: perplexity call ultimately failed: {last_err}")
                    if rate_limit_delay > 0:
                        time.sleep(rate_limit_delay)
                    continue

                choice = resp.choices[0] if resp.choices else None
                text_out = (choice.message.content if choice and choice.message else None) or None
                responses.append(text_out)
                errors.append(None)

                row_citations = _extract_citations(resp, choice)
                all_citations.append(row_citations)
                total_citations += len(row_citations)
                if include_search_results:
                    all_search_results.append(_extract_search_results(resp))

                related_qs = getattr(resp, "related_questions", None)
                if related_qs:
                    related_questions_seen.extend(list(related_qs))

                if text_out is not None:
                    success += 1
                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)

            df[output_column] = responses
            df[citations_column] = all_citations
            if include_search_results:
                df[f"{citations_column}_detail"] = all_search_results
            if any(errors):
                df[f"{output_column}_error"] = errors

            preview_md = df.head(5).to_markdown(index=False) or ""
            metadata: Dict[str, Any] = {
                "rows":            MetadataValue.int(len(df)),
                "responses":       MetadataValue.int(success),
                "total_citations": MetadataValue.int(total_citations),
                "model":           MetadataValue.text(model),
                "provider":        MetadataValue.text("Perplexity"),
                "preview":         MetadataValue.md(preview_md),
            }
            if related_questions_seen:
                metadata["sample_related_questions"] = MetadataValue.json(related_questions_seen[:10])
            context.add_output_metadata(metadata)
            return df

        return Definitions(assets=[_asset])
