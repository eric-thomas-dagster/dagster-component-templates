"""InferenceProviderABTestComponent.

Run the same prompt through N LLM providers side-by-side. Each provider's
response becomes a first-class Dagster asset with cost, latency, and
token metadata — so you can query "is qwen2.5:14b-local within 3% quality
of gpt-4o for triage prompts?" directly from asset history + Insights,
not from a benchmarking spreadsheet.

The point isn't "which LLM is best" (that changes weekly). The point is
that Dagster's substrate makes the tradeoff *empirical* for a given
workload — same code runs the A/B in dev, in a branch deployment (with
real production data), and in prod. Combined with `llm_evaluator` for
quality scoring and `inference_cost_report` for the aggregation, this
is the "should we go local" answer that lives with the assets, not in
a slide deck.

## Design

- Every provider is one asset: `{asset_name_prefix}_{provider_alias}`.
- Emit them from a single `@multi_asset` compute so all providers see
  the same prompt in the same run (no drift across separate
  materializations).
- Provider config is LiteLLM-compatible: `model` accepts `gpt-4o-mini`,
  `claude-3-5-haiku-latest`, `ollama/qwen2.5:14b`,
  `openai/<hf-model-id>` (LiteLLM's shape for OpenAI-compatible endpoints
  like vLLM / LM Studio / TGI). `api_base_env_var` points at the local
  endpoint when the model uses one.
- Each response asset carries `cost_usd`, `latency_ms`, `tokens_in`,
  `tokens_out`, `model_fingerprint` (`{model}@t{temperature}`), and
  `provider_alias` in metadata. Cost comes from `litellm.completion_cost`;
  for local models the cost is 0.0 (or a user-supplied
  `cost_per_1k_tokens_override` for custom pricing).
"""

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# ── Ingestion (mirrors AgenticPipelineComponent shape) ─────────────────

def _load_prompt(prompt: Dict[str, Any], partition_key: Optional[str] = None) -> str:
    """Same shapes as AgenticPipelineComponent.source: literal | file | url.
    `{partition_key}` is substituted so a partition-per-prompt setup Just Works.
    """
    def _sub(s: str) -> str:
        return s.replace("{partition_key}", str(partition_key)) if partition_key and "{partition_key}" in s else s

    kind = prompt.get("kind", "literal")
    if kind == "literal":
        return _sub(str(prompt["text"]))
    if kind == "file":
        with open(_sub(prompt["path"])) as fh:
            return fh.read()
    if kind == "url":
        import requests
        r = requests.get(_sub(prompt["url"]), timeout=prompt.get("timeout", 30))
        r.raise_for_status()
        return r.text
    raise ValueError(f"unknown prompt kind {kind!r}: valid are literal | file | url")


# ── LLM call (LiteLLM wrapper, standalone) ─────────────────────────────

def _completion(
    *,
    model: str,
    system_prompt: Optional[str],
    user_prompt: str,
    api_key_env_var: Optional[str],
    api_base_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    cost_per_1k_tokens_override: Optional[float] = None,
) -> Dict[str, Any]:
    """One LiteLLM call. Returns cost/latency/token metadata alongside content.

    For local endpoints where LiteLLM's cost table returns 0/None,
    `cost_per_1k_tokens_override` lets the caller supply a self-hosted
    dollar-equivalent (e.g. amortized GPU-hour cost) so cross-provider
    comparisons still have a meaningful cost axis.
    """
    try:
        import litellm
    except ImportError:
        raise ImportError(
            "inference_provider_ab_test requires litellm: "
            "pip install 'litellm>=1.30.0'"
        )

    litellm.drop_params = True

    messages: List[Dict[str, Any]] = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user_prompt})

    kwargs: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if api_key_env_var and os.environ.get(api_key_env_var):
        kwargs["api_key"] = os.environ[api_key_env_var]
    if api_base_env_var and os.environ.get(api_base_env_var):
        kwargs["api_base"] = os.environ[api_base_env_var]

    t0 = time.time()
    response = litellm.completion(**kwargs)
    latency_ms = int((time.time() - t0) * 1000)

    msg = response.choices[0].message
    content = msg.content or ""

    usage = getattr(response, "usage", None) or {}
    tokens_in = getattr(usage, "prompt_tokens", None) or (
        usage.get("prompt_tokens") if isinstance(usage, dict) else None
    )
    tokens_out = getattr(usage, "completion_tokens", None) or (
        usage.get("completion_tokens") if isinstance(usage, dict) else None
    )
    tokens_total = getattr(usage, "total_tokens", None) or (
        usage.get("total_tokens") if isinstance(usage, dict) else None
    )

    cost_usd: Optional[float] = None
    if cost_per_1k_tokens_override is not None and tokens_total is not None:
        cost_usd = round((tokens_total / 1000.0) * cost_per_1k_tokens_override, 6)
    else:
        try:
            cost_usd = float(litellm.completion_cost(completion_response=response))
        except Exception:  # noqa: BLE001
            cost_usd = None

    return {
        "content": content,
        "cost_usd": cost_usd,
        "latency_ms": latency_ms,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "tokens_total": tokens_total,
        "model_fingerprint": f"{model}@t{temperature}",
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Partition helper ───────────────────────────────────────────────────

def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name
):
    """Same shape as AgenticPipelineComponent's helper — small subset."""
    from dagster import (
        DailyPartitionsDefinition,
        DynamicPartitionsDefinition,
        StaticPartitionsDefinition,
    )
    if not partition_type:
        return None
    if partition_type == "daily":
        if not partition_start:
            raise ValueError("partition_type='daily' requires partition_start")
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        vals = partition_values
        if isinstance(vals, str):
            vals = [v.strip() for v in vals.split(",") if v.strip()]
        if not vals:
            raise ValueError("partition_type='static' requires partition_values")
        return StaticPartitionsDefinition(list(vals))
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# ── Component ──────────────────────────────────────────────────────────


class InferenceProviderABTestComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run the same prompt through N LLM providers side-by-side; each
    provider's response becomes a first-class Dagster asset with cost /
    latency / token metadata.

    Pairs with `llm_evaluator` (adds quality scores) and
    `inference_cost_report` (aggregates cost + quality across recent runs)
    for the full "should we go local" story.
    """

    asset_name_prefix: str = Field(
        description="Prefix for the emitted assets. Each provider produces `{prefix}_{alias}`."
    )
    prompt: Dict[str, Any] = Field(
        description=(
            "Prompt source. Shapes: "
            "`{kind: literal, text: '...'}` | "
            "`{kind: file, path: '...'}` | "
            "`{kind: url, url: '...'}`. "
            "All string fields are `{partition_key}`-templated."
        )
    )
    providers: List[Dict[str, Any]] = Field(
        description=(
            "LLM providers to compare. Each: `{alias, model, "
            "[api_key_env_var, api_base_env_var, system_prompt, temperature, "
            "max_tokens, cost_per_1k_tokens_override]}`. `alias` becomes the "
            "asset name suffix. `model` is LiteLLM-compatible: `gpt-4o-mini`, "
            "`claude-3-5-haiku-latest`, `ollama/qwen2.5:14b`, "
            "`openai/<hf-model>` (for vLLM / LM Studio / TGI). "
            "`cost_per_1k_tokens_override` optional for local endpoints "
            "where the real cost is amortized GPU-hour rather than token pricing."
        )
    )

    # Shared defaults (per-provider values override)
    system_prompt: Optional[str] = Field(
        default=None,
        description="Default system prompt applied to any provider that doesn't set its own."
    )
    temperature: float = Field(
        default=0.1,
        description="Default temperature. Providers can override."
    )
    max_tokens: int = Field(
        default=500,
        description="Default max_tokens. Providers can override."
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default="ab_test", description="Asset group.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['llm', 'ab-test']."
    )
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Extra tags.")
    description: Optional[str] = Field(default=None, description="Asset description.")

    # Partitioning — pair with a per-prompt dataset for scale demos.
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' | 'static' | 'dynamic' | None."
    )
    partition_start: Optional[str] = Field(
        default=None, description="ISO start date for daily partitions."
    )
    partition_values: Optional[Any] = Field(
        default=None, description="Values for static partitioning (comma string OR list)."
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None, description="Name for DynamicPartitionsDefinition."
    )

    @classmethod
    def get_form_config(cls):
        """UI-editable via the Dagster / Dagster+ Components tab."""
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Inference Provider A/B Test", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        prefix = self.asset_name_prefix
        providers = list(self.providers)
        prompt_config = dict(self.prompt)
        shared_system = self.system_prompt
        shared_temp = self.temperature
        shared_max_tokens = self.max_tokens

        if not providers:
            raise ValueError("providers must have at least one entry.")
        aliases = [p["alias"] for p in providers]
        if len(set(aliases)) != len(aliases):
            raise ValueError(f"provider aliases must be unique; got {aliases}")

        kinds = self.kinds or ["llm", "ab-test"]
        tag_map = dict(self.tags or {})
        for k in kinds:
            tag_map[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
        )

        outs = {
            f"{prefix}_{p['alias']}": dg.AssetOut(
                key=dg.AssetKey([f"{prefix}_{p['alias']}"]),
                description=self.description
                or f"Response from provider {p['alias']} (model={p['model']}).",
                group_name=self.group_name,
                owners=self.owners or [],
                tags=tag_map,
            )
            for p in providers
        }

        _providers_snapshot = providers  # capture in closure

        @dg.multi_asset(
            outs=outs,
            name=f"{prefix}_ab_test",
            partitions_def=partitions_def,
        )
        def _ab(context: dg.AssetExecutionContext):
            partition_key = context.partition_key if context.has_partition_key else None
            prompt_text = _load_prompt(prompt_config, partition_key=partition_key)
            context.log.info(
                f"[ab_test] running {len(_providers_snapshot)} provider(s) "
                f"against {len(prompt_text)}-char prompt "
                f"(partition_key={partition_key!r})"
            )

            for p in _providers_snapshot:
                alias = p["alias"]
                model = p["model"]
                system_prompt = p.get("system_prompt", shared_system)
                temperature = p.get("temperature", shared_temp)
                max_tokens = p.get("max_tokens", shared_max_tokens)

                context.log.info(f"[ab_test:{alias}] model={model} calling…")
                try:
                    result = _completion(
                        model=model,
                        system_prompt=system_prompt,
                        user_prompt=prompt_text,
                        api_key_env_var=p.get("api_key_env_var"),
                        api_base_env_var=p.get("api_base_env_var"),
                        temperature=temperature,
                        max_tokens=max_tokens,
                        cost_per_1k_tokens_override=p.get("cost_per_1k_tokens_override"),
                    )
                except Exception as e:  # noqa: BLE001
                    # One provider failing shouldn't tank the whole A/B — emit
                    # the failure as asset metadata so the report can call it out.
                    context.log.error(f"[ab_test:{alias}] failed: {type(e).__name__}: {e}")
                    output_value = {
                        "text": "",
                        "content": "",
                        "provider_alias": alias,
                        "model": model,
                        "model_fingerprint": f"{model}@t{temperature}",
                        "error": f"{type(e).__name__}: {e}",
                        "materialized_at": _now_iso(),
                    }
                    yield dg.Output(
                        output_value,
                        output_name=f"{prefix}_{alias}",
                        metadata={
                            "provider_alias": alias,
                            "model": model,
                            "status": "failed",
                            "error": str(e)[:500],
                        },
                    )
                    continue

                context.log.info(
                    f"[ab_test:{alias}] ok "
                    f"cost=${result['cost_usd']} latency={result['latency_ms']}ms "
                    f"tokens={result['tokens_total']}"
                )
                output_value = {
                    # Standard shape so downstream evaluators / reports can
                    # consume assets from this component OR from
                    # AgenticPipelineComponent's llm_call op interchangeably.
                    "text": result["content"],
                    "content": result["content"],
                    "provider_alias": alias,
                    "model": model,
                    "model_fingerprint": result["model_fingerprint"],
                    "cost_usd": result["cost_usd"],
                    "latency_ms": result["latency_ms"],
                    "tokens_in": result["tokens_in"],
                    "tokens_out": result["tokens_out"],
                    "tokens_total": result["tokens_total"],
                    "materialized_at": _now_iso(),
                    "op": "ab_test_provider",
                }
                yield dg.Output(
                    output_value,
                    output_name=f"{prefix}_{alias}",
                    metadata={
                        "provider_alias": alias,
                        "model": dg.MetadataValue.text(model),
                        "cost_usd": dg.MetadataValue.float(
                            result["cost_usd"] if result["cost_usd"] is not None else 0.0
                        ),
                        "latency_ms": dg.MetadataValue.int(result["latency_ms"]),
                        "tokens_total": dg.MetadataValue.int(
                            result["tokens_total"] or 0
                        ),
                        "tokens_in": dg.MetadataValue.int(result["tokens_in"] or 0),
                        "tokens_out": dg.MetadataValue.int(result["tokens_out"] or 0),
                        "preview": dg.MetadataValue.md(
                            "```\n" + (result["content"][:600] or "(empty)") + "\n```"
                        ),
                        **({"partition_key": dg.MetadataValue.text(str(partition_key))} if partition_key else {}),
                    },
                )

        return dg.Definitions(assets=[_ab])
