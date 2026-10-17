"""ProviderABEvaluatorComponent.

LLM-as-judge that scores N candidate outputs against a rubric in ONE
pass — so `InferenceProviderABTestComponent` gains a quality signal
alongside the cost + latency it already tracks.

Different job from `LLMEvaluatorComponent`:
- `llm_evaluator`  — one upstream, N quality dimensions (groundedness,
  helpfulness, harmfulness, ...). Deep single-output analysis.
- `provider_ab_evaluator` (this) — N candidates, one rubric, one comparative
  scoring pass. Cross-candidate delta is the point; single-call scoring
  keeps scores directly comparable (separate calls suffer from judge-drift
  on nominally identical rubrics).

Emits ONE asset containing per-candidate scores, winner alias, and delta
vs. a baseline. Optional `min_winner_score` asset check turns the pair
(A/B + evaluator) into a merge gate: "block promotion of a provider swap
that drops quality below threshold X."
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# ── Rubric loading ─────────────────────────────────────────────────────

# BEGIN reasoning-output normalization (inlined per component; keep copies identical — see tests/test_reasoning_output.py)
import json as _json_r  # noqa: E402
import re as _re_r  # noqa: E402
from typing import List, Optional, Tuple  # noqa: E402,F401

_THINK_BLOCK_R = _re_r.compile(r"<think>(.*?)</think>", _re_r.S | _re_r.I)
_TOOL_CALL_BLOCK_R = _re_r.compile(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", _re_r.S)


def split_reasoning(content: Optional[str]) -> Tuple[str, Optional[str]]:
    """Split `<think>…</think>` reasoning out of model text -> (answer, reasoning).

    Reasoning models (Qwen3, DeepSeek-R1, ...) often inline their chain of thought. Left in, it breaks JSON parsing,
    classification and routing downstream. Handles a closed block, an UNCLOSED block (output truncated by max_tokens
    mid-thought: everything after `<think>` is reasoning and there is no answer yet), and a bare closing tag (some chat
    templates put the opening tag in the prompt). Returns (content, None) untouched when there is no reasoning."""
    content = content or ""
    if "think>" not in content.lower():
        return content, None
    parts = [m.strip() for m in _THINK_BLOCK_R.findall(content) if m.strip()]
    rest = _THINK_BLOCK_R.sub("", content)
    m = _re_r.search(r"<think>(.*)$", rest, _re_r.S | _re_r.I)
    if m:
        if m.group(1).strip():
            parts.append(m.group(1).strip())
        rest = rest[: m.start()]
    elif _re_r.search(r"</think>", rest, _re_r.I) and not parts:
        before, after = _re_r.split(r"</think>", rest, maxsplit=1, flags=_re_r.I)
        if before.strip():
            parts.append(before.strip())
        rest = after
    return rest.strip(), ("\n\n".join(parts) if parts else None)


def parse_text_tool_calls(content: Optional[str]) -> Tuple[str, List[dict]]:
    """Recover `<tool_call>{"name":…,"arguments":{…}}</tool_call>` that Qwen/Hermes-style models emit as plain text when the
    server did not parse it into structured tool_calls -> (remaining text, [{name, arguments(json str)}])."""
    content = content or ""
    calls: List[dict] = []
    for raw in _TOOL_CALL_BLOCK_R.findall(content):
        try:
            obj = _json_r.loads(raw)
        except _json_r.JSONDecodeError:
            continue
        name = obj.get("name")
        if not name:
            continue
        args = obj.get("arguments", obj.get("parameters", {}))
        calls.append({"name": name, "arguments": args if isinstance(args, str) else _json_r.dumps(args)})
    return (_TOOL_CALL_BLOCK_R.sub("", content).strip() if calls else content), calls


def ollama_chat_model(model: str, has_tools: bool) -> str:
    """LiteLLM's `ollama/` route is /api/generate (no tool support); `ollama_chat/` is /api/chat, which has it."""
    if has_tools and model.lower().startswith("ollama/"):
        return "ollama_chat/" + model.split("/", 1)[1]
    return model
# END reasoning-output normalization


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value



def _load_rubric(rubric: Dict[str, Any]) -> str:
    kind = rubric.get("kind", "literal")
    if kind == "literal":
        return str(rubric["text"])
    if kind == "file":
        with open(rubric["path"]) as fh:
            return fh.read()
    if kind == "url":
        import requests
        r = requests.get(rubric["url"], timeout=rubric.get("timeout", 30))
        r.raise_for_status()
        return r.text
    raise ValueError(f"unknown rubric kind {kind!r}: valid literal | file | url")


def _extract_text(payload: Any) -> str:
    """Candidate assets come in a few shapes — grab the text field if there
    is one, otherwise str()-coerce."""
    if isinstance(payload, dict):
        for k in ("text", "content", "value"):
            v = payload.get(k)
            if isinstance(v, str):
                return v
        return json.dumps(payload, default=str, indent=2)
    if isinstance(payload, str):
        return payload
    return str(payload)


def _extract_alias(candidate_key: str, payload: Any) -> str:
    """Prefer `provider_alias` embedded in A/B outputs; fall back to the
    last segment of the asset key."""
    if isinstance(payload, dict):
        alias = payload.get("provider_alias") or payload.get("alias")
        if isinstance(alias, str) and alias.strip():
            return alias
    return candidate_key.split("/")[-1]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    """Same field surface as sibling `AgenticPipelineComponent` /
    `HumanApprovalGateComponent`, so upstream + downstream can co-partition
    without config drift."""
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
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
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
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# ── Judge call ────────────────────────────────────────────────────────

def _call_judge(
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    api_key_env_var: Optional[str],
    api_base_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    candidates_ordered: List[Dict[str, str]],
) -> Dict[str, Any]:
    """One LiteLLM call, tool-call-forced JSON so downstream parsing is
    deterministic. Returns {scores, latency_ms, cost_usd, tokens_total}."""
    try:
        import litellm
    except ImportError:
        raise ImportError(
            "provider_ab_evaluator requires litellm: pip install 'litellm>=1.30.0'"
        )

    litellm.drop_params = True

    # Force JSON via a tool call — every candidate becomes a required
    # property so the judge can't skip one.
    props: Dict[str, Any] = {}
    required: List[str] = []
    for c in candidates_ordered:
        props[c["alias"]] = {
            "type": "object",
            "properties": {
                "score": {"type": "integer", "description": "0-100 quality score."},
                "reasoning": {"type": "string", "description": "1-2 sentences citing rubric criteria."},
            },
            "required": ["score", "reasoning"],
        }
        required.append(c["alias"])
    tool = {
        "type": "function",
        "function": {
            "name": "record_scores",
            "description": "Record per-candidate quality scores against the rubric.",
            "parameters": {"type": "object", "properties": props, "required": required},
        },
    }

    kwargs: Dict[str, Any] = {
        "model": ollama_chat_model(model, True),  # ollama/ -> ollama_chat/ (the /api/generate route has no tools)
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "tools": [tool],
        # Ollama does not honour tool_choice='required'; use 'auto' there and recover text tool calls below.
        "tool_choice": "auto" if model.lower().startswith(("ollama/", "ollama_chat/")) else "required",
    }
    if api_key_env_var and os.environ.get(api_key_env_var):
        kwargs["api_key"] = _require_env(api_key_env_var)
    if api_base_env_var and os.environ.get(api_base_env_var):
        kwargs["api_base"] = _require_env(api_base_env_var)

    t0 = time.time()
    response = litellm.completion(**kwargs)
    latency_ms = int((time.time() - t0) * 1000)

    _msg = response.choices[0].message
    tool_calls = getattr(_msg, "tool_calls", None) or []
    _text, thinking = split_reasoning(getattr(_msg, "content", None))
    if not tool_calls:
        # Qwen/Hermes-style models served without a tool parser emit <tool_call>{...}</tool_call> as text.
        from types import SimpleNamespace
        _, _text_calls = parse_text_tool_calls(_text)
        tool_calls = [
            SimpleNamespace(function=SimpleNamespace(name=_c["name"], arguments=_c["arguments"]))
            for _c in _text_calls
        ]
    if not tool_calls:
        raise ValueError(
            "judge did not emit tool call — rubric may be too abstract or "
            "the model doesn't support tool_choice='required'. Consider a "
            "stronger judge (gpt-4o, claude-3-5-sonnet)."
        )
    raw = tool_calls[0].function.arguments or "{}"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"judge emitted invalid JSON: {e}: {raw[:400]}")

    cost_usd: Optional[float] = None
    try:
        cost_usd = float(litellm.completion_cost(completion_response=response))
    except Exception:  # noqa: BLE001
        cost_usd = None

    usage = getattr(response, "usage", None) or {}
    tokens_total = getattr(usage, "total_tokens", None) or (
        usage.get("total_tokens") if isinstance(usage, dict) else None
    )

    return {
        "scores": parsed,
        "thinking": thinking,
        "latency_ms": latency_ms,
        "cost_usd": cost_usd,
        "tokens_total": tokens_total,
    }


# ── Component ─────────────────────────────────────────────────────────



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


class ProviderABEvaluatorComponent(dg.Component, dg.Model, dg.Resolvable):
    """LLM-as-judge that scores N candidate outputs against a rubric in
    ONE pass. Pairs with `InferenceProviderABTestComponent` (upstream)
    to close the "should we go local" loop: A/B gives cost + latency +
    tokens per provider, evaluator gives quality per provider, and the
    pair drives the branch-deploy merge gate.
    """

    asset_name: str = Field(
        description="Name of the emitted asset (holds per-candidate scores + winner)."
    )
    candidates: List[str] = Field(
        description=(
            "Upstream asset keys to score. Multi-part keys use slash notation. "
            "Each candidate's text is loaded from the standard "
            "`{text, content, ...}` shape emitted by "
            "InferenceProviderABTestComponent or AgenticPipelineComponent's "
            "llm_call op."
        )
    )
    rubric: Dict[str, Any] = Field(
        description=(
            "Scoring rubric. Shapes: `{kind: literal, text: '...'}` | "
            "`{kind: file, path: '...'}` | `{kind: url, url: '...'}`. "
            "Include specific criteria + point weights."
        )
    )
    judge: Dict[str, Any] = Field(
        description=(
            "Judge model config: `{model, api_key_env_var, "
            "[api_base_env_var, system_prompt, temperature, max_tokens]}`. "
            "Use a strong model — gpt-4o or claude-3-5-sonnet — since the "
            "judge sets the ceiling on evaluation quality."
        )
    )

    min_winner_score: Optional[int] = Field(
        default=None,
        description=(
            "0-100. When set, emits an asset check `winner_meets_threshold` "
            "that fails ERROR when the winner's score is below this. Wire "
            "into branch-deploy CI to gate merges on quality holding above "
            "a baseline."
        )
    )
    baseline_alias: Optional[str] = Field(
        default=None,
        description=(
            "Optional baseline candidate alias for delta metrics. When set, "
            "the payload includes `delta_vs_baseline: {alias: score_delta}` "
            "— useful for 'how much did switching to Ollama drop us vs OpenAI'."
        )
    )

    include_reasoning: bool = Field(default=False, description="Reasoning models (Qwen3, DeepSeek-R1, ...) inline <think>...</think>. The thinking is always stripped; when true it is also returned as a 'judge_reasoning' key on the output dict (distinct from the per-candidate 'reasoning' the judge writes into scores).")
    group_name: Optional[str] = Field(default="ab_test", description="Asset group.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['llm', 'evaluator', 'ab-test']."
    )
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra tags.")
    description: Optional[str] = Field(default=None, description="Asset description.")

    # Partitioning — must match upstream candidate assets' partitions_def.
    # When paired with a partitioned InferenceProviderABTestComponent
    # (e.g. one A/B evaluation per prompt in a dataset), the evaluator
    # runs per-partition too and emits scores keyed on the partition key.
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'multi' | None."
    )
    partition_start: Optional[str] = Field(
        default=None, description="ISO date for time-based partition types."
    )
    partition_values: Optional[Any] = Field(
        default=None, description="Comma-separated string OR list for static/multi partitioning."
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None, description="Name for DynamicPartitionsDefinition."
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name}."
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('kinds', 'owners'), csv=(), tags=('tags',))

    @classmethod
    def get_form_config(cls):
        """UI-editable via the Dagster / Dagster+ Components tab."""
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Provider A/B Evaluator", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        include_reasoning = self.include_reasoning
        candidate_keys = list(self.candidates)
        rubric_config = dict(self.rubric)
        judge_config = dict(self.judge)
        min_winner_score = self.min_winner_score
        baseline_alias = self.baseline_alias
        description = self.description

        if not candidate_keys:
            raise ValueError("candidates must list at least one asset key.")

        kinds = self.kinds or ["llm", "evaluator", "ab-test"]
        tag_map = dict(self.tags or {})
        for k in kinds:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {
            f"c_{i}": dg.AssetIn(key=dg.AssetKey.from_user_string(k))
            for i, k in enumerate(candidate_keys)
        }

        check_specs = []
        if min_winner_score is not None:
            check_specs.append(
                dg.AssetCheckSpec(
                    name="winner_meets_threshold",
                    asset=dg.AssetKey.from_user_string(asset_name),
                    description=(
                        f"Winner LLM-as-judge score must be >= {min_winner_score}. "
                        f"Wire into branch-deploy CI to block merges that degrade quality."
                    ),
                )
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=description or f"LLM-as-judge scores for {candidate_keys}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            ins=ins,
            check_specs=check_specs,
            partitions_def=partitions_def,
        )
        def _evaluator(context: dg.AssetExecutionContext, **inputs):
            rubric_text = _load_rubric(rubric_config)

            # Deterministic candidate ordering by declared position.
            candidates_ordered: List[Dict[str, str]] = []
            for i, key in enumerate(candidate_keys):
                payload = inputs[f"c_{i}"]
                alias = _extract_alias(key, payload)
                text = _extract_text(payload)
                candidates_ordered.append({"alias": alias, "key": key, "text": text})
            aliases = [c["alias"] for c in candidates_ordered]
            if len(set(aliases)) != len(aliases):
                # Alias collision — force key-derived names.
                for c, key in zip(candidates_ordered, candidate_keys):
                    c["alias"] = key.replace("/", "_")
                aliases = [c["alias"] for c in candidates_ordered]

            context.log.info(
                f"[ab_evaluator] scoring {len(candidates_ordered)} candidates: {aliases}"
            )

            system_prompt = judge_config.get("system_prompt") or (
                "You are a rigorous, impartial evaluator. Score each candidate "
                "output against the RUBRIC. Be consistent across candidates — "
                "the SAME rubric criterion should yield comparable scores. Cite "
                "specific evidence from each candidate in your reasoning."
            )

            body = ["RUBRIC", "======", rubric_text, "", "CANDIDATES", "=========="]
            for c in candidates_ordered:
                body.append(f"\n### {c['alias']}\n{c['text'].strip()}\n")
            body.append("\nCall `record_scores` with a score (0-100) and reasoning per candidate.")
            user_prompt = "\n".join(body)

            judge_result = _call_judge(
                model=judge_config["model"],
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                api_key_env_var=judge_config.get("api_key_env_var"),
                api_base_env_var=judge_config.get("api_base_env_var"),
                temperature=judge_config.get("temperature", 0.0),
                max_tokens=judge_config.get("max_tokens", 1500),
                candidates_ordered=candidates_ordered,
            )

            scores_map: Dict[str, Dict[str, Any]] = judge_result["scores"]
            winner_alias = max(
                scores_map.items(), key=lambda kv: kv[1].get("score", -1)
            )[0]
            winner_score = int(scores_map[winner_alias].get("score", 0))

            deltas: Optional[Dict[str, int]] = None
            if baseline_alias and baseline_alias in scores_map:
                baseline_score = int(scores_map[baseline_alias].get("score", 0))
                deltas = {
                    a: int(s.get("score", 0)) - baseline_score
                    for a, s in scores_map.items()
                    if a != baseline_alias
                }

            output_value = {
                "scores": scores_map,
                "winner": winner_alias,
                "winner_score": winner_score,
                "baseline_alias": baseline_alias,
                "delta_vs_baseline": deltas,
                "judge_model": judge_config["model"],
                "judge_cost_usd": judge_result["cost_usd"],
                "judge_latency_ms": judge_result["latency_ms"],
                "judge_tokens_total": judge_result["tokens_total"],
                "n_candidates": len(candidates_ordered),
                "evaluated_at": _now_iso(),
                "op": "provider_ab_evaluator",
            }
            if include_reasoning:
                output_value["judge_reasoning"] = judge_result.get("thinking")

            md: Dict[str, Any] = {
                "winner": winner_alias,
                "winner_score": dg.MetadataValue.int(winner_score),
                "n_candidates": dg.MetadataValue.int(len(candidates_ordered)),
                "judge_cost_usd": dg.MetadataValue.float(
                    judge_result["cost_usd"] if judge_result["cost_usd"] is not None else 0.0
                ),
                "judge_latency_ms": dg.MetadataValue.int(judge_result["latency_ms"]),
                "scores_table": dg.MetadataValue.md(
                    "| alias | score | reasoning |\n|---|---|---|\n"
                    + "\n".join(
                        f"| {a} | {s.get('score', '?')} | {str(s.get('reasoning', ''))[:200]} |"
                        for a, s in scores_map.items()
                    )
                ),
            }
            if deltas is not None:
                md["delta_vs_baseline"] = dg.MetadataValue.json(deltas)

            yield dg.Output(output_value, metadata=md)

            if min_winner_score is not None:
                passed = winner_score >= min_winner_score
                yield dg.AssetCheckResult(
                    check_name="winner_meets_threshold",
                    passed=passed,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=(
                        f"winner={winner_alias} score={winner_score} "
                        f"threshold={min_winner_score} → "
                        f"{'PASS' if passed else 'FAIL — block promotion'}"
                    ),
                    metadata={
                        "winner": winner_alias,
                        "winner_score": winner_score,
                        "threshold": min_winner_score,
                    },
                )

        return dg.Definitions(assets=[_evaluator])
