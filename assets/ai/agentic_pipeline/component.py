"""AgenticPipelineComponent — one YAML, whole agentic pipeline.

Same "pipeline component" shape as `polars_pipeline`, `warehouse_pipeline`,
`pyspark_pipeline`, `snowpark_pipeline`, `ml_pipeline`: **one YAML file
declares the whole pipeline**, `steps:` list defines the DAG in reading order,
`outputs:` declares which step outputs become first-class Dagster assets and
where side text/JSON files get written.

Example (route → answer → critique → synthesize):

    type: dagster_community_components.AgenticPipelineComponent
    attributes:
      asset_name_prefix: research_bot
      source:
        kind: literal
        text: "Explain how a transformer attention head works, plus one concrete example."
      steps:
        - {id: routed,   op: route,
                         router: {model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY},
                         specialists:
                           - {name: technical, description: "Deep technical CS / ML questions.",
                              model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                              system_prompt: "You are a senior ML engineer. Be precise."}
                           - {name: general,   description: "General knowledge.",
                              model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY,
                              system_prompt: "You are a helpful assistant."}
                         fallback: general}
        - {id: critiqued, op: critique_loop, source: routed,
                          drafter: {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                                    system_prompt: "You are a technical writer."},
                          critic:  {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                                    system_prompt: "Critique for clarity + accuracy."},
                          iterations: 2}
        - {id: debated,   op: debate, source: critiqued,
                          proposers:
                            - {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                               system_prompt: "Argue this is a great answer."}
                            - {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                               system_prompt: "Argue this answer needs improvement."}
                          arbitrator: {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
                                       system_prompt: "Pick the stronger argument."}}
      outputs:
        assets: [routed, critiqued, debated]
        text_sinks:
          - {from: critiqued, path: /tmp/final_answer.txt}

Standardization — that's the point. Every agentic pipeline in the org uses
the same YAML shape, the same ops, the same output conventions.

Op coverage (v2 = 6):

- llm_call       — single LLM call; uses source step's text as user prompt
- route          — router picks best specialist; specialist answers
- debate         — N proposers → arbitrator picks winner
- critique_loop  — drafter → critic → drafter, N iterations
- synthesize     — merge multiple upstream step texts into one summary
- mcp_call       — direct MCP tool call (stdio / http / sse), no LLM;
                   turns "fetch grounding data" into a first-class asset with
                   lineage + metadata; string `tool_args` support `{text}`
                   substitution against source

State model:

Every step's output is a dict `{"text": str, ...op-specific fields}`. Steps
read text from a prior step via `source: <id>` (default = most recent step).
The `assets:` list picks which step outputs become first-class Dagster
assets; each is emitted as a dict with the full op output preserved.
"""
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field


# ── Ingestion ────────────────────────────────────────────────────────

def _apply_partition_template(s: str, partition_key: Optional[str]) -> str:
    """Substitute `{partition_key}` into a string. No-op when unpartitioned."""
    if partition_key is None or "{partition_key}" not in s:
        return s
    return s.replace("{partition_key}", str(partition_key))


def _parse_partition_key(partition_key: Optional[str], parser: Optional[str]) -> Dict[str, Any]:
    """Invert a `partition_key_parser` template against `partition_key` to
    extract named fields. Supports typed placeholders: `{name}` (str, default),
    `{name:int}`, `{name:float}`, `{name:bool}` — the extracted value is
    coerced to that type in the returned dict. Callers use this so
    substitution can preserve type when a target string is exactly a single
    `{partition.<name>}` placeholder (e.g. tool_args expecting int).

    Returns {} on any mismatch — substitution then leaves placeholders in
    place so the gap is visible in logs.
    """
    if not partition_key or not parser:
        return {}
    import re
    # Match `{name}` or `{name:type}`.
    field_re = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)(?::([a-z]+))?\}")
    # Split parser into literal + (field_name, type) pieces.
    parts: list = []
    last = 0
    for m in field_re.finditer(parser):
        parts.append(("lit", parser[last:m.start()]))
        parts.append(("field", m.group(1), m.group(2) or "str"))
        last = m.end()
    parts.append(("lit", parser[last:]))
    if not any(p[0] == "field" for p in parts):
        return {}
    regex_parts = []
    field_types: Dict[str, str] = {}
    field_seq = [i for i, p in enumerate(parts) if p[0] == "field"]
    for i, p in enumerate(parts):
        if p[0] == "lit":
            regex_parts.append(re.escape(p[1]))
        else:
            name, typ = p[1], p[2]
            field_types[name] = typ
            is_last = i == field_seq[-1]
            # Next literal char guides the greedy stop for middle fields.
            next_lit = parts[i + 1][1] if i + 1 < len(parts) and parts[i + 1][0] == "lit" else ""
            regex_parts.append(f"(?P<{name}>.+)" if is_last else f"(?P<{name}>[^{re.escape(next_lit[:1]) if next_lit else ''}]+)")
    pattern = "^" + "".join(regex_parts) + "$"
    m = re.match(pattern, partition_key)
    if not m:
        return {}
    raw = m.groupdict()
    typed: Dict[str, Any] = {}
    for k, v in raw.items():
        t = field_types.get(k, "str")
        try:
            if t == "int":
                typed[k] = int(v)
            elif t == "float":
                typed[k] = float(v)
            elif t == "bool":
                typed[k] = v.lower() in ("true", "1", "yes")
            else:
                typed[k] = v
        except (ValueError, TypeError):
            typed[k] = v  # fall back to string on coercion failure
    return typed


def _apply_ctx_substitutions(
    s: Any,
    partition_key: Optional[str],
    partition_fields: Optional[Dict[str, Any]],
) -> Any:
    """Apply run-context substitutions: `{partition_key}` + `{partition.<name>}`.

    Returns the input unchanged if it's not a string. When the input is
    EXACTLY a single `{partition.<name>}` placeholder (nothing else) and
    `partition_fields[name]` has a non-string type (int / float / bool),
    return the typed value rather than str-coerced — this is what makes
    `tool_args: {issue_number: "{partition.issue_number:int}"}` land as
    int 30000 in the MCP call instead of the string "30000".

    Ports (`{port_name}`) are handled separately by `_substitute_ports`
    since they're per-op inputs, not run-level context.
    """
    if not isinstance(s, str):
        return s
    # Type-preserving fast-path: whole value is `{partition.<name>}`.
    if partition_fields and s.startswith("{partition.") and s.endswith("}") and s.count("{") == 1:
        name = s[len("{partition."):-1]
        if name in partition_fields:
            return partition_fields[name]
    # Normal string substitution.
    if partition_key is not None and "{partition_key}" in s:
        s = s.replace("{partition_key}", str(partition_key))
    if partition_fields:
        for k, v in partition_fields.items():
            s = s.replace("{partition." + k + "}", str(v))
    return s


def _ctx_of(state: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch the run-context stash from state (partition_key + parsed fields).
    Empty dict when the pipeline isn't running under a partition."""
    return state.get("__ctx__") or {}


def _ingest(
    source_config: dict,
    context,
    partition_key: Optional[str] = None,
    partition_fields: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Return the initial state dict: {"text": str, "source_kind": str, ...}."""
    kind = source_config.get("kind", "literal")

    def _sub(s: Any) -> Any:
        return _apply_ctx_substitutions(s, partition_key, partition_fields)

    if kind == "literal":
        raw = source_config["text"]
        # YAML may parse `text: 30000` as int / `text: 3.14` as float —
        # downstream ops assume text is a string, so coerce here.
        text = _sub(raw if isinstance(raw, str) else str(raw))
        return {"text": text, "source_kind": "literal"}

    if kind == "file":
        path = _sub(source_config["path"])
        with open(path) as f:
            text = f.read()
        return {"text": text, "source_kind": "file", "source_path": path}

    if kind == "url":
        import requests
        url = _sub(source_config["url"])
        response = requests.get(url, timeout=source_config.get("timeout", 30))
        response.raise_for_status()
        return {"text": response.text, "source_kind": "url", "source_url": url}

    # upstream_asset ingestion happens outside this function (it's a compute input).
    raise ValueError(
        f"unknown source kind: {kind!r}. valid: literal | file | url | upstream_asset"
    )


# ── LLM helper ───────────────────────────────────────────────────────

def _completion(
    *,
    model: str,
    system_prompt: Optional[str],
    user_prompt: str,
    api_key_env_var: Optional[str],
    api_base_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    tools: Optional[List[Dict[str, Any]]] = None,
    tool_choice: Optional[str] = None,
    reasoning_effort: Optional[str] = None,
    thinking_budget: Optional[int] = None,
) -> Dict[str, Any]:
    """Thin LiteLLM wrapper. Returns {"content": str, "tool_calls": [...], "usage": {...}}.

    Reasoning params (both optional, silently dropped on non-reasoning models via
    `litellm.drop_params = True`):
      - reasoning_effort: 'low' | 'medium' | 'high' — OpenAI o1/o3, DeepSeek-R1
      - thinking_budget: int (max reasoning tokens) — Anthropic Claude thinking
                         mode, Gemini 2.5 thinking_budget. LiteLLM normalizes
                         the param name across providers.
    """
    try:
        import litellm
    except ImportError:
        raise ImportError("agentic_pipeline requires litellm: pip install 'litellm>=1.30.0'")

    # Silently drop params a specific model doesn't accept (e.g. Claude
    # Sonnet 5 only allows temperature=1; setting anything else without
    # this flag raises UnsupportedParamsError). Setting it here — not
    # globally at import time — keeps the change local to LLM calls
    # from this component.
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
    if api_key_env_var:
        kwargs["api_key"] = os.environ[api_key_env_var]
    if api_base_env_var:
        kwargs["api_base"] = os.environ[api_base_env_var]
    if tools:
        kwargs["tools"] = tools
    if tool_choice:
        kwargs["tool_choice"] = tool_choice
    # Reasoning params are provider-specific. LiteLLM's `drop_params=True`
    # doesn't reliably strip them when forwarding to a provider that
    # doesn't accept them (thinking_budget → OpenAI reaches the wire and
    # errors), so filter client-side by model family:
    #   - reasoning_effort: OpenAI o-series (o1/o3/o4), Groq (LiteLLM
    #     passes through). We include ALL OpenAI models (LiteLLM will
    #     drop for non-o models) but skip Anthropic/Gemini which reject.
    #   - thinking_budget: Gemini 2.5+ (native param), Anthropic thinking
    #     mode (LiteLLM canonical shape is `thinking={type: enabled,
    #     budget_tokens: N}`).
    m_lower = model.lower()
    is_openai_ish = m_lower.startswith(("gpt-", "o1", "o3", "o4", "openai/", "azure/", "groq/"))
    is_gemini = m_lower.startswith(("gemini/", "google/", "vertex_ai/gemini"))
    is_anthropic = (
        "claude" in m_lower
        or m_lower.startswith(("anthropic/", "bedrock/anthropic."))
    )
    if reasoning_effort is not None and (is_openai_ish or is_gemini):
        kwargs["reasoning_effort"] = reasoning_effort
    if thinking_budget is not None:
        if is_gemini:
            kwargs["thinking_budget"] = int(thinking_budget)
        elif is_anthropic:
            kwargs["thinking"] = {"type": "enabled", "budget_tokens": int(thinking_budget)}

    call_started_at = time.time()
    response = litellm.completion(**kwargs)
    latency_ms = int((time.time() - call_started_at) * 1000)
    msg = response.choices[0].message
    content = msg.content or ""

    # Surface reasoning traces when the provider returns them (Claude
    # thinking blocks, DeepSeek reasoning_content, some LiteLLM-wrapped
    # providers expose it on the message). Silently None for models that
    # don't emit reasoning.
    reasoning_content = getattr(msg, "reasoning_content", None) or None
    if not reasoning_content:
        # LiteLLM sometimes shoves it into `provider_specific_fields`.
        psf = getattr(msg, "provider_specific_fields", None)
        if isinstance(psf, dict):
            reasoning_content = psf.get("reasoning_content")

    tool_calls_out = []
    for tc in (msg.tool_calls or []):
        tool_calls_out.append({
            "name": tc.function.name,
            "arguments": tc.function.arguments,
        })

    usage = None
    u = getattr(response, "usage", None)
    if u is not None:
        try:
            usage = u.model_dump()
        except AttributeError:
            try:
                usage = dict(u)
            except (TypeError, ValueError):
                usage = None

    # LiteLLM ships a maintained pricing table; fall back to None on providers
    # it doesn't know about (self-hosted / niche models).
    cost_usd = None
    try:
        cost_usd = float(litellm.completion_cost(completion_response=response))
    except Exception:
        cost_usd = None

    tokens_total = None
    if usage is not None:
        # Both litellm and openai use the same key name.
        tt = usage.get("total_tokens")
        if isinstance(tt, (int, float)):
            tokens_total = int(tt)

    return {
        "content": content,
        "tool_calls": tool_calls_out,
        "usage": usage,
        "cost_usd": cost_usd,
        "latency_ms": latency_ms,
        "tokens_total": tokens_total,
        "temperature": temperature,
        "reasoning_content": reasoning_content,
    }


def _get_source_text(state: Dict[str, Any], source_id: str) -> str:
    """Extract text from a prior step's output."""
    if source_id not in state:
        raise ValueError(f"step source={source_id!r} not found; known steps: {sorted(state.keys())}")
    entry = state[source_id]
    if isinstance(entry, dict):
        if "text" not in entry:
            raise ValueError(f"step {source_id!r} output has no 'text' field (keys: {list(entry.keys())})")
        return entry["text"]
    return str(entry)


def _last_step_id(state: Dict[str, Any]) -> str:
    """The most recent step id in state (used when a step omits `source:`).
    Skips reserved keys prefixed with `__` (e.g. `__ctx__`)."""
    for k in reversed(list(state.keys())):
        if not (isinstance(k, str) and k.startswith("__")):
            return k
    raise ValueError("no user step outputs in state")


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    """Build a Dagster PartitionsDefinition from the first-class fields.

    Mirrors the pattern used by HumanApprovalGateComponent — same
    field names, same shape, same coercions (comma-string OR list for
    `partition_values`).
    """
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
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
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


def _resolve_inputs(step: dict, state: Dict[str, Any]) -> Dict[str, str]:
    """Resolve `inputs:` field into a `{port_name: text}` dict for template substitution.

    Shape (any op can use this):

        inputs:
          <port_name>: {from: <step_id>}      # read prior step's text output
          <port_name>: {literal: <value>}     # inline literal value (str-coerced)

    Returns an empty dict if `inputs:` is absent — every op falls back to
    its legacy single-source `source:` / `sources:` behavior in that case,
    so old YAML keeps working unchanged.

    This is the "typed named I/O" primitive that lets any step join from
    any number of upstream steps by port name — matches the fan-out /
    fan-in shape common in agentic-orchestration graphs (Prefect-style
    execution plans, LangGraph joins, etc.).
    """
    raw = step.get("inputs") or {}
    if not raw:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(
            f"step {step.get('id')!r}: `inputs:` must be a dict mapping port "
            f"name → {{from: <step_id>}} | {{literal: <value>}}."
        )
    resolved: Dict[str, str] = {}
    for name, spec in raw.items():
        if not isinstance(spec, dict):
            raise ValueError(
                f"step {step.get('id')!r}: input {name!r} must be "
                f"{{from: <step_id>}} or {{literal: <value>}}, got {type(spec).__name__}."
            )
        if "from" in spec:
            source_id = spec["from"]
            if source_id not in state:
                raise ValueError(
                    f"step {step.get('id')!r}: input {name!r} references "
                    f"unknown upstream step {source_id!r}. Known: {sorted(state.keys())}"
                )
            resolved[name] = _get_source_text(state, source_id)
        elif "literal" in spec:
            resolved[name] = str(spec["literal"])
        else:
            raise ValueError(
                f"step {step.get('id')!r}: input {name!r} needs `from:` or `literal:`."
            )
    return resolved


def _substitute_ports(template: str, ports: Dict[str, str]) -> str:
    """Substitute `{port_name}` placeholders in a template with resolved text.

    Uses plain str.replace (not `.format`) so `{` / `}` inside prompts
    (e.g. JSON braces) don't blow up. Ports missing from `ports` are
    left as `{port_name}` in the output — the LLM sees it as literal,
    which is a useful debugging signal when a template references a
    port name that wasn't declared.
    """
    if not template or not ports:
        return template
    for k, v in ports.items():
        template = template.replace("{" + k + "}", v)
    return template


# ── Op executors ─────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sum_cost(*results) -> Optional[float]:
    xs = [r.get("cost_usd") for r in results if r and r.get("cost_usd") is not None]
    return round(sum(xs), 6) if xs else None


def _sum_latency(*results) -> int:
    return sum((r.get("latency_ms") or 0) for r in results if r)


def _sum_tokens(*results) -> Optional[int]:
    xs = [r.get("tokens_total") for r in results if r and r.get("tokens_total") is not None]
    return sum(xs) if xs else None


def _do_llm_call(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Single LLM call.

    Two input modes (mutually compatible — inputs takes precedence for
    named substitution, source still resolves for the `{text}` fallback):

    1. Legacy single-source: `source: <step_id>` (or omit for last step) →
       prompt_template's `{text}` placeholder substitutes with that step's text.

    2. Typed named inputs: `inputs: {port_name: {from: step_id} | {literal: value}}` →
       each port name becomes a `{port_name}` placeholder in prompt_template
       AND system_prompt. Enables multi-input joins from arbitrary upstream
       steps (the "any op joins from any prior op" shape).

    prompt_template supports both — `{text}` for legacy source, `{port_name}`
    for typed inputs.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id)

    inputs = _resolve_inputs(step, state)

    # {partition_key} + {partition.<name>} were substituted upstream in
    # _run_step. Only {text} + {port_name} are per-op here.
    prompt_template = step.get("prompt_template", "{text}")
    user_prompt = prompt_template.replace("{text}", src_text)
    user_prompt = _substitute_ports(user_prompt, inputs)
    system_prompt = step.get("system_prompt")
    if system_prompt and inputs:
        system_prompt = _substitute_ports(system_prompt, inputs)

    temperature = step.get("temperature", 0.0)

    result = _completion(
        model=step["model"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=temperature,
        max_tokens=step.get("max_tokens", 2048),
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
    )
    return {
        "text": result["content"],
        "model": step["model"],
        "model_fingerprint": f"{step['model']}@t{temperature}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(result),
        "latency_ms": _sum_latency(result),
        "tokens_total": _sum_tokens(result),
        "n_llm_calls": 1,
        "usage": result["usage"],
        "prompt": user_prompt,
        "inputs_used": list(inputs.keys()) if inputs else None,
        "op": "llm_call",
    }


def _do_route(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Router picks best specialist; specialist answers.

    router: {model, api_key_env_var, [system_prompt, temperature, max_tokens]}
    specialists: [{name, description, model, [api_key_env_var, system_prompt, temperature, max_tokens]}]
    fallback: <specialist_name>  (optional)
    """
    import json

    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id)

    router = step["router"]
    specialists = step["specialists"]
    fallback = step.get("fallback")
    include_reasoning = step.get("include_reasoning", True)

    names = [s["name"] for s in specialists]
    if len(set(names)) != len(names):
        raise ValueError(f"specialist names must be unique: got {names}")
    for n in names:
        if not n.replace("_", "").isalnum():
            raise ValueError(
                f"specialist name {n!r} must be a valid identifier — router uses it as tool name."
            )
    if fallback is not None and fallback not in names:
        raise ValueError(f"fallback={fallback!r} not in specialists {names}")

    # One tool per specialist. Router calls exactly one.
    tools = []
    for s in specialists:
        params = {"type": "object", "properties": {}, "required": []}
        if include_reasoning:
            params["properties"]["reasoning"] = {
                "type": "string",
                "description": "One-sentence reason this specialist fits.",
            }
            params["required"].append("reasoning")
        tools.append({
            "type": "function",
            "function": {
                "name": s["name"],
                "description": s["description"],
                "parameters": params,
            },
        })

    router_system = router.get("system_prompt") or _default_router_prompt(specialists)
    context.log.info(f"[route:{step.get('id', '?')}] routing through {router['model']} across {len(specialists)} specialists")

    router_result = _completion(
        model=router["model"],
        system_prompt=router_system,
        user_prompt=src_text,
        api_key_env_var=router.get("api_key_env_var"),
        api_base_env_var=router.get("api_base_env_var"),
        temperature=router.get("temperature", 0.0),
        max_tokens=router.get("max_tokens", 512),
        tools=tools,
        tool_choice="required",
        reasoning_effort=router.get("reasoning_effort"),
        thinking_budget=router.get("thinking_budget"),
    )

    selected = None
    reasoning = None
    routing_source = "router_tool_call"
    if router_result["tool_calls"]:
        pick = router_result["tool_calls"][0]["name"]
        if pick in names:
            selected = pick
            if include_reasoning:
                try:
                    args = json.loads(router_result["tool_calls"][0]["arguments"] or "{}")
                    reasoning = args.get("reasoning")
                except json.JSONDecodeError:
                    reasoning = None
        else:
            context.log.warning(f"[route] router picked unknown specialist {pick!r}")

    if selected is None:
        if fallback is None:
            raise RuntimeError(f"[route] router failed to pick a valid specialist and no fallback set")
        context.log.warning(f"[route] falling back to {fallback!r}")
        selected = fallback
        routing_source = "fallback"

    specialist = next(s for s in specialists if s["name"] == selected)
    context.log.info(f"[route] selected specialist={selected} → {specialist['model']}")

    specialist_result = _completion(
        model=specialist["model"],
        system_prompt=specialist.get("system_prompt"),
        user_prompt=src_text,
        api_key_env_var=specialist.get("api_key_env_var"),
        api_base_env_var=specialist.get("api_base_env_var"),
        temperature=specialist.get("temperature", 0.0),
        max_tokens=specialist.get("max_tokens", 2048),
        reasoning_effort=specialist.get("reasoning_effort"),
        thinking_budget=specialist.get("thinking_budget"),
    )

    return {
        "text": specialist_result["content"],
        "selected_specialist": selected,
        "router_reasoning": reasoning,
        "routing_source": routing_source,
        "router_model": router["model"],
        "specialist_model": specialist["model"],
        "model_fingerprint": f"{router['model']}→{specialist['model']}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(router_result, specialist_result),
        "latency_ms": _sum_latency(router_result, specialist_result),
        "tokens_total": _sum_tokens(router_result, specialist_result),
        "n_llm_calls": 2,
        "usage": {"router": router_result["usage"], "specialist": specialist_result["usage"]},
        "op": "route",
    }


def _evaluate_condition(src_text: str, cond: Dict[str, Any]) -> bool:
    """Deterministic condition evaluator for `conditional_route`.

    Recognized predicates in `cond`:
      regex:      Python regex against src_text (re.search, case-insensitive).
      contains:   substring test (case-insensitive).
      equals:     exact match after strip().
      jsonpath:   dotted key/index path against src_text parsed as JSON.
                  e.g. `$.labels[0]` on `{"labels":["bug"]}` → "bug".
                  Non-JSON src_text → False. Pair with `equals:` / `contains:`
                  on `value:` to test the resolved node, e.g.
                  `{jsonpath: "$.priority", equals: "p0"}`.
    Exactly one comparison predicate must be present (regex | contains |
    equals | jsonpath+value).
    """
    keys = set(cond.keys())
    if "regex" in keys:
        return re.search(cond["regex"], src_text, re.IGNORECASE) is not None
    if "contains" in keys:
        return cond["contains"].lower() in src_text.lower()
    if "equals" in keys:
        return src_text.strip() == cond["equals"]
    if "jsonpath" in keys:
        try:
            payload = json.loads(src_text)
        except (json.JSONDecodeError, ValueError):
            return False
        path = cond["jsonpath"]
        if path.startswith("$."):
            path = path[2:]
        elif path.startswith("$"):
            path = path[1:].lstrip(".")
        node: Any = payload
        for part in path.split("."):
            if not part:
                continue
            # Array-index chunks: `labels[0]` → `labels` then `[0]`.
            while "[" in part and part.endswith("]"):
                base, idx = part[:-1].split("[", 1)
                if base:
                    if not isinstance(node, dict) or base not in node:
                        return False
                    node = node[base]
                try:
                    node = node[int(idx)]
                except (ValueError, IndexError, TypeError):
                    return False
                part = ""
            if part:
                if not isinstance(node, dict) or part not in node:
                    return False
                node = node[part]
        node_str = json.dumps(node) if not isinstance(node, str) else node
        if "equals" in keys:  # unreachable (already returned above) — kept for clarity
            return node_str == cond["equals"]
        if "value_equals" in keys:
            return node_str.strip() == str(cond["value_equals"])
        if "value_contains" in keys:
            return str(cond["value_contains"]).lower() in node_str.lower()
        # Bare jsonpath: truthy check on resolved node.
        return bool(node)
    raise ValueError(
        f"conditional_route: condition must contain one of "
        f"`regex` | `contains` | `equals` | `jsonpath`; got {sorted(keys)}"
    )


def _do_conditional_route(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Deterministic router — picks specialist by regex / contains / equals /
    JSON-path against upstream text (no router LLM). Then the picked
    specialist runs a normal LLM call. Sibling of `route`, but the pick
    is a code path, not a model call.

    conditions:  [{when: {regex|contains|equals|jsonpath: ...}, then: <specialist_name>}, ...]
                 (evaluated in order — first match wins)
    default:     <specialist_name>   (required — runs when no condition matches)
    specialists: [{name, description, model, [api_key_env_var, system_prompt,
                   temperature, max_tokens]}]

    Use when you want branching that's reviewable in code / cheap / testable:
    label-based triage (`labels: ["bug"]` → bug_specialist), priority tags
    (regex `p[01]` → high_priority), size-based routing, etc. When the
    signal is soft ("does this issue read as a question?"), prefer `route`.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id)

    conditions = step.get("conditions") or []
    default = step.get("default")
    specialists = step.get("specialists") or []
    if not specialists:
        raise ValueError("conditional_route requires `specialists: [...]`.")
    if not default:
        raise ValueError(
            "conditional_route requires `default: <specialist_name>` — "
            "the specialist that runs when no condition matches."
        )

    names = [s["name"] for s in specialists]
    if len(set(names)) != len(names):
        raise ValueError(f"specialist names must be unique: got {names}")
    if default not in names:
        raise ValueError(f"default={default!r} not in specialists {names}")
    for cond in conditions:
        then = cond.get("then")
        if not then:
            raise ValueError(f"conditional_route: condition {cond!r} missing `then: <specialist>`")
        if then not in names:
            raise ValueError(
                f"conditional_route: condition points at unknown specialist {then!r}. Known: {names}"
            )

    selected = None
    match_index = -1
    matched_condition: Optional[Dict[str, Any]] = None
    for i, cond in enumerate(conditions):
        when = cond.get("when") or {}
        if not when:
            raise ValueError(f"conditional_route: condition {cond!r} missing `when: {{...}}`")
        try:
            if _evaluate_condition(src_text, when):
                selected = cond["then"]
                match_index = i
                matched_condition = cond
                break
        except Exception as e:  # noqa: BLE001 — surface bad conditions clearly
            raise ValueError(
                f"conditional_route: condition #{i} ({cond!r}) failed to evaluate: {e}"
            ) from e

    routing_source = "condition_match"
    if selected is None:
        selected = default
        routing_source = "default"

    specialist = next(s for s in specialists if s["name"] == selected)
    context.log.info(
        f"[conditional_route:{step.get('id', '?')}] picked={selected!r} "
        f"via={routing_source} (condition #{match_index})"
    )

    specialist_result = _completion(
        model=specialist["model"],
        system_prompt=specialist.get("system_prompt"),
        user_prompt=src_text,
        api_key_env_var=specialist.get("api_key_env_var"),
        api_base_env_var=specialist.get("api_base_env_var"),
        temperature=specialist.get("temperature", 0.0),
        max_tokens=specialist.get("max_tokens", 2048),
        reasoning_effort=specialist.get("reasoning_effort"),
        thinking_budget=specialist.get("thinking_budget"),
    )

    return {
        "text": specialist_result["content"],
        "selected_specialist": selected,
        "routing_source": routing_source,
        "match_index": match_index,
        "matched_condition": matched_condition,
        "specialist_model": specialist["model"],
        "model_fingerprint": f"[deterministic]→{specialist['model']}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(specialist_result),
        "latency_ms": _sum_latency(specialist_result),
        "tokens_total": _sum_tokens(specialist_result),
        "n_llm_calls": 1,
        "usage": {"specialist": specialist_result["usage"]},
        "op": "conditional_route",
    }


def _do_debate(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """N proposers each write a proposal; arbitrator picks the winner.

    proposers:  [{model, [system_prompt, api_key_env_var, temperature, max_tokens]}]
    arbitrator: {model, [system_prompt, api_key_env_var, temperature, max_tokens]}
    """
    import json

    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id)

    proposers = step["proposers"]
    arbitrator = step["arbitrator"]

    if len(proposers) < 2:
        raise ValueError(f"debate requires >=2 proposers; got {len(proposers)}")

    context.log.info(f"[debate:{step.get('id', '?')}] {len(proposers)} proposers → arbitrator {arbitrator['model']}")

    proposals = []
    proposer_usage = []
    proposer_results = []
    for i, p in enumerate(proposers):
        context.log.info(f"[debate] proposer {i} ({p['model']}) writing proposal")
        r = _completion(
            model=p["model"],
            system_prompt=p.get("system_prompt"),
            user_prompt=src_text,
            api_key_env_var=p.get("api_key_env_var"),
            api_base_env_var=p.get("api_base_env_var"),
            temperature=p.get("temperature", 0.7),
            max_tokens=p.get("max_tokens", 2048),
            reasoning_effort=p.get("reasoning_effort"),
            thinking_budget=p.get("thinking_budget"),
        )
        proposals.append({"index": i, "model": p["model"], "text": r["content"]})
        proposer_usage.append(r["usage"])
        proposer_results.append(r)

    # Arbitrator picks via function call (one tool per proposal index).
    tools = []
    for i in range(len(proposals)):
        tools.append({
            "type": "function",
            "function": {
                "name": f"pick_{i}",
                "description": f"Select proposal {i} as the winner.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "reasoning": {"type": "string", "description": "Why this proposal wins."}
                    },
                    "required": ["reasoning"],
                },
            },
        })

    arb_prompt_parts = [f"Original question / context:\n{src_text}\n\nProposals to judge:\n"]
    for p in proposals:
        arb_prompt_parts.append(f"\n--- Proposal {p['index']} (model={p['model']}) ---\n{p['text']}\n")
    arb_prompt_parts.append(
        f"\nCall exactly one of pick_0..pick_{len(proposals) - 1} with a one-sentence reasoning."
    )
    arb_prompt = "".join(arb_prompt_parts)

    arb_system = arbitrator.get("system_prompt") or (
        "You are an impartial judge. Read the original question and every proposal, "
        "then pick the strongest by calling exactly one of the provided pick_N tools."
    )

    arb_result = _completion(
        model=arbitrator["model"],
        system_prompt=arb_system,
        user_prompt=arb_prompt,
        api_key_env_var=arbitrator.get("api_key_env_var"),
        api_base_env_var=arbitrator.get("api_base_env_var"),
        temperature=arbitrator.get("temperature", 0.0),
        max_tokens=arbitrator.get("max_tokens", 512),
        tools=tools,
        tool_choice="required",
        reasoning_effort=arbitrator.get("reasoning_effort"),
        thinking_budget=arbitrator.get("thinking_budget"),
    )

    winner_index = None
    arb_reasoning = None
    if arb_result["tool_calls"]:
        tool_name = arb_result["tool_calls"][0]["name"]
        if tool_name.startswith("pick_"):
            try:
                winner_index = int(tool_name.split("_", 1)[1])
                args = json.loads(arb_result["tool_calls"][0]["arguments"] or "{}")
                arb_reasoning = args.get("reasoning")
            except (ValueError, json.JSONDecodeError):
                winner_index = None

    if winner_index is None or winner_index not in range(len(proposals)):
        context.log.warning("[debate] arbitrator failed to pick; defaulting to proposal 0")
        winner_index = 0
        arb_reasoning = arb_reasoning or "(arbitrator failed to pick, defaulted to proposal 0)"

    winner = proposals[winner_index]
    context.log.info(f"[debate] winner=proposal {winner_index} ({winner['model']})")

    proposer_models = ",".join(p["model"] for p in proposers)
    return {
        "text": winner["text"],
        "winner_index": winner_index,
        "winner_model": winner["model"],
        "arbitrator_reasoning": arb_reasoning,
        "all_proposals": proposals,
        "arbitrator_model": arbitrator["model"],
        "model_fingerprint": f"[{proposer_models}]→{arbitrator['model']}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(*proposer_results, arb_result),
        "latency_ms": _sum_latency(*proposer_results, arb_result),
        "tokens_total": _sum_tokens(*proposer_results, arb_result),
        "n_llm_calls": len(proposer_results) + 1,
        "usage": {"proposers": proposer_usage, "arbitrator": arb_result["usage"]},
        "op": "debate",
    }


_SCORE_RE = re.compile(r"SCORE\s*[:=]\s*(\d+(?:\.\d+)?)\s*/\s*100", re.IGNORECASE)


def _extract_critic_score(text: str) -> Optional[float]:
    """Pull `SCORE: N/100` (case-insensitive, tolerant of `SCORE=` / whitespace)
    out of a critic response. Returns None if not present or unparseable —
    callers treat "no score" as "keep iterating"."""
    if not text:
        return None
    m = _SCORE_RE.search(text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _do_critique_loop(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Drafter writes; critic reviews; drafter revises. Repeat.

    drafter:   {model, [system_prompt, api_key_env_var, temperature, max_tokens]}
    critic:    {model, [system_prompt, api_key_env_var, temperature, max_tokens]}
    iterations: 2   (number of critique/revise cycles; >=1). Hard cap even when
                    `until_score_gte:` is set — think of it as the timeout.
    until_score_gte: Optional[float] (0-100). When set, appends a scoring
                    instruction to the critic's system prompt; after each
                    critique, extracts `SCORE: N/100` and stops early (skipping
                    the revise step) when N >= threshold. The current draft is
                    already good enough. Preserves `iterations` as an upper bound.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id)

    drafter = step["drafter"]
    critic = step["critic"]
    iterations = int(step.get("iterations", 2))
    if iterations < 1:
        raise ValueError(f"critique_loop iterations must be >=1; got {iterations}")

    until_score_gte = step.get("until_score_gte")
    if until_score_gte is not None:
        try:
            until_score_gte = float(until_score_gte)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"critique_loop until_score_gte must be a number (0-100); got {step['until_score_gte']!r}"
            ) from e
        if not (0 <= until_score_gte <= 100):
            raise ValueError(
                f"critique_loop until_score_gte must be between 0 and 100; got {until_score_gte}"
            )

    all_llm_results = []
    stop_reason = "iterations_exhausted"
    final_score: Optional[float] = None

    context.log.info(
        f"[critique_loop:{step.get('id', '?')}] drafter={drafter['model']} "
        f"critic={critic['model']} iterations={iterations}"
    )

    # Initial draft.
    draft_result = _completion(
        model=drafter["model"],
        system_prompt=drafter.get("system_prompt"),
        user_prompt=src_text,
        api_key_env_var=drafter.get("api_key_env_var"),
        api_base_env_var=drafter.get("api_base_env_var"),
        temperature=drafter.get("temperature", 0.0),
        max_tokens=drafter.get("max_tokens", 2048),
        reasoning_effort=drafter.get("reasoning_effort"),
        thinking_budget=drafter.get("thinking_budget"),
    )
    current_draft = draft_result["content"]
    history = [{"iteration": 0, "phase": "initial_draft", "text": current_draft}]
    drafter_usage = [draft_result["usage"]]
    critic_usage = []
    all_llm_results.append(draft_result)

    for i in range(iterations):
        # Critic reviews.
        base_system = (
            critic.get("system_prompt")
            or "You are a careful reviewer. Give short, specific, actionable critique."
        )
        if until_score_gte is not None:
            critic_system = (
                base_system
                + f"\n\nEnd your response with a line exactly of the form `SCORE: N/100` where N is your quality rating "
                + f"of the current draft (0-100). Iteration stops early when N >= {until_score_gte:g}."
            )
        else:
            critic_system = base_system

        critic_prompt = (
            f"Original task:\n{src_text}\n\n"
            f"Current draft:\n{current_draft}\n\n"
            f"Provide specific, actionable critique. Focus on what to improve, not what's good. "
            f"If the draft is already excellent, say so explicitly."
        )
        critique_result = _completion(
            model=critic["model"],
            system_prompt=critic_system,
            user_prompt=critic_prompt,
            api_key_env_var=critic.get("api_key_env_var"),
            api_base_env_var=critic.get("api_base_env_var"),
            temperature=critic.get("temperature", 0.0),
            max_tokens=critic.get("max_tokens", 1024),
            reasoning_effort=critic.get("reasoning_effort"),
            thinking_budget=critic.get("thinking_budget"),
        )
        critique = critique_result["content"]
        entry = {"iteration": i + 1, "phase": "critique", "text": critique}
        if until_score_gte is not None:
            score = _extract_critic_score(critique)
            if score is not None:
                entry["score"] = score
                final_score = score
            else:
                context.log.warning(
                    f"[critique_loop iter={i + 1}] until_score_gte={until_score_gte} set but critic omitted `SCORE: N/100`; continuing"
                )
        history.append(entry)
        critic_usage.append(critique_result["usage"])
        all_llm_results.append(critique_result)

        # Early termination: critic gave a high-enough score → don't revise;
        # the current draft is already the answer.
        if until_score_gte is not None and final_score is not None and final_score >= until_score_gte:
            stop_reason = f"score_gte_threshold (score={final_score:g} >= {until_score_gte:g})"
            context.log.info(
                f"[critique_loop iter={i + 1}] stopping early: critic score {final_score:g}/100 >= {until_score_gte:g}/100"
            )
            break

        # Drafter revises.
        revise_prompt = (
            f"Original task:\n{src_text}\n\n"
            f"Your previous draft:\n{current_draft}\n\n"
            f"Reviewer critique:\n{critique}\n\n"
            f"Revise the draft to address the critique. Output only the revised draft."
        )
        revise_result = _completion(
            model=drafter["model"],
            system_prompt=drafter.get("system_prompt"),
            user_prompt=revise_prompt,
            api_key_env_var=drafter.get("api_key_env_var"),
            api_base_env_var=drafter.get("api_base_env_var"),
            temperature=drafter.get("temperature", 0.0),
            max_tokens=drafter.get("max_tokens", 2048),
            reasoning_effort=drafter.get("reasoning_effort"),
            thinking_budget=drafter.get("thinking_budget"),
        )
        current_draft = revise_result["content"]
        history.append({"iteration": i + 1, "phase": "revised_draft", "text": current_draft})
        drafter_usage.append(revise_result["usage"])
        all_llm_results.append(revise_result)

    iterations_done = sum(1 for h in history if h.get("phase") == "critique")
    return {
        "text": current_draft,
        "iterations_done": iterations_done,
        "iterations_max": iterations,
        "stop_reason": stop_reason,
        "final_score": final_score,
        "until_score_gte": until_score_gte,
        "history": history,
        "drafter_model": drafter["model"],
        "critic_model": critic["model"],
        "model_fingerprint": f"{drafter['model']}//{critic['model']}×{iterations_done}"
            + (f"@score>={until_score_gte:g}" if until_score_gte is not None else ""),
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(*all_llm_results),
        "latency_ms": _sum_latency(*all_llm_results),
        "tokens_total": _sum_tokens(*all_llm_results),
        "n_llm_calls": len(all_llm_results),
        "usage": {"drafter": drafter_usage, "critic": critic_usage},
        "op": "critique_loop",
    }


def _do_synthesize(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Merge multiple upstream step outputs into one via an LLM.

    Two input modes (pick one):

    1. Legacy list: `sources: [<step_id>, ...]` → each becomes a labeled
       section, prompt_template's `{combined}` + `{n_sources}` substitute.

    2. Typed named inputs: `inputs: {port_name: {from: step_id} | {literal: value}}` →
       each port name becomes a `{port_name}` placeholder in prompt_template
       AND system_prompt. Named join — clearer than positional `sources:`
       when the synthesizer needs to reason about specific upstream roles.

    Shared: model, [system_prompt, api_key_env_var, temperature, max_tokens].
    """
    materialized_at = _now_iso()
    inputs = _resolve_inputs(step, state)
    source_ids = step.get("sources") or []
    if not inputs and not source_ids:
        raise ValueError(
            "synthesize needs either `inputs: {port: {from: id}, ...}` or "
            "`sources: [step_id, ...]` (>=1 upstream)."
        )

    temperature = step.get("temperature", 0.0)

    if inputs:
        prompt_template = step.get(
            "prompt_template",
            # Default: label each input with its port name
            "\n\n".join(f"## {k}\n{{{k}}}" for k in inputs.keys()),
        )
        user_prompt = _substitute_ports(prompt_template, inputs)
        system_prompt = step.get("system_prompt")
        if system_prompt:
            system_prompt = _substitute_ports(system_prompt, inputs)
    else:
        parts = []
        for sid in source_ids:
            parts.append(f"--- {sid} ---\n{_get_source_text(state, sid)}")
        combined = "\n\n".join(parts)

        prompt_template = step.get(
            "prompt_template",
            "You have {n_sources} labeled sections below. Synthesize them into a single coherent response.\n\n{combined}",
        )
        user_prompt = prompt_template.replace("{combined}", combined).replace("{n_sources}", str(len(source_ids)))
        system_prompt = step.get("system_prompt")

    result = _completion(
        model=step["model"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=temperature,
        max_tokens=step.get("max_tokens", 4096),
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
    )

    return {
        "text": result["content"],
        "sources_used": list(source_ids),
        "inputs_used": list(inputs.keys()) if inputs else None,
        "model": step["model"],
        "model_fingerprint": f"{step['model']}@t{temperature}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(result),
        "latency_ms": _sum_latency(result),
        "tokens_total": _sum_tokens(result),
        "n_llm_calls": 1,
        "usage": result["usage"],
        "op": "synthesize",
    }


# ── mcp_call op ──────────────────────────────────────────────────────

def _resolve_mcp_headers(cfg: Dict[str, Any], server_name: str) -> Dict[str, str]:
    """http/sse headers: `headers` = literal, `headers_env` = deferred env lookup."""
    import os
    headers: Dict[str, str] = {}
    for k, v in (cfg.get("headers") or {}).items():
        headers[k] = str(v)
    for header_name, env_var in (cfg.get("headers_env") or {}).items():
        val = os.environ.get(env_var)
        if val is None:
            raise ValueError(
                f"MCP server {server_name!r} references env var {env_var!r} for header "
                f"{header_name!r}, but it isn't set."
            )
        headers[header_name] = val
    return headers


async def _call_mcp_tool_async(
    *,
    log,
    server_cfg: Dict[str, Any],
    tool_name: str,
    tool_args: Dict[str, Any],
    parse_as: str,
) -> Dict[str, Any]:
    """Async MCP tool call — supports stdio / http / sse transports.

    Mirrors the helper in `mcp_tool_picker/component.py` — kept local to
    keep this component's imports self-contained.
    """
    from contextlib import AsyncExitStack

    name = server_cfg.get("name") or "server"
    transport = server_cfg.get("type", "stdio")

    async with AsyncExitStack() as stack:
        if transport == "stdio":
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client

            cmd = server_cfg.get("command") or []
            if not cmd:
                raise ValueError(f"MCP server {name!r} is stdio but command is empty.")
            if isinstance(cmd, str):
                # Common LLM-composed mistake: `command: "npx -y @foo"` (string)
                # rather than the required list form. Fail fast with a clear
                # message rather than letting `cmd[0]` return the letter 'n'
                # and confusing the eventual FileNotFoundError.
                raise ValueError(
                    f"MCP server {name!r} `command:` must be a LIST of "
                    f"strings, got a bare string: {cmd!r}. Use YAML list "
                    f"syntax: `command: [npx, -y, \"@modelcontextprotocol/server-github\"]` "
                    f"or the block form: `command:\\n  - npx\\n  - -y\\n  - \"@...\"`."
                )
            # Forward the full parent-process environment to the stdio
            # subprocess. Without this, the mcp Python library applies a
            # tight security whitelist (HOME/LOGNAME/PATH/SHELL/TERM/USER
            # on Unix) and silently strips everything else — including
            # `GITHUB_PERSONAL_ACCESS_TOKEN`, `OPENAI_API_KEY`, and any
            # other var the MCP server needs. YAML `env:` values override
            # inherited ones (last-write-wins).
            _base_env = dict(os.environ)
            _base_env.update(server_cfg.get("env") or {})
            params = StdioServerParameters(
                command=cmd[0], args=list(cmd[1:]), env=_base_env
            )
            log.info(f"[mcp:{name}] starting stdio server: {' '.join(cmd)}")
            read, write = await stack.enter_async_context(stdio_client(params))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        elif transport in ("http", "streamable_http", "streamable-http"):
            from mcp import ClientSession
            from mcp.client.streamable_http import streamablehttp_client

            url = server_cfg.get("url")
            if not url:
                raise ValueError(f"MCP server {name!r} is http but url is empty.")
            headers = _resolve_mcp_headers(server_cfg, name)
            read, write, _sid = await stack.enter_async_context(
                streamablehttp_client(url, headers=headers or None)
            )
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        elif transport == "sse":
            from mcp import ClientSession
            from mcp.client.sse import sse_client

            url = server_cfg.get("url")
            if not url:
                raise ValueError(f"MCP server {name!r} is sse but url is empty.")
            headers = _resolve_mcp_headers(server_cfg, name)
            read, write = await stack.enter_async_context(
                sse_client(url, headers=headers or None)
            )
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        elif transport == "fastmcp":
            # FastMCP v2 client — auto-transport detection (URL or
            # `mcp.json`-style config dict), first-class auth (bearer +
            # OAuth), better resilience than the raw MCP SDK. Preferred for
            # remote / production MCP servers; the raw http/sse transports
            # above remain available for compat.
            try:
                from fastmcp import Client as FastMCPClient
            except ImportError as e:
                raise ImportError(
                    "type: fastmcp requires the fastmcp package: "
                    "pip install 'fastmcp>=2.0'"
                ) from e

            url = server_cfg.get("url")
            config = server_cfg.get("config")  # optional inline mcp.json-style dict
            headers = _resolve_mcp_headers(server_cfg, name)

            # Optional bearer-token auth: `bearer_env: MCP_TOKEN` reads the
            # env var and adds `Authorization: Bearer <value>` at call time.
            bearer_env = server_cfg.get("bearer_env")
            if bearer_env and os.environ.get(bearer_env):
                headers = {
                    **(headers or {}),
                    "Authorization": f"Bearer {os.environ[bearer_env]}",
                }

            if config:
                client_ctx = FastMCPClient(config)
            elif url:
                if headers:
                    from fastmcp.client.transports import StreamableHttpTransport
                    transport_obj = StreamableHttpTransport(url=url, headers=headers)
                    client_ctx = FastMCPClient(transport_obj)
                else:
                    client_ctx = FastMCPClient(url)
            else:
                raise ValueError(
                    f"MCP server {name!r} is fastmcp but neither `url` nor "
                    f"`config` is set."
                )

            log.info(f"[mcp:{name}] fastmcp client connecting to {url or 'inline config'}")
            client = await stack.enter_async_context(client_ctx)

            # Shim so the shared `session.call_tool(...)` path below works —
            # FastMCP's `Client` exposes call_tool directly, no ClientSession.
            class _FastMCPShim:
                async def call_tool(self, tool_name, tool_args):
                    return await client.call_tool(tool_name, tool_args)
            session = _FastMCPShim()
        else:
            raise ValueError(f"MCP server {name!r} has unknown transport: {transport!r}")

        call_result = await session.call_tool(tool_name, tool_args)
        parts = []
        for c in call_result.content:
            text = getattr(c, "text", None)
            parts.append(text if text is not None else str(c))
        raw = "\n".join(parts) if parts else ""
        is_error = bool(getattr(call_result, "isError", False))

    if parse_as == "text":
        return {"value": raw, "raw": raw, "is_error": is_error, "kind": "text"}
    if parse_as in ("json", "auto"):
        try:
            value = json.loads(raw)
            return {"value": value, "raw": raw, "is_error": is_error, "kind": "json"}
        except (json.JSONDecodeError, ValueError):
            if parse_as == "json":
                raise
            return {"value": raw, "raw": raw, "is_error": is_error, "kind": "text"}
    return {"value": raw, "raw": raw, "is_error": is_error, "kind": "text"}


def _do_mcp_call(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Direct MCP tool call — no LLM, deterministic step in the pipeline.

    server: {name, type: stdio|http|sse|fastmcp, command|url, env|headers|headers_env}
    mcp_tool_name: <tool name as the MCP server exposes it>
    tool_args: {arg_name: value}      (string values are `{text}`-templated against source)
    parse_as: 'auto' | 'json' | 'text'  (default 'auto')

    Use for the "fetch grounding data before the LLM specialists fan out"
    pattern — swap a URL/file `source:` for a first-class MCP step so the
    fetch is a Dagster asset with lineage + metadata.
    """
    import asyncio
    import os

    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id) if source_id else ""

    inputs = _resolve_inputs(step, state)

    server_cfg = step.get("server") or {}
    if not server_cfg:
        raise ValueError("mcp_call requires `server: {name, type, ...}`")
    tool_name = step.get("mcp_tool_name")
    if not tool_name:
        raise ValueError("mcp_call requires `mcp_tool_name: <name-on-the-server>`")

    # {partition_key} + {partition.<name>} were already substituted deep in
    # `step` at dispatch time (see _run_step). {text} + {port_name} are per-op.
    raw_args = step.get("tool_args") or {}
    resolved_args: Dict[str, Any] = {}
    for k, v in raw_args.items():
        if isinstance(v, str):
            v = v.replace("{text}", src_text)
            v = _substitute_ports(v, inputs)
            resolved_args[k] = v
        else:
            resolved_args[k] = v

    parse_as = step.get("parse_as", "auto")

    context.log.info(
        f"[mcp_call:{step.get('id')}] tool={tool_name} args={resolved_args}"
    )

    t0 = time.time()
    try:
        result = asyncio.run(
            _call_mcp_tool_async(
                log=context.log,
                server_cfg=server_cfg,
                tool_name=tool_name,
                tool_args=resolved_args,
                parse_as=parse_as,
            )
        )
    except BaseExceptionGroup as eg:  # noqa: F821 (py311+)
        # Recursively unwrap nested anyio ExceptionGroups (mcp lib nests
        # session-level and stdio_client-level task groups) so the real
        # underlying error — subprocess exit, MCP protocol error, tool-arg
        # validation — surfaces in the Dagster UI instead of a generic
        # "unhandled errors in a TaskGroup".
        def _first_leaf(exc):
            while isinstance(exc, BaseExceptionGroup) and exc.exceptions:
                exc = exc.exceptions[0]
            return exc

        inner = _first_leaf(eg)
        context.log.error(
            f"[mcp_call:{step.get('id')}] failed: {type(inner).__name__}: {inner}"
        )
        raise inner from eg
    latency_ms = int((time.time() - t0) * 1000)

    # Stringify JSON value so downstream steps' `source: <this step id>`
    # can consume it as text.
    if result["kind"] == "json":
        text = json.dumps(result["value"], indent=2, default=str)
    else:
        text = result["raw"]

    if result["is_error"]:
        context.log.warning(
            f"[mcp_call:{step.get('id')}] server returned isError=True; treating as failure"
        )

    return {
        "text": text,
        "raw": result["raw"],
        "value": result["value"],
        "kind": result["kind"],
        "is_error": result["is_error"],
        "tool_name": tool_name,
        "server_name": server_cfg.get("name") or "server",
        "transport": server_cfg.get("type", "stdio"),
        "tool_args_resolved": resolved_args,
        "materialized_at": materialized_at,
        "latency_ms": latency_ms,
        "op": "mcp_call",
    }


# ── tool_use_loop op ────────────────────────────────────────────────
#
# Open-ended tool-use loop — LLM picks a tool, tool runs, LLM sees the
# result, picks the next tool, ..., emits a final answer. Bounded by
# `max_iterations`. Tools come from MCP servers declared on the step.
#
# This is the shape LangGraph is known for, done as a first-class Dagster
# asset: one asset materializes, containing the final answer + the full
# tool-call trajectory in metadata. Cost/latency/tokens are rolled up
# across every LLM call + tool call in the loop.


async def _discover_mcp_tools_async(
    log, mcp_servers: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """For each configured MCP server, connect + list_tools() + capture
    the JSON schema for each. Returns a flat list of
    `{server, name, description, input_schema}` — the `server` field lets
    us route tool calls back to the right MCP client during the loop.
    """
    from contextlib import AsyncExitStack
    tools: List[Dict[str, Any]] = []
    for server_cfg in mcp_servers:
        name = server_cfg.get("name") or "server"
        transport = server_cfg.get("type", "stdio")
        log.info(f"[tool_use_loop] discovering tools on {name!r} ({transport})")

        async with AsyncExitStack() as stack:
            # Reuse the same connection paths as _call_mcp_tool_async —
            # copy-paste rather than refactor since this helper is only
            # for the initial discovery step (one call per server, at
            # loop start).
            if transport == "stdio":
                from mcp import ClientSession, StdioServerParameters
                from mcp.client.stdio import stdio_client
                cmd = server_cfg.get("command") or []
                if not cmd or isinstance(cmd, str):
                    raise ValueError(
                        f"MCP server {name!r} stdio needs `command: [...]` (LIST of strings)."
                    )
                _base_env = dict(os.environ)
                _base_env.update(server_cfg.get("env") or {})
                params = StdioServerParameters(
                    command=cmd[0], args=list(cmd[1:]), env=_base_env
                )
                read, write = await stack.enter_async_context(stdio_client(params))
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            elif transport in ("http", "streamable_http", "streamable-http"):
                from mcp import ClientSession
                from mcp.client.streamable_http import streamablehttp_client
                url = server_cfg.get("url")
                if not url:
                    raise ValueError(f"MCP server {name!r} http needs url.")
                headers = _resolve_mcp_headers(server_cfg, name)
                read, write, _sid = await stack.enter_async_context(
                    streamablehttp_client(url, headers=headers or None)
                )
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            elif transport == "sse":
                from mcp import ClientSession
                from mcp.client.sse import sse_client
                url = server_cfg.get("url")
                if not url:
                    raise ValueError(f"MCP server {name!r} sse needs url.")
                headers = _resolve_mcp_headers(server_cfg, name)
                read, write = await stack.enter_async_context(
                    sse_client(url, headers=headers or None)
                )
                session = await stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
            elif transport == "fastmcp":
                try:
                    from fastmcp import Client as FastMCPClient
                except ImportError as e:
                    raise ImportError(
                        "type: fastmcp requires the fastmcp package: pip install 'fastmcp>=2.0'"
                    ) from e
                url = server_cfg.get("url")
                config = server_cfg.get("config")
                headers = _resolve_mcp_headers(server_cfg, name)
                bearer_env = server_cfg.get("bearer_env")
                if bearer_env and os.environ.get(bearer_env):
                    headers = {**(headers or {}), "Authorization": f"Bearer {os.environ[bearer_env]}"}
                if config:
                    client_ctx = FastMCPClient(config)
                elif url:
                    if headers:
                        from fastmcp.client.transports import StreamableHttpTransport
                        client_ctx = FastMCPClient(StreamableHttpTransport(url=url, headers=headers))
                    else:
                        client_ctx = FastMCPClient(url)
                else:
                    raise ValueError(f"MCP server {name!r} fastmcp needs url or config.")
                client = await stack.enter_async_context(client_ctx)

                class _FastMCPShim:
                    async def list_tools(_self):
                        return await client.list_tools()
                session = _FastMCPShim()
            else:
                raise ValueError(f"MCP server {name!r} unknown transport: {transport!r}")

            tools_result = await session.list_tools()
            # `.tools` on ListToolsResult is a list of Tool objects with
            # `.name`, `.description`, `.inputSchema`. FastMCP variants
            # can return a bare list.
            listed = getattr(tools_result, "tools", None) or tools_result
            for t in listed:
                tools.append({
                    "server": name,
                    "name": getattr(t, "name", None) or t["name"],
                    "description": getattr(t, "description", None) or t.get("description", ""),
                    "input_schema": (
                        getattr(t, "inputSchema", None)
                        or t.get("inputSchema")
                        or {"type": "object", "properties": {}}
                    ),
                })
    return tools


def _mcp_tools_to_openai_schema(
    mcp_tools: List[Dict[str, Any]], allowed: Optional[List[str]]
) -> List[Dict[str, Any]]:
    """MCP tool defs → OpenAI-style tool_call schema. Namespaces the tool
    name with `{server}__{name}` so the loop can route the call back
    without ambiguity when two servers expose tools of the same name."""
    schemas = []
    for t in mcp_tools:
        if allowed and t["name"] not in allowed:
            continue
        namespaced = f"{t['server']}__{t['name']}"
        schemas.append({
            "type": "function",
            "function": {
                "name": namespaced,
                "description": t["description"] or "",
                "parameters": t["input_schema"] or {"type": "object", "properties": {}},
            },
        })
    return schemas


def _do_tool_use_loop(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Open-ended tool-use loop. LLM picks tools, executes them via MCP,
    sees results, picks next tool — bounded by `max_iterations` OR the
    LLM emitting a final answer with no tool call OR the LLM calling
    the synthetic `finalize` tool.

    Emits ONE Dagster asset with the final answer text in `text` +
    the full tool-call trajectory in `tool_call_trace` metadata. Cost /
    latency / tokens are aggregated across all LLM calls + tool calls
    in the loop.

    Config:
      model, api_key_env_var: LLM the agent runs on
      mcp_servers: list of MCP server configs (same shape as mcp_call.server)
      max_iterations: hard cap on loop iterations (default 10)
      system_prompt: agent instructions
      allowed_tools: optional allowlist (defaults to all discovered tools)
      finalize_tool_name: synthetic tool name the LLM calls when done
                         (default "finalize")
    """
    import asyncio
    import time as _time

    try:
        import litellm
    except ImportError:
        raise ImportError("agentic_pipeline tool_use_loop requires litellm.")
    litellm.drop_params = True

    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    model = step["model"]
    api_key_env_var = step.get("api_key_env_var")
    api_base_env_var = step.get("api_base_env_var")
    max_iterations = int(step.get("max_iterations", 10))
    temperature = step.get("temperature", 0.0)
    max_tokens_per_call = step.get("max_tokens", 2048)
    reasoning_effort = step.get("reasoning_effort")
    thinking_budget = step.get("thinking_budget")
    finalize_name = step.get("finalize_tool_name", "finalize")

    system_prompt = step.get("system_prompt") or (
        "You are a tool-using agent. You have access to MCP tools. "
        "Call tools to gather information; when you have enough to answer, "
        f"call the special `{finalize_name}` tool with your final answer. "
        "Do not answer without calling `finalize` — the caller only sees "
        "what you pass to `finalize`."
    )
    system_prompt = _substitute_ports(system_prompt, inputs)

    user_prompt = step.get("prompt_template", "{text}").replace("{text}", src_text)
    user_prompt = _substitute_ports(user_prompt, inputs)

    mcp_servers = step.get("mcp_servers") or []
    if not mcp_servers:
        raise ValueError("tool_use_loop requires `mcp_servers: [...]`.")

    context.log.info(
        f"[tool_use_loop:{step.get('id', '?')}] discovering tools across "
        f"{len(mcp_servers)} MCP server(s)…"
    )
    mcp_tools = asyncio.run(_discover_mcp_tools_async(context.log, mcp_servers))
    allowed = step.get("allowed_tools")
    tool_schemas = _mcp_tools_to_openai_schema(mcp_tools, allowed)
    # Synthetic finalize tool — LLM calls this when done.
    tool_schemas.append({
        "type": "function",
        "function": {
            "name": finalize_name,
            "description": "Return your final answer to the caller and end the loop. Call this when you have enough information.",
            "parameters": {
                "type": "object",
                "properties": {
                    "answer": {"type": "string", "description": "The final answer text."}
                },
                "required": ["answer"],
            },
        },
    })
    # Index tools by their namespaced name for O(1) lookup during the loop.
    tool_index = {f"{t['server']}__{t['name']}": t for t in mcp_tools}
    if allowed:
        tool_index = {k: v for k, v in tool_index.items() if v["name"] in allowed}

    context.log.info(
        f"[tool_use_loop:{step.get('id', '?')}] {len(tool_index)} tool(s) "
        f"available: {sorted(tool_index.keys())[:10]}"
        + (" …" if len(tool_index) > 10 else "")
    )

    # Message history — accumulates across iterations.
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    trajectory: List[Dict[str, Any]] = []
    total_cost_usd = 0.0
    total_llm_latency_ms = 0
    total_tool_latency_ms = 0
    total_tokens_in = 0
    total_tokens_out = 0
    n_llm_calls = 0
    n_tool_calls = 0
    final_answer: Optional[str] = None
    stop_reason = "max_iterations"

    for iteration in range(1, max_iterations + 1):
        kwargs: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens_per_call,
            "tools": tool_schemas,
            "tool_choice": "auto",
        }
        # Provider-aware reasoning-param routing (see _completion()).
        _ml = model.lower()
        _is_openai_ish = _ml.startswith(("gpt-", "o1", "o3", "o4", "openai/", "azure/", "groq/"))
        _is_gemini = _ml.startswith(("gemini/", "google/", "vertex_ai/gemini"))
        _is_anthropic = (
            "claude" in _ml or _ml.startswith(("anthropic/", "bedrock/anthropic."))
        )
        if reasoning_effort is not None and (_is_openai_ish or _is_gemini):
            kwargs["reasoning_effort"] = reasoning_effort
        if thinking_budget is not None:
            if _is_gemini:
                kwargs["thinking_budget"] = int(thinking_budget)
            elif _is_anthropic:
                kwargs["thinking"] = {"type": "enabled", "budget_tokens": int(thinking_budget)}
        if api_key_env_var and os.environ.get(api_key_env_var):
            kwargs["api_key"] = os.environ[api_key_env_var]
        if api_base_env_var and os.environ.get(api_base_env_var):
            kwargs["api_base"] = os.environ[api_base_env_var]

        t0 = _time.time()
        response = litellm.completion(**kwargs)
        llm_ms = int((_time.time() - t0) * 1000)
        n_llm_calls += 1
        total_llm_latency_ms += llm_ms
        try:
            iter_cost = float(litellm.completion_cost(completion_response=response))
            total_cost_usd += iter_cost
        except Exception:  # noqa: BLE001
            pass
        usage = getattr(response, "usage", None) or {}
        total_tokens_in += (
            getattr(usage, "prompt_tokens", 0)
            or (usage.get("prompt_tokens", 0) if isinstance(usage, dict) else 0)
        )
        total_tokens_out += (
            getattr(usage, "completion_tokens", 0)
            or (usage.get("completion_tokens", 0) if isinstance(usage, dict) else 0)
        )

        msg = response.choices[0].message
        tool_calls = getattr(msg, "tool_calls", None) or []

        if not tool_calls:
            # LLM emitted content without a tool call — treat as final answer.
            content = msg.content or ""
            trajectory.append({
                "iteration": iteration,
                "phase": "final_answer_no_tool",
                "text": content,
                "llm_latency_ms": llm_ms,
            })
            final_answer = content
            stop_reason = "final_answer_no_tool"
            context.log.info(
                f"[tool_use_loop iter={iteration}] LLM emitted final answer without tool call ({len(content)} chars); stopping."
            )
            break

        # Append the assistant's tool-call message to history so the LLM
        # sees what it just did on the next iteration.
        messages.append({
            "role": "assistant",
            "content": msg.content or "",
            "tool_calls": [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                }
                for tc in tool_calls
            ],
        })

        # Handle each tool call in this iteration.
        # (LLMs typically emit ONE tool call per iteration in agent loops;
        # we handle N to be safe.)
        done_this_iter = False
        for tc in tool_calls:
            tool_name = tc.function.name
            try:
                tool_args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                tool_args = {"_raw_args": tc.function.arguments}

            # finalize → capture answer + stop
            if tool_name == finalize_name:
                answer = tool_args.get("answer", "")
                trajectory.append({
                    "iteration": iteration,
                    "phase": "finalize",
                    "tool_name": finalize_name,
                    "tool_args": tool_args,
                    "text": answer,
                    "llm_latency_ms": llm_ms,
                })
                final_answer = answer
                stop_reason = "finalize_called"
                done_this_iter = True
                context.log.info(
                    f"[tool_use_loop iter={iteration}] finalize called ({len(answer)} chars); stopping."
                )
                break

            # Real MCP tool → route to correct server + call.
            tool_meta = tool_index.get(tool_name)
            if tool_meta is None:
                error_msg = f"unknown tool: {tool_name!r}"
                trajectory.append({
                    "iteration": iteration,
                    "phase": "tool_call_error",
                    "tool_name": tool_name,
                    "tool_args": tool_args,
                    "error": error_msg,
                })
                messages.append({
                    "role": "tool", "tool_call_id": tc.id, "content": f"ERROR: {error_msg}",
                })
                n_tool_calls += 1
                continue

            # Find the server_cfg for this tool + invoke via existing helper.
            server_cfg = next(
                (s for s in mcp_servers if s.get("name") == tool_meta["server"]),
                None,
            )
            if server_cfg is None:
                error_msg = f"internal: server config missing for tool {tool_name!r}"
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": f"ERROR: {error_msg}"})
                trajectory.append({
                    "iteration": iteration, "phase": "tool_call_error",
                    "tool_name": tool_name, "tool_args": tool_args, "error": error_msg,
                })
                continue

            t_tool = _time.time()
            try:
                tool_result = asyncio.run(_call_mcp_tool_async(
                    log=context.log,
                    server_cfg=server_cfg,
                    tool_name=tool_meta["name"],  # un-namespaced real name
                    tool_args=tool_args,
                    parse_as="auto",
                ))
                tool_ms = int((_time.time() - t_tool) * 1000)
                total_tool_latency_ms += tool_ms
                n_tool_calls += 1

                # Feed the result back to the LLM as a tool message.
                result_text = tool_result.get("raw", "")
                messages.append({
                    "role": "tool", "tool_call_id": tc.id, "content": result_text[:20000],
                })
                trajectory.append({
                    "iteration": iteration,
                    "phase": "tool_call",
                    "tool_name": tool_name,
                    "tool_args": tool_args,
                    "tool_result_preview": result_text[:600],
                    "tool_result_kind": tool_result.get("kind"),
                    "tool_latency_ms": tool_ms,
                    "llm_latency_ms": llm_ms,
                })
                context.log.info(
                    f"[tool_use_loop iter={iteration}] {tool_name}({str(tool_args)[:80]}) → "
                    f"{tool_ms}ms {len(result_text)} chars"
                )
            except BaseExceptionGroup as eg:  # noqa: F821 (py311+)
                def _first_leaf(exc):
                    while isinstance(exc, BaseExceptionGroup) and exc.exceptions:
                        exc = exc.exceptions[0]
                    return exc
                inner = _first_leaf(eg)
                error_msg = f"{type(inner).__name__}: {inner}"
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": f"ERROR: {error_msg}"})
                trajectory.append({
                    "iteration": iteration, "phase": "tool_call_error",
                    "tool_name": tool_name, "tool_args": tool_args, "error": error_msg,
                })
                context.log.warning(f"[tool_use_loop iter={iteration}] {tool_name} failed: {error_msg}")
            except Exception as e:  # noqa: BLE001
                error_msg = f"{type(e).__name__}: {e}"
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": f"ERROR: {error_msg}"})
                trajectory.append({
                    "iteration": iteration, "phase": "tool_call_error",
                    "tool_name": tool_name, "tool_args": tool_args, "error": error_msg,
                })
                context.log.warning(f"[tool_use_loop iter={iteration}] {tool_name} failed: {error_msg}")

        if done_this_iter:
            break

    if final_answer is None:
        # Hit max_iterations without finalize. Use the last LLM content
        # or a fallback message.
        final_answer = (
            f"(loop terminated after {max_iterations} iterations without "
            f"calling `{finalize_name}`; showing raw trajectory)"
        )
        stop_reason = "max_iterations_hit"

    total_latency_ms = total_llm_latency_ms + total_tool_latency_ms

    return {
        "text": final_answer,
        "op": "tool_use_loop",
        "model": model,
        "model_fingerprint": f"{model}@t{temperature}",
        "n_iterations": len(trajectory) if stop_reason != "max_iterations_hit" else max_iterations,
        "n_llm_calls": n_llm_calls,
        "n_tool_calls": n_tool_calls,
        "stop_reason": stop_reason,
        "cost_usd": round(total_cost_usd, 6) if total_cost_usd > 0 else None,
        "latency_ms": total_latency_ms,
        "llm_latency_ms": total_llm_latency_ms,
        "tool_latency_ms": total_tool_latency_ms,
        "tokens_in": total_tokens_in or None,
        "tokens_out": total_tokens_out or None,
        "tokens_total": (total_tokens_in + total_tokens_out) or None,
        "tool_call_trace": trajectory,
        "tools_available": sorted(tool_index.keys()),
        "materialized_at": materialized_at,
    }


# ── Op dispatcher ────────────────────────────────────────────────────

def _do_handoff(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Framework-composition op. Hands off to a user-provided callable —
    bring your own LangGraph / AutoGen / CrewAI / Llama Index Agents /
    DSPy. The callable receives `initial_state` (post-substitution) as
    kwargs and returns a dict; we pull the final text from
    `output_text_key`. Framework's internal node-by-node lineage is
    lost (one asset materializes, containing the final answer +
    framework metadata) — that's the intentional trade at ONE step of a
    Dagster pipeline. Adjacent steps stay Dagster-native (fan-out, MCP,
    HITL gates), so this is composition, not wrapping.

    Config:
      entry_module     — Python module path with the callable
      entry_callable   — name of the callable in that module. Signature:
                         `def fn(**initial_state) -> dict`
      initial_state    — dict of kwargs passed to the callable. String
                         values get `{text}` (source) + `{port_name}`
                         (typed input) substitution before the call.
      output_text_key  — key in the returned dict whose value is the
                         final text downstream steps consume (default
                         `final_answer`)
      framework        — metadata-only label ("langgraph" / "autogen" /
                         "crewai" / "dspy" / etc.); surfaces in asset
                         metadata + logs; not enforced.

    Example (`initial_state` with source-text substitution):

        - id: complex_reasoning
          op: handoff
          framework: langgraph
          entry_module: my_project.agents
          entry_callable: run_deep_reasoning
          initial_state:
            prompt: "{text}"
            max_depth: 5
          output_text_key: final_answer

    User's callable — 5 lines of glue in their own project:

        def run_deep_reasoning(prompt: str, max_depth: int = 5) -> dict:
            graph = build_graph()  # user's LangGraph
            result = graph.invoke({"prompt": prompt, "max_depth": max_depth})
            return {"final_answer": result["output"], "n_nodes": result.get("n_nodes")}
    """
    import importlib
    import time as _time

    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    entry_module = step.get("entry_module")
    entry_callable = step.get("entry_callable")
    if not entry_module or not entry_callable:
        raise ValueError(
            "handoff requires `entry_module` + `entry_callable` — the Python "
            "module path + function name of a callable that takes "
            "**initial_state and returns a dict."
        )
    initial_state = dict(step.get("initial_state") or {})
    output_text_key = step.get("output_text_key", "final_answer")
    framework = step.get("framework")  # metadata only

    # {text} + {port_name} substitution on string values of initial_state.
    # {partition_key} + {partition.<name>} are already substituted by
    # _run_step's _deep_apply_ctx_substitutions.
    for k, v in list(initial_state.items()):
        if isinstance(v, str):
            v = v.replace("{text}", src_text)
            v = _substitute_ports(v, inputs)
            initial_state[k] = v

    try:
        mod = importlib.import_module(entry_module)
    except ImportError as e:
        raise ImportError(
            f"handoff: could not import {entry_module!r}: {e}. "
            f"Install the module (or its parent package) in the project's "
            f"venv, or fix the module path."
        ) from e
    fn = getattr(mod, entry_callable, None)
    if fn is None or not callable(fn):
        raise ValueError(
            f"handoff: {entry_callable!r} is not a callable in {entry_module!r}. "
            f"Available names: {[n for n in dir(mod) if callable(getattr(mod, n, None)) and not n.startswith('_')][:20]}"
        )

    context.log.info(
        f"[handoff:{step.get('id', '?')}] {framework or 'user'} → "
        f"{entry_module}.{entry_callable}({sorted(initial_state.keys())})"
    )
    t0 = _time.time()
    try:
        result = fn(**initial_state)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            f"handoff to {entry_module}.{entry_callable} raised "
            f"{type(e).__name__}: {e}"
        ) from e
    latency_ms = int((_time.time() - t0) * 1000)

    # Extract downstream text via output_text_key.
    text = ""
    if isinstance(result, dict):
        raw_text = result.get(output_text_key, "")
        text = raw_text if isinstance(raw_text, str) else json.dumps(raw_text, default=str)
    elif isinstance(result, str):
        text = result
    else:
        text = str(result)

    context.log.info(
        f"[handoff:{step.get('id', '?')}] completed in {latency_ms}ms, "
        f"{len(text)} chars"
    )

    # Pass the whole framework result through as `framework_result` so
    # downstream steps or the emitting asset can inspect internal keys
    # (n_nodes_executed, tool_call_trace, cost_usd if the framework
    # tracks it, etc.).
    framework_result = result if isinstance(result, dict) else {"_result": result}
    # Best-effort roll-up: if the callable's return dict has these keys,
    # surface them at the top level so they land in Insights.
    rolled: Dict[str, Any] = {}
    for k in ("cost_usd", "n_nodes_executed", "n_llm_calls", "n_tool_calls", "tokens_total"):
        if isinstance(result, dict) and k in result:
            rolled[k] = result[k]

    return {
        "text": text,
        "framework": framework,
        "entry_module": entry_module,
        "entry_callable": entry_callable,
        "framework_result": framework_result,
        "latency_ms": latency_ms,
        "materialized_at": materialized_at,
        "op": "handoff",
        **rolled,
    }


# ── map op ────────────────────────────────────────────────────────────
#
# Fan-out an LLM call over each item in a list source (JSON array).
# Aggregates per-item results into one asset. Sequential by default;
# `max_concurrent` opt-in threading for I/O-bound workloads.


def _parse_items(text: str) -> List[Any]:
    """Best-effort parse of a source text into a list of items.
    Accepts a JSON array OR falls back to non-empty newlines."""
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            # Common shape: a dict with a `data:` or `items:` list.
            for key in ("data", "items", "results", "values"):
                v = parsed.get(key)
                if isinstance(v, list):
                    return v
    except (json.JSONDecodeError, ValueError):
        pass
    # Fallback: split on newlines.
    return [ln for ln in text.splitlines() if ln.strip()]


def _do_map(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Apply an LLM call to each item in a list source; aggregate.

    Config:
      source / inputs   — usual upstream ref; source's text is parsed as JSON list
      model, api_key_env_var, system_prompt, temperature, max_tokens
      prompt_template   — supports `{item}` (current item) + `{index}` (0-based)
                          + `{n}` (total count) + `{text}` + `{port_name}`
      max_concurrent    — 1 (sequential; default) OR N (thread-pooled)
      output_join       — 'newlines' (default) | 'jsonl' | 'none'
                          (`none` returns empty text — downstream reads items[])
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    items = _parse_items(src_text)
    if not items:
        context.log.warning(f"[map:{step.get('id', '?')}] no items parsed from source")

    model = step["model"]
    api_key_env_var = step.get("api_key_env_var")
    api_base_env_var = step.get("api_base_env_var")
    temperature = step.get("temperature", 0.0)
    max_tokens = step.get("max_tokens", 1024)
    reasoning_effort = step.get("reasoning_effort")
    thinking_budget = step.get("thinking_budget")
    prompt_template = step.get("prompt_template", "{item}")
    system_prompt = step.get("system_prompt")
    if system_prompt:
        system_prompt = _substitute_ports(system_prompt, inputs)
    max_concurrent = int(step.get("max_concurrent", 1))
    output_join = step.get("output_join", "newlines")

    def _run_one(index_and_item):
        i, item = index_and_item
        item_str = json.dumps(item) if not isinstance(item, str) else item
        user_prompt = (
            prompt_template
            .replace("{item}", item_str)
            .replace("{index}", str(i))
            .replace("{n}", str(len(items)))
            .replace("{text}", src_text)
        )
        user_prompt = _substitute_ports(user_prompt, inputs)
        return i, item, _completion(
            model=model, system_prompt=system_prompt, user_prompt=user_prompt,
            api_key_env_var=api_key_env_var, api_base_env_var=api_base_env_var,
            temperature=temperature, max_tokens=max_tokens,
            reasoning_effort=reasoning_effort, thinking_budget=thinking_budget,
        )

    results: List[Any] = [None] * len(items)  # per-item output blobs
    all_llm_results = []

    context.log.info(
        f"[map:{step.get('id', '?')}] {len(items)} item(s) × max_concurrent={max_concurrent}"
    )

    if max_concurrent > 1 and len(items) > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_concurrent) as pool:
            for i, item, result in pool.map(_run_one, enumerate(items)):
                results[i] = {"item": item, "text": result["content"]}
                all_llm_results.append(result)
    else:
        for pair in enumerate(items):
            i, item, result = _run_one(pair)
            results[i] = {"item": item, "text": result["content"]}
            all_llm_results.append(result)

    # Join per-item texts into the top-level `text` field.
    joined_text: str
    if output_join == "newlines":
        joined_text = "\n\n".join(r["text"] for r in results if r)
    elif output_join == "jsonl":
        joined_text = "\n".join(json.dumps(r) for r in results if r)
    elif output_join == "none":
        joined_text = ""
    else:
        raise ValueError(
            f"map: output_join must be 'newlines' | 'jsonl' | 'none'; got {output_join!r}"
        )

    return {
        "text": joined_text,
        "items": results,
        "n_items": len(items),
        "model": model,
        "model_fingerprint": f"{model}@t{temperature}×{len(items)}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(*all_llm_results),
        "latency_ms": _sum_latency(*all_llm_results),
        "tokens_total": _sum_tokens(*all_llm_results),
        "n_llm_calls": len(all_llm_results),
        "op": "map",
    }


# ── extract op ────────────────────────────────────────────────────────
#
# Structured JSON extraction — text → dict matching a schema. Uses
# tool_choice="required" with a single function that has the schema as
# parameters. More reliable than prompt-engineering "return JSON" —
# LiteLLM+model round-trips the JSON as a tool call.


def _do_extract(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Extract structured JSON from unstructured text using an output schema.

    Config:
      source / inputs        — upstream ref
      model, api_key_env_var — LLM to use
      output_schema          — JSON Schema (object) — the shape of the returned dict
      system_prompt          — override default ("You extract structured data...")
      prompt_template        — override default (`{text}`)
      strict                 — true (default; missing required fields raise) |
                               false (missing → None)
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    output_schema = step.get("output_schema")
    if not output_schema or not isinstance(output_schema, dict):
        raise ValueError(
            "extract requires `output_schema: {type: object, properties: {...}}` "
            "(a JSON Schema object)."
        )
    strict = bool(step.get("strict", True))

    system_prompt = step.get("system_prompt") or (
        "Extract structured metadata from the input text. Call the "
        "`extract_data` tool with the extracted fields. Return only the tool call."
    )
    system_prompt = _substitute_ports(system_prompt, inputs)
    user_prompt = step.get("prompt_template", "{text}").replace("{text}", src_text)
    user_prompt = _substitute_ports(user_prompt, inputs)

    tool = {
        "type": "function",
        "function": {
            "name": "extract_data",
            "description": "Return extracted data matching the schema.",
            "parameters": output_schema,
        },
    }

    result = _completion(
        model=step["model"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=step.get("temperature", 0.0),
        max_tokens=step.get("max_tokens", 1024),
        tools=[tool],
        tool_choice="required",
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
    )

    extracted: Optional[Dict[str, Any]] = None
    if result["tool_calls"]:
        raw_args = result["tool_calls"][0].get("arguments") or "{}"
        try:
            extracted = json.loads(raw_args)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"extract: LLM returned invalid JSON in tool call: {e}. "
                f"Raw: {raw_args[:300]}"
            ) from e

    if extracted is None:
        raise RuntimeError(
            "extract: LLM did not emit a tool call. This usually means the "
            "model doesn't support forced tool calls — try a different model."
        )

    if strict:
        required = output_schema.get("required") or []
        missing = [k for k in required if k not in extracted]
        if missing:
            raise RuntimeError(
                f"extract (strict): missing required fields {missing} in extracted data."
            )

    return {
        "text": json.dumps(extracted, indent=2),
        "extracted": extracted,
        "output_schema": output_schema,
        "model": step["model"],
        "model_fingerprint": f"{step['model']}@extract",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(result),
        "latency_ms": _sum_latency(result),
        "tokens_total": _sum_tokens(result),
        "n_llm_calls": 1,
        "op": "extract",
    }


# ── classify op ───────────────────────────────────────────────────────
#
# Text → label from a fixed set. Simplest, most common enterprise use case.
# Uses tool_choice="required" with a single-field enum parameter.


def _do_classify(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Classify source text into one of a fixed set of labels.

    Config:
      source / inputs        — upstream ref
      model, api_key_env_var — LLM
      labels                 — list of label strings (enum)
      include_rationale      — true (default) — include a one-sentence rationale
                               false → skip; smaller / cheaper prompt
      system_prompt          — override default
      prompt_template        — override default (`{text}`)
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    labels = step.get("labels")
    if not labels or not isinstance(labels, list):
        raise ValueError("classify requires `labels: [...]` (non-empty list of strings).")
    labels = [str(l) for l in labels]

    include_rationale = bool(step.get("include_rationale", True))

    props: Dict[str, Any] = {
        "label": {"type": "string", "enum": labels, "description": "The chosen label."},
    }
    required = ["label"]
    if include_rationale:
        props["rationale"] = {"type": "string", "description": "One-sentence reason for the choice."}
        required.append("rationale")

    tool = {
        "type": "function",
        "function": {
            "name": "classify",
            "description": f"Pick one label from: {', '.join(labels)}.",
            "parameters": {"type": "object", "properties": props, "required": required},
        },
    }

    system_prompt = step.get("system_prompt") or (
        f"You are a classifier. Read the input and call `classify` with one of: "
        f"{', '.join(labels)}."
    )
    system_prompt = _substitute_ports(system_prompt, inputs)
    user_prompt = step.get("prompt_template", "{text}").replace("{text}", src_text)
    user_prompt = _substitute_ports(user_prompt, inputs)

    result = _completion(
        model=step["model"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=step.get("temperature", 0.0),
        max_tokens=step.get("max_tokens", 256),
        tools=[tool],
        tool_choice="required",
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
    )

    label: Optional[str] = None
    rationale: Optional[str] = None
    if result["tool_calls"]:
        try:
            args = json.loads(result["tool_calls"][0].get("arguments") or "{}")
            label = args.get("label")
            rationale = args.get("rationale")
        except json.JSONDecodeError:
            label = None

    if label not in labels:
        raise RuntimeError(
            f"classify: LLM returned invalid label {label!r} (not in {labels})."
        )

    return {
        "text": label,
        "label": label,
        "rationale": rationale,
        "labels": labels,
        "model": step["model"],
        "model_fingerprint": f"{step['model']}@classify",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(result),
        "latency_ms": _sum_latency(result),
        "tokens_total": _sum_tokens(result),
        "n_llm_calls": 1,
        "op": "classify",
    }


# ── reduce op ─────────────────────────────────────────────────────────
#
# LLM-fold over a list — chunk-by-chunk, prior summary + next chunk →
# updated summary. Solves the "list too big for one context window"
# problem without hand-unrolling.


def _do_reduce(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Fold an LLM over chunks of a list.

    Config:
      source / inputs           — upstream returning a JSON list
      model, api_key_env_var    — LLM
      chunk_size                — items per LLM call (default 10)
      initial_prompt_template   — first-chunk prompt.
                                  Placeholders: {items}, {n}
      fold_prompt_template      — subsequent-chunk prompt.
                                  Placeholders: {prior}, {items}, {n}, {chunk_index}, {n_chunks}
      system_prompt             — optional
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    items = _parse_items(src_text)
    if not items:
        raise ValueError("reduce: source parsed as empty list. Provide a JSON array.")

    chunk_size = int(step.get("chunk_size", 10))
    model = step["model"]
    api_key_env_var = step.get("api_key_env_var")
    api_base_env_var = step.get("api_base_env_var")
    temperature = step.get("temperature", 0.0)
    max_tokens = step.get("max_tokens", 2048)

    system_prompt = step.get("system_prompt")
    if system_prompt:
        system_prompt = _substitute_ports(system_prompt, inputs)

    initial_tmpl = step.get(
        "initial_prompt_template",
        "You have {n} items below. Summarize them into a single coherent paragraph.\n\n{items}",
    )
    fold_tmpl = step.get(
        "fold_prompt_template",
        "Prior summary (do NOT lose information from it):\n{prior}\n\n"
        "New items ({chunk_index}/{n_chunks}, {n} items in this batch):\n{items}\n\n"
        "Update the summary to include the new items. Return one coherent paragraph.",
    )

    def _render_items(chunk: List[Any]) -> str:
        return "\n".join(
            f"- {json.dumps(it) if not isinstance(it, str) else it}"
            for it in chunk
        )

    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    n_chunks = len(chunks)
    context.log.info(
        f"[reduce:{step.get('id', '?')}] folding {len(items)} item(s) in {n_chunks} chunk(s)"
    )

    all_llm_results = []
    prior_summary: str = ""

    for i, chunk in enumerate(chunks):
        rendered = _render_items(chunk)
        if i == 0:
            user_prompt = (
                initial_tmpl
                .replace("{items}", rendered)
                .replace("{n}", str(len(chunk)))
            )
        else:
            user_prompt = (
                fold_tmpl
                .replace("{prior}", prior_summary)
                .replace("{items}", rendered)
                .replace("{n}", str(len(chunk)))
                .replace("{chunk_index}", str(i + 1))
                .replace("{n_chunks}", str(n_chunks))
            )
        user_prompt = _substitute_ports(user_prompt, inputs)
        result = _completion(
            model=model, system_prompt=system_prompt, user_prompt=user_prompt,
            api_key_env_var=api_key_env_var, api_base_env_var=api_base_env_var,
            temperature=temperature, max_tokens=max_tokens,
            reasoning_effort=step.get("reasoning_effort"),
            thinking_budget=step.get("thinking_budget"),
        )
        all_llm_results.append(result)
        prior_summary = result["content"]

    return {
        "text": prior_summary,
        "n_items": len(items),
        "n_chunks": n_chunks,
        "chunk_size": chunk_size,
        "model": model,
        "model_fingerprint": f"{model}@reduce×{n_chunks}",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(*all_llm_results),
        "latency_ms": _sum_latency(*all_llm_results),
        "tokens_total": _sum_tokens(*all_llm_results),
        "n_llm_calls": len(all_llm_results),
        "op": "reduce",
    }


# ── self_reflect op ───────────────────────────────────────────────────
#
# ONE LLM call producing draft + critique + revised. Cost-sensitive
# alternative to critique_loop (which is 2N+1 calls). Prompt forces the
# model to structure its response so we can parse out the revised part.


_REFLECT_RE = re.compile(
    r"REVISED\s*:\s*(.+?)(?:$|\Z)", re.IGNORECASE | re.DOTALL,
)
_DRAFT_RE = re.compile(r"DRAFT\s*:\s*(.+?)(?:CRITIQUE|REVISED)", re.IGNORECASE | re.DOTALL)
_CRITIQUE_RE = re.compile(r"CRITIQUE\s*:\s*(.+?)(?:REVISED)", re.IGNORECASE | re.DOTALL)


def _do_self_reflect(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """One-shot draft + self-critique + revised output.

    Config:
      source / inputs        — upstream ref
      model, api_key_env_var — LLM
      system_prompt          — override default (see below)
      prompt_template        — override default (`{text}`)

    The default system prompt asks for three-section output:
        DRAFT: ...
        CRITIQUE: ...
        REVISED: ...

    Parses the REVISED section as `text`; DRAFT + CRITIQUE surface in
    metadata. If the model returns a non-structured response, the entire
    content is returned as `text` and the metadata sections are None.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    system_prompt = step.get("system_prompt") or (
        "You are a careful writer. Produce your response in EXACTLY three sections, "
        "each on its own set of lines:\n\n"
        "DRAFT: <your initial answer>\n\n"
        "CRITIQUE: <one paragraph of self-critique — what's weak in the draft>\n\n"
        "REVISED: <the revised answer, addressing the critique>"
    )
    system_prompt = _substitute_ports(system_prompt, inputs)
    user_prompt = step.get("prompt_template", "{text}").replace("{text}", src_text)
    user_prompt = _substitute_ports(user_prompt, inputs)

    result = _completion(
        model=step["model"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=step.get("temperature", 0.0),
        max_tokens=step.get("max_tokens", 4096),
        reasoning_effort=step.get("reasoning_effort"),
        thinking_budget=step.get("thinking_budget"),
    )
    content = result["content"] or ""

    draft = _DRAFT_RE.search(content)
    critique = _CRITIQUE_RE.search(content)
    revised = _REFLECT_RE.search(content)

    parsed = revised is not None
    text = revised.group(1).strip() if revised else content
    if not parsed:
        context.log.warning(
            f"[self_reflect:{step.get('id', '?')}] response didn't match DRAFT/CRITIQUE/REVISED structure; "
            f"returning full content as text"
        )

    return {
        "text": text,
        "draft": draft.group(1).strip() if draft else None,
        "critique": critique.group(1).strip() if critique else None,
        "revised": revised.group(1).strip() if revised else None,
        "parsed": parsed,
        "model": step["model"],
        "model_fingerprint": f"{step['model']}@self_reflect",
        "materialized_at": materialized_at,
        "cost_usd": _sum_cost(result),
        "latency_ms": _sum_latency(result),
        "tokens_total": _sum_tokens(result),
        "n_llm_calls": 1,
        "op": "self_reflect",
    }


# ── sub_pipeline op ───────────────────────────────────────────────────
#
# Invoke an inline sub-pipeline (a `steps:` list) as ONE step of the
# outer pipeline. Enables composition + reuse of common patterns
# without duplicating YAML. The sub-pipeline runs in the same process;
# its state is completely isolated from the outer pipeline's state.


def _do_sub_pipeline(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Execute an inline sub-pipeline. Returns the specified sub-step's output.

    Config:
      source / inputs   — upstream ref (its text becomes the sub-pipeline's initial source.text)
      steps             — sub-pipeline's step list (same schema as top-level `steps:`)
      output_step_id    — which sub-step's output flows back into this asset's `text`.
                          Defaults to the last step in the sub-pipeline.
      sub_source        — optional override: `{kind: literal|file|url, ...}`
                          If unset, source.text = the upstream text from `source:` / `inputs:`.
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    sub_steps = step.get("steps")
    if not sub_steps or not isinstance(sub_steps, list):
        raise ValueError("sub_pipeline requires `steps: [...]` (inline sub-pipeline).")

    # Isolated state — no leakage from outer pipeline.
    sub_state: Dict[str, Any] = {
        "source": {"text": src_text, "source_kind": "sub_pipeline_input"},
        "__ctx__": _ctx_of(state),  # inherit partition context
        "__personas__": state.get("__personas__") or {},  # inherit personas
        "__agents__": state.get("__agents__") or {},  # inherit agents
    }
    sub_source = step.get("sub_source")
    if sub_source:
        sub_ctx = _ctx_of(state)
        sub_entry = _ingest(
            sub_source, context,
            partition_key=sub_ctx.get("partition_key"),
            partition_fields=sub_ctx.get("partition_fields"),
        )
        sub_state["source"] = sub_entry

    # Merge outer inputs into sub-pipeline's source text via port substitution
    # in EACH sub-step's string fields is not possible transparently; instead
    # we make outer inputs available as pre-populated sub-state entries. Any
    # sub-step can read them via `source: <port_name>`.
    for port_name, port_value in inputs.items():
        # inputs values are already resolved to text (via _resolve_inputs);
        # wrap in a step-like dict so `_get_source_text(sub_state, port_name)` works.
        sub_state[port_name] = {"text": port_value if isinstance(port_value, str) else str(port_value)}

    total_llm_calls = 0
    total_cost = 0.0
    total_latency_ms = 0
    total_tokens = 0

    for sub_step in sub_steps:
        pre_keys = set(sub_state.keys())
        _run_step(sub_step, sub_state, context)
        # Sum cost/latency across the sub-step's output.
        new_key = next(iter(set(sub_state.keys()) - pre_keys), None)
        if new_key and isinstance(sub_state[new_key], dict):
            r = sub_state[new_key]
            if r.get("cost_usd") is not None:
                total_cost += float(r["cost_usd"])
            if r.get("latency_ms") is not None:
                total_latency_ms += int(r["latency_ms"])
            if r.get("tokens_total") is not None:
                total_tokens += int(r["tokens_total"])
            if r.get("n_llm_calls") is not None:
                total_llm_calls += int(r["n_llm_calls"])

    # Return the requested step's output text.
    output_step_id = step.get("output_step_id") or _last_step_id(sub_state)
    if output_step_id not in sub_state:
        raise ValueError(
            f"sub_pipeline: output_step_id {output_step_id!r} not in sub-pipeline state. "
            f"Available: {[k for k in sub_state if not k.startswith('__')]}"
        )
    output_entry = sub_state[output_step_id]
    output_text = (
        output_entry.get("text", "") if isinstance(output_entry, dict) else str(output_entry)
    )

    return {
        "text": output_text,
        "sub_steps_run": [s.get("id") for s in sub_steps if s.get("id")],
        "output_step_id": output_step_id,
        "cost_usd": total_cost if total_cost > 0 else None,
        "latency_ms": total_latency_ms,
        "tokens_total": total_tokens if total_tokens > 0 else None,
        "n_llm_calls": total_llm_calls,
        "model_fingerprint": f"sub_pipeline[{len(sub_steps)} steps]",
        "materialized_at": materialized_at,
        "op": "sub_pipeline",
    }


# ── agent_call op ─────────────────────────────────────────────────────
#
# Universal dispatcher for pre-built agents declared in the top-level
# `agents:` block. The step-level YAML is agent-kind-agnostic:
#
#     - id: security_review
#       op: agent_call
#       agent: cisobot                    # name in agents: block
#       prompt_template: "Review: {text}"
#       extra_context:                    # optional dict merged into payload
#         priority: "{partition.priority}"
#
# Three kinds handled: openai_assistant, remote_agent, handoff.


def _json_path_get(obj: Any, path: str) -> Any:
    """Very light JSON-path — supports dot navigation into dicts + [N]
    integer indexing into lists. `$.foo.bar[0].baz`. Returns None on any
    miss rather than raising."""
    if not path or path == "$":
        return obj
    if path.startswith("$."):
        path = path[2:]
    elif path.startswith("$"):
        path = path[1:].lstrip(".")
    node: Any = obj
    for part in path.split("."):
        if not part:
            continue
        while "[" in part and part.endswith("]"):
            base, idx = part[:-1].split("[", 1)
            if base:
                if not isinstance(node, dict) or base not in node:
                    return None
                node = node[base]
            try:
                node = node[int(idx)]
            except (ValueError, IndexError, TypeError):
                return None
            part = ""
        if part:
            if not isinstance(node, dict) or part not in node:
                return None
            node = node[part]
    return node


def _sub_agent_placeholders(value: Any, prompt: str, extra: Dict[str, Any]) -> Any:
    """Deep-substitute `{prompt}` and `{extra.<key>}` into all string
    values in a dict/list/scalar tree. Non-strings pass through."""
    if isinstance(value, str):
        result = value.replace("{prompt}", prompt)
        for k, v in (extra or {}).items():
            result = result.replace("{extra." + k + "}", str(v) if not isinstance(v, str) else v)
        return result
    if isinstance(value, list):
        return [_sub_agent_placeholders(x, prompt, extra) for x in value]
    if isinstance(value, dict):
        return {k: _sub_agent_placeholders(v, prompt, extra) for k, v in value.items()}
    return value


def _call_openai_assistant(agent_cfg: Dict[str, Any], prompt: str, context) -> Dict[str, Any]:
    """OpenAI Assistants API — create thread + add message + create run +
    poll to completion + extract latest assistant message."""
    try:
        from openai import OpenAI
    except ImportError as e:
        raise ImportError(
            "agent_call (kind=openai_assistant) requires the openai SDK: "
            "pip install 'openai>=1.30.0'"
        ) from e

    assistant_id = agent_cfg.get("assistant_id")
    if not assistant_id and agent_cfg.get("assistant_id_env_var"):
        assistant_id = os.environ.get(agent_cfg["assistant_id_env_var"])
    if not assistant_id:
        raise ValueError(
            "openai_assistant agent needs `assistant_id:` OR `assistant_id_env_var:`"
        )

    client_kwargs: Dict[str, Any] = {}
    if agent_cfg.get("api_key_env_var"):
        v = os.environ.get(agent_cfg["api_key_env_var"])
        if v:
            client_kwargs["api_key"] = v
    if agent_cfg.get("api_base_env_var"):
        v = os.environ.get(agent_cfg["api_base_env_var"])
        if v:
            client_kwargs["base_url"] = v
    client = OpenAI(**client_kwargs)

    max_wait = int(agent_cfg.get("max_wait_seconds", 300))
    thread_id = agent_cfg.get("thread_id")
    if not thread_id and agent_cfg.get("thread_id_env_var"):
        thread_id = os.environ.get(agent_cfg["thread_id_env_var"])

    t0 = time.time()
    if thread_id:
        context.log.info(f"[agent_call/openai_assistant] reusing thread {thread_id}")
        thread = client.beta.threads.retrieve(thread_id)
    else:
        thread = client.beta.threads.create()

    client.beta.threads.messages.create(thread_id=thread.id, role="user", content=prompt)
    run = client.beta.threads.runs.create_and_poll(
        thread_id=thread.id, assistant_id=assistant_id, timeout=max_wait,
    )
    latency_ms = int((time.time() - t0) * 1000)

    if run.status != "completed":
        raise RuntimeError(
            f"openai_assistant run status={run.status!r}: {getattr(run, 'last_error', None)}"
        )

    messages = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=1)
    if not messages.data:
        raise RuntimeError("openai_assistant run completed but returned no messages")
    latest = messages.data[0]
    parts = [b.text.value for b in (latest.content or []) if getattr(b, "type", None) == "text"]
    text = "\n".join(parts)

    tokens_total = None
    usage = getattr(run, "usage", None)
    if usage is not None:
        try:
            tokens_total = int(usage.total_tokens)
        except (AttributeError, TypeError):
            tokens_total = None

    return {
        "text": text,
        "thread_id": thread.id,
        "run_id": run.id,
        "assistant_id": assistant_id,
        "status": run.status,
        "latency_ms": latency_ms,
        "tokens_total": tokens_total,
        "n_llm_calls": 1,
    }


def _call_remote_agent(
    agent_cfg: Dict[str, Any], prompt: str, extra: Dict[str, Any], context,
) -> Dict[str, Any]:
    """Generic HTTP agent — sync POST/GET/PUT OR async poll pattern.
    Auth via bearer OR arbitrary headers (literal + env-backed)."""
    import requests

    url = agent_cfg.get("url")
    if not url and agent_cfg.get("url_env_var"):
        url = os.environ.get(agent_cfg["url_env_var"])
    if not url:
        raise ValueError("remote_agent needs `url:` OR `url_env_var:`")

    method = str(agent_cfg.get("method", "POST")).upper()
    timeout = int(agent_cfg.get("timeout_seconds", 60))

    headers: Dict[str, str] = {}
    if agent_cfg.get("auth_bearer_env_var"):
        tok = os.environ.get(agent_cfg["auth_bearer_env_var"])
        if not tok:
            raise ValueError(
                f"remote_agent auth_bearer_env_var {agent_cfg['auth_bearer_env_var']!r} not set"
            )
        headers["Authorization"] = f"Bearer {tok}"
    for k, v in (agent_cfg.get("headers") or {}).items():
        headers[str(k)] = str(v)
    for header_name, env_var in (agent_cfg.get("headers_env") or {}).items():
        val = os.environ.get(env_var)
        if val is None:
            raise ValueError(
                f"remote_agent headers_env {header_name!r} references env {env_var!r} but it's unset"
            )
        headers[header_name] = val

    payload = agent_cfg.get("payload_template")
    body_str = agent_cfg.get("body_template")
    content_type = agent_cfg.get("content_type", "application/json")

    request_kwargs: Dict[str, Any] = {"headers": headers, "timeout": timeout}
    if payload is not None:
        request_kwargs["json"] = _sub_agent_placeholders(payload, prompt, extra)
    elif body_str is not None:
        request_kwargs["data"] = _sub_agent_placeholders(body_str, prompt, extra)
        headers.setdefault("Content-Type", content_type)
    else:
        if method == "POST":
            request_kwargs["json"] = {"prompt": prompt, **(extra or {})}

    t0 = time.time()
    context.log.info(f"[agent_call/remote_agent] {method} {url}")
    resp = requests.request(method, url, **request_kwargs)
    resp.raise_for_status()
    body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"_raw": resp.text}

    poll_url_path = agent_cfg.get("poll_url_path")
    poll_url: Optional[str] = _json_path_get(body, poll_url_path) if poll_url_path else None
    if poll_url:
        status_path = agent_cfg.get("poll_terminal_status_path", "$.status")
        success = set(agent_cfg.get("poll_terminal_success", ["completed", "succeeded", "done"]))
        failure = set(agent_cfg.get("poll_terminal_failure", ["failed", "cancelled", "error"]))
        poll_interval = int(agent_cfg.get("poll_interval_seconds", 5))
        poll_timeout = int(agent_cfg.get("poll_timeout_seconds", 300))

        deadline = time.time() + poll_timeout
        while time.time() < deadline:
            time.sleep(poll_interval)
            pr = requests.get(poll_url, headers=headers, timeout=timeout)
            pr.raise_for_status()
            body = pr.json()
            status = _json_path_get(body, status_path)
            context.log.info(f"[agent_call/remote_agent] poll status={status!r}")
            if status in success:
                break
            if status in failure:
                raise RuntimeError(f"remote_agent async job failed: status={status!r}")
        else:
            raise RuntimeError(f"remote_agent async job did not complete within {poll_timeout}s")

    latency_ms = int((time.time() - t0) * 1000)

    text_path = agent_cfg.get("response_text_path", "$.text")
    text = _json_path_get(body, text_path)
    if text is None:
        text = json.dumps(body, indent=2, default=str)[:2000]
        context.log.warning(
            f"[agent_call/remote_agent] response_text_path {text_path!r} extracted no value; "
            f"returning raw body preview instead"
        )

    return {
        "text": str(text),
        "url": url,
        "method": method,
        "status_code": resp.status_code,
        "response_body": body,
        "polled": bool(poll_url),
        "latency_ms": latency_ms,
    }


def _call_handoff_agent(
    agent_cfg: Dict[str, Any], prompt: str, extra: Dict[str, Any], context,
) -> Dict[str, Any]:
    """Handoff kind — invoke a Python callable. Same shape as the
    stand-alone `handoff` op, packaged as an agents:-block entry so
    step-level YAML matches openai_assistant / remote_agent."""
    import importlib

    entry_module = agent_cfg.get("entry_module")
    entry_callable = agent_cfg.get("entry_callable")
    if not entry_module or not entry_callable:
        raise ValueError("handoff agent needs `entry_module` + `entry_callable`")
    output_text_key = agent_cfg.get("output_text_key", "final_answer")
    framework = agent_cfg.get("framework")

    initial_state = {"prompt": prompt}
    initial_state.update(extra or {})

    try:
        mod = importlib.import_module(entry_module)
    except ImportError as e:
        raise ImportError(f"handoff agent: could not import {entry_module!r}: {e}") from e
    fn = getattr(mod, entry_callable, None)
    if fn is None or not callable(fn):
        raise ValueError(f"handoff agent: {entry_callable!r} is not callable in {entry_module!r}")

    t0 = time.time()
    context.log.info(f"[agent_call/handoff] {framework or 'user'} → {entry_module}.{entry_callable}")
    result = fn(**initial_state)
    latency_ms = int((time.time() - t0) * 1000)

    if isinstance(result, dict):
        text_val = result.get(output_text_key, "")
        text = text_val if isinstance(text_val, str) else json.dumps(text_val, default=str)
    elif isinstance(result, str):
        text = result
    else:
        text = str(result)

    return {
        "text": text,
        "framework": framework,
        "entry_module": entry_module,
        "entry_callable": entry_callable,
        "framework_result": result if isinstance(result, dict) else {"_result": result},
        "latency_ms": latency_ms,
    }


_AGENT_KIND_HANDLERS = {
    "openai_assistant": _call_openai_assistant,
    "remote_agent": _call_remote_agent,
    "handoff": _call_handoff_agent,
}


def _do_agent_call(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Dispatch to a pre-built agent declared in the top-level `agents:` block.

    Config:
      agent           — name in `agents:` (required)
      prompt_template — user prompt (default `{text}`). Placeholders:
                        `{text}` (source), `{port_name}` (typed inputs),
                        `{partition.<name>}` (composite partition key parts).
      source / inputs — usual upstream ref
      extra_context   — optional dict; merged into the agent's payload
                        (available as `{extra.<key>}` in payload_template).
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id) if source_id else ""
    inputs = _resolve_inputs(step, state)

    agents = state.get("__agents__") or {}
    agent_name = step.get("agent")
    if not agent_name:
        raise ValueError("agent_call requires `agent: <name>` (a name in the top-level agents: block)")
    if agent_name not in agents:
        raise ValueError(
            f"agent_call: agent {agent_name!r} not in `agents:` block. "
            f"Available: {sorted(agents.keys())}"
        )
    agent_cfg = dict(agents[agent_name])
    kind = agent_cfg.pop("kind", None)
    if kind not in _AGENT_KIND_HANDLERS:
        raise ValueError(
            f"agent_call: agent {agent_name!r} has invalid kind {kind!r}. "
            f"Valid: {sorted(_AGENT_KIND_HANDLERS.keys())}"
        )

    prompt_template = step.get("prompt_template", "{text}")
    prompt = prompt_template.replace("{text}", src_text)
    prompt = _substitute_ports(prompt, inputs)
    extra_context = _sub_agent_placeholders(step.get("extra_context") or {}, src_text, {})
    for k, v in inputs.items():
        prompt = prompt.replace("{" + k + "}", v if isinstance(v, str) else str(v))

    handler = _AGENT_KIND_HANDLERS[kind]
    if kind == "openai_assistant":
        result = handler(agent_cfg, prompt, context)
    else:
        result = handler(agent_cfg, prompt, extra_context, context)

    return {
        **result,
        "agent": agent_name,
        "agent_kind": kind,
        "materialized_at": materialized_at,
        "op": "agent_call",
    }


_OPS = {
    "llm_call": _do_llm_call,
    "route": _do_route,
    "conditional_route": _do_conditional_route,
    "debate": _do_debate,
    "critique_loop": _do_critique_loop,
    "synthesize": _do_synthesize,
    "mcp_call": _do_mcp_call,
    "tool_use_loop": _do_tool_use_loop,
    "handoff": _do_handoff,
    "map": _do_map,
    "extract": _do_extract,
    "classify": _do_classify,
    "reduce": _do_reduce,
    "self_reflect": _do_self_reflect,
    "sub_pipeline": _do_sub_pipeline,
    "agent_call": _do_agent_call,
}


def _build_step_metadata(
    aid: str, entry: Any, partition_key: Optional[str]
) -> Dict[str, Any]:
    """Shared metadata builder for both the single-op multi_asset path
    and the per-step-ops graph_multi_asset path — same fields, same
    Insights-friendly shape, same op-specific rich blobs. Keys are
    prefixed with `{aid}__` so a multi-asset materialization event
    surfaces per-asset fields without collision."""
    import dagster as dg  # local import so this helper is safe to call
    md: Dict[str, Any] = {}
    if not isinstance(entry, dict):
        return md
    text = entry.get("text", "")
    md[f"{aid}__text"] = dg.MetadataValue.md(text[:2000] if text else "_(empty)_")
    if entry.get("cost_usd") is not None:
        md[f"{aid}__cost_usd"] = dg.MetadataValue.float(float(entry["cost_usd"]))
    if entry.get("latency_ms") is not None:
        md[f"{aid}__latency_ms"] = dg.MetadataValue.int(int(entry["latency_ms"]))
    if entry.get("tokens_total") is not None:
        md[f"{aid}__tokens_total"] = dg.MetadataValue.int(int(entry["tokens_total"]))
    if entry.get("n_llm_calls") is not None:
        md[f"{aid}__n_llm_calls"] = dg.MetadataValue.int(int(entry["n_llm_calls"]))
    if entry.get("model_fingerprint"):
        md[f"{aid}__model_fingerprint"] = dg.MetadataValue.text(str(entry["model_fingerprint"]))
    if entry.get("materialized_at"):
        try:
            md[f"{aid}__materialized_at"] = dg.MetadataValue.timestamp(
                datetime.fromisoformat(entry["materialized_at"])
            )
        except (TypeError, ValueError):
            md[f"{aid}__materialized_at"] = dg.MetadataValue.text(str(entry["materialized_at"]))
    if entry.get("op"):
        md[f"{aid}__op"] = dg.MetadataValue.text(str(entry["op"]))
    if partition_key is not None:
        md[f"{aid}__partition_key"] = dg.MetadataValue.text(str(partition_key))
    for k, v in entry.items():
        if k in ("text", "cost_usd", "latency_ms", "tokens_total",
                 "n_llm_calls", "model_fingerprint", "materialized_at", "op"):
            continue
        if k == "usage" and v is not None:
            md[f"{aid}__usage"] = dg.MetadataValue.json(v)
        elif k == "all_proposals" and v is not None:
            md[f"{aid}__proposals"] = dg.MetadataValue.json(v)
        elif k == "history" and v is not None:
            md[f"{aid}__history"] = dg.MetadataValue.json(v)
        elif k == "tool_call_trace" and v is not None:
            md[f"{aid}__tool_call_trace"] = dg.MetadataValue.json(v)
        elif isinstance(v, (str, int, float, bool)):
            md[f"{aid}__{k}"] = dg.MetadataValue.text(str(v))
    return md


def _default_router_prompt(specialists: List[dict]) -> str:
    lines = [
        "You are a router. Read the user's input and call exactly ONE of the tools below — the one whose description best matches the input.",
        "",
        "Available specialists:",
    ]
    for s in specialists:
        lines.append(f"  - {s['name']}: {s['description']}")
    lines.append("")
    lines.append("Call the chosen specialist's tool. Do not answer the user's question yourself.")
    return "\n".join(lines)


def _deep_apply_ctx_substitutions(obj: Any, partition_key: Optional[str], partition_fields: Optional[Dict[str, str]]) -> Any:
    """Recursively substitute `{partition_key}` + `{partition.<name>}` in every
    string leaf of a step dict / list / scalar. Non-strings pass through
    unchanged. Port substitution (`{port_name}`) is NOT done here — it
    happens per-op after inputs are resolved."""
    if isinstance(obj, str):
        return _apply_ctx_substitutions(obj, partition_key, partition_fields)
    if isinstance(obj, list):
        return [_deep_apply_ctx_substitutions(x, partition_key, partition_fields) for x in obj]
    if isinstance(obj, dict):
        return {k: _deep_apply_ctx_substitutions(v, partition_key, partition_fields) for k, v in obj.items()}
    return obj


# ── Personas resolution ──────────────────────────────────────────────
#
# `personas:` is a top-level named-reusable-config block. Steps and
# sub-configs reference a persona by name (`persona: <name>`); the
# persona's fields are merged in at dispatch time. Inline fields on the
# step always win over persona-provided fields — the persona is a
# defaults-provider, not an override.
#
# Persona reference sites (recursive traversal):
#   step level: llm_call / classify / extract / reduce / self_reflect /
#               map / tool_use_loop
#   sub-config: route.router, route.specialists[*],
#               conditional_route.specialists[*],
#               debate.proposers[*], debate.arbitrator,
#               critique_loop.drafter, critique_loop.critic
#
# The bundle fields (all optional): model, api_key_env_var,
# api_base_env_var, system_prompt, temperature, max_tokens,
# reasoning_effort, thinking_budget.

_PERSONA_FIELDS = (
    "model", "api_key_env_var", "api_base_env_var",
    "system_prompt", "temperature", "max_tokens",
    "reasoning_effort", "thinking_budget",
)


def _merge_persona(target: dict, persona: dict) -> dict:
    """Merge persona fields into `target`. Explicit inline fields on
    `target` win — persona is a defaults-provider. Non-persona fields on
    the persona bundle (e.g. accidentally-declared tools) are silently
    ignored so users can't stuff arbitrary things into a persona and
    have them leak into unrelated sub-configs."""
    merged = dict(target)
    for k in _PERSONA_FIELDS:
        if k in persona and merged.get(k) is None:
            merged[k] = persona[k]
    merged.pop("persona", None)  # consumed
    return merged


def _resolve_persona(node: Any, personas: Optional[Dict[str, Dict[str, Any]]]) -> Any:
    """Recursively walk a step dict, expanding `persona: <name>` references.
    Dicts and lists are traversed; scalars pass through."""
    if not personas:
        return node
    if isinstance(node, list):
        return [_resolve_persona(item, personas) for item in node]
    if not isinstance(node, dict):
        return node
    # Recurse first so nested `persona:` refs are handled at every level.
    walked = {k: _resolve_persona(v, personas) for k, v in node.items()}
    persona_name = walked.get("persona")
    if isinstance(persona_name, str):
        persona = personas.get(persona_name)
        if persona is None:
            raise ValueError(
                f"persona {persona_name!r} referenced but not declared in top-level "
                f"`personas:` block. Available: {sorted(personas.keys())}"
            )
        return _merge_persona(walked, persona)
    return walked


def _run_step(step: dict, state: Dict[str, Any], context) -> None:
    op = step.get("op")
    step_id = step.get("id")
    if not step_id:
        raise ValueError(f"every step must have an `id`; got {step!r}")
    if op not in _OPS:
        raise ValueError(f"unknown op: {op!r}. valid: {sorted(_OPS.keys())}")

    # Apply run-context substitutions ({partition_key}, {partition.<name>}) to
    # every string in the step dict before dispatch. Port substitution
    # (`{port_name}`) still happens per-op since it needs resolved inputs.
    _ctx = _ctx_of(state)
    step = _deep_apply_ctx_substitutions(step, _ctx.get("partition_key"), _ctx.get("partition_fields"))

    # Expand `persona: <name>` references — step-level AND every sub-config.
    # Personas live in the reserved `__personas__` state entry, put there by
    # the compute fn before it starts running steps.
    personas = state.get("__personas__")
    if personas:
        step = _resolve_persona(step, personas)

    context.log.info(f"[step {step_id!r}] op={op!r}")
    state[step_id] = _OPS[op](step, state, context)


# ── Component class ──────────────────────────────────────────────────

class AgenticPipelineComponent(dg.Component, dg.Model, dg.Resolvable):
    """Standardized agentic pipeline — one YAML, one asset (with named outputs).

    Same "single component, `steps:` list, multiple outputs" shape as the
    other pipeline components (polars_pipeline, warehouse_pipeline,
    pyspark_pipeline, snowpark_pipeline, ml_pipeline).

    9 ops:
      - llm_call:          single LLM call over source text
      - route:             router picks best specialist; specialist answers
      - conditional_route: deterministic branching (regex/contains/equals/
                           jsonpath) picks specialist; no router LLM
      - debate:            N proposers → arbitrator picks winner
      - critique_loop:     drafter → critic → drafter, N iterations;
                           optional `until_score_gte:` stops early on score
      - synthesize:        merge multiple upstream step texts into one
      - mcp_call:          direct MCP tool call (stdio/http/sse/fastmcp),
                           no LLM; `{text}` + `{port_name}` +
                           `{partition.<name>}` substitution in tool_args
      - tool_use_loop:     open-ended LLM+MCP tool loop, bounded by
                           max_iterations, one asset with full trajectory
      - handoff:           hand off to user-provided callable (LangGraph /
                           AutoGen / CrewAI / DSPy)

    Ops share YAML idioms with the other pipeline components:
      - `id:` names the step output for downstream reference
      - `source: <step_id>` picks upstream text (default = most recent step)
      - `outputs.assets: [step_ids]` picks which step outputs become assets
      - `outputs.text_sinks: [{from, path}]` writes text side files
      - `outputs.json_sinks: [{from, path}]` dumps full step output as JSON
    """

    asset_name_prefix: str = Field(
        description="Prefix for emitted asset names. Each step in outputs.assets becomes '{prefix}_{step_id}'.",
    )
    source: Dict[str, Any] = Field(
        description=(
            "Data source. Shapes: "
            "{kind: literal, text: '...'} | "
            "{kind: file, path: '...'} | "
            "{kind: url, url: '...'} | "
            "{kind: upstream_asset, upstream_asset_key: '...'}. "
            "All string fields are {partition_key}-templated."
        ),
    )
    steps: List[Dict[str, Any]] = Field(
        description=(
            "Ordered pipeline steps. Each step: {id, op, ...op-specific args}. "
            "\n\nTwo wiring modes (choose per step, they compose):\n"
            "  1. **Legacy single-source**: `source: <step_id>` reads that step's "
            "text into `{text}` in the prompt (default: most recent step). Reserved "
            "id `source` = initial pipeline source (use `source: source` to fan "
            "multiple steps off the same starting text).\n"
            "  2. **Typed named inputs** (recommended for joins): "
            "`inputs: {<port_name>: {from: <step_id>} | {literal: <value>}}`. "
            "Each port becomes a `{<port_name>}` placeholder in `prompt_template` "
            "AND `system_prompt` (and, for `mcp_call`, in string `tool_args`). "
            "Any step can join from any number of prior steps by port name — the "
            "shape common in agentic-orchestration graphs (fan-out → typed-join).\n\n"
            "8 ops. LLM ops (llm_call/route/debate/critique_loop/synthesize) all "
            "support optional `max_tokens`, `temperature`, `system_prompt`, "
            "`prompt_template`:\n"
            "  - **llm_call**: {model, api_key_env_var}. One LLM call. Supports "
            "both `source:` and `inputs:` for multi-input joins.\n"
            "  - **route**: {router: {model, api_key_env_var}, specialists: [{name, "
            "description, model, api_key_env_var, system_prompt}], fallback: name}. "
            "Router picks specialist, specialist answers.\n"
            "  - **debate**: {proposers: [{model, api_key_env_var, system_prompt}], "
            "arbitrator: {model, api_key_env_var, system_prompt}}. N proposers, "
            "arbitrator picks winner.\n"
            "  - **critique_loop**: {drafter: {model, api_key_env_var, system_prompt}, "
            "critic: {model, api_key_env_var, system_prompt}, iterations: int}. "
            "Drafter → critic → drafter, N iterations.\n"
            "  - **synthesize**: {model, api_key_env_var, sources: [<step_ids>] | "
            "inputs: {port: {from: id}}}. Merge multiple upstream step outputs. "
            "Prefer `inputs:` for named typed joins (Prefect-style execution-plan "
            "shape); `sources:` for positional legacy shape.\n"
            "  - **mcp_call**: {server: {name, type: stdio|http|sse|fastmcp, "
            "command|url, env|headers|headers_env}, mcp_tool_name, tool_args, "
            "parse_as: auto|json|text}. Direct MCP tool call (no LLM); "
            "string `tool_args` support `{text}` substitution against source "
            "AND `{port_name}` substitution from `inputs:`.\n"
            "  - **tool_use_loop**: {model, api_key_env_var, mcp_servers: "
            "[...], max_iterations, [system_prompt, allowed_tools, "
            "finalize_tool_name, temperature, max_tokens]}. Open-ended "
            "tool-use loop — LLM sees MCP tools, picks one, tool runs, "
            "LLM sees result, picks next tool, etc. Bounded by "
            "max_iterations OR the LLM calling the synthetic "
            "`finalize` tool with its final answer. The shape LangGraph "
            "is known for, done as ONE Dagster asset with the final "
            "answer as `text` + full tool-call trajectory in metadata. "
            "Cost/latency/tokens roll up across every internal call."
        ),
    )
    outputs: Dict[str, Any] = Field(
        description=(
            "Output declaration. Shape: "
            "{assets: [<step_ids>], text_sinks: [{from, path}], json_sinks: [{from, path}]}. "
            "`assets:` step outputs become first-class Dagster assets; "
            "`text_sinks:` writes step text to disk; `json_sinks:` writes full step dict."
        ),
    )
    personas: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named reusable LLM sub-configs. Each persona bundles "
            "`{model, api_key_env_var, api_base_env_var, system_prompt, "
            "temperature, max_tokens, reasoning_effort, thinking_budget}`. "
            "Reference from any step / sub-config via `persona: <name>`; "
            "declared fields on the persona are merged into the step's "
            "sub-config (explicit inline fields win). Applies to: step-level "
            "(llm_call, classify, extract, reduce, self_reflect, map, "
            "tool_use_loop) and sub-configs (route.router, "
            "route.specialists[*], debate.proposers[*], debate.arbitrator, "
            "critique_loop.drafter, critique_loop.critic, conditional_route."
            "specialists[*])."
        ),
    )
    agents: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named pre-built agents. Each entry declares `kind:` plus "
            "kind-specific connection config. Reference from a step via "
            "`op: agent_call, agent: <name>`. Supported kinds: "
            "`openai_assistant` (assistant_id + api_key_env_var — creates "
            "thread + message + run + polls), `remote_agent` (arbitrary "
            "HTTP endpoint — bearer / headers auth, POST / GET, sync or "
            "async polling, JSON-path response extraction), `handoff` "
            "(user-provided Python callable — same shape as the `handoff` "
            "op). Use to unify tier-3 pre-built agents behind a stable "
            "step-level interface: swap where an agent lives (OpenAI → "
            "self-hosted → Vercel) by editing the `agents:` block; every "
            "step referencing that agent keeps working."
        ),
    )
    group_name: Optional[str] = Field(default="agents", description="Group name for emitted assets.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds. Default: ['llm', 'agent', 'pipeline'].")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Additional tags on emitted assets.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    description: Optional[str] = Field(default=None, description="Description on emitted assets.")

    # ── Per-step ops (opt-in; changes the shape of the emitted job) ────
    #
    # When True, the pipeline is emitted as a `@dg.graph_multi_asset` with
    # one @op per step (plus an ingest op and an extract op) instead of a
    # single @multi_asset with one op. Trade-offs:
    #
    #   default (False, single @multi_asset):
    #     - One op per RUN in the Runs page → coarse-grained retry
    #       (if step 6 fails, the whole partition restarts)
    #     - Zero IO-manager overhead between steps (in-memory state dict)
    #
    #   per_step_ops=True (@dg.graph_multi_asset):
    #     - One op PER STEP visible in the Runs page → finer-grained retry
    #       (Dagster's native re-execution can restart from any failed op)
    #     - Each step's state dict passes through the IO manager (default
    #       pickle to filesystem). ~50-200KB per hop for text-heavy
    #       pipelines. Real but not painful.
    #
    # Also enables `can_subset` on the emitted graph — you can materialize
    # just a subset of the declared output assets without running the
    # whole pipeline.
    per_step_ops: bool = Field(
        default=False,
        description=(
            "When True, emit as a `@dg.graph_multi_asset` with one @op per "
            "step (visible in the Runs page + finer-grained retry). State "
            "dict flows through the IO manager between ops. Default False "
            "keeps the single-op @multi_asset shape (coarse retry, no IO "
            "overhead)."
        ),
    )

    # ── Partitioning (first-class fields, matching sibling components) ──
    # All emitted assets share a partitions_def built from these fields.
    # Use `{partition_key}` in source text / URL / file path / sink paths
    # to substitute per partition at compute time.
    partition_type: Optional[str] = Field(
        default=None,
        description=(
            "Partition type: 'daily' | 'weekly' | 'monthly' | 'hourly' | "
            "'static' | 'dynamic' | 'multi' | None (unpartitioned)."
        ),
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO date for time-based partition types (daily/weekly/monthly/hourly/multi).",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description=(
            "Comma-separated string OR list — the fixed partition keys "
            "for static/multi partitioning. e.g. ['NVDA', 'TSLA', 'META'] "
            "or 'NVDA,TSLA,META'."
        ),
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Multi-axis partition spec: list of {name, type, start, values, "
            "dynamic_partition_name}. Set instead of partition_type for "
            "multi-dimensional partitioning."
        ),
    )
    partition_key_parser: Optional[str] = Field(
        default=None,
        description=(
            "Format template for parsing composite partition keys into named "
            "fields — e.g. '{owner}/{repo}#{issue_number}' means partition key "
            "'dagster-io/dagster#30000' → {owner: 'dagster-io', repo: 'dagster', "
            "issue_number: '30000'}. Each parsed field is then available as "
            "`{partition.<name>}` in tool_args, prompt_template, and "
            "system_prompt (in addition to the raw `{partition_key}`). "
            "Pair with PartitionedAssetLauncherJobComponent for the "
            "config-driven-partition pattern."
        ),
    )

    @classmethod
    def get_form_config(cls):
        """Register as an App Managed Component so the Dagster / Dagster+
        UI can create + edit instances of this pipeline via a
        schema-driven form. Requires `dagster>=1.13.8` and the
        `flagComponentInstanceUI` feature flag."""
        from dagster.components.resolved.form_config import ComponentFormConfig

        return ComponentFormConfig(label="Agentic Pipeline", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        prefix = self.asset_name_prefix
        steps = list(self.steps)
        source_config = dict(self.source)
        outputs = dict(self.outputs)
        asset_ids: List[str] = list(outputs.get("assets", []))
        text_sinks: List[Dict[str, Any]] = list(outputs.get("text_sinks", []) or [])
        json_sinks: List[Dict[str, Any]] = list(outputs.get("json_sinks", []) or [])
        partition_key_parser = self.partition_key_parser

        if not asset_ids:
            raise ValueError("outputs.assets must list at least one step id.")

        group_name = self.group_name or "agents"
        base_kinds = list(self.kinds or ["llm", "agent"])
        base_tags = dict(self.tags or {})

        # Map step id → op so we can add the op name to that asset's kinds.
        # Router → kinds: [..., 'router'] etc. Lets users filter the asset
        # catalog by "show me every debate step" or "every route step".
        op_by_id = {s.get("id"): s.get("op") for s in steps if s.get("id") and s.get("op")}

        outs = {}
        for aid in asset_ids:
            per_asset_tags = dict(base_tags)
            for k in base_kinds:
                per_asset_tags[f"dagster/kind/{k}"] = ""
            op = op_by_id.get(aid)
            if op:
                per_asset_tags[f"dagster/kind/{op}"] = ""
            outs[f"{prefix}_{aid}"] = dg.AssetOut(
                group_name=group_name,
                description=self.description,
                owners=self.owners or None,
                tags=per_asset_tags,
                # is_required=False lets can_subset=True omit outputs when
                # the caller materializes a strict subset. When ALL outputs
                # are selected (the default UX), every emission still fires
                # and Dagster verifies completeness — same as before.
                is_required=False,
            )

        ins: Dict[str, dg.AssetIn] = {}
        if source_config.get("kind") == "upstream_asset":
            ins["source"] = dg.AssetIn(
                key=dg.AssetKey.from_user_string(source_config["upstream_asset_key"])
            )

        # ── Internal asset deps — the lineage inside the multi_asset ──
        # The compute function reads step-to-step data via the pipeline's
        # in-memory state dict. But Dagster's asset graph won't SHOW the
        # dependencies between the emitted assets unless we declare
        # `internal_asset_deps` explicitly — otherwise every emitted
        # asset renders as an orphan node.
        #
        # For each emitted step, collect:
        # 1. Every upstream STEP id (from `source:` / `sources:` / typed
        #    `inputs:` `{from: <id>}` refs) that is also in `outputs.assets`.
        # 2. If the step reads from the reserved id `source` (or omits
        #    source and thus defaults to the initial pipeline source)
        #    AND the pipeline source is `upstream_asset`, include the
        #    upstream_asset_key as a parent — otherwise the emitted asset
        #    would show as disconnected from the pipeline's real input.
        emitted_set = set(asset_ids)
        step_by_id = {s.get("id"): s for s in steps if s.get("id")}
        # Reserved id `source` refers to the initial pipeline source.
        # When the source is `upstream_asset`, treat that reserved id as
        # a stand-in for the real upstream asset key.
        _source_upstream_key = (
            source_config["upstream_asset_key"]
            if source_config.get("kind") == "upstream_asset"
            else None
        )
        # A step that OMITS `source:` reads from the last preceding step
        # in the `steps:` list. Precompute that fallback per step id.
        _prev_step_id: Dict[str, Optional[str]] = {}
        _prev: Optional[str] = None
        for s in steps:
            sid = s.get("id")
            if sid:
                _prev_step_id[sid] = _prev
                _prev = sid

        def _upstream_refs_for_step(step: dict) -> List[str]:
            """Return list of upstream refs (step ids OR the reserved 'source').

            Only includes explicit refs — the omitted-source fallback is
            handled by callers so they can decide how to resolve it.
            """
            ups: List[str] = []
            src = step.get("source")
            if isinstance(src, str):
                ups.append(src)
            for sid in (step.get("sources") or []):
                if isinstance(sid, str):
                    ups.append(sid)
            for _spec in (step.get("inputs") or {}).values():
                if isinstance(_spec, dict) and "from" in _spec:
                    _f = _spec["from"]
                    if isinstance(_f, str):
                        ups.append(_f)
            return ups

        def _resolve_step_parents(step_id: str, seen: Optional[set] = None) -> set:
            """Return the set of AssetKeys that are true asset-graph parents
            of the given emitted step. Walks upstream, skipping over any
            non-emitted intermediate step so lineage stays coherent.
            """
            if seen is None:
                seen = set()
            if step_id in seen:
                return set()
            seen.add(step_id)
            step = step_by_id.get(step_id) or {}
            parents: set = set()
            refs = _upstream_refs_for_step(step)
            # If the step has typed `inputs:` OR `sources:` OR an explicit
            # `source:`, those are authoritative. Otherwise fall back to the
            # last preceding step (the AgenticPipeline default).
            if not refs:
                # No explicit source. Runtime default: read from the
                # last-inserted state entry — which for step 1 is the
                # reserved id "source", and for step N>1 is the
                # previous step in the `steps:` list.
                _prev_id = _prev_step_id.get(step_id)
                refs = [_prev_id] if _prev_id else ["source"]
            for ref in refs:
                if ref == "source":
                    if _source_upstream_key:
                        parents.add(dg.AssetKey.from_user_string(_source_upstream_key))
                elif ref in emitted_set:
                    parents.add(dg.AssetKey([f"{prefix}_{ref}"]))
                elif ref in step_by_id:
                    # Non-emitted intermediate step — inherit its parents.
                    parents.update(_resolve_step_parents(ref, seen))
                # else: unknown ref — ignore silently (compute will error)
            return parents

        internal_asset_deps: Dict[str, set] = {}
        for aid in asset_ids:
            _parents = _resolve_step_parents(aid)
            if _parents:
                internal_asset_deps[f"{prefix}_{aid}"] = _parents

        # ── Partitions_def from first-class fields ──
        # Mirrors HumanApprovalGateComponent's shape. All emitted assets
        # share one partitions_def (they come from ONE multi_asset).
        _partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )

        # ── Branch: per_step_ops selects the emitted shape ────────────
        # (True → graph_multi_asset with N+2 ops; False → single multi_asset)
        if self.per_step_ops:
            return self._build_per_step_ops_defs(
                prefix=prefix, steps=steps, source_config=source_config,
                asset_ids=asset_ids, text_sinks=text_sinks, json_sinks=json_sinks,
                outs=outs, ins=ins, internal_asset_deps=internal_asset_deps,
                partitions_def=_partitions_def,
                partition_key_parser=partition_key_parser,
            )

        # ── Step-dep DAG for can_subset resume ──
        # For any step_id, resolve the transitive-upstream set of OTHER step
        # ids that must have run before it. Used at runtime when the caller
        # subset-materializes a subset of outputs — we run only the needed
        # steps, not the whole pipeline.
        def _direct_step_deps(step: dict) -> List[str]:
            """Step ids this step needs before it can run (excluding 'source')."""
            refs = _upstream_refs_for_step(step)
            if not refs:
                p = _prev_step_id.get(step.get("id"))
                refs = [p] if p else []
            return [r for r in refs if r != "source" and r in step_by_id]

        _step_direct_deps: Dict[str, List[str]] = {
            s["id"]: _direct_step_deps(s) for s in steps if s.get("id")
        }

        def _closure(target_ids: List[str]) -> List[str]:
            """Return step ids to run, in original `steps:` order, so that
            every target_id has its upstream deps materialized first."""
            need: set = set()
            stack: List[str] = list(target_ids)
            while stack:
                sid = stack.pop()
                if sid in need:
                    continue
                need.add(sid)
                for u in _step_direct_deps.get(sid, []):
                    if u not in need:
                        stack.append(u)
            # Preserve original declared order — steps are already
            # topologically ordered because `_run_step` requires upstreams
            # to be in `state` before dispatch.
            return [s["id"] for s in steps if s.get("id") in need]

        @dg.multi_asset(
            outs=outs,
            name=f"{prefix}_pipeline",
            ins=ins or None,
            internal_asset_deps=internal_asset_deps or None,
            partitions_def=_partitions_def,
            can_subset=True,
        )
        def _pipeline(context: dg.AssetExecutionContext, **kwargs):
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key:
                context.log.info(f"partition-aware materialization: partition_key={partition_key!r}")

            # can_subset: figure out which asset outputs were requested. When
            # the caller selects a strict subset, we run only the steps
            # transitively needed for those outputs — dbt-style per-step
            # resume without splitting into N @asset decorators.
            selected_output_names = set(context.selected_output_names)
            all_output_names = set(outs.keys())
            is_subset = 0 < len(selected_output_names) < len(all_output_names)
            selected_step_ids = (
                [aid for aid in asset_ids if f"{prefix}_{aid}" in selected_output_names]
                if is_subset else list(asset_ids)
            )
            steps_to_run_ids = _closure(selected_step_ids) if is_subset else [s["id"] for s in steps if s.get("id")]
            if is_subset:
                context.log.info(
                    f"can_subset ACTIVE: selected {len(selected_step_ids)}/{len(asset_ids)} asset(s) "
                    f"({sorted(selected_step_ids)}); running {len(steps_to_run_ids)}/{len(steps)} step(s) "
                    f"({steps_to_run_ids}) — skipping the rest"
                )

            # Parse the composite partition_key into named fields BEFORE ingest
            # so source.text / source.path / source.url can reference
            # {partition.<name>} placeholders too.
            partition_fields = _parse_partition_key(partition_key, partition_key_parser)
            if partition_fields:
                context.log.info(
                    f"partition_key_parser matched: {partition_fields}"
                )

            # Ingest.
            if source_config.get("kind") == "upstream_asset":
                upstream = kwargs["source"]
                if isinstance(upstream, dict) and "text" in upstream:
                    initial_text = upstream["text"]
                elif isinstance(upstream, str):
                    initial_text = upstream
                else:
                    initial_text = str(upstream)
                initial_entry = {"text": initial_text, "source_kind": "upstream_asset"}
                context.log.info(f"ingested {len(initial_text)} chars from upstream asset")
            else:
                initial_entry = _ingest(
                    source_config, context,
                    partition_key=partition_key,
                    partition_fields=partition_fields,
                )
                context.log.info(
                    f"ingested {len(initial_entry.get('text', ''))} chars via {source_config.get('kind', 'literal')}"
                )

            # Stash run-context so every op can reach it via _ctx_of(state).
            # Reserved keys `__ctx__` / `__personas__` — never collide with
            # user step ids (`_last_step_id` skips `__`-prefixed keys).
            state: Dict[str, Any] = {
                "source": initial_entry,
                "__ctx__": {
                    "partition_key": partition_key,
                    "partition_fields": partition_fields,
                },
                "__personas__": self.personas or {},
                "__agents__": self.agents or {},
            }

            steps_to_run_set = set(steps_to_run_ids)
            for step in steps:
                if step.get("id") in steps_to_run_set:
                    _run_step(step, state, context)

            # Text sinks (partition-aware paths). Creates parent dirs so
            # {partition_key}-templated subdirs Just Work — matters both
            # locally and for Serverless container filesystems.
            # When can_subset is active, only emit sinks whose upstream
            # step is in the selected/needed set.
            for sink in text_sinks:
                if sink["from"] not in state:
                    continue  # subset skipped this step; skip its sink too
                from_id = sink["from"]
                path = _apply_ctx_substitutions(sink["path"], partition_key, partition_fields)
                if from_id not in state:
                    raise ValueError(f"text_sinks: unknown step id {from_id!r}")
                text = state[from_id].get("text", "") if isinstance(state[from_id], dict) else str(state[from_id])
                parent = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(path, "w") as f:
                    f.write(text)
                context.log.info(f"text_sink {from_id!r} → {path}")

            # JSON sinks (full step dict, partition-aware paths).
            for sink in json_sinks:
                import json
                from_id = sink["from"]
                if from_id not in state:
                    continue  # subset skipped this step; skip its sink too
                path = _apply_ctx_substitutions(sink["path"], partition_key, partition_fields)
                parent = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(path, "w") as f:
                    json.dump(state[from_id], f, indent=2, default=str)
                context.log.info(f"json_sink {from_id!r} → {path}")

            # Emit assets in declared order — subset only emits SELECTED
            # step_ids (never the upstream-closure ones we had to run to
            # produce them; those would be "unselected outputs" and Dagster
            # rejects yielding those).
            asset_ids_to_emit = (
                [aid for aid in asset_ids if aid in set(selected_step_ids)]
                if is_subset else list(asset_ids)
            )
            missing = [aid for aid in asset_ids_to_emit if aid not in state]
            if missing:
                raise ValueError(f"outputs.assets references unknown step ids: {missing}")

            # Add materialization metadata for each asset output.
            #
            # Every step surfaces a common core of metadata (text, cost_usd,
            # latency_ms, model_fingerprint, materialized_at, n_llm_calls, op,
            # partition_key) — this is what makes Dagster's asset history the
            # thing you browse instead of job logs. Op-specific fields
            # (router_reasoning, all_proposals, history) come after.
            # can_subset requires yielding Output per selected asset — a
            # positional tuple return would need to line up with declared
            # outs order, which subset mode breaks. Yielding is the modern
            # multi_asset pattern anyway.
            for aid in asset_ids_to_emit:
                entry = state[aid]
                md: Dict[str, Any] = {}
                if isinstance(entry, dict):
                    text = entry.get("text", "")
                    md[f"{aid}__text"] = dg.MetadataValue.md(text[:2000] if text else "_(empty)_")

                    # Typed core fields — these show up prominently in the UI
                    # and are filterable across the asset's materialization history.
                    # Numeric fields — Dagster+ Insights turns these into
                    # dashboardable time-series with per-metric alerts.
                    if entry.get("cost_usd") is not None:
                        md[f"{aid}__cost_usd"] = dg.MetadataValue.float(float(entry["cost_usd"]))
                    if entry.get("latency_ms") is not None:
                        md[f"{aid}__latency_ms"] = dg.MetadataValue.int(int(entry["latency_ms"]))
                    if entry.get("tokens_total") is not None:
                        md[f"{aid}__tokens_total"] = dg.MetadataValue.int(int(entry["tokens_total"]))
                    if entry.get("n_llm_calls") is not None:
                        md[f"{aid}__n_llm_calls"] = dg.MetadataValue.int(int(entry["n_llm_calls"]))
                    if entry.get("model_fingerprint"):
                        md[f"{aid}__model_fingerprint"] = dg.MetadataValue.text(str(entry["model_fingerprint"]))
                    if entry.get("materialized_at"):
                        try:
                            md[f"{aid}__materialized_at"] = dg.MetadataValue.timestamp(
                                datetime.fromisoformat(entry["materialized_at"])
                            )
                        except (TypeError, ValueError):
                            md[f"{aid}__materialized_at"] = dg.MetadataValue.text(str(entry["materialized_at"]))
                    if entry.get("op"):
                        md[f"{aid}__op"] = dg.MetadataValue.text(str(entry["op"]))
                    if partition_key is not None:
                        md[f"{aid}__partition_key"] = dg.MetadataValue.text(str(partition_key))
                    if is_subset:
                        md[f"{aid}__subset_mode"] = dg.MetadataValue.text(
                            f"selected={sorted(selected_step_ids)}; ran={steps_to_run_ids}"
                        )

                    # Op-specific rich fields (JSON blobs the UI renders inline).
                    for k, v in entry.items():
                        if k in ("text", "cost_usd", "latency_ms", "tokens_total",
                                 "n_llm_calls", "model_fingerprint",
                                 "materialized_at", "op"):
                            continue
                        if k == "usage" and v is not None:
                            md[f"{aid}__usage"] = dg.MetadataValue.json(v)
                        elif k == "all_proposals" and v is not None:
                            md[f"{aid}__proposals"] = dg.MetadataValue.json(v)
                        elif k == "history" and v is not None:
                            md[f"{aid}__history"] = dg.MetadataValue.json(v)
                        elif isinstance(v, (str, int, float, bool)):
                            md[f"{aid}__{k}"] = dg.MetadataValue.text(str(v))
                yield dg.Output(value=entry, output_name=f"{prefix}_{aid}", metadata=md)

        return dg.Definitions(assets=[_pipeline])

    # ── Per-step-ops build path ──────────────────────────────────────
    #
    # Same runtime semantics as the single-op path — same _run_step,
    # same op executors, same state dict shape. But each step becomes
    # its OWN Dagster @op, so the Runs page shows N ops instead of 1
    # and Dagster's built-in per-op retry can restart from any failed
    # step. State dict passes through the IO manager between ops.

    def _build_per_step_ops_defs(
        self,
        *,
        prefix: str,
        steps: List[Dict[str, Any]],
        source_config: Dict[str, Any],
        asset_ids: List[str],
        text_sinks: List[Dict[str, Any]],
        json_sinks: List[Dict[str, Any]],
        outs: Dict[str, Any],
        ins: Dict[str, Any],
        internal_asset_deps: Dict[str, set],
        partitions_def,
        partition_key_parser: Optional[str],
    ) -> dg.Definitions:
        _self = self  # captured for closure use in ops

        # --- ingest op ---
        # Same behavior as the multi_asset's inline ingest: read source
        # config (literal / file / url / upstream_asset), build initial
        # state dict with `source` + `__ctx__`.
        source_kind = source_config.get("kind", "literal")
        upstream_asset_source = source_kind == "upstream_asset"

        if upstream_asset_source:
            # Special-case: ingest op has an asset input (the upstream).
            upstream_key = source_config["upstream_asset_key"]

            @dg.op(name=f"{prefix}_ingest", ins={"source": dg.In(dagster_type=Any)})
            def _ingest_op(context: dg.OpExecutionContext, source):
                partition_key = context.partition_key if context.has_partition_key else None
                partition_fields = _parse_partition_key(partition_key, partition_key_parser)
                if isinstance(source, dict) and "text" in source:
                    initial_text = source["text"]
                elif isinstance(source, str):
                    initial_text = source
                else:
                    initial_text = str(source)
                initial_entry = {"text": initial_text, "source_kind": "upstream_asset"}
                context.log.info(
                    f"[{prefix}_ingest] {len(initial_text)} chars from upstream asset "
                    f"partition_key={partition_key!r}"
                )
                return {
                    "source": initial_entry,
                    "__ctx__": {
                        "partition_key": partition_key,
                        "partition_fields": partition_fields,
                    },
                    "__personas__": _self.personas or {},
                    "__agents__": _self.agents or {},
                }
        else:
            @dg.op(name=f"{prefix}_ingest")
            def _ingest_op(context: dg.OpExecutionContext):
                partition_key = context.partition_key if context.has_partition_key else None
                partition_fields = _parse_partition_key(partition_key, partition_key_parser)
                initial_entry = _ingest(
                    source_config, context,
                    partition_key=partition_key, partition_fields=partition_fields,
                )
                context.log.info(
                    f"[{prefix}_ingest] {len(initial_entry.get('text', ''))} chars "
                    f"via {source_kind} partition_key={partition_key!r}"
                )
                return {
                    "source": initial_entry,
                    "__ctx__": {
                        "partition_key": partition_key,
                        "partition_fields": partition_fields,
                    },
                    "__personas__": _self.personas or {},
                    "__agents__": _self.agents or {},
                }

        # --- step ops (one per step) ---
        # Each takes prior state, runs its step, returns updated state.
        # Names are namespaced with the prefix so multiple pipelines in
        # the same project don't collide.

        def _make_step_op(step: Dict[str, Any]):
            step_id = step["id"]
            op_name = f"{prefix}_{step_id}"

            @dg.op(name=op_name)
            def _step_op(context: dg.OpExecutionContext, state: Dict[str, Any]) -> Dict[str, Any]:
                # State comes in from the IO manager (a fresh dict each hop
                # is what we want anyway). _run_step mutates in place.
                new_state = dict(state)
                _run_step(step, new_state, context)
                return new_state

            return _step_op

        step_ops = [_make_step_op(step) for step in steps]

        # --- extract op (yields all declared asset outputs) ---
        # Also handles text_sinks + json_sinks (same as the single-op path).
        # Yields one Output per declared asset, with full metadata.
        extract_outs = {f"{prefix}_{aid}": dg.Out(is_required=False) for aid in asset_ids}

        @dg.op(name=f"{prefix}_extract", out=extract_outs)
        def _extract_op(context: dg.OpExecutionContext, state: Dict[str, Any]):
            partition_key = context.partition_key if context.has_partition_key else None
            partition_fields = _parse_partition_key(partition_key, partition_key_parser)

            # Sinks (same code as single-op path).
            for sink in text_sinks:
                from_id = sink["from"]
                path = _apply_ctx_substitutions(sink["path"], partition_key, partition_fields)
                if from_id not in state:
                    raise ValueError(f"text_sinks: unknown step id {from_id!r}")
                text = state[from_id].get("text", "") if isinstance(state[from_id], dict) else str(state[from_id])
                parent = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(path, "w") as f:
                    f.write(text)
                context.log.info(f"text_sink {from_id!r} → {path}")

            for sink in json_sinks:
                import json as _json
                from_id = sink["from"]
                path = _apply_ctx_substitutions(sink["path"], partition_key, partition_fields)
                if from_id not in state:
                    raise ValueError(f"json_sinks: unknown step id {from_id!r}")
                parent = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(path, "w") as f:
                    _json.dump(state[from_id], f, indent=2, default=str)
                context.log.info(f"json_sink {from_id!r} → {path}")

            # Emit each declared asset as its own Output with metadata.
            missing = [aid for aid in asset_ids if aid not in state]
            if missing:
                raise ValueError(f"outputs.assets references unknown step ids: {missing}")

            for aid in asset_ids:
                entry = state[aid]
                md = _build_step_metadata(aid, entry, partition_key)
                yield dg.Output(
                    entry, output_name=f"{prefix}_{aid}", metadata=md,
                )

        # --- wire the graph ---
        # graph_multi_asset derives asset-to-asset deps from the graph
        # topology automatically — no `internal_asset_deps` needed here.
        # can_subset works natively because each asset comes from the
        # same terminal `_extract_op` yielding is_required=False Outputs.
        @dg.graph_multi_asset(
            outs=outs,
            name=f"{prefix}_pipeline",
            ins=ins or None,
            partitions_def=partitions_def,
            can_subset=True,
        )
        def _pipeline_graph(**kwargs):
            # Ingest → chain step ops → extract.
            if upstream_asset_source:
                state = _ingest_op(source=kwargs["source"])
            else:
                state = _ingest_op()
            for step_op in step_ops:
                state = step_op(state)
            # Extract op yields multiple named outputs; graph_multi_asset
            # picks them up by asset name.
            return _extract_op(state)

        return dg.Definitions(assets=[_pipeline_graph])
