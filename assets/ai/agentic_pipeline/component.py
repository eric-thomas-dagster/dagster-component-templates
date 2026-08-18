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


def _ingest(source_config: dict, context, partition_key: Optional[str] = None) -> Dict[str, Any]:
    """Return the initial state dict: {"text": str, "source_kind": str, ...}."""
    kind = source_config.get("kind", "literal")

    if kind == "literal":
        raw = source_config["text"]
        # YAML may parse `text: 30000` as int / `text: 3.14` as float —
        # downstream ops assume text is a string, so coerce here.
        text = _apply_partition_template(raw if isinstance(raw, str) else str(raw), partition_key)
        return {"text": text, "source_kind": "literal"}

    if kind == "file":
        path = _apply_partition_template(source_config["path"], partition_key)
        with open(path) as f:
            text = f.read()
        return {"text": text, "source_kind": "file", "source_path": path}

    if kind == "url":
        import requests
        url = _apply_partition_template(source_config["url"], partition_key)
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
) -> Dict[str, Any]:
    """Thin LiteLLM wrapper. Returns {"content": str, "tool_calls": [...], "usage": {...}}."""
    try:
        import litellm
    except ImportError:
        raise ImportError("agentic_pipeline requires litellm: pip install 'litellm>=1.30.0'")

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

    call_started_at = time.time()
    response = litellm.completion(**kwargs)
    latency_ms = int((time.time() - call_started_at) * 1000)
    msg = response.choices[0].message
    content = msg.content or ""

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
    """The most recent step id in state (used when a step omits `source:`)."""
    return list(state.keys())[-1]


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


def _do_critique_loop(step: dict, state: Dict[str, Any], context) -> Dict[str, Any]:
    """Drafter writes; critic reviews; drafter revises. Repeat.

    drafter:   {model, [system_prompt, api_key_env_var, temperature, max_tokens]}
    critic:    {model, [system_prompt, api_key_env_var, temperature, max_tokens]}
    iterations: 2   (number of critique/revise cycles; >=1)
    """
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id)

    drafter = step["drafter"]
    critic = step["critic"]
    iterations = int(step.get("iterations", 2))
    if iterations < 1:
        raise ValueError(f"critique_loop iterations must be >=1; got {iterations}")

    all_llm_results = []

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
    )
    current_draft = draft_result["content"]
    history = [{"iteration": 0, "phase": "initial_draft", "text": current_draft}]
    drafter_usage = [draft_result["usage"]]
    critic_usage = []
    all_llm_results.append(draft_result)

    for i in range(iterations):
        # Critic reviews.
        critic_prompt = (
            f"Original task:\n{src_text}\n\n"
            f"Current draft:\n{current_draft}\n\n"
            f"Provide specific, actionable critique. Focus on what to improve, not what's good. "
            f"If the draft is already excellent, say so explicitly."
        )
        critique_result = _completion(
            model=critic["model"],
            system_prompt=critic.get("system_prompt")
                or "You are a careful reviewer. Give short, specific, actionable critique.",
            user_prompt=critic_prompt,
            api_key_env_var=critic.get("api_key_env_var"),
            api_base_env_var=critic.get("api_base_env_var"),
            temperature=critic.get("temperature", 0.0),
            max_tokens=critic.get("max_tokens", 1024),
        )
        critique = critique_result["content"]
        history.append({"iteration": i + 1, "phase": "critique", "text": critique})
        critic_usage.append(critique_result["usage"])
        all_llm_results.append(critique_result)

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
        )
        current_draft = revise_result["content"]
        history.append({"iteration": i + 1, "phase": "revised_draft", "text": current_draft})
        drafter_usage.append(revise_result["usage"])
        all_llm_results.append(revise_result)

    return {
        "text": current_draft,
        "iterations_done": iterations,
        "history": history,
        "drafter_model": drafter["model"],
        "critic_model": critic["model"],
        "model_fingerprint": f"{drafter['model']}//{critic['model']}×{iterations}",
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

    server: {name, type: stdio|http|sse, command|url, env|headers|headers_env}
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

    t0 = time.time()
    result = asyncio.run(
        _call_mcp_tool_async(
            log=context.log,
            server_cfg=server_cfg,
            tool_name=tool_name,
            tool_args=resolved_args,
            parse_as=parse_as,
        )
    )
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


# ── Op dispatcher ────────────────────────────────────────────────────

_OPS = {
    "llm_call": _do_llm_call,
    "route": _do_route,
    "debate": _do_debate,
    "critique_loop": _do_critique_loop,
    "synthesize": _do_synthesize,
    "mcp_call": _do_mcp_call,
}


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


def _run_step(step: dict, state: Dict[str, Any], context) -> None:
    op = step.get("op")
    step_id = step.get("id")
    if not step_id:
        raise ValueError(f"every step must have an `id`; got {step!r}")
    if op not in _OPS:
        raise ValueError(f"unknown op: {op!r}. valid: {sorted(_OPS.keys())}")
    context.log.info(f"[step {step_id!r}] op={op!r}")
    state[step_id] = _OPS[op](step, state, context)


# ── Component class ──────────────────────────────────────────────────

class AgenticPipelineComponent(dg.Component, dg.Model, dg.Resolvable):
    """Standardized agentic pipeline — one YAML, one asset (with named outputs).

    Same "single component, `steps:` list, multiple outputs" shape as the
    other pipeline components (polars_pipeline, warehouse_pipeline,
    pyspark_pipeline, snowpark_pipeline, ml_pipeline).

    6 ops:
      - llm_call:        single LLM call over source text
      - route:           router picks best specialist; specialist answers
      - debate:          N proposers → arbitrator picks winner
      - critique_loop:   drafter → critic → drafter, N iterations
      - synthesize:      merge multiple upstream step texts into one
      - mcp_call:        direct MCP tool call (stdio/http/sse), no LLM;
                         `{text}` substitution against source in tool_args

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
            "6 ops. LLM ops (llm_call/route/debate/critique_loop/synthesize) all "
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
            "  - **mcp_call**: {server: {name, type: stdio|http|sse, "
            "command|url, env|headers|headers_env}, mcp_tool_name, tool_args, "
            "parse_as: auto|json|text}. Direct MCP tool call (no LLM); "
            "string `tool_args` support `{text}` substitution against source "
            "AND `{port_name}` substitution from `inputs:`."
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
    group_name: Optional[str] = Field(default="agents", description="Group name for emitted assets.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds. Default: ['llm', 'agent', 'pipeline'].")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Additional tags on emitted assets.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    description: Optional[str] = Field(default=None, description="Description on emitted assets.")

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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        prefix = self.asset_name_prefix
        steps = list(self.steps)
        source_config = dict(self.source)
        outputs = dict(self.outputs)
        asset_ids: List[str] = list(outputs.get("assets", []))
        text_sinks: List[Dict[str, Any]] = list(outputs.get("text_sinks", []) or [])
        json_sinks: List[Dict[str, Any]] = list(outputs.get("json_sinks", []) or [])

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

        @dg.multi_asset(
            outs=outs,
            name=f"{prefix}_pipeline",
            ins=ins or None,
            internal_asset_deps=internal_asset_deps or None,
            partitions_def=_partitions_def,
        )
        def _pipeline(context: dg.AssetExecutionContext, **kwargs):
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key:
                context.log.info(f"partition-aware materialization: partition_key={partition_key!r}")

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
                initial_entry = _ingest(source_config, context, partition_key=partition_key)
                context.log.info(
                    f"ingested {len(initial_entry.get('text', ''))} chars via {source_config.get('kind', 'literal')}"
                )

            state: Dict[str, Any] = {"source": initial_entry}

            for step in steps:
                _run_step(step, state, context)

            # Text sinks (partition-aware paths). Creates parent dirs so
            # {partition_key}-templated subdirs Just Work — matters both
            # locally and for Serverless container filesystems.
            for sink in text_sinks:
                from_id = sink["from"]
                path = _apply_partition_template(sink["path"], partition_key)
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
                path = _apply_partition_template(sink["path"], partition_key)
                if from_id not in state:
                    raise ValueError(f"json_sinks: unknown step id {from_id!r}")
                parent = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(path, "w") as f:
                    json.dump(state[from_id], f, indent=2, default=str)
                context.log.info(f"json_sink {from_id!r} → {path}")

            # Emit assets in declared order.
            missing = [aid for aid in asset_ids if aid not in state]
            if missing:
                raise ValueError(f"outputs.assets references unknown step ids: {missing}")

            # Add materialization metadata for each asset output.
            #
            # Every step surfaces a common core of metadata (text, cost_usd,
            # latency_ms, model_fingerprint, materialized_at, n_llm_calls, op,
            # partition_key) — this is what makes Dagster's asset history the
            # thing you browse instead of job logs. Op-specific fields
            # (router_reasoning, all_proposals, history) come after.
            for aid in asset_ids:
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
                context.add_output_metadata(md, output_name=f"{prefix}_{aid}")

            return tuple(state[aid] for aid in asset_ids)

        return dg.Definitions(assets=[_pipeline])
