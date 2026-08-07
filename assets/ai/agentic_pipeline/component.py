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

Op coverage (v1 = 5):

- llm_call       — single LLM call; uses source step's text as user prompt
- route          — router picks best specialist; specialist answers
- debate         — N proposers → arbitrator picks winner
- critique_loop  — drafter → critic → drafter, N iterations
- synthesize     — merge multiple upstream step texts into one summary

State model:

Every step's output is a dict `{"text": str, ...op-specific fields}`. Steps
read text from a prior step via `source: <id>` (default = most recent step).
The `assets:` list picks which step outputs become first-class Dagster
assets; each is emitted as a dict with the full op output preserved.
"""
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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
        text = _apply_partition_template(source_config["text"], partition_key)
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
    """Single LLM call. Uses source step's text as user prompt (or prompt_template with {text})."""
    materialized_at = _now_iso()
    source_id = step.get("source", _last_step_id(state))
    src_text = _get_source_text(state, source_id)

    prompt_template = step.get("prompt_template", "{text}")
    user_prompt = prompt_template.replace("{text}", src_text)
    temperature = step.get("temperature", 0.0)

    result = _completion(
        model=step["model"],
        system_prompt=step.get("system_prompt"),
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
    """Merge multiple upstream step texts into one via an LLM.

    sources: [step_id, step_id, ...]   (required — list of source step ids to synthesize)
    model, [system_prompt, api_key_env_var, temperature, max_tokens]
    prompt_template: default uses labeled sections.
    """
    materialized_at = _now_iso()
    source_ids = step.get("sources") or []
    if not source_ids:
        raise ValueError("synthesize requires `sources: [step_id, ...]` (>=1 id)")

    parts = []
    for sid in source_ids:
        parts.append(f"--- {sid} ---\n{_get_source_text(state, sid)}")
    combined = "\n\n".join(parts)

    prompt_template = step.get(
        "prompt_template",
        "You have {n_sources} labeled sections below. Synthesize them into a single coherent response.\n\n{combined}",
    )
    user_prompt = prompt_template.replace("{combined}", combined).replace("{n_sources}", str(len(source_ids)))
    temperature = step.get("temperature", 0.0)

    result = _completion(
        model=step["model"],
        system_prompt=step.get("system_prompt"),
        user_prompt=user_prompt,
        api_key_env_var=step.get("api_key_env_var"),
        api_base_env_var=step.get("api_base_env_var"),
        temperature=temperature,
        max_tokens=step.get("max_tokens", 4096),
    )

    return {
        "text": result["content"],
        "sources_used": list(source_ids),
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


# ── Op dispatcher ────────────────────────────────────────────────────

_OPS = {
    "llm_call": _do_llm_call,
    "route": _do_route,
    "debate": _do_debate,
    "critique_loop": _do_critique_loop,
    "synthesize": _do_synthesize,
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

    5 ops in v1:
      - llm_call:        single LLM call over source text
      - route:           router picks best specialist; specialist answers
      - debate:          N proposers → arbitrator picks winner
      - critique_loop:   drafter → critic → drafter, N iterations
      - synthesize:      merge multiple upstream step texts into one

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
            "Steps chain by id — a step with `source: <id>` reads that step's text; "
            "omit `source:` and it defaults to the most recent step's text. "
            "5 ops: llm_call | route | debate | critique_loop | synthesize."
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

        @dg.multi_asset(
            outs=outs,
            name=f"{prefix}_pipeline",
            ins=ins or None,
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
