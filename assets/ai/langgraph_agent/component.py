"""LangGraph Agent Component.

Two mutually exclusive modes:

  steps:    Declarative prompt-chain DSL. Each step is a templated LLM call
            that reads the shared state and appends its output back to
            state. A step continues to another step ONLY if it explicitly
            sets `next`; omitting `next` terminates that branch at END
            (this applies to every step, not just the last one — a step
            reached only via `condition_regex` is usually meant to be a
            terminal leaf, so there's no implicit "fall through to the
            next declared step"). A step can also route conditionally to
            another step name or to END based on a regex check. Every node
            is hardcoded to "format a prompt, call one LLM" — no custom
            per-node logic, tool calls, or retrieval. Good for pure prompt
            chains with no bespoke node behavior.

  graph_fn: Bring-your-own-graph. A dotted path to a callable, defined in
            your own project, that returns an already-built LangGraph graph
            (a compiled StateGraph, or anything exposing `.invoke(state)`).
            The graph's actual logic — tool calls, custom control flow,
            retrieval, subgraphs, checkpointing, whatever LangGraph can do —
            lives in your own committed Python. This component just wires
            it up as a Dagster asset (metadata, retries, lineage) and
            invokes it with the run's initial state. Use this whenever a
            node needs to do more than "prompt → LLM → text".

State shape stored on each run (steps mode only; graph_fn mode's state
shape is whatever your own graph defines):
  {
    "input": <initial user prompt>,
    "outputs": {step_name: <text>, ...},
    "final": <last step's text>,
    "steps_run": [<step_name>, ...],
    "stopped_by": "end_of_pipeline" | "conditional_end" | "step_error",
  }

Providers supported (steps mode): openai, anthropic, google, azure_openai,
ollama — same set as ``langchain_chain_asset``. Provider packages are
optional extras; install only what you use.
"""
import importlib
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext,
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


def _resolve(callable_path: str, field_name: str):
    """Resolve a `module.path:function_name` reference to the actual
    callable. Same convention as dynamic_fanout_asset/warm_scheduled_job's
    `_resolve` — colon-separated, not dotted, so a dotted module path is
    unambiguous from the function name."""
    if ":" not in callable_path:
        raise ValueError(
            f"langgraph_agent: {field_name}={callable_path!r} must be "
            f"'module.path:function_name' (colon-separated), e.g. "
            f"'myproject.graphs.support:build_graph'."
        )
    module_path, fn_name = callable_path.split(":", 1)
    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(f"langgraph_agent: {field_name}: cannot import module {module_path!r}: {e}") from e
    try:
        return getattr(mod, fn_name)
    except AttributeError as e:
        raise AttributeError(f"langgraph_agent: {field_name}: module {module_path!r} has no attribute {fn_name!r}") from e


def _json_safe(obj: Any) -> Any:
    """Recursively coerce a graph_fn's returned state into JSON-serializable
    data for asset metadata — LangGraph state dicts commonly hold
    LangChain message objects or other non-JSON types that MetadataValue.json
    can't serialize directly."""
    import json

    try:
        json.dumps(obj)
        return obj
    except TypeError:
        pass
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    return str(obj)


class LangGraphStep(dg.Model, dg.Resolvable):
    """One node in the LangGraph pipeline."""

    name: str = Field(description="Unique node name. Referenced by `next` and templated as {outputs.<name>}.")
    prompt: str = Field(
        description=(
            "Prompt template. Use {input} for the initial user prompt and "
            "{outputs.<step_name>} to reference a prior step's output."
        ),
    )
    system_message: Optional[str] = Field(
        default=None,
        description="Optional system message for this step (overrides component-level system_message).",
    )
    model: Optional[str] = Field(
        default=None,
        description="Optional per-step model override (defaults to component-level model).",
    )
    temperature: Optional[float] = Field(
        default=None,
        description="Optional per-step temperature override.",
    )
    max_tokens: Optional[int] = Field(
        default=None,
        description="Optional per-step max_tokens override.",
    )
    next: Optional[str] = Field(
        default=None,
        description=(
            "Next step name. Omitting this (or setting 'END') terminates this branch at "
            "END — there is no implicit fall-through to the next declared step, so every "
            "step that should continue must set this explicitly."
        ),
    )
    condition_regex: Optional[str] = Field(
        default=None,
        description=(
            "Optional regex. If set, the step's output is tested against it: "
            "on match, routes to `next`; on no-match, routes to `condition_else` "
            "(or END if unset). Great for early-exit or self-review loops."
        ),
    )
    condition_else: Optional[str] = Field(
        default=None,
        description="Where to route when condition_regex does NOT match. Defaults to END.",
    )


class LangGraphAgentComponent(Component, Model, Resolvable):
    """Run a LangGraph pipeline as one Dagster asset — either a declarative
    prompt chain (`steps`) or your own pre-built graph (`graph_fn`). Exactly
    one of the two must be set.

    Example A — declarative prompt chain (3-step research pipeline). Linear
    by default; supports conditional routing via `condition_regex`. Every
    step is hardcoded to "template a prompt, call one LLM":
        ```yaml
        type: dagster_component_templates.LangGraphAgentComponent
        attributes:
          asset_name: research_report
          input_prompt: "How do vector databases handle high-cardinality metadata filters?"
          llm_provider: openai
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          system_message: "You are a rigorous technical researcher."
          steps:
            - name: plan
              prompt: |
                Break the question below into 3 focused sub-questions.
                Output ONLY a numbered list.

                Question: {input}
              next: research
            - name: research
              prompt: |
                Answer each sub-question below in 2-3 sentences. Cite
                specifics (algorithms, papers, techniques) where relevant.

                Sub-questions:
                {outputs.plan}
              next: synthesize
            - name: synthesize
              prompt: |
                Combine the research findings below into a single-paragraph
                answer to the original question.

                Original question: {input}
                Findings:
                {outputs.research}
        ```

    Example B — bring your own graph. `build_graph` is real Python you own,
    using LangGraph's full API (tool calls, custom node logic, subgraphs,
    checkpointers) — this component just invokes it:
        ```yaml
        type: dagster_component_templates.LangGraphAgentComponent
        attributes:
          asset_name: support_triage
          input_prompt: "{run_id}: escalation queue sweep"
          graph_fn: "myproject.graphs.support:build_graph"
        ```
        ```python
        # myproject/graphs/support.py
        from langgraph.graph import StateGraph, END

        def build_graph(context):
            graph = StateGraph(dict)
            graph.add_node("triage", my_triage_node)   # real logic, tool calls, etc.
            graph.add_node("escalate", my_escalate_node)
            graph.set_entry_point("triage")
            graph.add_conditional_edges("triage", my_router, {"escalate": "escalate", END: END})
            graph.add_edge("escalate", END)
            return graph.compile()
        ```

    Output metadata: `steps` mode surfaces the final answer, per-step
    outputs (as a collapsible JSON block), model, and steps_run. `graph_fn`
    mode surfaces the raw final state (JSON-safe coerced) and, if present, a
    `final` key as the markdown answer.
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    input_prompt: Optional[str] = Field(
        default=None,
        description=(
            "Initial user prompt. In `steps` mode: required, available as {input} in every "
            "step's template. In `graph_fn` mode: optional, merged into the initial state "
            "under the `input` key. Supports {run_id}, {partition_key}, {partition_keys.<dim>} substitutions."
        ),
    )
    steps: Optional[List[LangGraphStep]] = Field(
        default=None,
        description=(
            "Ordered list of LLM steps (declarative prompt-chain mode). Linear chain unless "
            "a step overrides `next`. Mutually exclusive with `graph_fn` — set exactly one."
        ),
    )
    graph_fn: Optional[str] = Field(
        default=None,
        description=(
            "'module.path:function_name' dotted path (colon-separated) to a callable, defined "
            "in your own project, that returns an already-built LangGraph graph exposing "
            "`.invoke(state) -> state` (a compiled StateGraph, or anything with that interface). "
            "Called as `graph_fn(context) -> graph`. Use this when a node needs custom Python "
            "logic, tool calls, retrieval, or anything beyond a templated prompt + LLM call — "
            "the graph's real logic lives in your own committed Python, not in this component's "
            "YAML. Mutually exclusive with `steps` — set exactly one. Resolved at materialize "
            "time (not component-load time), so example/manifest validation doesn't require "
            "your project modules to be importable."
        ),
    )
    initial_state: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Only used with `graph_fn`. Extra keys merged into the state dict passed to "
            "`graph.invoke(...)`. String values support {run_id}, {partition_key}, "
            "{partition_keys.<dim>} substitution (same as input_prompt). If `input_prompt` is "
            "also set, it's added first under the `input` key, so `initial_state` keys win on conflict."
        ),
    )

    llm_provider: str = Field(
        default="openai",
        description="LLM provider: openai, anthropic, azure_openai, google, ollama.",
    )
    model: str = Field(default="gpt-4o-mini", description="Default model. Overridable per step.")
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var containing the provider API key.",
    )
    api_base_env_var: Optional[str] = Field(
        default=None,
        description="Env var containing a custom base URL (for Azure / Ollama / self-hosted).",
    )
    system_message: Optional[str] = Field(
        default=None,
        description="Default system message applied to every step. Overridable per step.",
    )
    temperature: float = Field(default=0.0, description="Default sampling temperature.")
    max_tokens: int = Field(default=1024, description="Default max tokens per step.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Defaults to ['ai', 'langgraph', 'agent'].",
    )
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on failure.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        _self = self
        has_steps = bool(self.steps)
        has_graph_fn = bool(self.graph_fn)
        if has_steps and has_graph_fn:
            raise ValueError(
                f"langgraph_agent {self.asset_name!r}: `steps` and `graph_fn` are mutually "
                f"exclusive — set one or the other, not both."
            )
        if not has_steps and not has_graph_fn:
            raise ValueError(
                f"langgraph_agent {self.asset_name!r}: must set either `steps` (declarative "
                f"prompt-chain DSL) or `graph_fn` (bring your own pre-built LangGraph graph)."
            )

        step_names: List[str] = []
        if has_steps:
            if not self.input_prompt:
                raise ValueError(f"langgraph_agent {self.asset_name!r}: `input_prompt` is required when using `steps`.")
            step_names = [s.name for s in self.steps]
            if len(set(step_names)) != len(step_names):
                raise ValueError(f"langgraph_agent {self.asset_name!r}: step names must be unique. Got {step_names}.")

        _kinds = list(self.kinds or ["ai", "langgraph", "agent"])
        _all_tags = dict(self.asset_tags or {})
        for k in _kinds:
            _all_tags[f"dagster/kind/{k}"] = ""

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _default_description = (
            f"LangGraph pipeline ({_self.llm_provider}/{_self.model}) with "
            f"{len(_self.steps)} steps: {' → '.join(step_names)}"
            if has_steps
            else f"LangGraph graph from {_self.graph_fn!r} (bring-your-own-graph)"
        )

        @asset(
            key=AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            description=_self.description or _default_description,
            owners=_self.owners,
            tags=_all_tags,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _langgraph_asset(context: AssetExecutionContext) -> Dict[str, Any]:
            substitutions = {"run_id": context.run_id}
            if context.has_partition_key:
                pk = context.partition_key
                if hasattr(pk, "keys_by_dimension"):
                    substitutions["partition_key"] = str(pk)
                    substitutions["partition_keys"] = dict(pk.keys_by_dimension)
                else:
                    substitutions["partition_key"] = str(pk)
                    substitutions["partition_keys"] = {}

            if _self.graph_fn:
                graph_callable = _resolve(_self.graph_fn, "graph_fn")
                graph = graph_callable(context)

                state: Dict[str, Any] = {}
                if _self.input_prompt:
                    state["input"] = _substitute(_self.input_prompt, substitutions)
                for k, v in (_self.initial_state or {}).items():
                    state[k] = _substitute(v, substitutions) if isinstance(v, str) else v

                context.log.info(f"[langgraph] graph_fn={_self.graph_fn!r} initial_state_keys={list(state.keys())}")
                try:
                    final_state = graph.invoke(state)
                except Exception as e:
                    context.log.error(f"[langgraph] graph_fn invocation failed: {e}")
                    raise

                safe_state = _json_safe(final_state)
                md: Dict[str, Any] = {
                    "mode": MetadataValue.text("graph_fn"),
                    "graph_fn": MetadataValue.text(_self.graph_fn),
                    "final_state": MetadataValue.json(safe_state),
                }
                if isinstance(final_state, dict) and final_state.get("final"):
                    md["final_answer"] = MetadataValue.md(str(final_state["final"]))
                context.add_output_metadata(md)
                return final_state if isinstance(final_state, dict) else {"result": safe_state}

            resolved_input = _substitute(_self.input_prompt, substitutions)

            result = _run_graph(
                log=context.log,
                initial_input=resolved_input,
                steps=[s.model_dump() for s in _self.steps],
                llm_provider=_self.llm_provider,
                default_model=_self.model,
                api_key_env_var=_self.api_key_env_var,
                api_base_env_var=_self.api_base_env_var,
                default_system_message=_self.system_message,
                default_temperature=_self.temperature,
                default_max_tokens=_self.max_tokens,
            )

            md = {
                "mode": MetadataValue.text("steps"),
                "final_answer": MetadataValue.md(result["final"] or "_(empty)_"),
                "steps_run": MetadataValue.text(" → ".join(result["steps_run"])),
                "steps_run_count": MetadataValue.int(len(result["steps_run"])),
                "model": MetadataValue.text(_self.model),
                "provider": MetadataValue.text(_self.llm_provider),
                "stopped_by": MetadataValue.text(result["stopped_by"]),
                "step_outputs": MetadataValue.json(result["outputs"]),
            }
            context.add_output_metadata(md)
            return result

        return Definitions(assets=[_langgraph_asset])


def _substitute(s: str, substitutions: Dict[str, Any]) -> str:
    """Substitute `{run_id}` / `{partition_key}` / `{partition_keys.<dim>}` in a string.

    Called BEFORE the step's own {input}/{outputs.<name>} formatting, so
    it uses str.replace to avoid clashing with LangGraph's runtime substitutions.
    """
    if "{" not in s:
        return s
    out = s
    out = out.replace("{run_id}", str(substitutions.get("run_id", "")))
    out = out.replace("{partition_key}", str(substitutions.get("partition_key", "")))
    for dim, val in (substitutions.get("partition_keys") or {}).items():
        out = out.replace("{partition_keys." + dim + "}", str(val))
    return out


def _format_step_prompt(template: str, state: Dict[str, Any]) -> str:
    """Format a step template using {input} and {outputs.<name>} tokens.

    Uses simple str.replace so LangGraph consumers don't have to escape
    literal curly braces the way str.format would demand.
    """
    out = template.replace("{input}", str(state.get("input", "")))
    outputs = state.get("outputs") or {}
    for name, val in outputs.items():
        out = out.replace("{outputs." + name + "}", str(val))
    return out


def _build_llm(provider: str, model: str, api_key_env_var, api_base_env_var,
               temperature: float, max_tokens: int):
    import os

    api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
    api_base = os.environ.get(api_base_env_var) if api_base_env_var else None
    provider = provider.lower()
    if provider == "openai":
        from langchain_openai import ChatOpenAI
        kw: Dict[str, Any] = {"model": model, "temperature": temperature, "max_tokens": max_tokens}
        if api_key:
            kw["api_key"] = api_key
        if api_base:
            kw["base_url"] = api_base
        return ChatOpenAI(**kw)
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        kw = {"model": model, "temperature": temperature, "max_tokens": max_tokens}
        if api_key:
            kw["api_key"] = api_key
        return ChatAnthropic(**kw)
    if provider == "azure_openai":
        from langchain_openai import AzureChatOpenAI
        kw = {"azure_deployment": model, "temperature": temperature, "max_tokens": max_tokens}
        if api_key:
            kw["api_key"] = api_key
        if api_base:
            kw["azure_endpoint"] = api_base
        return AzureChatOpenAI(**kw)
    if provider == "google":
        from langchain_google_genai import ChatGoogleGenerativeAI
        kw = {"model": model, "temperature": temperature}
        if api_key:
            kw["google_api_key"] = api_key
        return ChatGoogleGenerativeAI(**kw)
    if provider == "ollama":
        from langchain_ollama import ChatOllama
        kw = {"model": model, "temperature": temperature}
        if api_base:
            kw["base_url"] = api_base
        return ChatOllama(**kw)
    raise ValueError(f"Unsupported llm_provider: {provider!r}")


def _run_graph(
    log,
    initial_input: str,
    steps: List[Dict[str, Any]],
    llm_provider: str,
    default_model: str,
    api_key_env_var,
    api_base_env_var,
    default_system_message,
    default_temperature: float,
    default_max_tokens: int,
) -> Dict[str, Any]:
    from langgraph.graph import StateGraph, END
    from langchain_core.messages import HumanMessage, SystemMessage

    step_by_name = {s["name"]: s for s in steps}
    step_names = [s["name"] for s in steps]

    def _make_node(step_cfg: Dict[str, Any]):
        model = step_cfg.get("model") or default_model
        temperature = step_cfg.get("temperature") if step_cfg.get("temperature") is not None else default_temperature
        max_tokens = step_cfg.get("max_tokens") or default_max_tokens
        system_message = step_cfg.get("system_message") or default_system_message
        name = step_cfg["name"]
        template = step_cfg["prompt"]

        def _node(state: Dict[str, Any]) -> Dict[str, Any]:
            llm = _build_llm(llm_provider, model, api_key_env_var, api_base_env_var, temperature, max_tokens)
            user_prompt = _format_step_prompt(template, state)
            msgs = []
            if system_message:
                msgs.append(SystemMessage(content=system_message))
            msgs.append(HumanMessage(content=user_prompt))
            log.info(f"[langgraph] step={name} model={model} prompt_chars={len(user_prompt)}")
            resp = llm.invoke(msgs)
            text = resp.content if hasattr(resp, "content") else str(resp)
            new_outputs = dict(state.get("outputs") or {})
            new_outputs[name] = text
            steps_run = list(state.get("steps_run") or []) + [name]
            return {"outputs": new_outputs, "final": text, "steps_run": steps_run}

        return _node

    graph = StateGraph(dict)
    for s in steps:
        graph.add_node(s["name"], _make_node(s))

    # First step is the entry point.
    graph.set_entry_point(step_names[0])

    # Wire edges.
    import re

    def _make_cond(step_cfg: Dict[str, Any]):
        regex = re.compile(step_cfg["condition_regex"])
        next_step = step_cfg.get("next")
        else_step = step_cfg.get("condition_else")

        def _router(state: Dict[str, Any]) -> str:
            last = (state.get("outputs") or {}).get(step_cfg["name"], "")
            if regex.search(str(last)):
                return next_step or END
            return else_step or END

        return _router

    for i, s in enumerate(steps):
        name = s["name"]
        explicit_next = s.get("next")
        cond = s.get("condition_regex")
        if cond:
            # Conditional edge.
            router = _make_cond(s)
            possible = set()
            if explicit_next and explicit_next != "END":
                possible.add(explicit_next)
            if s.get("condition_else") and s["condition_else"] != "END":
                possible.add(s["condition_else"])
            path_map = {n: n for n in possible}
            path_map[END] = END
            graph.add_conditional_edges(name, router, path_map)
        elif explicit_next and explicit_next != "END":
            if explicit_next not in step_by_name:
                raise ValueError(f"step {name!r} next={explicit_next!r} not found in steps.")
            graph.add_edge(name, explicit_next)
        else:
            # No explicit `next` (and no condition_regex): terminate at END.
            # This is NOT "fall through to the next step in the list" --
            # that used to be the behavior here, but it silently broke any
            # step reached only as a conditional branch target (e.g. this
            # component's own README quarantine/allow example: neither
            # branch sets `next`, and both are meant to be terminal leaves,
            # not a continuation into the next declared step). A step that
            # wants to continue must say so explicitly via `next`.
            graph.add_edge(name, END)

    compiled = graph.compile()

    initial_state = {
        "input": initial_input,
        "outputs": {},
        "final": "",
        "steps_run": [],
    }
    stopped_by = "end_of_pipeline"
    try:
        final_state = compiled.invoke(initial_state)
    except Exception as e:
        log.error(f"[langgraph] graph invocation failed: {e}")
        stopped_by = "step_error"
        raise

    # If not all steps ran, we hit a conditional END early.
    ran = final_state.get("steps_run") or []
    if len(ran) < len(steps) and set(ran) != set(step_names):
        stopped_by = "conditional_end"

    return {
        "input": initial_input,
        "outputs": final_state.get("outputs") or {},
        "final": final_state.get("final") or "",
        "steps_run": ran,
        "stopped_by": stopped_by,
        "model": default_model,
        "provider": llm_provider,
    }
