"""LangGraph Agent Component.

Runs a multi-step LangGraph ``StateGraph`` as a single Dagster asset. Each
step is an LLM call that reads the shared state (initial input + all prior
step outputs) and appends its own output back to state. Steps chain
linearly by default; a step can optionally route conditionally to another
step name or to END based on a regex or Python-callable check.

Why LangGraph vs a plain chain?
  - Explicit stateful graph. Every node reads/writes a typed state dict.
  - Cheap conditional routing (early exit, retry-on-parse-fail, branch).
  - First-class support for streaming intermediate node outputs.
  - Checkpointed execution — future: swap in a Dagster-backed checkpointer.

State shape stored on each run:
  {
    "input": <initial user prompt>,
    "outputs": {step_name: <text>, ...},
    "final": <last step's text>,
    "steps_run": [<step_name>, ...],
    "stopped_by": "end_of_pipeline" | "conditional_end" | "step_error",
  }

Providers supported: openai, anthropic, google, azure_openai, ollama —
same set as ``langchain_chain_asset``. Provider packages are optional
extras; install only what you use.
"""
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
        description="Next step name. Omit on the last step. Use 'END' to terminate.",
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
    """Run a LangGraph pipeline of LLM steps as one Dagster asset.

    Each step is an LLM call over a template that can reference prior outputs.
    Linear by default; supports conditional routing via `condition_regex`.

    Example (3-step research pipeline):
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

    Output metadata surfaces the final answer, per-step outputs (as a
    collapsible JSON block), model, and steps_run.
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    input_prompt: str = Field(
        description=(
            "Initial user prompt. Available as {input} in every step's template. "
            "Supports {run_id}, {partition_key}, {partition_keys.<dim>} substitutions."
        ),
    )
    steps: List[LangGraphStep] = Field(
        description="Ordered list of LLM steps. Linear chain unless a step overrides `next`.",
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
        if not self.steps:
            raise ValueError(f"langgraph_agent {self.asset_name!r}: `steps` must contain at least one step.")
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

        @asset(
            key=AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            description=_self.description or (
                f"LangGraph pipeline ({_self.llm_provider}/{_self.model}) with "
                f"{len(_self.steps)} steps: {' → '.join(step_names)}"
            ),
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

            md: Dict[str, Any] = {
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
        elif explicit_next == "END" or i == len(steps) - 1:
            graph.add_edge(name, END)
        else:
            # Middle step with no next: fall through to the next in the list.
            graph.add_edge(name, step_names[i + 1])

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
