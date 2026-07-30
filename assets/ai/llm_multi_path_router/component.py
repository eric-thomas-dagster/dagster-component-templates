"""LlmMultiPathRouterComponent — a router agent as ONE asset with N branches.

The clean shape for the "agent picks the next action from a bounded set" pattern:

  ┌──────────────────────────────────────────────────────────┐
  │  <asset_name>  (graph-backed @multi_asset per partition) │
  │                                                          │
  │   ┌─ plan_step_1 ─┐   ┌─ plan_step_2 ─┐  … plan_step_N   │
  │   │  (op)          │   │  (op)          │                │  ← Ops in the RUN view
  │   │  planner picks │   │  planner picks │                │    (not in asset graph)
  │   │  next tool /   │   │  next tool /   │                │
  │   │  says done     │   │  says done     │                │
  │   └────────────────┘   └────────────────┘                │
  │            │                    │                        │
  │            ▼                    ▼                        │
  │           short-circuit if any prior step said done       │
  │                                                          │
  │                     synthesizer op (classify → emit)      │
  └──────────────────────────────────────────────────────────┘
              │            │            │
              ▼            ▼            ▼
       <output_1>    <output_2>    <output_N>       ← Each is its own Dagster asset
     (only emitted if the classifier said "this path applies to this case")

Why this shape:
  - **One asset per case, multiple downstream branches.** Cleaner lineage than
    5 fixed step assets × K cases; each downstream sink shows only the cases
    that actually flowed through it.
  - **Ops visible in the run view.** Click a partition's run and see
    plan_step_1 → plan_step_2 → … → synthesizer as separate op boxes with
    their own logs.
  - **Trajectory in metadata.** The full ReAct transcript (planner
    reasoning + tool calls + tool outputs) lands in every emitted output's
    materialization metadata.
  - **Same tool-set safety as iterative_supervisor_agent** — planner picks
    tools BY NAME from a YAML-declared bounded set; cannot invent tools.

Use `IterativeSupervisorAgentComponent` when you want each ReAct step as
its own Dagster asset (per-step re-runs, per-step lineage).
Use this component when the agent is a single unit of work with multiple
downstream branches — one asset per case, ops for the loop, multi-asset
outputs for the branches.
"""

import json
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class LlmMultiPathRouterToolSpec(dg.Resolvable, dg.Model):
    """One tool the router can pick at any ReAct step."""

    name: str = Field(description="Tool name — planner picks by this.")
    description: str = Field(
        description=(
            "One-line description shown to the planner LLM. Be specific — "
            "the planner picks based on this text."
        ),
    )
    system_message: str = Field(
        description=(
            "The tool's LLM system prompt. Receives the planner's `tool_input` "
            "as the user message. Pre-seed with any ground-truth data the tool "
            "should have access to (e.g., simulated DB rows for a demo)."
        ),
    )


class LlmMultiPathRouterOutputSpec(dg.Resolvable, dg.Model):
    """One downstream branch — a multi-asset output emitted when the classifier
    picks this path."""

    name: str = Field(description="Asset name for this branch (Dagster AssetKey).")
    description: str = Field(
        description=(
            "One-line description shown to the classifier LLM. Used to decide "
            "whether this path applies to a case."
        ),
    )
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds.")


class LlmMultiPathRouterComponent(dg.Component, dg.Model, dg.Resolvable):
    """Router agent as a graph-backed multi-asset — steps as ops, branches as assets."""

    asset_name: str = Field(
        description=(
            "Base name for the router graph. Used in op naming; the actual "
            "assets emitted are the `outputs:` names below."
        ),
    )
    upstream_asset_key: str = Field(
        description=(
            "Upstream asset providing the per-case DataFrame. Filtered by "
            "partition_static_column to a single row per partition."
        ),
    )
    task_template: str = Field(
        description=(
            "The task template the planner sees for each case. Column names "
            "from the upstream row substitute via .format() — e.g. "
            "'Resolve baggage-loss for {passenger} on flight {flight}'."
        ),
    )
    tools: List[LlmMultiPathRouterToolSpec] = Field(
        description="Bounded list of tools the planner can pick at each step.",
    )
    outputs: List[LlmMultiPathRouterOutputSpec] = Field(
        description=(
            "The downstream branches. Each is its own Dagster asset. Only the "
            "outputs the classifier picks are emitted per partition; others "
            "are skipped (no materialization for that case)."
        ),
    )

    max_iterations: int = Field(
        default=5, ge=1, le=15,
        description="Max ReAct steps (= number of plan_step ops in the graph).",
    )
    model: str = Field(default="gpt-4o-mini")
    api_key_env_var: str = Field(default="OPENAI_API_KEY")
    api_base_env_var: Optional[str] = Field(default=None)
    temperature: float = Field(default=0.0)
    planner_max_tokens: int = Field(default=400)
    tool_max_tokens: int = Field(default=500)
    classifier_max_tokens: int = Field(default=400)
    classifier_system_message: Optional[str] = Field(
        default=None,
        description=(
            "Optional override for the classifier's system prompt. Default: "
            "picks output names from the declared set based on the trajectory."
        ),
    )

    # Standard fields
    group_name: Optional[str] = Field(default=None, description="Asset group.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags.")

    partition_type: Optional[str] = Field(
        default=None,
        description="'daily'|'weekly'|'monthly'|'hourly'|'static'|'dynamic'|None",
    )
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partitions.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated static values.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    partition_static_column: Optional[str] = Field(
        default=None,
        description=(
            "Upstream column to filter on for the current partition. e.g. "
            "'case_id' with partition_values 'c1,c2,c3'."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        # ─── Partitions ────────────────────────────────────────────
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _vals = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start.")
            if _pt == "daily": partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly": partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly": partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly": partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _vals: raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_vals)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        # ─── LLM client factory ─────────────────────────────────────
        def _client():
            import os
            try:
                from openai import OpenAI
            except ImportError as e:
                raise ImportError("llm_multi_path_router requires openai>=1.0.0") from e
            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                raise RuntimeError(f"{_self.api_key_env_var!r} env var not set.")
            kwargs: Dict[str, Any] = {"api_key": api_key}
            if _self.api_base_env_var:
                base_url = os.environ.get(_self.api_base_env_var)
                if base_url:
                    kwargs["base_url"] = base_url
            return OpenAI(**kwargs)

        tool_by_name = {t.name: t for t in self.tools}
        output_names = [o.name for o in self.outputs]

        # ─── Ops: one plan_step op per iteration + classifier op ────
        # Each step op takes (task_str, prior_step_json) and returns
        # step_json = {"iteration": n, "done": bool, "tool": str|None,
        #              "args": str|None, "reasoning": str, "tool_output": str|None,
        #              "trajectory_so_far": list}
        # Uses is_required=False so an early-done short-circuits the rest.

        def _make_step_op(iteration: int):
            @dg.op(
                name=f"plan_step_{iteration}",
                ins={
                    "task_str": dg.In(str),
                    "prior_step": dg.In(dict, default_value={"done": False, "trajectory": []}),
                },
                out={"step": dg.Out(dict)},
                description=f"ReAct step {iteration}: planner picks the next tool or declares done.",
            )
            def _step_op(context, task_str, prior_step):
                # Short-circuit: any prior step said done → pass state through
                # unchanged so the downstream classifier still receives the
                # trajectory. Don't call the LLM.
                if prior_step.get("done"):
                    context.log.info(f"[step {iteration}] short-circuit — prior step declared done")
                    yield dg.Output({
                        "iteration": iteration,
                        "done": True,
                        "tool": None,
                        "args": None,
                        "reasoning": "short-circuited — prior step done",
                        "tool_output": None,
                        "trajectory": list(prior_step.get("trajectory", [])),
                    }, "step")
                    return

                trajectory = list(prior_step.get("trajectory", []))
                tool_list = "\n".join(f"  - {t.name}: {t.description}" for t in _self.tools)
                valid_names = ", ".join(f'"{t.name}"' for t in _self.tools)

                prior_summary = ""
                if trajectory:
                    for st in trajectory:
                        prior_summary += (
                            f"Step {st['iteration']}: reasoning={st.get('reasoning','')} | "
                            f"tool={st.get('tool')}({st.get('args')}) | "
                            f"output={st.get('tool_output','')[:200]}\n"
                        )
                else:
                    prior_summary = "(this is step 1 — no prior work)"

                planner_prompt = (
                    f"Task:\n{task_str}\n\n"
                    f"Prior work so far:\n{prior_summary}\n"
                    f"Available tools:\n{tool_list}\n\n"
                    f"Decide: (a) call one more tool by picking from {valid_names}, "
                    f"or (b) declare done. Reply in JSON:\n"
                    f'{{"done": true|false, "tool": "<name>|null", "args": "<args-string>|null", "reasoning": "<one clause>"}}'
                )

                client = _client()
                resp = client.chat.completions.create(
                    model=_self.model,
                    temperature=_self.temperature,
                    max_tokens=_self.planner_max_tokens,
                    messages=[
                        {"role": "system", "content": "You are a strict tool-picking planner. Reply ONLY with the JSON — no prose, no markdown fences."},
                        {"role": "user", "content": planner_prompt},
                    ],
                )
                raw = (resp.choices[0].message.content or "").strip()
                # Strip common ```json fences if the LLM added them
                if raw.startswith("```"):
                    raw = raw.strip("`")
                    if raw.startswith("json"):
                        raw = raw[len("json"):].strip()
                    if raw.endswith("```"):
                        raw = raw[:-3].strip()
                try:
                    plan = json.loads(raw)
                except Exception as e:
                    context.log.error(f"[step {iteration}] planner returned non-JSON: {raw[:200]}")
                    raise ValueError(f"planner returned non-JSON: {e}") from e

                context.log.info(
                    f"[step {iteration}] plan: done={plan.get('done')} tool={plan.get('tool')} "
                    f"args={str(plan.get('args'))[:120]} reasoning={plan.get('reasoning','')[:120]}"
                )

                # If done, emit a terminal step (no tool call).
                if plan.get("done"):
                    yield dg.Output(
                        {
                            "iteration": iteration,
                            "done": True,
                            "tool": None,
                            "args": None,
                            "reasoning": plan.get("reasoning", ""),
                            "tool_output": None,
                            "trajectory": trajectory,
                        },
                        "step",
                    )
                    return

                # Execute the picked tool.
                tool_name = plan.get("tool")
                if tool_name not in tool_by_name:
                    raise ValueError(
                        f"planner picked unknown tool {tool_name!r}; valid tools: {list(tool_by_name)}"
                    )
                spec = tool_by_name[tool_name]
                tool_resp = client.chat.completions.create(
                    model=_self.model,
                    temperature=_self.temperature,
                    max_tokens=_self.tool_max_tokens,
                    messages=[
                        {"role": "system", "content": spec.system_message},
                        {"role": "user", "content": str(plan.get("args") or "")},
                    ],
                )
                tool_output = (tool_resp.choices[0].message.content or "").strip()
                context.log.info(f"[step {iteration}] tool_output: {tool_output[:200]}")

                new_step = {
                    "iteration": iteration,
                    "done": False,
                    "tool": tool_name,
                    "args": plan.get("args"),
                    "reasoning": plan.get("reasoning", ""),
                    "tool_output": tool_output,
                }
                trajectory.append(new_step)
                yield dg.Output(
                    {**new_step, "trajectory": trajectory},
                    "step",
                )
            return _step_op

        step_ops = [_make_step_op(i) for i in range(1, self.max_iterations + 1)]

        # ─── Task-builder op: convert upstream row + partition_key into task string
        _task_template = self.task_template
        _partition_static_column = self.partition_static_column
        _upstream_asset_key = self.upstream_asset_key

        @dg.op(
            name="build_task",
            ins={"upstream": dg.In(dg.Nothing)},
            out={"task_str": dg.Out(str)},
            description="Filter upstream to the current partition + build task string via template.",
            required_resource_keys=set(),
        )
        def _build_task_op(context):
            import pandas as pd
            _pk = context.partition_key if context.has_partition_key else None
            # Load upstream through the IO manager
            upstream = context.op_execution_context.load_asset_value(
                dg.AssetKey.from_user_string(_upstream_asset_key)
            )
            df = upstream if isinstance(upstream, pd.DataFrame) else pd.DataFrame(upstream)
            if _partition_static_column and _pk and _partition_static_column in df.columns:
                df = df[df[_partition_static_column].astype(str) == str(_pk)]
            if df.empty:
                raise ValueError(
                    f"No upstream rows for partition {_pk!r} (filter: "
                    f"{_partition_static_column}={_pk!r})"
                )
            row = df.iloc[0].to_dict()
            row.setdefault("partition_key", str(_pk or ""))
            try:
                task_str = _task_template.format(**row)
            except KeyError as e:
                raise ValueError(
                    f"task_template references {e} but upstream row has columns: {list(row)}"
                ) from e
            return task_str

        # ─── Classifier op: emits the multi-output ─────────────────
        outputs_list = list(self.outputs)
        classifier_system = self.classifier_system_message or (
            "You classify an agent's ReAct trajectory into which downstream "
            "branches to emit. Reply ONLY with a JSON object of the form "
            '{"emit": ["<name>", ...], "summary": "<one line>"}. Pick '
            "output names ONLY from the provided set."
        )

        @dg.op(
            name="classify_and_emit",
            ins={f"step_{i+1}": dg.In(dict, default_value={"done": False, "trajectory": []}) for i in range(self.max_iterations)},
            out={
                o.name: dg.Out(
                    is_required=False,
                    description=o.description,
                )
                for o in outputs_list
            },
            description="Classify the trajectory and yield one output per branch that applies.",
        )
        def _classify_op(context, **kwargs):
            import pandas as pd

            # Pull the latest non-empty trajectory from the ordered steps.
            steps_ordered = [kwargs[f"step_{i+1}"] for i in range(_self.max_iterations)]
            final_trajectory: List[Dict[str, Any]] = []
            for s in steps_ordered:
                if s and s.get("trajectory"):
                    final_trajectory = s["trajectory"]
            trajectory_summary = "\n".join(
                f"Step {st['iteration']}: reasoning={st.get('reasoning','')} | "
                f"tool={st.get('tool')}({st.get('args')}) | output={str(st.get('tool_output',''))[:250]}"
                for st in final_trajectory
            ) or "(agent invoked no tools)"

            options_str = "\n".join(f"  - {o.name}: {o.description}" for o in outputs_list)

            classifier_prompt = (
                f"Agent trajectory:\n{trajectory_summary}\n\n"
                f"Available downstream branches:\n{options_str}\n\n"
                f"Which branches apply to this case? Reply with JSON:\n"
                f'{{"emit": ["<name>", ...], "summary": "<one line>"}}'
            )
            client = _client()
            resp = client.chat.completions.create(
                model=_self.model,
                temperature=_self.temperature,
                max_tokens=_self.classifier_max_tokens,
                messages=[
                    {"role": "system", "content": classifier_system},
                    {"role": "user", "content": classifier_prompt},
                ],
            )
            raw = (resp.choices[0].message.content or "").strip()
            if raw.startswith("```"):
                raw = raw.strip("`")
                if raw.startswith("json"):
                    raw = raw[len("json"):].strip()
                if raw.endswith("```"):
                    raw = raw[:-3].strip()
            try:
                classification = json.loads(raw)
            except Exception as e:
                raise ValueError(f"classifier returned non-JSON: {raw[:300]}") from e

            picked = [n for n in classification.get("emit", []) if n in {o.name for o in outputs_list}]
            summary = str(classification.get("summary", ""))
            context.log.info(f"classifier picked outputs: {picked} — {summary}")

            # Build a small DataFrame per emitted output. Every emitted output
            # carries the same shape: [branch, summary, n_iterations, trajectory_json].
            # Downstream is free to enrich; this keeps the router honest.
            trajectory_md = "\n".join(
                f"**Step {st['iteration']}** — {st.get('reasoning','')}\n"
                f"- tool: `{st.get('tool')}({st.get('args')})`\n"
                f"- output: `{str(st.get('tool_output',''))[:400]}`"
                for st in final_trajectory
            )
            for out_spec in outputs_list:
                if out_spec.name in picked:
                    df = pd.DataFrame([{
                        "branch": out_spec.name,
                        "summary": summary,
                        "n_iterations": len(final_trajectory),
                        "trajectory": json.dumps(final_trajectory),
                    }])
                    yield dg.Output(df, output_name=out_spec.name, metadata={
                        "branch": dg.MetadataValue.text(out_spec.name),
                        "summary": dg.MetadataValue.text(summary),
                        "n_iterations": dg.MetadataValue.int(len(final_trajectory)),
                        "trajectory": dg.MetadataValue.md(trajectory_md or "*(no tools invoked)*"),
                    })

        # ─── Build the graph_multi_asset ───────────────────────────
        _upstream_key = dg.AssetKey.from_user_string(self.upstream_asset_key)
        # Every declared output becomes an AssetOut. All depend on upstream +
        # partition scheme + optional group/kinds.
        _kinds_base = {"ai", "agent", "router"}
        asset_outs: Dict[str, dg.AssetOut] = {}
        for o in outputs_list:
            _o_kinds = set(o.kinds or []) | _kinds_base
            asset_outs[o.name] = dg.AssetOut(
                key=dg.AssetKey.from_user_string(o.name),
                description=o.description,
                is_required=False,
                group_name=self.group_name,
                kinds=_o_kinds,
                owners=list(self.owners or []),
                tags=dict(self.tags or {}),
            )

        @dg.graph_multi_asset(
            outs=asset_outs,
            ins={"upstream": dg.AssetIn(key=_upstream_key)},
            partitions_def=partitions_def,
            group_name=self.group_name,
        )
        def _router_graph(upstream):
            # Establish ordering-only dep from upstream to build_task (Nothing In).
            task_str = _build_task_op(upstream)
            # Chain the plan_step ops sequentially.
            steps = []
            prior = None
            for op_fn in step_ops:
                if prior is None:
                    step = op_fn(task_str=task_str)
                else:
                    step = op_fn(task_str=task_str, prior_step=prior)
                steps.append(step)
                prior = step
            # Feed all step outputs into the classifier keyed by step_N.
            classifier_inputs = {f"step_{i+1}": s for i, s in enumerate(steps)}
            outs = _classify_op(**classifier_inputs)
            # graph_multi_asset expects a dict keyed by output name.
            if len(outputs_list) == 1:
                return {outputs_list[0].name: outs}
            return {o.name: getattr(outs, o.name) for o in outputs_list}

        return dg.Definitions(assets=[_router_graph])
