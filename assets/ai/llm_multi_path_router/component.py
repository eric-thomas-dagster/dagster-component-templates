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


def _invoke_tool(
    spec,
    args_str: str,
    *,
    context,
    llm_client,
    model: str,
    temperature: float,
    max_tokens: int,
) -> str:
    """Dispatch a tool call by tool_type. Returns the tool's textual output.

    - llm_roleplay: chat completion with spec.system_message as system prompt.
    - sql: format spec.sql_template with args and execute against
      `context.resources.<spec.resource>.get_connection()`. Returns rows as
      pipe-delimited text.
    - http: format spec.http_url + optional body_template with args, dispatch
      via `context.resources.<spec.resource>.request(...)` (if the resource
      exposes .request()) or plain `requests` otherwise.

    Missing / malformed configuration raises with an actionable message so the
    error surfaces on the failing step in the UI, not in a stack trace later.
    """
    tool_type = (spec.tool_type or "llm_roleplay").lower()

    if tool_type == "llm_roleplay":
        if not spec.system_message:
            raise ValueError(
                f"tool {spec.name!r}: tool_type='llm_roleplay' requires system_message."
            )
        resp = llm_client.chat.completions.create(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": spec.system_message},
                {"role": "user", "content": args_str},
            ],
        )
        return (resp.choices[0].message.content or "").strip()

    if tool_type == "sql":
        if not spec.resource:
            raise ValueError(f"tool {spec.name!r}: tool_type='sql' requires resource.")
        if not spec.sql_template:
            raise ValueError(f"tool {spec.name!r}: tool_type='sql' requires sql_template.")
        resource = getattr(context.resources, spec.resource, None)
        if resource is None:
            raise ValueError(
                f"tool {spec.name!r}: resource {spec.resource!r} not found on context. "
                f"Add a resource component registering key={spec.resource!r}."
            )
        # Template substitution — try to parse args as JSON so named-arg
        # templates like `... WHERE id = '{baggage_id}'` work when the LLM
        # emits JSON args. If it's not JSON, fall back to bare `{args}`.
        # Also expose the first JSON value as `{args}` so single-value
        # templates work regardless of what the LLM emits.
        _format_kwargs: Dict[str, Any] = {"args": args_str.strip()}
        _stripped = args_str.strip()
        if _stripped.startswith("{") and _stripped.endswith("}"):
            try:
                _parsed = json.loads(_stripped)
                if isinstance(_parsed, dict):
                    _format_kwargs.update({str(k): v for k, v in _parsed.items()})
                    # Override {args} with the first value if it's a single-key dict.
                    if len(_parsed) == 1:
                        _format_kwargs["args"] = next(iter(_parsed.values()))
            except json.JSONDecodeError:
                pass
        # Also strip surrounding quotes if the LLM emitted `"BAG-001"`.
        if isinstance(_format_kwargs["args"], str) and len(_format_kwargs["args"]) >= 2:
            _a = _format_kwargs["args"]
            if (_a[0], _a[-1]) in {('"', '"'), ("'", "'")}:
                _format_kwargs["args"] = _a[1:-1]
        try:
            query = spec.sql_template.format(**_format_kwargs)
        except KeyError as e:
            raise ValueError(
                f"tool {spec.name!r}: sql_template references {e} — "
                f"planner emitted args={args_str!r}. Either the planner must "
                f"emit that field as a JSON key or the template should use "
                f"the raw `{{args}}` substitution."
            ) from e
        with resource.get_connection() as conn:
            cur = conn.execute(query) if hasattr(conn, "execute") else conn.cursor().execute(query)
            try:
                rows = cur.fetchall()
            except Exception:
                rows = []
            try:
                cols = [d[0] for d in (cur.description or [])]
            except Exception:
                cols = []
        if not rows:
            return "(no rows)"
        if cols:
            return " | ".join(cols) + "\n" + "\n".join(" | ".join(str(v) for v in r) for r in rows)
        return "\n".join(" | ".join(str(v) for v in r) for r in rows)

    if tool_type == "http":
        if not spec.http_url:
            raise ValueError(f"tool {spec.name!r}: tool_type='http' requires http_url.")
        url = spec.http_url.format(args=args_str)
        body = spec.http_body_template.format(args=args_str) if spec.http_body_template else None
        headers = dict(spec.http_headers or {})
        method = (spec.http_method or "GET").upper()
        if spec.resource:
            resource = getattr(context.resources, spec.resource, None)
            if resource is None:
                raise ValueError(
                    f"tool {spec.name!r}: resource {spec.resource!r} not found on context."
                )
            if hasattr(resource, "request"):
                resp = resource.request(method=method, url=url, headers=headers, data=body)
                # Support both requests-style and plain-text .text attribute.
                return getattr(resp, "text", str(resp))
        import requests as _req
        resp = _req.request(method=method, url=url, headers=headers, data=body, timeout=30)
        resp.raise_for_status()
        return resp.text

    raise ValueError(f"tool {spec.name!r}: unknown tool_type={spec.tool_type!r}.")


class LlmMultiPathRouterToolSpec(dg.Resolvable, dg.Model):
    """One tool the router can pick at any ReAct step.

    Set `tool_type` to bind the tool to a real backend:

      - `llm_roleplay` (default): the tool is an LLM invocation with
        `system_message` — the LLM plays the tool's role. Great for demos
        where you don't have real endpoints yet. Requires `system_message`.

      - `sql`: args are substituted into `sql_template` and the query runs
        against `resource` (a Dagster resource key registered elsewhere in
        the project). Returns rows as a text block. Requires `resource` and
        `sql_template`.

      - `http`: args are substituted into `http_url` (and optionally
        `http_body_template`); an HTTP call is made via `resource`
        (a Dagster resource providing a `.request(method, url, ...)`
        interface, or the built-in `requests` fallback if `resource` is
        omitted). Requires `http_url`; optional `resource`, `http_method`,
        `http_headers`.

    `{args}` in any template resolves to the planner's raw args string.
    """

    name: str = Field(description="Tool name — planner picks by this.")
    description: str = Field(
        description=(
            "One-line description shown to the planner LLM. Be specific — "
            "the planner picks based on this text."
        ),
    )
    tool_type: str = Field(
        default="llm_roleplay",
        description="'llm_roleplay' (LLM plays the tool) | 'sql' (real SQL) | 'http' (real HTTP call).",
    )
    system_message: Optional[str] = Field(
        default=None,
        description=(
            "Only used when tool_type='llm_roleplay'. System prompt for the "
            "tool's LLM. Receives the planner's args string as the user "
            "message."
        ),
    )
    resource: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key. For tool_type='sql': the resource must "
            "expose a get_connection() context manager returning a DB-API "
            "connection (e.g., dagster-duckdb's DuckDBResource). For "
            "tool_type='http': the resource may expose a .request() method; "
            "if omitted, plain `requests` is used."
        ),
    )
    sql_template: Optional[str] = Field(
        default=None,
        description=(
            "Only used when tool_type='sql'. SQL template with `{args}` "
            "substitution. Example: \"SELECT * FROM baggage WHERE id = '{args}'\"."
        ),
    )
    http_method: str = Field(
        default="GET",
        description="Only used when tool_type='http'. HTTP method.",
    )
    http_url: Optional[str] = Field(
        default=None,
        description=(
            "Only used when tool_type='http'. URL template with `{args}` "
            "substitution. Example: \"https://airports/api/{args}\"."
        ),
    )
    http_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Only used when tool_type='http'. Request headers.",
    )
    http_body_template: Optional[str] = Field(
        default=None,
        description="Only used when tool_type='http'. Body template with `{args}` substitution.",
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
    output_schema: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Optional. Field name → short description of what to extract for "
            "this branch. When set, the classifier reads the ReAct trajectory "
            "and fills in these fields per case; the branch asset returns a "
            "single-row DataFrame with those columns (branch-specific data "
            "for downstream sinks). When None, the branch returns the raw "
            "trajectory + summary passthrough shape. Example: "
            "{'delivery_id': 'D<number> emitted by organize_delivery', "
            "'address': 'delivery address from the DB row'}."
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

    use_dynamic_partitions: bool = Field(
        default=True,
        description=(
            "Shape selector. True (default) → dynamic-partitions shape: emits "
            "one intermediate 'agent' asset (partitioned per case) PLUS N branch "
            "assets, each with its own DynamicPartitionsDefinition. Each branch "
            "asset only shows the case keys the router actually registered on "
            "it — clean per-branch lineage, no red-failed slots for cases that "
            "didn't take that branch. False → the older shape: single "
            "@graph_multi_asset with all outputs sharing one partitions_def "
            "and is_required=False (sparse slots, all in one asset). "
            "IGNORED when fanout_mode=True."
        ),
    )
    fanout_mode: bool = Field(
        default=False,
        description=(
            "Batch fan-out shape. When True the router becomes a @graph_asset "
            "that fans out over the upstream DataFrame's rows via DynamicOut. "
            "N per-case ReAct triages run in parallel inside ONE run, then "
            "collect into a batch result. Branch assets downstream are "
            "unpartitioned DataFrames with N rows for the cases that took "
            "that branch. Overrides use_dynamic_partitions when True. "
            "Compose with `partition_type` (daily/hourly/etc.) for the "
            "canonical production shape: daily-partitioned batch, fan-out "
            "inside — partition_key is the day, mapping_keys are the row ids "
            "within the day's data."
        ),
    )
    fanout_mapping_key_column: Optional[str] = Field(
        default=None,
        description=(
            "Only used when fanout_mode=True. Column in the upstream "
            "DataFrame to use as the DynamicOutput mapping_key per row (so "
            "per-item retries are stable). Falls back to `partition_static_"
            "column`, then to the row index."
        ),
    )
    fanout_batch_filter_column: Optional[str] = Field(
        default=None,
        description=(
            "Only used when fanout_mode=True AND the upstream is unpartitioned "
            "but the batch asset IS partitioned (e.g. daily-partitioned batch "
            "reading from an unpartitioned CSV with a date column). Column to "
            "filter upstream to `partition_key`'s rows. If upstream shares "
            "the same partition scheme, Dagster's IO manager handles the "
            "filter automatically — leave this unset."
        ),
    )
    router_asset_name: Optional[str] = Field(
        default=None,
        description=(
            "Only used when use_dynamic_partitions=True. Name for the "
            "intermediate 'agent' asset that runs the ReAct loop + registers "
            "case keys on branch dynamic partition sets. Defaults to "
            "`<asset_name>` (i.e., the base name).  If asset_name is 'baggage_triage_agent', "
            "the intermediate asset is `baggage_triage_agent` and the branches "
            "are the `outputs:` names."
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
        if self.fanout_mode:
            return self._build_fanout_shape(context)
        if self.use_dynamic_partitions:
            return self._build_dynamic_shape(context)
        return self._build_multi_asset_shape(context)

    def _build_multi_asset_shape(self, context: dg.ComponentLoadContext) -> dg.Definitions:
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
        # Union of resource keys any tool depends on — declared on every step
        # op so context.resources.<key> is available inside _invoke_tool.
        _tool_resource_keys = {
            t.resource for t in self.tools if t.resource
        }

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
                required_resource_keys=_tool_resource_keys,
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
                tool_output = _invoke_tool(
                    spec,
                    str(plan.get("args") or ""),
                    context=context,
                    llm_client=client,
                    model=_self.model,
                    temperature=_self.temperature,
                    max_tokens=_self.tool_max_tokens,
                )
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

    def _build_dynamic_shape(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """The clean shape: 1 router asset (static-partitioned per case, ReAct
        steps as ops in the run view) + N branch assets, each with its own
        DynamicPartitionsDefinition. At runtime, the router registers case_id
        on the appropriate branch's dynamic partition set. Each branch's UI
        shows ONLY the cases the router actually picked for it. No red-failed
        slots for cases that didn't take that branch.
        """
        _self = self

        if not self.partition_values:
            raise ValueError(
                "llm_multi_path_router.use_dynamic_partitions=True requires "
                "partition_values (comma-separated case_ids)."
            )
        case_ids = [v.strip() for v in self.partition_values.split(",") if v.strip()]
        router_partitions = dg.StaticPartitionsDefinition(case_ids)

        # Per-branch DynamicPartitionsDefinition. Named `<output>_cases` for clarity.
        branch_partitions: Dict[str, dg.DynamicPartitionsDefinition] = {
            o.name: dg.DynamicPartitionsDefinition(name=f"{o.name}_cases")
            for o in self.outputs
        }

        # ─── LLM client (same as multi_asset shape) ─────────────────
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
        outputs_list = list(self.outputs)
        output_names = {o.name for o in outputs_list}
        _tool_resource_keys = {t.resource for t in self.tools if t.resource}

        # ─── Op: build task string from upstream row ────────────────
        _task_template = self.task_template
        _partition_static_column = self.partition_static_column
        _upstream_asset_key = self.upstream_asset_key
        _router_asset_key = self.router_asset_name or self.asset_name

        @dg.op(
            name="build_task",
            ins={"upstream": dg.In(dg.Nothing)},
            out={"task_str": dg.Out(str)},
            description="Load upstream + filter to current partition + build task string.",
        )
        def _build_task_op(context):
            import pandas as pd
            _pk = context.partition_key if context.has_partition_key else None
            upstream = context.op_execution_context.load_asset_value(
                dg.AssetKey.from_user_string(_upstream_asset_key)
            )
            df = upstream if isinstance(upstream, pd.DataFrame) else pd.DataFrame(upstream)
            if _partition_static_column and _pk and _partition_static_column in df.columns:
                df = df[df[_partition_static_column].astype(str) == str(_pk)]
            if df.empty:
                raise ValueError(f"No upstream rows for partition {_pk!r}")
            row = df.iloc[0].to_dict()
            row.setdefault("partition_key", str(_pk or ""))
            try:
                return _task_template.format(**row)
            except KeyError as e:
                raise ValueError(
                    f"task_template references {e} but upstream row has columns: {list(row)}"
                ) from e

        # ─── ReAct step ops — same shape as the multi_asset variant ─
        def _make_step_op(iteration: int):
            @dg.op(
                name=f"plan_step_{iteration}",
                ins={
                    "task_str": dg.In(str),
                    "prior_step": dg.In(dict, default_value={"done": False, "trajectory": []}),
                },
                out={"step": dg.Out(dict)},
                required_resource_keys=_tool_resource_keys,
                description=f"ReAct step {iteration}: planner picks the next tool or declares done.",
            )
            def _step_op(context, task_str, prior_step):
                if prior_step.get("done"):
                    context.log.info(f"[step {iteration}] short-circuit — prior step declared done")
                    yield dg.Output({
                        "iteration": iteration,
                        "done": True,
                        "tool": None, "args": None,
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
                if raw.startswith("```"):
                    raw = raw.strip("`")
                    if raw.startswith("json"): raw = raw[len("json"):].strip()
                    if raw.endswith("```"): raw = raw[:-3].strip()
                try:
                    plan = json.loads(raw)
                except Exception as e:
                    raise ValueError(f"planner returned non-JSON: {raw[:200]}") from e

                context.log.info(
                    f"[step {iteration}] plan: done={plan.get('done')} tool={plan.get('tool')} "
                    f"args={str(plan.get('args'))[:120]} reasoning={plan.get('reasoning','')[:120]}"
                )

                if plan.get("done"):
                    yield dg.Output({
                        "iteration": iteration, "done": True,
                        "tool": None, "args": None,
                        "reasoning": plan.get("reasoning", ""),
                        "tool_output": None,
                        "trajectory": trajectory,
                    }, "step")
                    return

                tool_name = plan.get("tool")
                if tool_name not in tool_by_name:
                    raise ValueError(f"planner picked unknown tool {tool_name!r}; valid tools: {list(tool_by_name)}")
                spec = tool_by_name[tool_name]
                tool_output = _invoke_tool(
                    spec,
                    str(plan.get("args") or ""),
                    context=context,
                    llm_client=client,
                    model=_self.model,
                    temperature=_self.temperature,
                    max_tokens=_self.tool_max_tokens,
                )
                context.log.info(f"[step {iteration}] tool_output: {tool_output[:200]}")

                new_step = {
                    "iteration": iteration, "done": False,
                    "tool": tool_name, "args": plan.get("args"),
                    "reasoning": plan.get("reasoning", ""),
                    "tool_output": tool_output,
                }
                trajectory.append(new_step)
                yield dg.Output({**new_step, "trajectory": trajectory}, "step")
            return _step_op

        step_ops = [_make_step_op(i) for i in range(1, self.max_iterations + 1)]

        # ─── Classifier + partition-registration op ─────────────────
        # The classifier's job:
        #   1. Read the ReAct trajectory.
        #   2. Decide which branches apply to this case.
        #   3. For each picked branch, fill in the branch's output_schema
        #      fields (or omit if the branch has no schema declared).
        #   4. Register the current partition_key on each picked branch's
        #      DynamicPartitionsDefinition — that's what makes the branch
        #      asset materializable for this case.

        # Build a per-branch schema summary the classifier can see.
        _branch_schema_lines: List[str] = []
        for o in outputs_list:
            if o.output_schema:
                _fields = ", ".join(f"{k} ({v})" for k, v in o.output_schema.items())
                _branch_schema_lines.append(
                    f"  - {o.name}: {o.description}\n    payload fields: {{{_fields}}}"
                )
            else:
                _branch_schema_lines.append(
                    f"  - {o.name}: {o.description}\n    payload fields: (none — will use trajectory passthrough)"
                )
        _options_str = "\n".join(_branch_schema_lines)

        classifier_system = self.classifier_system_message or (
            "You classify an agent's ReAct trajectory into which downstream "
            "branches to emit AND extract branch-specific payloads. Reply ONLY "
            "with JSON of the form:\n"
            '  {"emit": {"<branch_name>": {payload-fields}, ...}, '
            '"summary": "<one line>"}\n'
            "Rules:\n"
            "  - Only pick branch names from the provided set.\n"
            "  - Fill in EVERY payload field listed for each picked branch — "
            "extract values from the tool outputs in the trajectory.\n"
            "  - For branches with `payload fields: (none — will use trajectory passthrough)`, "
            "emit an empty object `{}`.\n"
            "  - Omit branches that don't apply — don't include them in `emit`."
        )

        @dg.op(
            name="classify_and_register",
            ins={f"step_{i+1}": dg.In(dict, default_value={"done": False, "trajectory": []}) for i in range(self.max_iterations)},
            out={"classification": dg.Out(dict)},
            description=(
                "Classify the trajectory, extract per-branch payloads, and "
                "register the current partition_key on each picked branch's "
                "DynamicPartitionsDefinition."
            ),
        )
        def _classify_op(context, **kwargs):
            steps_ordered = [kwargs[f"step_{i+1}"] for i in range(_self.max_iterations)]
            final_trajectory: List[Dict[str, Any]] = []
            for s in steps_ordered:
                if s and s.get("trajectory"):
                    final_trajectory = s["trajectory"]

            trajectory_summary = "\n".join(
                f"Step {st['iteration']}: reasoning={st.get('reasoning','')} | "
                f"tool={st.get('tool')}({st.get('args')}) | output={str(st.get('tool_output',''))[:300]}"
                for st in final_trajectory
            ) or "(agent invoked no tools)"

            client = _client()
            resp = client.chat.completions.create(
                model=_self.model,
                temperature=_self.temperature,
                max_tokens=_self.classifier_max_tokens,
                messages=[
                    {"role": "system", "content": classifier_system},
                    {"role": "user", "content": (
                        f"Agent trajectory:\n{trajectory_summary}\n\n"
                        f"Available downstream branches:\n{_options_str}\n\n"
                        f'Which branches apply? Reply with JSON per the format above.'
                    )},
                ],
            )
            raw = (resp.choices[0].message.content or "").strip()
            if raw.startswith("```"):
                raw = raw.strip("`")
                if raw.startswith("json"): raw = raw[len("json"):].strip()
                if raw.endswith("```"): raw = raw[:-3].strip()
            try:
                classification = json.loads(raw)
            except Exception as e:
                raise ValueError(f"classifier returned non-JSON: {raw[:300]}") from e

            # Normalize `emit`: accept both new shape {name: {...}} and legacy
            # list shape [name, name] for backward compat with existing YAMLs.
            emit_raw = classification.get("emit")
            emit_payloads: Dict[str, Dict[str, Any]] = {}
            if isinstance(emit_raw, dict):
                for k, v in emit_raw.items():
                    if k in output_names:
                        emit_payloads[k] = v if isinstance(v, dict) else {}
            elif isinstance(emit_raw, list):
                for k in emit_raw:
                    if isinstance(k, str) and k in output_names:
                        emit_payloads[k] = {}

            picked = list(emit_payloads.keys())
            summary = str(classification.get("summary", ""))
            case_id = context.partition_key if context.has_partition_key else None
            context.log.info(f"classifier picked outputs: {picked} — {summary} (case_id={case_id})")
            for bname, payload in emit_payloads.items():
                context.log.info(f"  {bname}: {payload}")

            # Register the current case_id on each picked branch's dynamic
            # partition set → makes those branches materializable for this case.
            if case_id:
                for branch_name in picked:
                    pdef_name = f"{branch_name}_cases"
                    context.instance.add_dynamic_partitions(pdef_name, [case_id])
                    context.log.info(f"registered partition {case_id!r} on {pdef_name!r}")

            return {
                "picked": picked,
                "emit_payloads": emit_payloads,
                "summary": summary,
                "n_iterations": len(final_trajectory),
                "trajectory": final_trajectory,
                "case_id": case_id,
            }

        # ─── Router asset: graph-backed, static-partitioned per case ─
        router_kinds = set(self.tags.values() if isinstance(self.tags, dict) else []) if False else set()
        router_kinds |= {"ai", "agent", "router"}

        @dg.graph_asset(
            name=_router_asset_key,
            group_name=self.group_name,
            partitions_def=router_partitions,
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            kinds=router_kinds,
            description=(
                "Router agent. Per case: runs the ReAct loop (steps as ops), "
                "classifies the trajectory, and registers the case_id on each "
                "picked branch's DynamicPartitionsDefinition."
            ),
        )
        def _router_asset(upstream):
            task_str = _build_task_op(upstream)
            steps = []
            prior = None
            for op_fn in step_ops:
                step = op_fn(task_str=task_str) if prior is None else op_fn(task_str=task_str, prior_step=prior)
                steps.append(step)
                prior = step
            return _classify_op(**{f"step_{i+1}": s for i, s in enumerate(steps)})

        # ─── Branch assets — one per declared output ────────────────
        # Factory to bind the branch name + schema into a fresh closure per output.
        # Uses AssetDep+AllPartitionsMapping so the static-per-case router can
        # feed dynamic-per-branch downstream without a partition-scheme error.
        # (Runtime load is scoped to the branch's own partition_key = case_id.)
        #
        # When output_schema is declared, the branch returns a single-row DF
        # with the schema's columns filled from the classifier's emit_payloads
        # for this branch. Without output_schema it falls back to the legacy
        # trajectory-passthrough shape.
        def _make_branch_asset(bname: str, bdesc: str, bkinds: set, bpdef, router_key, bschema):
            @dg.asset(
                key=dg.AssetKey.from_user_string(bname),
                description=bdesc,
                group_name=self.group_name,
                kinds=bkinds,
                partitions_def=bpdef,
                owners=list(self.owners or []),
                tags=dict(self.tags or {}),
                deps=[dg.AssetDep(asset=router_key, partition_mapping=dg.AllPartitionMapping())],
            )
            def _branch_asset(context: dg.AssetExecutionContext):
                import pandas as pd
                case_id = context.partition_key if context.has_partition_key else None
                if not case_id:
                    raise RuntimeError(
                        f"branch asset {bname!r} materialized without a partition_key"
                    )
                router_val = context.op_execution_context.load_asset_value(
                    router_key, partition_key=case_id
                )
                picked = router_val.get("picked", []) if isinstance(router_val, dict) else []
                if bname not in picked:
                    raise RuntimeError(
                        f"branch {bname!r} materialized for case {case_id!r} but router "
                        f"didn't pick it (picked={picked}). Dynamic-partition registration "
                        f"may be out of sync."
                    )
                trajectory = router_val.get("trajectory", [])
                summary = str(router_val.get("summary", ""))
                emit_payloads = router_val.get("emit_payloads", {}) or {}
                trajectory_md = "\n".join(
                    f"**Step {st['iteration']}** — {st.get('reasoning','')}\n"
                    f"- tool: `{st.get('tool')}({st.get('args')})`\n"
                    f"- output: `{str(st.get('tool_output',''))[:400]}`"
                    for st in trajectory
                )

                if bschema:
                    # Structured payload: one row per branch with the schema's
                    # fields. Downstream sinks get branch-specific columns
                    # (delivery_id, address, eta for delivery_request;
                    # voucher_id, amount for voucher_issued; etc.)
                    payload = emit_payloads.get(bname, {}) or {}
                    row: Dict[str, Any] = {"case_id": case_id}
                    for field_name in bschema:
                        row[field_name] = payload.get(field_name)
                    df = pd.DataFrame([row])
                else:
                    # Legacy passthrough shape.
                    df = pd.DataFrame([{
                        "branch": bname,
                        "case_id": case_id,
                        "summary": summary,
                        "n_iterations": len(trajectory),
                        "trajectory": json.dumps(trajectory),
                    }])

                context.add_output_metadata({
                    "branch": dg.MetadataValue.text(bname),
                    "case_id": dg.MetadataValue.text(case_id),
                    "summary": dg.MetadataValue.text(summary),
                    "n_iterations": dg.MetadataValue.int(len(trajectory)),
                    "trajectory": dg.MetadataValue.md(trajectory_md or "*(no tools invoked)*"),
                    **({
                        f"payload/{k}": dg.MetadataValue.text(str(v))
                        for k, v in (emit_payloads.get(bname, {}) or {}).items()
                    } if bschema else {}),
                })
                return df
            return _branch_asset

        branch_assets: List[Any] = []
        _router_key = dg.AssetKey.from_user_string(_router_asset_key)
        for out_spec in outputs_list:
            branch_assets.append(_make_branch_asset(
                bname=out_spec.name,
                bdesc=out_spec.description,
                bkinds=set(out_spec.kinds or []) | {"ai", "agent", "branch"},
                bpdef=branch_partitions[out_spec.name],
                router_key=_router_key,
                bschema=out_spec.output_schema,
            ))

        return dg.Definitions(assets=[_router_asset, *branch_assets])

    def _build_fanout_shape(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Batch fan-out: one unpartitioned @graph_asset that reads all cases
        from upstream, fans out to N per-case ReAct triages via DynamicOut,
        and .collect()s into a single classification DataFrame. N unpartitioned
        branch assets each contain the rows for cases where THAT branch was
        picked.

        Use for high-volume batch processing where per-case partition catalog
        overhead isn't worth it. Each case is a row in a batch DataFrame, not
        a Dagster partition.
        """
        _self = self

        outputs_list = list(self.outputs)
        output_names = {o.name for o in outputs_list}
        tool_by_name = {t.name: t for t in self.tools}
        _tool_resource_keys = {t.resource for t in self.tools if t.resource}

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

        # ─── Per-case triage op: internal Python ReAct loop (no per-step ops) ─
        # Small per-item retry — a single bad LLM call on one case shouldn't
        # kill the whole batch.
        _per_item_retry = dg.RetryPolicy(max_retries=1, delay=2, backoff=dg.Backoff.EXPONENTIAL)

        @dg.op(
            name="triage_one_case",
            required_resource_keys=_tool_resource_keys,
            retry_policy=_per_item_retry,
            description="ReAct loop + classifier for ONE case. Called via .map() from fan_out_cases.",
        )
        def _triage_one_case_op(context, case: dict) -> dict:
            client = _client()
            case_dict = dict(case)
            # Build task template from row data.
            try:
                task = _self.task_template.format(**case_dict, partition_key="")
            except KeyError as e:
                raise ValueError(
                    f"task_template references {e} but case row has columns: {list(case_dict)}"
                ) from e

            tool_list = "\n".join(f"  - {t.name}: {t.description}" for t in _self.tools)
            valid_names = ", ".join(f'"{t.name}"' for t in _self.tools)

            trajectory: List[Dict[str, Any]] = []
            for i in range(1, _self.max_iterations + 1):
                prior_txt = "\n".join(
                    f"Step {t['iteration']}: {t.get('tool')}({t.get('args')}) → {str(t.get('tool_output',''))[:200]}"
                    for t in trajectory
                ) or "(no prior work)"
                planner_prompt = (
                    f"Task:\n{task}\n\nPrior:\n{prior_txt}\n\n"
                    f"Available tools:\n{tool_list}\n\n"
                    f'Decide: call one more tool from {valid_names} or declare done. '
                    f'Reply in JSON: {{"done": bool, "tool": "<name>|null", "args": "<string>|null", "reasoning": "<one clause>"}}'
                )
                resp = client.chat.completions.create(
                    model=_self.model, temperature=_self.temperature, max_tokens=_self.planner_max_tokens,
                    messages=[
                        {"role": "system", "content": "You are a strict tool-picking planner. Reply ONLY with JSON."},
                        {"role": "user", "content": planner_prompt},
                    ],
                )
                raw = (resp.choices[0].message.content or "").strip()
                if raw.startswith("```"):
                    raw = raw.strip("`")
                    if raw.startswith("json"): raw = raw[len("json"):].strip()
                    if raw.endswith("```"): raw = raw[:-3].strip()
                plan = json.loads(raw)
                case_id = case_dict.get(_self.partition_static_column) if _self.partition_static_column else "?"
                context.log.info(f"[{case_id} step {i}] plan={plan.get('tool')} done={plan.get('done')}")
                if plan.get("done"):
                    break
                tool_name = plan.get("tool")
                if tool_name not in tool_by_name:
                    raise ValueError(f"planner picked unknown tool {tool_name!r}")
                tool_output = _invoke_tool(
                    tool_by_name[tool_name],
                    str(plan.get("args") or ""),
                    context=context, llm_client=client,
                    model=_self.model, temperature=_self.temperature,
                    max_tokens=_self.tool_max_tokens,
                )
                trajectory.append({
                    "iteration": i, "done": False, "tool": tool_name,
                    "args": plan.get("args"), "reasoning": plan.get("reasoning",""),
                    "tool_output": tool_output,
                })

            # Classifier
            traj_txt = "\n".join(
                f"{t['iteration']}: {t.get('tool')}({t.get('args')}) → {str(t.get('tool_output',''))[:300]}"
                for t in trajectory
            ) or "(no tools)"
            _branch_schema_lines = []
            for o in outputs_list:
                if o.output_schema:
                    _fields = ", ".join(f"{k} ({v})" for k, v in o.output_schema.items())
                    _branch_schema_lines.append(f"  - {o.name}: {o.description}\n    payload fields: {{{_fields}}}")
                else:
                    _branch_schema_lines.append(f"  - {o.name}: {o.description}\n    payload fields: (none)")
            options_str = "\n".join(_branch_schema_lines)
            classifier_system = _self.classifier_system_message or (
                "You classify an agent's ReAct trajectory into which downstream "
                "branches to emit AND extract branch-specific payloads. Reply ONLY "
                "with JSON of the form:\n"
                '  {"emit": {"<branch_name>": {payload-fields}, ...}, "summary": "<one line>"}\n'
                "Only pick branch names from the provided set. Fill EVERY payload field."
            )
            resp = client.chat.completions.create(
                model=_self.model, temperature=_self.temperature, max_tokens=_self.classifier_max_tokens,
                messages=[
                    {"role": "system", "content": classifier_system},
                    {"role": "user", "content": f"Trajectory:\n{traj_txt}\n\nBranches:\n{options_str}"},
                ],
            )
            raw = (resp.choices[0].message.content or "").strip()
            if raw.startswith("```"):
                raw = raw.strip("`")
                if raw.startswith("json"): raw = raw[len("json"):].strip()
                if raw.endswith("```"): raw = raw[:-3].strip()
            cls = json.loads(raw)
            emit_raw = cls.get("emit")
            emit_payloads: Dict[str, Dict[str, Any]] = {}
            if isinstance(emit_raw, dict):
                for k, v in emit_raw.items():
                    if k in output_names:
                        emit_payloads[k] = v if isinstance(v, dict) else {}
            elif isinstance(emit_raw, list):
                for k in emit_raw:
                    if isinstance(k, str) and k in output_names:
                        emit_payloads[k] = {}

            case_id_val = case_dict.get(_self.partition_static_column) if _self.partition_static_column else None
            return {
                "case_id": case_id_val,
                "case_row": case_dict,
                "picked": list(emit_payloads),
                "emit_payloads": emit_payloads,
                "summary": str(cls.get("summary", "")),
                "n_iterations": len(trajectory),
                "trajectory": trajectory,
            }

        # ─── Fan-out op ─────────────────────────────────────────────────────
        # Filter to the current partition_key's rows if upstream isn't
        # partitioned but batch is (via fanout_batch_filter_column).
        _mapping_col = self.fanout_mapping_key_column or self.partition_static_column
        _batch_filter_col = self.fanout_batch_filter_column

        @dg.op(name="fan_out_cases", out=dg.DynamicOut(dict))
        def _fan_out_op(context, upstream):
            import pandas as pd
            df = upstream if isinstance(upstream, pd.DataFrame) else pd.DataFrame(upstream)
            # If batch is partitioned + user set fanout_batch_filter_column,
            # filter to just the rows for this partition_key.
            if _batch_filter_col and context.has_partition_key and _batch_filter_col in df.columns:
                pk = str(context.partition_key)
                df = df[df[_batch_filter_col].astype(str) == pk]
                context.log.info(f"filtered upstream to partition {pk!r} on {_batch_filter_col!r}: {len(df)} row(s)")
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                key = (str(row_dict[_mapping_col]) if (_mapping_col and _mapping_col in row_dict)
                       else str(row.name))
                context.log.info(f"fanning out {key}")
                yield dg.DynamicOutput(row_dict, mapping_key=key)

        # ─── Collect op ─────────────────────────────────────────────────────
        @dg.op(name="collect_batch")
        def _collect_batch_op(context, triaged: list) -> dict:
            context.log.info(f"collected {len(triaged)} case result(s)")
            return {"results": triaged}

        # ─── The graph_asset router ─────────────────────────────────────────
        # Build optional partitions_def for the BATCH (e.g. daily). This is
        # the "daily batch fans out over today's records" production shape.
        _batch_partitions_def = None
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
            if _pt == "daily":
                _batch_partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                _batch_partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                _batch_partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                _batch_partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _vals: raise ValueError("partition_type='static' requires partition_values.")
                _batch_partitions_def = StaticPartitionsDefinition(_vals)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                _batch_partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        _router_asset_key = self.router_asset_name or self.asset_name

        _router_kwargs: Dict[str, Any] = dict(
            name=_router_asset_key,
            group_name=self.group_name,
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            kinds={"ai", "agent", "router", "fanout"},
            description=(
                "Batch fan-out router: reads upstream, fans out to N per-case "
                "triages via DynamicOut, collects. Optional daily/hourly/static "
                "partition on the batch itself."
            ),
        )
        if _batch_partitions_def is not None:
            _router_kwargs["partitions_def"] = _batch_partitions_def

        @dg.graph_asset(**_router_kwargs)
        def _router_graph(upstream):
            cases = _fan_out_op(upstream)
            triaged = cases.map(_triage_one_case_op)
            return _collect_batch_op(triaged.collect())

        # ─── Branch assets — DataFrames per branch ──────────────────────────
        # When the batch is partitioned (e.g. daily), branches share the same
        # partition scheme so batch[2026-07-30] flows to branch[2026-07-30].
        def _make_batch_branch_asset(bname: str, bdesc: str, bkinds: set, bschema, router_key):
            _branch_kwargs: Dict[str, Any] = dict(
                key=dg.AssetKey.from_user_string(bname),
                description=bdesc,
                group_name=self.group_name,
                kinds=bkinds,
                owners=list(self.owners or []),
                tags=dict(self.tags or {}),
                ins={"router": dg.AssetIn(key=router_key)},
            )
            if _batch_partitions_def is not None:
                _branch_kwargs["partitions_def"] = _batch_partitions_def

            @dg.asset(**_branch_kwargs)
            def _branch_asset(context: dg.AssetExecutionContext, router) -> Any:
                import pandas as pd
                results = router.get("results", []) if isinstance(router, dict) else []
                rows = []
                for r in results:
                    payload = (r.get("emit_payloads") or {}).get(bname)
                    if payload is None:
                        continue
                    row: Dict[str, Any] = {
                        "case_id": r.get("case_id"),
                        "summary": r.get("summary"),
                    }
                    if bschema:
                        for field in bschema:
                            row[field] = payload.get(field)
                    else:
                        row["payload_json"] = json.dumps(payload)
                    rows.append(row)
                df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=(["case_id","summary"] + (list(bschema) if bschema else ["payload_json"])))
                context.add_output_metadata({
                    "branch": dg.MetadataValue.text(bname),
                    "row_count": dg.MetadataValue.int(len(df)),
                    "case_ids": dg.MetadataValue.text(", ".join(str(r.get("case_id")) for r in results if (r.get("emit_payloads") or {}).get(bname) is not None)),
                })
                return df
            return _branch_asset

        branch_assets: List[Any] = []
        _router_key = dg.AssetKey.from_user_string(_router_asset_key)
        for out_spec in outputs_list:
            branch_assets.append(_make_batch_branch_asset(
                bname=out_spec.name,
                bdesc=out_spec.description,
                bkinds=set(out_spec.kinds or []) | {"ai", "agent", "branch"},
                bschema=out_spec.output_schema,
                router_key=_router_key,
            ))

        return dg.Definitions(assets=[_router_graph, *branch_assets])
