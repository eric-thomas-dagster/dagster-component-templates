"""Iterative Supervisor Agent — planner LOOPS with visibility into prior tool outputs.

The chaining variant of `SupervisorAgentComponent`. The planner runs once per
step, sees all prior steps' tool outputs, and picks the NEXT tool call — or
declares `done`. Each step is its own Dagster asset, so the full reasoning
trajectory is visible in `dg dev`.

Static DAG shape, dynamic termination: the component declares `max_iterations`
step assets at YAML load. Whichever step decides `done` short-circuits all
subsequent steps (they materialize as no-ops). Final answer synthesizer
reads every step and writes the grounded final response.

Why this is different from SupervisorAgentComponent:
  - **Chaining**: step N's planner sees step N-1's tool output; it can adapt
    its next-tool pick to what it just learned.
  - **Per-step visibility**: every planner decision + every tool call is its
    own asset materialization. Full audit trail of the ReAct loop.
  - **Same safety**: bounded tool set, planner picks BY NAME, cannot invent
    tools or write code.

Assets emitted (`2 + max_iterations` per YAML block):
  1. `<step_asset_prefix>_1` … `<step_asset_prefix>_<max_iterations>`
     — one asset per iteration; each emits {done, tool, args, reasoning, tool_output}
  2. `<synthesis_asset_name>` — reads all step outputs, writes final answer

For truly-unbounded iteration counts, wrap the step asset with a dynamic
partitions definition instead. This shape trades that flexibility for a
demoable, statically-visible DAG.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class IterativeSupervisorAgentToolSpec(dg.Resolvable, dg.Model):
    """One tool the iterative supervisor can pick at any step."""

    name: str = Field(description="Tool name — planner picks by this.")
    description: str = Field(description="One-line description shown to the planner.")
    system_message: str = Field(
        description=(
            "System prompt for this tool's LLM persona. Receives the "
            "planner's `args` field (JSON-encoded) as the user message."
        ),
    )


class IterativeSupervisorAgentComponent(dg.Component, dg.Model, dg.Resolvable):
    """Iterative / chained supervisor — planner loops, seeing prior tool outputs.

    Example (multi-step math + translate task):

        ```yaml
        type: dagster_community_components.IterativeSupervisorAgentComponent
        attributes:
          step_asset_prefix: agent_step
          synthesis_asset_name: agent_final_answer
          task: |
            Compute 149 euros × 12 months to find the annual cost. THEN
            translate the answer to French. Return the French sentence.
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          max_iterations: 5
          tools:
            - name: math_expert
              description: "Do arithmetic on numbers. Args: a math expression as string."
              system_message: "You are a calculator. Given a math expression, return the number and a one-line explanation."
            - name: translator
              description: "Translate text between languages. Args: {text, target_language}."
              system_message: "You are a translator. Given text + target language, return the translated text."
        ```

    Emits 6 assets: `agent_step_1` … `agent_step_5` + `agent_final_answer`.
    The planner might finish in 2 steps (math → translate → done at step 3),
    leaving steps 4 and 5 as no-ops.
    """

    step_asset_prefix: str = Field(
        default="agent_step",
        description="Prefix for the N step assets. Assets are named <prefix>_1, <prefix>_2, ...",
    )
    synthesis_asset_name: str = Field(description="Final synthesis asset name.")
    task: str = Field(description="The task the iterative agent works on.")
    tools: List[IterativeSupervisorAgentToolSpec] = Field(
        description="Bounded list of tools the planner can pick at each step.",
    )
    max_iterations: int = Field(
        default=5,
        ge=1,
        le=15,
        description="Max steps before forced termination. Also the number of step assets declared.",
    )
    model: str = Field(default="gpt-4o-mini")
    api_key_env_var: str = Field(default="OPENAI_API_KEY")
    api_base_env_var: Optional[str] = Field(default=None)
    temperature: float = Field(default=0.1, description="Lower is better for tool-picking discipline.")
    planner_max_tokens: int = Field(default=400)
    tool_max_tokens: int = Field(default=500)
    synthesis_max_tokens: int = Field(default=600)
    synthesis_system_message: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"ai", "agent", "iterative"})

        def _client():
            import os
            try:
                from openai import OpenAI
            except ImportError as e:
                raise ImportError("iterative_supervisor_agent requires openai>=1.0.0") from e
            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                raise RuntimeError(f"{_self.api_key_env_var!r} env var not set.")
            kwargs: Dict[str, Any] = {"api_key": api_key}
            if _self.api_base_env_var:
                base_url = os.environ.get(_self.api_base_env_var)
                if base_url:
                    kwargs["base_url"] = base_url
            return OpenAI(**kwargs)

        def _plan_and_execute_step(context, iteration: int, prior_step_dfs: List[Any]):
            import json
            import pandas as pd

            # Any prior step said done? → short-circuit
            for prior_df in prior_step_dfs:
                if prior_df is None:
                    continue
                df = prior_df if isinstance(prior_df, pd.DataFrame) else pd.DataFrame(prior_df)
                if not df.empty and bool(df.iloc[0].get("done", False)):
                    context.log.info(f"[step {iteration}] short-circuit — prior step reported done")
                    context.add_output_metadata({
                        "skipped": dg.MetadataValue.bool(True),
                        "reason": dg.MetadataValue.text("prior step declared done"),
                    })
                    return pd.DataFrame([{
                        "iteration": iteration,
                        "done": True,
                        "tool": None,
                        "args": None,
                        "reasoning": "short-circuited — prior step done",
                        "tool_output": None,
                    }])

            client = _client()

            tool_list_str = "\n".join(
                f"  - {t.name}: {t.description}" for t in _self.tools
            )
            valid_names = ", ".join(f'"{t.name}"' for t in _self.tools)

            # Build the "prior work" section for the planner.
            prior_summary_parts: List[str] = []
            for i, prior_df in enumerate(prior_step_dfs, start=1):
                if prior_df is None:
                    continue
                df = prior_df if isinstance(prior_df, pd.DataFrame) else pd.DataFrame(prior_df)
                if df.empty:
                    continue
                r = df.iloc[0]
                if r.get("tool"):
                    prior_summary_parts.append(
                        f"Step {i}:\n"
                        f"  reasoning: {r.get('reasoning', '')}\n"
                        f"  tool called: {r.get('tool')}({r.get('args')})\n"
                        f"  tool output: {r.get('tool_output', '')}\n"
                    )
            prior_summary = "\n".join(prior_summary_parts) or "(no prior steps yet — this is step 1)"

            planner_prompt = (
                f"You are an iterative agent. You've already done the work "
                f"below. Decide whether ONE MORE tool call is needed, or if "
                f"the task is complete.\n\n"
                f"Task:\n{_self.task}\n\n"
                f"Available tools:\n{tool_list_str}\n\n"
                f"Prior steps:\n{prior_summary}\n\n"
                f"Output ONLY a JSON object (no markdown fences). If more work "
                f"is needed:\n"
                f"  {{\"done\": false, \"tool\": \"<one of: {valid_names}>\","
                f" \"args\": <string OR object>,"
                f" \"reasoning\": \"<one sentence — why this tool now>\"}}\n"
                f"If the task is complete:\n"
                f"  {{\"done\": true, \"reasoning\": \"<one sentence — why we're done>\"}}"
            )

            context.log.info(f"[step {iteration}] planner deciding next action")
            resp = client.chat.completions.create(
                model=_self.model,
                temperature=_self.temperature,
                max_tokens=_self.planner_max_tokens,
                messages=[
                    {"role": "system", "content": "You are a careful iterative agent that picks tools."},
                    {"role": "user", "content": planner_prompt},
                ],
            )
            raw = (resp.choices[0].message.content or "").strip()
            if raw.startswith("```"):
                raw = raw.strip("`").split("\n", 1)[-1]
                if raw.endswith("```"):
                    raw = raw.rsplit("```", 1)[0]
            try:
                plan = json.loads(raw)
            except json.JSONDecodeError as e:
                context.log.warning(f"[step {iteration}] plan JSON parse failed: {e}; raw={raw[:200]}")
                plan = {"done": True, "reasoning": f"plan parse failed: {e}"}

            done = bool(plan.get("done", False))
            tool_name = plan.get("tool")
            args = plan.get("args", "")
            reasoning = plan.get("reasoning", "")

            if done or not tool_name:
                context.log.info(f"[step {iteration}] planner reports done: {reasoning}")
                context.add_output_metadata({
                    "done": dg.MetadataValue.bool(True),
                    "reasoning": dg.MetadataValue.text(reasoning),
                    "tool": dg.MetadataValue.text("(none)"),
                })
                return pd.DataFrame([{
                    "iteration": iteration,
                    "done": True,
                    "tool": None,
                    "args": None,
                    "reasoning": reasoning,
                    "tool_output": None,
                }])

            valid_tool_names = {t.name for t in _self.tools}
            if tool_name not in valid_tool_names:
                context.log.warning(f"[step {iteration}] planner picked invalid tool {tool_name!r}; forcing done")
                return pd.DataFrame([{
                    "iteration": iteration,
                    "done": True,
                    "tool": None,
                    "args": None,
                    "reasoning": f"planner picked invalid tool {tool_name!r}",
                    "tool_output": None,
                }])

            tool_spec = next(t for t in _self.tools if t.name == tool_name)
            args_str = args if isinstance(args, str) else json.dumps(args)
            context.log.info(f"[step {iteration}] running tool {tool_name}({args_str[:80]}); reason: {reasoning}")

            tool_resp = client.chat.completions.create(
                model=_self.model,
                temperature=_self.temperature,
                max_tokens=_self.tool_max_tokens,
                messages=[
                    {"role": "system", "content": tool_spec.system_message},
                    {"role": "user", "content": args_str},
                ],
            )
            tool_output = tool_resp.choices[0].message.content or ""

            context.add_output_metadata({
                "done": dg.MetadataValue.bool(False),
                "tool": dg.MetadataValue.text(tool_name),
                "args": dg.MetadataValue.text(args_str[:500]),
                "reasoning": dg.MetadataValue.text(reasoning),
                "tool_output_preview": dg.MetadataValue.md(f"```\n{tool_output[:1000]}\n```"),
            })
            return pd.DataFrame([{
                "iteration": iteration,
                "done": False,
                "tool": tool_name,
                "args": args_str,
                "reasoning": reasoning,
                "tool_output": tool_output,
            }])

        assets: list = []
        step_keys: List[str] = []

        def _make_step_asset(iteration: int):
            step_name = f"{_self.step_asset_prefix}_{iteration}"
            prior_names = [f"{_self.step_asset_prefix}_{i}" for i in range(1, iteration)]
            _ins = {
                name: dg.AssetIn(key=dg.AssetKey.from_user_string(name))
                for name in prior_names
            }

            @dg.asset(
                key=dg.AssetKey.from_user_string(step_name),
                group_name=_self.group_name,
                kinds=_kinds | {"step"},
                description=f"Iterative agent step {iteration}/{_self.max_iterations}.",
                ins=_ins if _ins else None,
            )
            def _step_asset(context: dg.AssetExecutionContext, **kwargs):
                # Preserve declared order in prior step outputs
                prior_dfs = [kwargs.get(name) for name in prior_names]
                return _plan_and_execute_step(context, iteration, prior_dfs)

            return _step_asset, step_name

        for i in range(1, _self.max_iterations + 1):
            _step_asset, _name = _make_step_asset(i)
            assets.append(_step_asset)
            step_keys.append(_name)

        # ── Synthesizer reads all step assets ──────────────────────────
        _syn_default = _self.synthesis_system_message or (
            "You are a research synthesizer. Given the original task and the "
            "iterative agent's trajectory (each step's tool call + tool output), "
            "write the final answer. Cite which step provided each fact."
        )
        _syn_ins = {
            name: dg.AssetIn(key=dg.AssetKey.from_user_string(name)) for name in step_keys
        }

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.synthesis_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"synthesizer"},
            description="Final answer synthesized from all step trajectories.",
            ins=_syn_ins,
        )
        def _synthesis_asset(context: dg.AssetExecutionContext, **kwargs):
            import pandas as pd

            steps = []
            for name in step_keys:
                out = kwargs.get(name)
                df = out if isinstance(out, pd.DataFrame) else pd.DataFrame(out)
                if df.empty:
                    continue
                r = df.iloc[0]
                if r.get("tool"):
                    steps.append(
                        f"### step {r['iteration']}\n"
                        f"reasoning: {r.get('reasoning', '')}\n"
                        f"tool: {r.get('tool')}\n"
                        f"args: {r.get('args')}\n"
                        f"output: {r.get('tool_output', '')}\n"
                    )

            if not steps:
                answer = "(no tools were invoked — the agent decided the task was already complete)"
                n = 0
            else:
                client = _client()
                user_msg = (
                    f"Task:\n{_self.task}\n\n"
                    f"Iterative agent trajectory ({len(steps)} tool call(s)):\n\n"
                    + "\n".join(steps)
                )
                resp = client.chat.completions.create(
                    model=_self.model,
                    temperature=_self.temperature,
                    max_tokens=_self.synthesis_max_tokens,
                    messages=[
                        {"role": "system", "content": _syn_default},
                        {"role": "user", "content": user_msg},
                    ],
                )
                answer = resp.choices[0].message.content or ""
                n = len(steps)

            df = pd.DataFrame([{
                "task": _self.task,
                "n_tool_calls": n,
                "answer": answer,
            }])
            context.add_output_metadata({
                "task": dg.MetadataValue.text(_self.task),
                "n_tool_calls": dg.MetadataValue.int(n),
                "answer": dg.MetadataValue.md(answer),
            })
            return df

        assets.append(_synthesis_asset)

        return dg.Definitions(assets=assets)
