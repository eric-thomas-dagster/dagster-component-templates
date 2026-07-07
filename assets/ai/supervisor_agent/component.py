"""Supervisor Agent — planner picks which tools to invoke from a bounded set.

The "agent of agents" pattern. A planner LLM reads a task and picks which
specialist tools to invoke. Each tool is a downstream Dagster asset with its
own LLM persona. A final synthesizer reads all tool outputs and writes the
answer.

Why this shape:
  - **Bounded**: the tool set is declared in YAML at pipeline-write time.
    The planner picks BY NAME. It cannot invent tools.
  - **Auditable**: every pick has a reason. Every tool's output is a Dagster
    asset — full lineage in `dg dev`.
  - **Composable**: gate the plan on an asset check, route it through review,
    publish to Slack for approval, replay with the same inputs, etc.

This component emits multiple assets from one YAML block:
  1. <plan_asset_name>          — the planner's picks + reasons
  2. <tool.name>_result (×N)     — one asset per tool in the bounded set
  3. <synthesis_asset_name>     — final synthesized answer

The tools that were NOT picked by the planner still materialize — as empty
DataFrames — so the shape of the DAG stays static. That trades true dynamic
fan-out (via `DynamicOutput` or dynamic partitions) for a demoable,
visually-consistent asset graph. When N would be large or truly unbounded,
use dynamic partitions instead (see walkthrough).
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class SupervisorAgentToolSpec(dg.Resolvable, dg.Model):
    """One tool the supervisor can pick.

    The `name` is what the planner emits. The `description` is what the
    planner sees when deciding. The `system_message` is what the tool's
    LLM sees when it runs (the tool's persona).
    """

    name: str = Field(description="Tool name — the planner picks by this name.")
    description: str = Field(
        description=(
            "One-line description of the tool. Shown to the planner LLM. "
            "Be specific so the planner picks well: 'search recent web' "
            "not 'search'."
        ),
    )
    system_message: str = Field(
        description=(
            "System prompt for the tool's LLM. This is the tool's persona: "
            "e.g., 'You are a web search agent. Return 3 relevant factual "
            "snippets with sources.' The tool receives the planner's "
            "`tool_input` field as the user message."
        ),
    )


class SupervisorAgentComponent(dg.Component, dg.Model, dg.Resolvable):
    """Supervisor agent that picks tools from a bounded set and executes each.

    Emits `2 + len(tools)` assets from one YAML block: the plan, one asset
    per tool, and the final synthesis.

    Example:

        ```yaml
        type: dagster_community_components.SupervisorAgentComponent
        attributes:
          plan_asset_name: supervisor_plan
          synthesis_asset_name: final_answer
          task: "How does Dagster compare to Airflow for ML pipelines?"
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          tools:
            - name: web_search
              description: "Search the current web for recent info + citations."
              system_message: |
                You are a web search agent. Return 2-3 relevant factual
                snippets with source names for the given query.
            - name: kb_expert
              description: "Query internal docs / KB for authoritative product answers."
              system_message: |
                You are a docs-QA agent. Answer strictly from the following
                Dagster+Airflow context: (embedded doc summary here). If
                unsure, say so.
            - name: math_expert
              description: "Do arithmetic on numbers cited in the task."
              system_message: |
                You are a calculation agent. Given a math request, return
                just the number and a one-line explanation.
        ```

    The planner is prompted with the task + the tool descriptions. It emits
    a JSON list of `{tool, tool_input, reason}` — one entry per pick. The
    component uses that list to invoke each picked tool's LLM with its
    persona and `tool_input`. The synthesis asset reads all tool outputs +
    the original task and writes the final grounded answer.
    """

    plan_asset_name: str = Field(
        description="Name of the plan asset (the planner's picks).",
    )
    synthesis_asset_name: str = Field(
        description="Name of the final synthesis asset.",
    )
    task: str = Field(
        description="The task / question the supervisor is planning against.",
    )
    tools: List[SupervisorAgentToolSpec] = Field(
        description="Bounded list of tools the planner can pick from.",
    )
    model: str = Field(
        default="gpt-4o-mini",
        description="OpenAI-compatible model name for the planner + tool LLMs + synthesis.",
    )
    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Env var containing the OpenAI-compatible API key.",
    )
    api_base_env_var: Optional[str] = Field(
        default=None,
        description="Optional env var for a custom base URL — e.g. Vercel AI Gateway.",
    )
    temperature: float = Field(default=0.2, description="LLM temperature.")
    planner_max_tokens: int = Field(
        default=400,
        description="Max tokens for the planner's response.",
    )
    tool_max_tokens: int = Field(
        default=400,
        description="Max tokens per tool call.",
    )
    synthesis_max_tokens: int = Field(
        default=600,
        description="Max tokens for the synthesizer's response.",
    )
    synthesis_system_message: Optional[str] = Field(
        default=None,
        description=(
            "Optional override for the synthesizer's system prompt. "
            "Default: 'You are a research synthesizer. Given the original "
            "task and the outputs from each picked tool, write a concise, "
            "grounded answer that cites which tool provided each fact.'"
        ),
    )
    group_name: Optional[str] = Field(default=None, description="Asset group.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'ai', 'agent').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"ai", "agent"})
        assets: list = []

        # ── Planner asset ──────────────────────────────────────────────
        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.plan_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"planner"},
            description=(
                f"Planner picks from {len(_self.tools)} tools for task: "
                f"{_self.task[:80]}"
            ),
        )
        def _plan_asset(context: dg.AssetExecutionContext):
            import json
            import os
            import pandas as pd

            try:
                from openai import OpenAI
            except ImportError as e:
                raise ImportError("supervisor_agent requires openai>=1.0.0") from e

            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                raise RuntimeError(f"{_self.api_key_env_var!r} env var not set.")
            client_kwargs: Dict[str, Any] = {"api_key": api_key}
            if _self.api_base_env_var:
                base_url = os.environ.get(_self.api_base_env_var)
                if base_url:
                    client_kwargs["base_url"] = base_url
            client = OpenAI(**client_kwargs)

            tool_list = "\n".join(
                f"  - {t.name}: {t.description}" for t in _self.tools
            )
            valid_names = ", ".join(f'"{t.name}"' for t in _self.tools)
            planner_prompt = (
                "You are a supervisor agent. Given a task, pick a subset "
                "(minimum 1, maximum 4) of these tools to invoke. For each, "
                "pass the concrete tool_input the tool needs.\n\n"
                f"Available tools:\n{tool_list}\n\n"
                f"Task: {_self.task}\n\n"
                "Output ONLY a JSON array (no markdown fences). Each element:\n"
                "  {\"tool\": \"<one of: " + valid_names + ">\","
                " \"tool_input\": \"<what to send the tool>\","
                " \"reason\": \"<why you picked this tool>\"}\n"
            )

            context.log.info(f"[supervisor] planning against {len(_self.tools)} tools")
            resp = client.chat.completions.create(
                model=_self.model,
                temperature=_self.temperature,
                max_tokens=_self.planner_max_tokens,
                messages=[
                    {"role": "system", "content": "You are a helpful supervisor agent that picks tools."},
                    {"role": "user", "content": planner_prompt},
                ],
            )
            raw = resp.choices[0].message.content or ""
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.strip("`").split("\n", 1)[-1]
                if raw.endswith("```"):
                    raw = raw.rsplit("```", 1)[0]
            try:
                picks = json.loads(raw)
                if not isinstance(picks, list):
                    picks = [picks]
            except json.JSONDecodeError as e:
                context.log.warning(f"[supervisor] plan JSON parse failed: {e}. raw={raw[:200]}")
                picks = []

            valid_tool_names = {t.name for t in _self.tools}
            filtered = []
            for p in picks:
                if not isinstance(p, dict):
                    continue
                tool = p.get("tool")
                if tool not in valid_tool_names:
                    context.log.warning(f"[supervisor] planner picked invalid tool {tool!r}; dropping")
                    continue
                filtered.append({
                    "tool": tool,
                    "tool_input": p.get("tool_input", ""),
                    "reason": p.get("reason", ""),
                })

            df = pd.DataFrame(filtered) if filtered else pd.DataFrame(columns=["tool", "tool_input", "reason"])
            picked_names = sorted(set(df["tool"].tolist())) if not df.empty else []
            context.log.info(f"[supervisor] planner picked {len(df)} tool call(s): {picked_names}")

            context.add_output_metadata({
                "task": dg.MetadataValue.text(_self.task),
                "n_picks": dg.MetadataValue.int(len(df)),
                "tools_picked": dg.MetadataValue.text(", ".join(picked_names) or "(none)"),
                "plan": dg.MetadataValue.md(
                    df.to_markdown(index=False) if not df.empty else "_no picks_"
                ),
            })
            return df

        assets.append(_plan_asset)

        # ── Per-tool assets — each conditional on planner picking it ────
        tool_result_keys = []

        def _make_tool_asset(tool_spec: SupervisorAgentToolSpec):
            _asset_name = f"{tool_spec.name}_result"

            @dg.asset(
                key=dg.AssetKey.from_user_string(_asset_name),
                group_name=_self.group_name,
                kinds=_kinds | {"tool"},
                description=f"Tool {tool_spec.name}: {tool_spec.description}",
                ins={"plan": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.plan_asset_name))},
            )
            def _tool_asset(context: dg.AssetExecutionContext, plan):
                import os
                import pandas as pd

                try:
                    from openai import OpenAI
                except ImportError as e:
                    raise ImportError("supervisor_agent requires openai>=1.0.0") from e

                plan_df = plan if isinstance(plan, pd.DataFrame) else pd.DataFrame(plan)
                my_rows = plan_df[plan_df["tool"] == tool_spec.name] if not plan_df.empty else plan_df

                if my_rows.empty:
                    context.log.info(f"[tool:{tool_spec.name}] planner did not pick this tool — skipping")
                    context.add_output_metadata({
                        "invoked": dg.MetadataValue.bool(False),
                        "note": dg.MetadataValue.text("planner did not pick this tool"),
                    })
                    return pd.DataFrame(columns=["tool", "tool_input", "output"])

                api_key = os.environ.get(_self.api_key_env_var)
                if not api_key:
                    raise RuntimeError(f"{_self.api_key_env_var!r} env var not set.")
                client_kwargs: Dict[str, Any] = {"api_key": api_key}
                if _self.api_base_env_var:
                    base_url = os.environ.get(_self.api_base_env_var)
                    if base_url:
                        client_kwargs["base_url"] = base_url
                client = OpenAI(**client_kwargs)

                outputs = []
                for _, row in my_rows.iterrows():
                    tool_input = row["tool_input"]
                    context.log.info(f"[tool:{tool_spec.name}] running with input: {str(tool_input)[:80]}")
                    resp = client.chat.completions.create(
                        model=_self.model,
                        temperature=_self.temperature,
                        max_tokens=_self.tool_max_tokens,
                        messages=[
                            {"role": "system", "content": tool_spec.system_message},
                            {"role": "user", "content": str(tool_input)},
                        ],
                    )
                    outputs.append({
                        "tool": tool_spec.name,
                        "tool_input": tool_input,
                        "output": resp.choices[0].message.content or "",
                    })

                out_df = pd.DataFrame(outputs)
                context.add_output_metadata({
                    "invoked": dg.MetadataValue.bool(True),
                    "n_calls": dg.MetadataValue.int(len(out_df)),
                    "preview": dg.MetadataValue.md(out_df.to_markdown(index=False)),
                })
                return out_df

            return _tool_asset, _asset_name

        for tool_spec in _self.tools:
            _tool_asset, _asset_name = _make_tool_asset(tool_spec)
            tool_result_keys.append(_asset_name)
            assets.append(_tool_asset)

        # ── Synthesis asset — reads plan + all tool outputs ─────────────
        _syn_default = _self.synthesis_system_message or (
            "You are a research synthesizer. Given the original task and "
            "the outputs from each picked tool, write a concise, grounded "
            "answer. Cite which tool provided each fact by name in "
            "parentheses. If a tool's output was empty because the planner "
            "didn't pick it, ignore it."
        )

        _syn_ins = {
            "plan": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.plan_asset_name)),
        }
        for _k in tool_result_keys:
            _syn_ins[_k] = dg.AssetIn(key=dg.AssetKey.from_user_string(_k))

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.synthesis_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"synthesizer"},
            description="Final synthesized answer grounded in tool outputs.",
            ins=_syn_ins,
        )
        def _synthesis_asset(context: dg.AssetExecutionContext, **kwargs):
            import os
            import pandas as pd

            try:
                from openai import OpenAI
            except ImportError as e:
                raise ImportError("supervisor_agent requires openai>=1.0.0") from e

            tool_sections: List[str] = []
            for _k in tool_result_keys:
                out = kwargs.get(_k)
                out_df = out if isinstance(out, pd.DataFrame) else pd.DataFrame(out)
                if not out_df.empty:
                    for _, r in out_df.iterrows():
                        tool_sections.append(
                            f"### tool: {r['tool']}\n"
                            f"input: {r['tool_input']}\n"
                            f"output: {r['output']}\n"
                        )

            if not tool_sections:
                answer = "(no tools invoked — the supervisor did not pick any tools for this task)"
            else:
                api_key = os.environ.get(_self.api_key_env_var)
                if not api_key:
                    raise RuntimeError(f"{_self.api_key_env_var!r} env var not set.")
                client_kwargs: Dict[str, Any] = {"api_key": api_key}
                if _self.api_base_env_var:
                    base_url = os.environ.get(_self.api_base_env_var)
                    if base_url:
                        client_kwargs["base_url"] = base_url
                client = OpenAI(**client_kwargs)

                user_msg = (
                    f"Original task:\n{_self.task}\n\n"
                    f"Tool outputs:\n\n" + "\n".join(tool_sections)
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

            df = pd.DataFrame([{
                "task": _self.task,
                "n_tools_invoked": len(tool_sections),
                "answer": answer,
            }])
            context.add_output_metadata({
                "task": dg.MetadataValue.text(_self.task),
                "n_tools_invoked": dg.MetadataValue.int(len(tool_sections)),
                "answer": dg.MetadataValue.md(answer),
            })
            return df

        assets.append(_synthesis_asset)

        return dg.Definitions(assets=assets)
