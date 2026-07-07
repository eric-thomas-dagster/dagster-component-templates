"""MCP Tool Picker — planner LLM picks MCP tools + args to invoke.

Same overall shape as `SupervisorAgentComponent`, but the tools are real
MCP tool calls (stdio / http / sse) instead of LLM personas. The planner
LLM reads a task, decides which MCP tools to invoke with what args, each
call becomes a Dagster asset, and a synthesizer LLM writes the final
grounded answer.

Bounded action space is still enforced: the tool list is declared in YAML
at pipeline-write time (name + description + which MCP server). The
planner emits args as a JSON object per pick — those get passed straight
to the MCP tool. If the planner picks a tool NOT in the declared list,
the call is dropped with a warning.

Assets emitted (`2 + N` per YAML block):
  1. <plan_asset_name>            — the planner's picks (tool + args + reason)
  2. <tool.name>_result (×N)      — one asset per declared MCP tool
  3. <synthesis_asset_name>       — grounded final answer

Uses the same MCP client machinery as `MCPToolCallComponent` — stdio,
http, sse transports all supported via a per-tool `MCPServerSpec`.
"""
import asyncio
import json
from contextlib import AsyncExitStack
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class MCPPickerServerSpec(dg.Model, dg.Resolvable):
    name: str = Field(description="Short label for logs.")
    type: str = Field(default="stdio", description="Transport: 'stdio' | 'http' | 'sse'.")
    command: Optional[List[str]] = Field(default=None, description="stdio: [executable, ...args].")
    url: Optional[str] = Field(default=None, description="http/sse: MCP endpoint URL.")
    env: Optional[Dict[str, str]] = Field(default=None, description="stdio: extra env vars.")
    headers: Optional[Dict[str, str]] = Field(default=None, description="http/sse: literal HTTP headers.")
    headers_env: Optional[Dict[str, str]] = Field(
        default=None,
        description="http/sse: map of header_name → env_var_name (value read from env).",
    )


class MCPToolPickerToolSpec(dg.Model, dg.Resolvable):
    """One MCP tool the picker can invoke.

    The `name` is what the planner emits. The `description` is what the
    planner sees when deciding. The `server` + `mcp_tool_name` say WHICH
    MCP call to make when the planner picks this tool.
    """

    name: str = Field(description="Tool name — the planner picks by this.")
    description: str = Field(
        description=(
            "One-line description shown to the planner. Be concrete: "
            "'read file contents given a path' not 'file ops'."
        ),
    )
    server: MCPPickerServerSpec = Field(description="MCP server the tool lives on.")
    mcp_tool_name: str = Field(
        description="Name of the tool as the MCP server exposes it (e.g. 'read_file').",
    )
    args_schema_hint: Optional[str] = Field(
        default=None,
        description=(
            "Optional hint about the argument schema, shown to the planner "
            "so it knows what args to emit. Example: '{path: string}'."
        ),
    )
    parse_as: str = Field(
        default="auto",
        description="'json' / 'text' / 'auto' (try JSON, fall back to text).",
    )


class MCPToolPickerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Planner LLM picks MCP tools to invoke; each pick becomes an asset; synthesizer writes the answer.

    Example (filesystem MCP server):

        ```yaml
        type: dagster_community_components.MCPToolPickerComponent
        attributes:
          plan_asset_name: mcp_plan
          synthesis_asset_name: mcp_final_answer
          task: "Find the largest .py file under /tmp/demo and show its first 20 lines."
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          tools:
            - name: list_dir
              description: "List files and sizes in a directory."
              args_schema_hint: '{path: string}'
              server:
                name: fs
                type: stdio
                command: ["npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp/demo"]
              mcp_tool_name: list_directory_with_sizes
            - name: read_head
              description: "Read a file's contents (first N lines)."
              args_schema_hint: '{path: string, head: int}'
              server:
                name: fs
                type: stdio
                command: ["npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp/demo"]
              mcp_tool_name: read_text_file
        ```
    """

    plan_asset_name: str = Field(description="Planner asset name.")
    synthesis_asset_name: str = Field(description="Synthesizer asset name.")
    task: str = Field(description="The task the planner is planning against.")
    tools: List[MCPToolPickerToolSpec] = Field(description="Bounded list of MCP tools.")
    model: str = Field(default="gpt-4o-mini", description="OpenAI-compatible model name.")
    api_key_env_var: str = Field(default="OPENAI_API_KEY")
    api_base_env_var: Optional[str] = Field(default=None)
    temperature: float = Field(default=0.2)
    planner_max_tokens: int = Field(default=500)
    synthesis_max_tokens: int = Field(default=800)
    max_picks: int = Field(
        default=5,
        ge=1,
        description=(
            "Upper bound on MCP tool calls the planner may pick in one shot. "
            "The prompt tells the LLM 'pick minimum 1, maximum N'."
        ),
    )
    synthesis_system_message: Optional[str] = Field(
        default=None,
        description="Optional override for the synthesizer's system prompt.",
    )
    group_name: Optional[str] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags.")

    # Standard fields — partitions / freshness / retries.
    partition_type: Optional[str] = Field(
        default=None,
        description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic'",
    )
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partitions.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static partitions.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy cron schedule.")
    retry_policy_max_retries: Optional[int] = Field(default=None, description="RetryPolicy max retries.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"ai", "agent", "mcp"})
        assets: list = []

        # Build partitions / freshness / retry policies from the standard fields.
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
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _vals:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_vals)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from datetime import timedelta
            from dagster import FreshnessPolicy
            if self.freshness_cron:
                freshness_policy = FreshnessPolicy.cron(
                    deadline_cron=self.freshness_cron,
                    lower_bound_delta=timedelta(minutes=self.freshness_max_lag_minutes),
                )
            else:
                freshness_policy = FreshnessPolicy.time_window(
                    fail_window=timedelta(minutes=self.freshness_max_lag_minutes),
                )
        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        std_kwargs: Dict[str, Any] = {}
        if partitions_def is not None: std_kwargs["partitions_def"] = partitions_def
        if freshness_policy is not None: std_kwargs["freshness_policy"] = freshness_policy
        if retry_policy is not None: std_kwargs["retry_policy"] = retry_policy
        if self.owners: std_kwargs["owners"] = list(self.owners)
        if self.tags: std_kwargs["tags"] = dict(self.tags)

        # ── Planner asset ──────────────────────────────────────────────
        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.plan_asset_name),
            group_name=_self.group_name,
            kinds=_kinds | {"planner"},
            description=(
                f"MCP planner: pick from {len(_self.tools)} MCP tools for task: "
                f"{_self.task[:80]}"
            ),
            **std_kwargs,
        )
        def _plan_asset(context: dg.AssetExecutionContext):
            import os
            import pandas as pd

            try:
                from openai import OpenAI
            except ImportError as e:
                raise ImportError("mcp_tool_picker requires openai>=1.0.0") from e

            # Template substitution for {partition_key} / {run_id}.
            _task = _self.task
            if isinstance(_task, str) and "{" in _task:
                _rid = getattr(context, "run_id", "") or ""
                _task = _task.replace("{run_id}", str(_rid))
                _has_pk = False
                try: _has_pk = context.has_partition_key
                except Exception: pass
                if _has_pk:
                    try: _pk = context.partition_key
                    except Exception: _pk = ""
                    if hasattr(_pk, "keys_by_dimension"):
                        _task = _task.replace("{partition_key}", str(_pk))
                        for dim, val in _pk.keys_by_dimension.items():
                            _task = _task.replace("{partition_keys." + dim + "}", str(val))
                    else:
                        _task = _task.replace("{partition_key}", str(_pk or ""))
                else:
                    _task = _task.replace("{partition_key}", "")

            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                raise RuntimeError(f"{_self.api_key_env_var!r} env var not set.")
            client_kwargs: Dict[str, Any] = {"api_key": api_key}
            if _self.api_base_env_var:
                base_url = os.environ.get(_self.api_base_env_var)
                if base_url:
                    client_kwargs["base_url"] = base_url
            client = OpenAI(**client_kwargs)

            tool_list_str = "\n".join(
                f"  - {t.name}: {t.description}"
                + (f" args: {t.args_schema_hint}" if t.args_schema_hint else "")
                for t in _self.tools
            )
            valid_names = ", ".join(f'"{t.name}"' for t in _self.tools)

            planner_prompt = (
                f"You are an MCP-tool supervisor. Given a task, pick MCP tool "
                f"calls to make (minimum 1, maximum {_self.max_picks}). For each pick you must "
                "supply the concrete args dict the tool needs.\n\n"
                f"Available tools:\n{tool_list_str}\n\n"
                f"Task: {_task}\n\n"
                "Output ONLY a JSON array (no markdown fences). Each element:\n"
                "  {\"tool\": \"<one of: " + valid_names + ">\","
                " \"args\": {<args object>},"
                " \"reason\": \"<why you picked this call>\"}\n"
                "The args object gets passed straight to the MCP tool — pick real values."
            )

            context.log.info(f"[mcp_picker] planning against {len(_self.tools)} MCP tools")
            resp = client.chat.completions.create(
                model=_self.model,
                temperature=_self.temperature,
                max_tokens=_self.planner_max_tokens,
                messages=[
                    {"role": "system", "content": "You are a helpful supervisor that picks MCP tool calls."},
                    {"role": "user", "content": planner_prompt},
                ],
            )
            raw = (resp.choices[0].message.content or "").strip()
            if raw.startswith("```"):
                raw = raw.strip("`").split("\n", 1)[-1]
                if raw.endswith("```"):
                    raw = raw.rsplit("```", 1)[0]
            try:
                picks = json.loads(raw)
                if not isinstance(picks, list):
                    picks = [picks]
            except json.JSONDecodeError as e:
                context.log.warning(f"[mcp_picker] plan JSON parse failed: {e}; raw={raw[:200]}")
                picks = []

            valid_tool_names = {t.name for t in _self.tools}
            filtered = []
            for p in picks:
                if not isinstance(p, dict):
                    continue
                tool = p.get("tool")
                if tool not in valid_tool_names:
                    context.log.warning(f"[mcp_picker] dropping invalid tool {tool!r}")
                    continue
                filtered.append({
                    "tool": tool,
                    "args": json.dumps(p.get("args", {}) or {}),
                    "reason": p.get("reason", ""),
                })

            df = pd.DataFrame(filtered) if filtered else pd.DataFrame(columns=["tool", "args", "reason"])
            picked = sorted(set(df["tool"].tolist())) if not df.empty else []
            context.log.info(f"[mcp_picker] planner picked {len(df)} call(s) across tools: {picked}")

            context.add_output_metadata({
                "task": dg.MetadataValue.text(_task),
                "n_picks": dg.MetadataValue.int(len(df)),
                "tools_picked": dg.MetadataValue.text(", ".join(picked) or "(none)"),
                "plan": dg.MetadataValue.md(
                    df.to_markdown(index=False) if not df.empty else "_no picks_"
                ),
            })
            return df

        assets.append(_plan_asset)

        # ── Per-tool assets — each conditional on planner picking it ────
        tool_result_keys: List[str] = []

        def _make_tool_asset(tool_spec: MCPToolPickerToolSpec):
            _asset_name = f"{tool_spec.name}_result"

            @dg.asset(
                key=dg.AssetKey.from_user_string(_asset_name),
                group_name=_self.group_name,
                kinds=_kinds | {"tool"},
                description=f"MCP tool {tool_spec.name}: {tool_spec.description}",
                ins={"plan": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.plan_asset_name))},
                **std_kwargs,
            )
            def _tool_asset(context: dg.AssetExecutionContext, plan):
                import pandas as pd

                plan_df = plan if isinstance(plan, pd.DataFrame) else pd.DataFrame(plan)
                my_rows = plan_df[plan_df["tool"] == tool_spec.name] if not plan_df.empty else plan_df

                if my_rows.empty:
                    context.log.info(f"[mcp:{tool_spec.name}] not picked — skipping")
                    context.add_output_metadata({
                        "invoked": dg.MetadataValue.bool(False),
                        "note": dg.MetadataValue.text("planner did not pick this tool"),
                    })
                    return pd.DataFrame(columns=["tool", "args", "result", "is_error"])

                outputs = []
                for _, row in my_rows.iterrows():
                    try:
                        args = json.loads(row["args"]) if isinstance(row["args"], str) else (row["args"] or {})
                    except Exception:
                        args = {}
                    if not isinstance(args, dict):
                        args = {}
                    context.log.info(
                        f"[mcp:{tool_spec.name}] calling {tool_spec.mcp_tool_name}({args!r})"
                    )
                    try:
                        result = asyncio.run(_call_mcp_tool(
                            log=context.log,
                            server_cfg=tool_spec.server.model_dump(),
                            tool_name=tool_spec.mcp_tool_name,
                            tool_args=args,
                            parse_as=tool_spec.parse_as,
                        ))
                        outputs.append({
                            "tool": tool_spec.name,
                            "args": json.dumps(args),
                            "result": (
                                json.dumps(result["value"])
                                if result["kind"] == "json"
                                else result["raw"]
                            ),
                            "is_error": result["is_error"],
                        })
                    except Exception as e:  # noqa: BLE001
                        context.log.warning(f"[mcp:{tool_spec.name}] call failed: {e}")
                        outputs.append({
                            "tool": tool_spec.name,
                            "args": json.dumps(args),
                            "result": f"ERROR: {e}",
                            "is_error": True,
                        })

                out_df = pd.DataFrame(outputs)
                context.add_output_metadata({
                    "invoked": dg.MetadataValue.bool(True),
                    "n_calls": dg.MetadataValue.int(len(out_df)),
                    "preview": dg.MetadataValue.md(
                        out_df.to_markdown(index=False)[:4000]
                    ),
                })
                return out_df

            return _tool_asset, _asset_name

        for tool_spec in _self.tools:
            _tool_asset, _asset_name = _make_tool_asset(tool_spec)
            tool_result_keys.append(_asset_name)
            assets.append(_tool_asset)

        # ── Synthesizer ────────────────────────────────────────────────
        _syn_default = _self.synthesis_system_message or (
            "You are a research synthesizer. Given the original task and the "
            "outputs from the MCP tool calls the supervisor invoked, write a "
            "concise, grounded answer. Cite each MCP tool by name in "
            "parentheses. If a tool errored, note that."
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
            description="Grounded final answer.",
            ins=_syn_ins,
            **std_kwargs,
        )
        def _synthesis_asset(context: dg.AssetExecutionContext, **kwargs):
            import os
            import pandas as pd

            try:
                from openai import OpenAI
            except ImportError as e:
                raise ImportError("mcp_tool_picker requires openai>=1.0.0") from e

            # Template substitution for {partition_key} / {run_id}.
            _task = _self.task
            if isinstance(_task, str) and "{" in _task:
                _rid = getattr(context, "run_id", "") or ""
                _task = _task.replace("{run_id}", str(_rid))
                _has_pk = False
                try: _has_pk = context.has_partition_key
                except Exception: pass
                if _has_pk:
                    try: _pk = context.partition_key
                    except Exception: _pk = ""
                    if hasattr(_pk, "keys_by_dimension"):
                        _task = _task.replace("{partition_key}", str(_pk))
                        for dim, val in _pk.keys_by_dimension.items():
                            _task = _task.replace("{partition_keys." + dim + "}", str(val))
                    else:
                        _task = _task.replace("{partition_key}", str(_pk or ""))
                else:
                    _task = _task.replace("{partition_key}", "")

            tool_sections: List[str] = []
            for _k in tool_result_keys:
                out = kwargs.get(_k)
                out_df = out if isinstance(out, pd.DataFrame) else pd.DataFrame(out)
                if not out_df.empty:
                    for _, r in out_df.iterrows():
                        tool_sections.append(
                            f"### tool: {r['tool']}\n"
                            f"args: {r['args']}\n"
                            f"is_error: {r.get('is_error', False)}\n"
                            f"result: {str(r['result'])[:2000]}\n"
                        )

            if not tool_sections:
                answer = "(no MCP tools invoked — the planner did not pick any tools)"
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
                    f"Task:\n{_task}\n\n"
                    f"MCP tool outputs:\n\n" + "\n".join(tool_sections)
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
                "task": _task,
                "n_tool_calls": len(tool_sections),
                "answer": answer,
            }])
            context.add_output_metadata({
                "task": dg.MetadataValue.text(_task),
                "n_tool_calls": dg.MetadataValue.int(len(tool_sections)),
                "answer": dg.MetadataValue.md(answer),
            })
            return df

        assets.append(_synthesis_asset)

        return dg.Definitions(assets=assets)


async def _call_mcp_tool(
    *,
    log,
    server_cfg: Dict[str, Any],
    tool_name: str,
    tool_args: Dict[str, Any],
    parse_as: str,
) -> Dict[str, Any]:
    """Async MCP tool call — supports stdio / http / sse transports.

    Mirrors the helper in `mcp_tool_call/component.py`; kept local to
    avoid cross-package imports.
    """
    name = server_cfg.get("name") or "server"
    transport = server_cfg.get("type", "stdio")

    async with AsyncExitStack() as stack:
        if transport == "stdio":
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client

            cmd = server_cfg.get("command") or []
            if not cmd:
                raise ValueError(f"MCP server {name!r} is stdio but command is empty.")
            params = StdioServerParameters(
                command=cmd[0], args=list(cmd[1:]), env=server_cfg.get("env")
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
        return {"value": raw, "raw": raw, "raw_size": len(raw), "is_error": is_error, "kind": "text"}
    if parse_as in ("json", "auto"):
        try:
            value = json.loads(raw)
            return {"value": value, "raw": raw, "raw_size": len(raw), "is_error": is_error, "kind": "json"}
        except (json.JSONDecodeError, ValueError):
            if parse_as == "json":
                raise
            return {"value": raw, "raw": raw, "raw_size": len(raw), "is_error": is_error, "kind": "text"}
    return {"value": raw, "raw": raw, "raw_size": len(raw), "is_error": is_error, "kind": "text"}


def _resolve_mcp_headers(cfg: Dict[str, Any], server_name: str) -> Dict[str, str]:
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
