"""LiteLLM Agent Component.

Single-shot LLM agent that uses Model Context Protocol (MCP) servers as its
tool layer. Materializes one agent run per asset materialization.

The agent loop is the standard tool-calling shape:
  1. List tools from every configured MCP server (prefix tool names by
     server name so cross-server collisions are impossible).
  2. Send the system + user prompt to the LLM with all tool defs.
  3. If the model emits tool calls, dispatch each to the owning MCP
     session, append the result as a `role: tool` message, and loop.
  4. Stop when the model returns a plain text response or
     `max_iterations` is hit.

Output is a Python dict (loaded by IO manager) carrying the final answer,
full transcript, and tool-call/iteration counts.
"""
from typing import Any, Dict, List, Literal, Optional, Union
import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Failure,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import ConfigDict, Field, model_validator


# BEGIN reasoning-output normalization (inlined per component; keep copies identical — see tests/test_reasoning_output.py)
import json as _json_r  # noqa: E402
import re as _re_r  # noqa: E402
from typing import List, Optional, Tuple  # noqa: E402,F401

_THINK_BLOCK_R = _re_r.compile(r"<think>(.*?)</think>", _re_r.S | _re_r.I)
_TOOL_CALL_BLOCK_R = _re_r.compile(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", _re_r.S)


def split_reasoning(content: Optional[str]) -> Tuple[str, Optional[str]]:
    """Split `<think>…</think>` reasoning out of model text -> (answer, reasoning).

    Reasoning models (Qwen3, DeepSeek-R1, ...) often inline their chain of thought. Left in, it breaks JSON parsing,
    classification and routing downstream. Handles a closed block, an UNCLOSED block (output truncated by max_tokens
    mid-thought: everything after `<think>` is reasoning and there is no answer yet), and a bare closing tag (some chat
    templates put the opening tag in the prompt). Returns (content, None) untouched when there is no reasoning."""
    content = content or ""
    if "think>" not in content.lower():
        return content, None
    parts = [m.strip() for m in _THINK_BLOCK_R.findall(content) if m.strip()]
    rest = _THINK_BLOCK_R.sub("", content)
    m = _re_r.search(r"<think>(.*)$", rest, _re_r.S | _re_r.I)
    if m:
        if m.group(1).strip():
            parts.append(m.group(1).strip())
        rest = rest[: m.start()]
    elif _re_r.search(r"</think>", rest, _re_r.I) and not parts:
        before, after = _re_r.split(r"</think>", rest, maxsplit=1, flags=_re_r.I)
        if before.strip():
            parts.append(before.strip())
        rest = after
    return rest.strip(), ("\n\n".join(parts) if parts else None)


def parse_text_tool_calls(content: Optional[str]) -> Tuple[str, List[dict]]:
    """Recover `<tool_call>{"name":…,"arguments":{…}}</tool_call>` that Qwen/Hermes-style models emit as plain text when the
    server did not parse it into structured tool_calls -> (remaining text, [{name, arguments(json str)}])."""
    content = content or ""
    calls: List[dict] = []
    for raw in _TOOL_CALL_BLOCK_R.findall(content):
        try:
            obj = _json_r.loads(raw)
        except _json_r.JSONDecodeError:
            continue
        name = obj.get("name")
        if not name:
            continue
        args = obj.get("arguments", obj.get("parameters", {}))
        calls.append({"name": name, "arguments": args if isinstance(args, str) else _json_r.dumps(args)})
    return (_TOOL_CALL_BLOCK_R.sub("", content).strip() if calls else content), calls


def ollama_chat_model(model: str, has_tools: bool) -> str:
    """LiteLLM's `ollama/` route is /api/generate (no tool support); `ollama_chat/` is /api/chat, which has it."""
    if has_tools and model.lower().startswith("ollama/"):
        return "ollama_chat/" + model.split("/", 1)[1]
    return model
# END reasoning-output normalization


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value



class MCPServerSpec(dg.Model, dg.Resolvable):
    """One MCP server connection — stdio (subprocess), http (streamable), or sse.

    Declared inline (set `name` + the transport-specific fields), or shared via
    `server_resource_key` (an MCPServerResource component's resource_key) instead --
    exactly one of the two is required per entry.
    """

    name: Optional[str] = Field(
        default=None,
        description="Short name used to prefix tool names (e.g. 'fs' → 'fs__list_files'). Required when declaring the server inline; leave unset when using `server_resource_key`. Exactly one of `name` (inline spec) or `server_resource_key` is required.",
    )
    type: str = Field(
        default="stdio",
        description="Transport: 'stdio' (subprocess), 'http' (streamable-HTTP — modern MCP, what `claude mcp add --transport http` uses), or 'sse' (legacy server-sent events).",
    )
    command: Optional[List[str]] = Field(
        default=None,
        description="stdio only: [executable, ...args]. Example: ['npx', '-y', '@modelcontextprotocol/server-filesystem', '/tmp'].",
    )
    url: Optional[str] = Field(
        default=None,
        description="http / sse: full URL of the MCP endpoint. Example: 'https://mcp.agent.dagster.cloud/mcp/'.",
    )
    env: Optional[Dict[str, str]] = Field(
        default=None,
        description="stdio only: extra environment variables to set on the subprocess.",
    )
    headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="http / sse: literal HTTP headers (don't put secrets here — use `headers_env` instead).",
    )
    headers_env: Optional[Dict[str, str]] = Field(
        default=None,
        description="http / sse: map of header_name → env_var_name; the header value is read from the named env var at materialization time. Use this for tokens (e.g. {'Authorization': 'DAGSTER_PLUS_TOKEN'} where DAGSTER_PLUS_TOKEN env var holds 'Bearer xyz123').",
    )
    server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Resource key of an MCPServerResource component to use instead of declaring "
            "this server inline -- lets several agent components (and mcp_tool_call / "
            "mcp_tool_picker) share one registered server. Exactly one of `name` (inline "
            "spec) or `server_resource_key` is required."
        ),
    )
    instructions: Optional[str] = Field(
        default=None,
        description=(
            "Free-text usage guidance for this server (e.g. 'link each ticket as "
            "https://.../issues?issueNumber=<number>', 'show names, not IDs', 'this server is "
            "read-only') -- appended to the system prompt under a heading with the server's "
            "name, so every agent that uses this server (inline or via server_resource_key) "
            "gets the same guidance with no copy-paste. Combined with whatever `instructions` "
            "the MCP server itself returns in its own initialize response, if it sets one."
        ),
    )

    @model_validator(mode="after")
    def _exactly_one_server_source(self):
        if bool(self.name) == bool(self.server_resource_key):
            raise ValueError(
                "MCPServerSpec requires exactly one of `name` (+ transport fields, declared "
                "inline) or `server_resource_key` (an MCPServerResource component's resource_key)."
            )
        return self



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


# BEGIN agent outcome ledger (inlined per component; keep copies identical -- see tests/test_agent_outcome_ledger_drift.py)
# A tool server (github_pr_tools/linear_ticket_tools) WRITES one JSON-lines entry per consequential
# action it takes, keyed by the Dagster run_id an agent component threads through MCP's out-of-band
# request _meta (see each agent component's own `meta={"dagster_run_id": ...}` call in session.call_tool).
# An agent's own companion outcome asset READS that run's entries and collapses them into four buckets:
# filed (a real write happened), would_file (practice mode -- would have, but didn't), refused (a
# guardrail declined it -- e.g. a duplicate, a protected path), decisions_for_you (flagged for a human,
# e.g. an APPROVE a risk rule refuses). Matched by run_id, not a wall-clock time window -- a window
# mixes concurrent runs together.
import json as _oal_json
import os as _oal_os
import threading as _oal_threading
from datetime import datetime as _oal_datetime, timezone as _oal_timezone

_OUTCOME_LEDGER_LOCK = _oal_threading.Lock()


def _outcome_ledger_file(ledger_path: str, run_id: str) -> str:
    return _oal_os.path.join(_oal_os.path.expanduser(ledger_path), f"{run_id}.jsonl")


def write_outcome_ledger_entry(ledger_path: Optional[str], run_id: Optional[str], entry: Dict[str, Any]) -> None:
    """No-op unless both ledger_path and run_id are set -- zero behavior change for a tool server that
    hasn't configured a ledger_path, and zero entries for a call made with no run_id behind it (e.g. a
    human using `claude mcp add` directly, outside any Dagster run)."""
    if not ledger_path or not run_id:
        return
    path = _outcome_ledger_file(ledger_path, run_id)
    record = dict(entry)
    record.setdefault("ts", _oal_datetime.now(_oal_timezone.utc).isoformat(timespec="seconds"))
    record.setdefault("run_id", run_id)
    with _OUTCOME_LEDGER_LOCK:
        _oal_os.makedirs(_oal_os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(_oal_json.dumps(record, default=str) + "\n")


def read_outcome_ledger_entries(ledger_path: Optional[str], run_id: Optional[str]) -> List[Dict[str, Any]]:
    if not ledger_path or not run_id:
        return []
    path = _outcome_ledger_file(ledger_path, run_id)
    if not _oal_os.path.isfile(path):
        return []
    entries: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(_oal_json.loads(line))
            except ValueError:
                continue
    return entries


def summarize_outcome_ledger(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Collapse one run's ledger entries into {status, filed, would_file, refused, decisions_for_you}."""
    filed, would_file, refused, decisions_for_you = [], [], [], []
    for e in entries:
        bucket = e.get("bucket")
        detail = {k: v for k, v in e.items() if k not in ("bucket", "run_id")}
        if bucket == "filed":
            filed.append(detail)
        elif bucket == "would_file":
            would_file.append(detail)
        elif bucket == "refused":
            refused.append(detail)
        elif bucket == "decision_needed":
            decisions_for_you.append(detail)
    if decisions_for_you:
        status = "needs_human"
    elif refused and not filed and not would_file:
        status = "refused"
    else:
        status = "ok"
    return {
        "status": status, "filed": filed, "would_file": would_file,
        "refused": refused, "decisions_for_you": decisions_for_you,
    }
# END agent outcome ledger


def _drafts_markdown(filed: List[Dict[str, Any]], would_file: List[Dict[str, Any]]) -> str:
    """The full text of what was filed, or what would have been filed in practice mode, as one
    markdown blob readable in the Dagster UI without digging through the run's own tool-call
    logs -- a readable outcome, not just counts."""
    parts = []
    for row in would_file:
        parts.append(f"## {row.get('title', '(untitled)')}\n\n_Practice mode, not filed ({row.get('component', '')})._\n\n{row.get('body', '')}")
    for row in filed:
        link = f" {row['url']}" if row.get("url") else ""
        parts.append(f"## {row.get('title', row.get('kind', 'filed'))}\n\n_Filed ({row.get('kind', '')}).{link}_\n\n{row.get('body', '')}")
    return "\n\n---\n\n".join(parts) or "Nothing was filed or drafted."


class LiteLLMAgentComponent(Component, Model, Resolvable):
    """Single-shot LiteLLM agent with MCP tool support.

    Example:
        ```yaml
        type: dagster_community_components.LiteLLMAgentComponent
        attributes:
          asset_name: filesystem_agent
          prompt: "Find the three largest files in /tmp and return their paths."
          system_prompt: "You are a helpful filesystem assistant."
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          max_iterations: 10
          mcp_servers:
            - name: fs
              type: stdio
              command:
                - npx
                - -y
                - "@modelcontextprotocol/server-filesystem"
                - /tmp
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output Dagster asset name.")
    prompt: str = Field(description="User prompt sent to the model on the first turn.")
    system_prompt: Optional[str] = Field(
        default=None,
        description="System prompt prepended to the message list.",
    )
    context_notes: Optional[str] = Field(
        default=None,
        description=(
            "Free text appended to the system prompt under its own heading, naming what's "
            "deliberate in the repo/environment this agent reviews -- a demo's artificial "
            "flakiness, a test fixture that's supposed to fail, a known limitation that isn't a "
            "bug. Without this, an advisor agent has filed 'fix' findings against exactly the "
            "things a repo was built to demonstrate. Not a substitute for reading the repo's own "
            "README/CONTRIBUTING first (put that instruction in the prompt or system_prompt "
            "instead) -- this is for the handful of things worth saying explicitly even after "
            "that, or when no such doc exists."
        ),
    )
    model_id: str = Field(
        alias="model",
        default="gpt-4o-mini",
        description="LiteLLM model string (e.g. gpt-4o, claude-haiku-4-5-20251001, gemini/gemini-2.5-flash).",
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the provider API key.",
    )
    api_base_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a custom API base URL (proxies, self-hosted).",
    )
    temperature: float = Field(default=0.0, description="Sampling temperature.")
    max_tokens: int = Field(default=2048, description="Max tokens per model call.")
    reasoning_effort: Optional[str] = Field(
        default=None,
        description=(
            "Reasoning-model effort level: 'low' | 'medium' | 'high'. "
            "Forwarded to OpenAI o1/o3 and Gemini 2.5+ reasoning models "
            "(silently dropped for other providers)."
        ),
    )
    thinking_budget: Optional[int] = Field(
        default=None,
        description=(
            "Max reasoning-trace tokens. Forwarded to Gemini 2.5+ "
            "(native `thinking_budget`) and Anthropic thinking mode "
            "(auto-mapped to `thinking: {type: enabled, budget_tokens: N}`). "
            "On Gemini, set 0 for short structured outputs to avoid "
            "silent truncation (thinking tokens draw from `max_tokens`). "
            "Silently dropped for OpenAI / Groq / other non-thinking providers."
        ),
    )
    prompt_caching: bool = Field(
        default=False,
        description=(
            "Anthropic prompt caching. When true AND the model is "
            "Anthropic-family (claude / anthropic / bedrock/anthropic.), "
            "wraps the system prompt with `cache_control: {type: ephemeral}` "
            "so subsequent runs within ~5 min hit the cache. Big win for "
            "long system prompts + MCP tool schemas that repeat across "
            "loop iterations (~90% cheaper on cached prefix). Silently "
            "ignored on non-Anthropic providers."
        ),
    )
    max_iterations: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Max tool-call rounds before bailing.",
    )
    mcp_servers: List[MCPServerSpec] = Field(
        default_factory=list,
        description="List of MCP servers to expose as tools. Empty list = no tools (chat-only).",
    )
    allowed_tools: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional allowlist of namespaced tool names (e.g. 'dagster_plus__list_runs'), "
            "'*' wildcards supported (fnmatch), applied across every mcp_servers entry. "
            "Default: all discovered tools. A server can expose many tools the agent "
            "shouldn't be able to call (e.g. a hosted Dagster+ MCP server's "
            "terminate_run/rerun_run/delete_alert_policy alongside read-only ones) -- this "
            "is the lever to restrict an agent to a safe subset, e.g. "
            "['dagster_plus__list_*', 'dagster_plus__get_*']."
        ),
    )
    required_tool_calls: Optional[List[Union[str, List[str]]]] = Field(
        default=None,
        description=(
            "Tool name(s)/fnmatch patterns the model MUST call at least once before it's allowed "
            "to finish -- a model that writes findings and then answers 'to be filed' without "
            "actually calling the filing tool is a real, repeatedly observed failure mode that "
            "prompt wording alone doesn't reliably prevent. Each entry is either a single "
            "pattern (that exact tool is required) or a list of patterns (an 'any of' group -- "
            "satisfied by calling any one of them). When the model tries to stop without having "
            "called every required entry, it gets ONE reminder message (consuming one of "
            "max_iterations' own rounds, not extra budget) and a chance to call them before "
            "finishing; whether a call itself succeeded or errored doesn't matter here, only "
            "whether it was attempted. See on_required_tool_calls_missing for what happens if "
            "it still hasn't after that."
        ),
    )
    on_required_tool_calls_missing: Literal["fail", "warn"] = Field(
        default="fail",
        description=(
            "What happens when required_tool_calls is set and still unsatisfied after the one "
            "reminder. 'fail' (default) raises, so the run fails and the reason is in the run's "
            "own failure message -- turns 'did it act' into a guarantee instead of a hope. "
            "'warn' never raises; the result (and the materialization's own metadata) instead "
            "carries required_tools_missing, the list of entries that were never satisfied "
            "(empty list when everything required was called)."
        ),
    )
    skill_keys: Optional[List[str]] = Field(
        default=None,
        description="SkillResource resource_keys (from SkillResourceComponent) to make available to this agent. Each listed skill's name/description go into the agent's context up front; the full SKILL.md only loads if the agent actually calls load_skill.",
    )
    include_reasoning: bool = Field(default=False, description="Reasoning models (Qwen3, DeepSeek-R1, ...) inline <think>...</think>. The thinking is always stripped from the transcript and final_answer; when true it is also returned as a 'reasoning' key in the result dict (all iterations joined) and as asset metadata.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['llm', 'agent', 'litellm'].",
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )
    outcome_ledger_path: Optional[str] = Field(
        default=None,
        description=(
            "Directory a ledger-aware tool server (github_pr_tools/linear_ticket_tools, its own "
            "ledger_path field pointed at this SAME directory) writes this run's consequential "
            "actions into, keyed by run_id -- read by the companion outcome asset (see "
            "emit_outcome_asset). Unset: the outcome asset still emits (when enabled), just with "
            "empty filed/would_file/refused/decisions_for_you."
        ),
    )
    emit_outcome_asset: bool = Field(
        default=False,
        description=(
            "Build a companion '{asset_name}_outcome' asset (same partitions_def as this asset) that "
            "collapses this run's outcome-ledger entries (see outcome_ledger_path) plus this asset's "
            "own final_answer into {status, filed, would_file, refused, decisions_for_you, text} -- "
            "matched by run_id, not a wall-clock time window (which mixes concurrent runs together). "
            "Shaped to feed agent_fixture_check directly."
        ),
    )

    # Partitions (with {partition_key} / {partition_keys.<dim>} substitution into prompt + system_prompt).
    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'multi' | 'dynamic'.")

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partitions.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static/multi.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values} dicts.",
    )

    # Freshness.
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag in minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy deadline cron schedule.")

    # Retry policy.
    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on failure (transient API / network).")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps', 'skill_keys'), csv=('partition_values',), tags=('asset_tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Lite LLM Agent', editable=True)

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        try:
            import litellm  # noqa: F401
        except ImportError:
            import warnings
            warnings.warn(
                f"litellm_agent '{self.asset_name}': litellm is not installed (pip install "
                "'litellm>=1.30.0') -- dg check/defs-load succeeds, but every materialization will "
                "fail at run time with this exact error. Install it now instead of waiting for the "
                "first real run to find out.",
                stacklevel=2,
            )
        asset_name = self.asset_name
        prompt = self.prompt
        system_prompt = self.system_prompt
        context_notes = self.context_notes
        model = self.model_id
        api_key_env_var = self.api_key_env_var
        api_base_env_var = self.api_base_env_var
        temperature = self.temperature
        max_tokens = self.max_tokens
        reasoning_effort = self.reasoning_effort
        thinking_budget = self.thinking_budget
        prompt_caching = self.prompt_caching
        max_iterations = self.max_iterations
        mcp_servers = self.mcp_servers
        allowed_tools = self.allowed_tools
        required_tool_calls = self.required_tool_calls
        on_required_tool_calls_missing = self.on_required_tool_calls_missing
        skill_keys = self.skill_keys
        include_reasoning = self.include_reasoning
        group_name = self.group_name
        description = self.description
        owners = self.owners or []
        outcome_ledger_path = self.outcome_ledger_path
        emit_outcome_asset = self.emit_outcome_asset

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        _inferred_kinds = list(self.kinds or ["llm", "agent", "litellm"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""
        if skill_keys:
            # Dagster's own strict tag-value validation only allows alpha-numeric/'_'/'-'/'.'
            # (<=63 chars) -- a literal comma-joined value would crash build_defs with
            # DagsterInvalidDefinitionError the moment 2+ skill_keys are configured, so
            # commas are swapped for '-' here (',' isn't in the allowed set).
            import re as _re_tag
            _skill_tag_value = _re_tag.sub(r"[^A-Za-z0-9_.\-]", "-", ",".join(skill_keys))[:63]
            _all_tags.setdefault("dagster-community/skill", _skill_tag_value)

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from datetime import timedelta
            from dagster import FreshnessPolicy
            _lag = timedelta(minutes=int(self.freshness_max_lag_minutes))
            _freshness_policy = (
                FreshnessPolicy.cron(deadline_cron=self.freshness_cron, lower_bound_delta=_lag)
                if self.freshness_cron
                else FreshnessPolicy.time_window(fail_window=_lag)
            )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        # Union of every mcp_servers[].server_resource_key in use -- resolved once at
        # build_defs time (the YAML is static even though which path each entry takes
        # is only read at materialization time).
        _mcp_resource_keys = {s.server_resource_key for s in mcp_servers if s.server_resource_key}
        _skill_resource_keys = set(skill_keys or [])
        _required_resource_keys = _mcp_resource_keys | _skill_resource_keys

        @asset(
            key=AssetKey.from_user_string(asset_name),
            partitions_def=partitions_def,
            group_name=group_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            required_resource_keys=_required_resource_keys or None,
        )
        def _agent_asset(context: AssetExecutionContext) -> Dict[str, Any]:
            import asyncio

            # Resolve every skill_keys entry to its SkillResource up front -- before any
            # model call, before the system prompt is built. Dagster only binds
            # cross-component resources at run time (not at this component's own
            # build_defs/parse time), so this is the earliest point a bad skill_keys
            # entry can actually be caught; fail loudly, naming the exact key.
            skill_resources: Dict[str, Any] = {}
            for _key in (skill_keys or []):
                try:
                    _res = getattr(context.resources, _key)
                except Exception as e:
                    raise ValueError(
                        f"skill_keys references {_key!r} but no resource is registered "
                        f"under that key: {e}"
                    ) from e
                if not (hasattr(_res, "describe") and hasattr(_res, "load")):
                    raise ValueError(
                        f"skill_keys references {_key!r}, but the resource registered "
                        "under that key is not a SkillResource (missing describe()/load())."
                    )
                _info = _res.describe()
                skill_resources[_info["name"]] = _res

            # Resolved once per run: each entry's shared MCPServerResource config, or
            # its own inline spec -- either way the same dict shape _run_agent expects.
            resolved_mcp_servers = [
                getattr(context.resources, s.server_resource_key).model_dump()
                if s.server_resource_key
                else s.model_dump()
                for s in mcp_servers
            ]

            substitutions = {"run_id": context.run_id, "tags": dict(context.run.tags or {})}
            if context.has_partition_key:
                pk = context.partition_key
                if hasattr(pk, "keys_by_dimension"):
                    substitutions["partition_key"] = str(pk)
                    substitutions["partition_keys"] = dict(pk.keys_by_dimension)
                else:
                    substitutions["partition_key"] = str(pk)
                    substitutions["partition_keys"] = {}

            resolved_prompt = _substitute(prompt, substitutions)
            resolved_system = _substitute(system_prompt, substitutions) if system_prompt else None
            if context_notes:
                _notes_block = f"## What's deliberate here (not a problem)\n{context_notes}"
                resolved_system = (resolved_system + "\n\n" + _notes_block) if resolved_system else _notes_block
            # `{tag.<key>}` in a stdio server's own `env` -- e.g. env: {REQUESTER: "{tag.slack/user}"}
            # -- is how a per-run identifier reaches a spawned MCP tool subprocess's environment
            # WITHOUT the model ever supplying it as a tool argument (see slack_agent_request_sensor
            # / dagster_workflow_tools' launch_job for why that distinction matters).
            for _srv in resolved_mcp_servers:
                if isinstance(_srv.get("env"), dict):
                    _srv["env"] = {k: (_substitute(v, substitutions) if isinstance(v, str) else v) for k, v in _srv["env"].items()}

            try:
                result = asyncio.run(
                    _run_agent(
                        log=context.log,
                        prompt=resolved_prompt,
                        system_prompt=resolved_system,
                        model=model,
                        api_key_env_var=api_key_env_var,
                        api_base_env_var=api_base_env_var,
                        temperature=temperature,
                        max_tokens=max_tokens,
                        max_iterations=max_iterations,
                        mcp_servers=resolved_mcp_servers,
                        reasoning_effort=reasoning_effort,
                        thinking_budget=thinking_budget,
                        prompt_caching=prompt_caching,
                        include_reasoning=include_reasoning,
                        skill_resources=skill_resources,
                        allowed_tools=allowed_tools,
                        required_tool_calls=required_tool_calls,
                        run_id=context.run_id,
                    )
                )
            except BaseException as e:
                leaf = _unwrap_exception_group(e)
                if leaf is e:
                    raise
                info = _provider_error_metadata(leaf)
                headline = info["error_class"]
                if info["provider"] or info["status_code"] is not None:
                    headline += f" ({info['provider'] or 'unknown provider'}, status {info['status_code']})"
                raise Failure(
                    description=f"Agent run failed: {headline}: {info['error_message']}",
                    metadata={k: MetadataValue.text(str(v)) for k, v in info.items() if v is not None},
                ) from leaf
            if result.get("required_tools_missing") and on_required_tool_calls_missing == "fail":
                raise RuntimeError(
                    "required_tool_calls not satisfied: " + _required_tools_reminder_text(result["required_tools_missing"])
                )

            md = {
                "final_answer": MetadataValue.md(result["final_answer"] or "_(empty)_"),
                "iterations": MetadataValue.int(result["iterations"]),
                "tool_calls_made": MetadataValue.int(result["tool_calls_made"]),
                "model": MetadataValue.text(model),
                "stopped_reason": MetadataValue.text(result["stopped_reason"]),
            }
            if include_reasoning and result.get("reasoning"):
                md["reasoning"] = MetadataValue.md(result["reasoning"])
            if result["tool_call_details"]:
                md["tool_calls"] = MetadataValue.json(result["tool_call_details"])
            if result.get("combined_server_instructions"):
                md["mcp_server_instructions"] = MetadataValue.md(result["combined_server_instructions"])
            if "required_tools_missing" in result:
                md["required_tools_missing"] = MetadataValue.json(result["required_tools_missing"])
            if skill_keys:
                md["skills_available"] = MetadataValue.json(
                    [res.describe() for res in skill_resources.values()]
                )
                md["skills_loaded"] = MetadataValue.json(result.get("loaded_skill_names") or [])
                md["skill_load_count"] = MetadataValue.int(result.get("skill_load_count") or 0)
            context.add_output_metadata(md)
            return result

        assets_list: List[Any] = [_agent_asset]
        if emit_outcome_asset:
            @asset(
                key=AssetKey.from_user_string(f"{asset_name}_outcome"),
                partitions_def=partitions_def,
                group_name=group_name,
                freshness_policy=_freshness_policy,
                retry_policy=_retry_policy,
                ins={"agent_result": dg.AssetIn(AssetKey.from_user_string(asset_name))},
                description=(
                    f"Structured outcome for {asset_name} -- status, filed, would_file, refused, "
                    "decisions_for_you -- from its outcome ledger, matched by this run's own run_id."
                ),
            )
            def _agent_outcome_asset(context: AssetExecutionContext, agent_result: Dict[str, Any]) -> Dict[str, Any]:
                entries = read_outcome_ledger_entries(outcome_ledger_path, context.run_id)
                outcome = summarize_outcome_ledger(entries)
                outcome["text"] = agent_result.get("final_answer", "")
                if "required_tools_missing" in agent_result:
                    outcome["required_tools_missing"] = agent_result["required_tools_missing"]
                md = {
                    "status": MetadataValue.text(outcome["status"]),
                    "filed_count": MetadataValue.int(len(outcome["filed"])),
                    "would_file_count": MetadataValue.int(len(outcome["would_file"])),
                    "refused_count": MetadataValue.int(len(outcome["refused"])),
                    "decisions_for_you_count": MetadataValue.int(len(outcome["decisions_for_you"])),
                    "drafts": MetadataValue.md(_drafts_markdown(outcome["filed"], outcome["would_file"])),
                }
                if "required_tools_missing" in outcome:
                    md["required_tools_missing"] = MetadataValue.json(outcome["required_tools_missing"])
                context.add_output_metadata(md)
                return outcome

            assets_list.append(_agent_outcome_asset)

        return Definitions(assets=assets_list)


def _substitute(s: str, substitutions: Dict[str, Any]) -> str:
    """Substitute `{partition_key}` / `{partition_keys.<dim>}` / `{run_id}` / `{tag.<key>}` in a
    string. `{tag.<key>}` reads a Dagster run tag -- any key, including slash-namespaced ones
    like `slack/thread_ts` -- so a chat-triggered run can hand its own identifiers (channel,
    thread, requester) into a prompt or an MCP server's `env` without ever putting message text
    in a tag. A tag that isn't set leaves the placeholder untouched (same graceful-fallback
    shape as agentic_pipeline's `{input}`/run_input_tag), rather than silently substituting an
    empty string."""
    if "{" not in s:
        return s
    out = s
    out = out.replace("{run_id}", str(substitutions.get("run_id", "")))
    out = out.replace("{partition_key}", str(substitutions.get("partition_key", "")))
    for dim, val in (substitutions.get("partition_keys") or {}).items():
        out = out.replace("{partition_keys." + dim + "}", str(val))
    tags = substitutions.get("tags") or {}
    if tags and "{tag." in out:
        out = _re_r.sub(r"\{tag\.([^{}]+)\}", lambda m: str(tags[m.group(1)]) if m.group(1) in tags else m.group(0), out)
    return out


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a partitions_def from the canonical fields. Strict combinations only."""
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or partition_dimensions "
            "(multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start'")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_values and partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _resolve_headers(cfg: Dict[str, Any], server_name: str) -> Dict[str, str]:
    """Build the headers dict from literal `headers` + env-backed `headers_env`."""
    import os

    headers: Dict[str, str] = {}
    literal = cfg.get("headers") or {}
    for k, v in literal.items():
        headers[k] = str(v)
    env_backed = cfg.get("headers_env") or {}
    for header_name, env_var in env_backed.items():
        val = os.environ.get(env_var)
        if val is None:
            raise ValueError(
                f"MCP server '{server_name}' references env var {env_var!r} for header "
                f"{header_name!r}, but it isn't set."
            )
        headers[header_name] = val
    return headers


def _tool_name_allowed(name: str, allowed: Optional[List[str]]) -> bool:
    """`allowed` is a list of names/fnmatch patterns ('*' wildcards) matched against the
    namespaced tool name ('{server}__{tool}'). No allowlist = everything allowed."""
    if not allowed:
        return True
    import fnmatch
    return any(fnmatch.fnmatch(name, pattern) for pattern in allowed)


def _missing_required_tool_groups(called_names: List[str], required_tool_calls: List[Any]) -> List[Any]:
    """required_tool_calls: each entry is a single pattern (required) or a list of patterns (an
    'any of' group). Returns the entries never satisfied by any name in called_names -- whether a
    matched call itself errored doesn't matter, only whether it was attempted."""
    missing = []
    for entry in required_tool_calls:
        patterns = entry if isinstance(entry, list) else [entry]
        if not any(_tool_name_allowed(name, patterns) for name in called_names):
            missing.append(entry)
    return missing


def _required_tools_reminder_text(missing: List[Any]) -> str:
    parts = [("one of: " + ", ".join(g)) if isinstance(g, list) else g for g in missing]
    return (
        "Before finishing, you still need to call: " + "; ".join(parts) + ". "
        "Call the required tool(s) now, then give your final answer."
    )


def _unwrap_exception_group(exc: BaseException) -> BaseException:
    """Walk down nested ExceptionGroup/BaseExceptionGroup wrappers to the real leaf error.

    A provider error (e.g. OpenAI insufficient_quota) raised from inside `_run_agent`'s
    `async with AsyncExitStack() as stack:` block -- which owns the MCP stdio client -- gets
    wrapped in a BaseExceptionGroup by anyio's TaskGroup during the client's teardown,
    burying the real error one or more levels deep behind an opaque "unhandled errors in a
    TaskGroup" message. Duck-typed on `.exceptions` (both the 3.11+ builtin and the 3.10
    `exceptiongroup` backport expose it) so this needs no import of either. Cancellation
    noise from sibling tasks being torn down is skipped in favor of the real error."""
    seen = exc
    while True:
        subs = getattr(seen, "exceptions", None)
        if not subs:
            return seen
        real = [s for s in subs if "Cancelled" not in type(s).__name__]
        if not real:
            return seen
        seen = real[0]


def _provider_error_metadata(exc: BaseException) -> Dict[str, Any]:
    """Best-effort provider/status/message extraction. Duck-typed across litellm's own
    exception shape (which mirrors openai's: `.llm_provider`, `.status_code`, `.message`) so
    this needs no import of litellm's exception classes, which vary by version."""
    provider = getattr(exc, "llm_provider", None) or getattr(exc, "provider", None)
    status = getattr(exc, "status_code", None)
    if status is None:
        status = getattr(exc, "code", None)
    message = getattr(exc, "message", None) or str(exc) or type(exc).__name__
    return {
        "error_class": type(exc).__name__,
        "provider": str(provider) if provider else None,
        "status_code": status,
        "error_message": str(message),
    }


async def _run_agent(
    log,
    prompt: str,
    system_prompt: Optional[str],
    model: str,
    api_key_env_var: Optional[str],
    api_base_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    max_iterations: int,
    mcp_servers: List[Dict[str, Any]],
    reasoning_effort: Optional[str] = None,
    thinking_budget: Optional[int] = None,
    prompt_caching: bool = False,
    include_reasoning: bool = False,
    skill_resources: Optional[Dict[str, Any]] = None,
    allowed_tools: Optional[List[str]] = None,
    required_tool_calls: Optional[List[Any]] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the agent loop. Returns the final result dict.

    `skill_resources` is a name -> SkillResource mapping (already resolved by the caller,
    which has access to `context.resources` -- this function does not). Each skill's
    name/description is listed in the system prompt up front; `load_skill` /
    `read_skill_file` are added as tools alongside the MCP ones so the model can fetch a
    skill's full instructions (or a bundled file) only once it actually decides to use it."""
    import json
    import os
    from contextlib import AsyncExitStack

    skill_resources = skill_resources or {}
    # A skill that needs code execution (bundled scripts, or SKILL.md text that tells the
    # model to run one) can't actually be used through load_skill/read_skill_file -- this
    # loop has no code execution at all (that's a separate, hosted-sandbox mechanism).
    # Dropped here, at the very top, so every later use of skill_resources below (the
    # listing AND the load_skill/read_skill_file dispatch) only ever sees usable skills.
    _unusable_skills = [name for name, res in skill_resources.items() if not res.describe().get("runnable_here", True)]
    for _name in _unusable_skills:
        _d = skill_resources[_name].describe()
        log.warning(
            f"skill {_name!r} needs code execution ({_d.get('code_execution_files')}) -- omitted "
            "from skill_keys (this agent's load_skill/read_skill_file loop has no code execution)."
        )
    skill_resources = {k: v for k, v in skill_resources.items() if k not in _unusable_skills}

    try:
        import litellm
    except ImportError:
        raise ImportError("pip install 'litellm>=1.30.0'")
    # Silently drop provider-neutral params that a specific model doesn't
    # accept (temperature-only Anthropic quirks, etc.). Reasoning params
    # are still filtered client-side below since drop_params doesn't
    # strip provider-specific ones like thinking_budget → OpenAI.
    litellm.drop_params = True

    # Provider detection for reasoning-param + prompt_caching routing.
    _ml = model.lower()
    _is_openai_ish = _ml.startswith(("gpt-", "o1", "o3", "o4", "openai/", "azure/", "groq/"))
    _is_gemini = _ml.startswith(("gemini/", "google/", "vertex_ai/gemini"))
    _is_anthropic = (
        "claude" in _ml or _ml.startswith(("anthropic/", "bedrock/anthropic."))
    )

    # Connect to every MCP server. Use one AsyncExitStack so cleanup happens
    # even if any single connection fails mid-startup.
    async with AsyncExitStack() as stack:
        # tool_name (prefixed) → (session, tool_object, original_name)
        tool_index: Dict[str, Any] = {}
        # OpenAI function-calling format for litellm.completion(tools=…)
        llm_tools: List[Dict[str, Any]] = []
        servers_used: List[str] = []
        # name -> combined (saved config `instructions` + the server's own initialize()
        # `instructions`, if either is set) -- appended to the system prompt below, and
        # surfaced in run metadata so it's visible what the model was actually told.
        server_instructions: Dict[str, str] = {}

        for cfg in mcp_servers:
            transport = cfg.get("type", "stdio")
            name = cfg["name"]
            if transport == "stdio":
                import os
                from mcp import ClientSession, StdioServerParameters
                from mcp.client.stdio import stdio_client

                cmd = cfg.get("command") or []
                if not cmd:
                    raise ValueError(f"MCP server '{name}' is stdio but command is empty.")
                params = StdioServerParameters(
                    command=cmd[0],
                    args=list(cmd[1:]),
                    # Inherit the deployment's own environment (tokens, DAGSTER_* / ENG_* settings) --
                    # the stdio client's own default env is a short allowlist (PATH/HOME/...), not the
                    # parent process's environment, so a config-only `env:` would otherwise silently
                    # start every tool server with none of the deployment's real variables set.
                    env={**os.environ, **(cfg.get("env") or {})},
                )
                log.info(f"[mcp:{name}] starting stdio server: {' '.join(cmd)}")
                read, write = await stack.enter_async_context(stdio_client(params))
                session = await stack.enter_async_context(ClientSession(read, write))
                init_result = await session.initialize()
            elif transport in ("http", "streamable_http", "streamable-http"):
                from mcp import ClientSession

                url = cfg.get("url")
                if not url:
                    raise ValueError(f"MCP server '{name}' is http but url is empty.")
                headers = _resolve_headers(cfg, name)
                # Don't log header values — they may contain bearer tokens.
                log.info(
                    f"[mcp:{name}] connecting via streamable-http: {url}"
                    + (f" (headers: {sorted(headers.keys())})" if headers else "")
                )
                # streamablehttp_client returns a 3-tuple — we only need read/write.
                read, write = await _open_mcp_streamable_http(stack, url, headers)
                session = await stack.enter_async_context(ClientSession(read, write))
                init_result = await session.initialize()
            elif transport == "sse":
                from mcp import ClientSession
                from mcp.client.sse import sse_client

                url = cfg.get("url")
                if not url:
                    raise ValueError(f"MCP server '{name}' is sse but url is empty.")
                headers = _resolve_headers(cfg, name)
                log.info(
                    f"[mcp:{name}] connecting to sse: {url}"
                    + (f" (headers: {sorted(headers.keys())})" if headers else "")
                )
                read, write = await stack.enter_async_context(
                    sse_client(url, headers=headers or None)
                )
                session = await stack.enter_async_context(ClientSession(read, write))
                init_result = await session.initialize()
            else:
                raise ValueError(f"MCP server '{name}' has unknown transport: {transport!r}")

            _saved_instructions = cfg.get("instructions")
            _server_instructions = getattr(init_result, "instructions", None)
            if _saved_instructions or _server_instructions:
                _block = ""
                if _saved_instructions:
                    _block += _saved_instructions
                if _server_instructions:
                    _block += ("\n\n" if _block else "") + f"(server-provided): {_server_instructions}"
                server_instructions[name] = _block

            tools_response = await session.list_tools()
            for tool in tools_response.tools:
                prefixed = f"{name}__{tool.name}"
                if not _tool_name_allowed(prefixed, allowed_tools):
                    continue
                tool_index[prefixed] = (session, tool, tool.name)
                llm_tools.append(
                    {
                        "type": "function",
                        "function": {
                            "name": prefixed,
                            "description": tool.description or "",
                            "parameters": _mcp_field(tool, "inputSchema", None) or {"type": "object", "properties": {}},
                        },
                    }
                )
            servers_used.append(name)
            log.info(f"[mcp:{name}] discovered {len(tools_response.tools)} tools")

        # Skill tools — same OpenAI function-calling shape as the MCP tool defs above.
        # Only added when skill_keys were configured; the model sees each skill's
        # name/description in the system prompt (below) and calls load_skill to get the
        # full SKILL.md body once it decides to use one.
        if skill_resources:
            llm_tools.append({
                "type": "function",
                "function": {
                    "name": "load_skill",
                    "description": "Load a skill's full instructions (its SKILL.md body) before using it. Call this once you've decided a listed skill is relevant.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "The skill's name, exactly as shown in the 'Available skills' listing.",
                            },
                        },
                        "required": ["name"],
                    },
                },
            })
            llm_tools.append({
                "type": "function",
                "function": {
                    "name": "read_skill_file",
                    "description": "Read one file bundled alongside a skill (e.g. a reference doc, template, or script), by its path relative to the skill's folder.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "The skill's name, exactly as shown in the 'Available skills' listing.",
                            },
                            "relative_path": {
                                "type": "string",
                                "description": "Path to the file, relative to the skill's folder (as listed in load_skill's 'files').",
                            },
                        },
                        "required": ["name", "relative_path"],
                    },
                },
            })

        # Per-server usage guidance (saved `instructions` + whatever the server itself
        # returned in its own initialize() response) — one heading per server, combined in
        # mcp_servers order so every agent using the same server gets the same text with no
        # copy-paste; combined text is also surfaced in run metadata (see below).
        combined_server_instructions = ""
        if server_instructions:
            _blocks = [f"## {name}\n{server_instructions[name]}" for name in servers_used if name in server_instructions]
            combined_server_instructions = "\n\n".join(_blocks)
            system_prompt = (system_prompt + "\n\n" + combined_server_instructions) if system_prompt else combined_server_instructions

        # Skills listing — the cheap part (name/description) shown to the model up front;
        # the full SKILL.md only loads if the model actually calls load_skill.
        if skill_resources:
            _skill_lines = []
            for res in skill_resources.values():
                _d = res.describe()
                _skill_lines.append(f"- {_d['name']}: {_d['description']}")
            _skills_block = (
                "\n\nAvailable skills (call load_skill with the skill's name to load its "
                "full instructions before using it):\n" + "\n".join(_skill_lines)
            )
            system_prompt = (system_prompt + _skills_block) if system_prompt else _skills_block.lstrip("\n")

        # Build the initial message list. Anthropic prompt caching wraps
        # the system prompt with a cache_control block; LiteLLM translates
        # this to Anthropic's native header on the outbound request.
        messages: List[Dict[str, Any]] = []
        if system_prompt:
            if prompt_caching and _is_anthropic:
                messages.append({
                    "role": "system",
                    "content": [
                        {"type": "text", "text": system_prompt,
                         "cache_control": {"type": "ephemeral"}}
                    ],
                })
            else:
                messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        completion_kwargs: Dict[str, Any] = {
            "model": model,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if api_key_env_var:
            completion_kwargs["api_key"] = _require_env(api_key_env_var)
        if api_base_env_var:
            completion_kwargs["api_base"] = _require_env(api_base_env_var)
        # Reasoning params — filtered by provider family since LiteLLM's
        # drop_params doesn't strip thinking_budget when forwarding to
        # OpenAI (which then 400s).
        if reasoning_effort is not None and (_is_openai_ish or _is_gemini):
            completion_kwargs["reasoning_effort"] = reasoning_effort
        if thinking_budget is not None:
            if _is_gemini:
                completion_kwargs["thinking_budget"] = int(thinking_budget)
            elif _is_anthropic:
                completion_kwargs["thinking"] = {
                    "type": "enabled",
                    "budget_tokens": int(thinking_budget),
                }

        tool_call_details: List[Dict[str, Any]] = []
        reasoning_parts: List[str] = []
        loaded_skill_names: set = set()
        skill_load_count = 0
        iterations = 0
        stopped_reason = "max_iterations"
        required_tools_reminder_sent = False

        for i in range(max_iterations):
            iterations = i + 1
            kwargs = dict(completion_kwargs)
            if llm_tools:
                kwargs["tools"] = llm_tools
                # ollama/ -> ollama_chat/: the /api/generate route has no tool support
                kwargs["model"] = ollama_chat_model(model, True)

            response = await litellm.acompletion(messages=messages, **kwargs)
            assistant_msg = response.choices[0].message
            # Reasoning models inline <think>...</think>: keep it out of the transcript / final answer.
            _clean_text, _reasoning = split_reasoning(assistant_msg.content)
            if _reasoning:
                reasoning_parts.append(_reasoning)
            # Convert to dict for the next message list. LiteLLM message objects
            # have a `.model_dump()` shim on newer versions, else `.dict()`.
            try:
                assistant_dict = assistant_msg.model_dump(exclude_none=True)
            except AttributeError:
                assistant_dict = assistant_msg.dict(exclude_none=True)
            # OpenAI requires `role: assistant`
            assistant_dict.setdefault("role", "assistant")

            tool_calls = assistant_msg.tool_calls or []
            if not tool_calls and llm_tools:
                # Qwen/Hermes-style models served without a tool parser emit <tool_call>{...}</tool_call> as text.
                _clean_text, _text_calls = parse_text_tool_calls(_clean_text)
                if _text_calls:
                    from types import SimpleNamespace
                    tool_calls = [
                        SimpleNamespace(
                            id=f"call_text_{iterations}_{_n}",
                            type="function",
                            function=SimpleNamespace(name=_c["name"], arguments=_c["arguments"]),
                        )
                        for _n, _c in enumerate(_text_calls)
                    ]
                    assistant_dict["tool_calls"] = [
                        {"id": tc.id, "type": "function",
                         "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                        for tc in tool_calls
                    ]
            if assistant_dict.get("content") is not None or _clean_text:
                assistant_dict["content"] = _clean_text
            messages.append(assistant_dict)
            if not tool_calls:
                if required_tool_calls and not required_tools_reminder_sent:
                    _missing = _missing_required_tool_groups([d["tool"] for d in tool_call_details], required_tool_calls)
                    if _missing:
                        required_tools_reminder_sent = True
                        messages.append({"role": "user", "content": _required_tools_reminder_text(_missing)})
                        continue
                stopped_reason = "final_answer"
                break

            for tc in tool_calls:
                fn_name = tc.function.name
                fn_args_raw = tc.function.arguments or "{}"
                try:
                    fn_args = json.loads(fn_args_raw)
                except json.JSONDecodeError as e:
                    fn_args = {}
                    log.warning(f"tool call arguments not valid JSON: {fn_args_raw!r} ({e})")

                if fn_name in ("load_skill", "read_skill_file"):
                    _skill_name = fn_args.get("name")
                    _skill_res = skill_resources.get(_skill_name)
                    if _skill_res is None:
                        tool_output = (
                            f"Error: no skill named {_skill_name!r} is available. "
                            f"Available skills: {sorted(skill_resources.keys())}"
                        )
                        is_error = True
                    elif fn_name == "load_skill":
                        try:
                            tool_output = _skill_res.load()["content"]
                            is_error = False
                            loaded_skill_names.add(_skill_name)
                            skill_load_count += 1
                        except Exception as e:  # noqa: BLE001
                            tool_output = f"Error loading skill {_skill_name!r}: {e}"
                            is_error = True
                    else:  # read_skill_file
                        try:
                            tool_output = _skill_res.read_file(fn_args.get("relative_path"))
                            is_error = False
                        except (ValueError, FileNotFoundError) as e:
                            tool_output = f"Error: {e}"
                            is_error = True

                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": tool_output,
                        }
                    )
                    tool_call_details.append(
                        {
                            "iteration": iterations,
                            "tool": fn_name,
                            "args": fn_args,
                            "result_preview": tool_output[:500],
                            "is_error": is_error,
                        }
                    )
                    continue

                if fn_name not in tool_index:
                    err = f"tool {fn_name!r} not registered with any MCP server"
                    log.warning(err)
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": f"Error: {err}",
                        }
                    )
                    tool_call_details.append(
                        {
                            "iteration": iterations,
                            "tool": fn_name,
                            "args": fn_args,
                            "error": err,
                        }
                    )
                    continue

                session, _tool_obj, original_name = tool_index[fn_name]
                log.info(f"[iter {iterations}] calling {fn_name} args={fn_args!r}")
                try:
                    # meta (MCP's own out-of-band request metadata, NOT a tool argument -- a tool
                    # server reads it via Context.request_context.meta, confirmed against the
                    # installed mcp package) carries this run's id, so a ledger-aware tool server
                    # can record which Dagster run a write action belongs to -- matched exactly,
                    # not inferred from a wall-clock window that mixes concurrent runs.
                    call_result = await session.call_tool(
                        original_name, fn_args, meta={"dagster_run_id": run_id} if run_id else None,
                    )
                    # MCP returns a CallToolResult with .content (list of TextContent etc).
                    parts, is_error = _mcp_result_parts(call_result)
                    tool_output = "\n".join(parts) if parts else "(empty)"
                except Exception as e:  # noqa: BLE001
                    tool_output = f"Tool execution error: {e}"
                    is_error = True
                    log.warning(f"tool {fn_name} raised: {e}")

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": tool_output,
                    }
                )
                tool_call_details.append(
                    {
                        "iteration": iterations,
                        "tool": fn_name,
                        "args": fn_args,
                        "result_preview": tool_output[:500],
                        "is_error": is_error,
                    }
                )
        else:
            # for/else: hit when the loop exits via exhaustion, not break.
            stopped_reason = "max_iterations"

        final_answer = ""
        # Walk backwards to find the last assistant text message.
        for m in reversed(messages):
            if m.get("role") == "assistant" and m.get("content"):
                final_answer = m["content"]
                break

        # Reported here as data; _run_agent never raises for this on its own. The caller (the
        # asset body in build_defs, well outside any stdio-client context) decides whether
        # on_required_tool_calls_missing="fail" raises -- raising from inside this function's own
        # `async with` block gets wrapped in an anyio/mcp BaseExceptionGroup by the stdio client's
        # own TaskGroup teardown, which a plain `except RuntimeError` / `pytest.raises` upstream
        # wouldn't match.
        required_tools_missing: List[Any] = (
            _missing_required_tool_groups([d["tool"] for d in tool_call_details], required_tool_calls)
            if required_tool_calls else []
        )

        result = {
            "final_answer": final_answer,
            "iterations": iterations,
            "tool_calls_made": len(tool_call_details),
            "tool_call_details": tool_call_details,
            "transcript": messages,
            "stopped_reason": stopped_reason,
            "model": model,
            "mcp_servers": servers_used,
            "combined_server_instructions": combined_server_instructions or None,
        }
        if include_reasoning:
            result["reasoning"] = "\n\n".join(reasoning_parts) or None
        if skill_resources:
            result["loaded_skill_names"] = sorted(loaded_skill_names)
            result["skill_load_count"] = skill_load_count
        if required_tool_calls:
            result["required_tools_missing"] = required_tools_missing
        return result


# --- mcp-compat-helpers:begin (identical in every MCP-using component; kept in sync by tests/test_mcp_compat_helpers_drift.py) ---
def _mcp_field(obj, key, default=None):
    """Read `key` (camelCase) from an MCP SDK object or dict. mcp 2.x renamed model attributes to snake_case
    (`inputSchema`->`input_schema`, `isError`->`is_error`, `structuredContent`->`structured_content`) and the models
    have no dict-style `.get`; fastmcp-client variants may return plain dicts."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    import re
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()
    for k in (key, snake):  # mcp 1.x models expose camelCase (inputSchema/isError); mcp 2.x snake_case (input_schema/is_error)
        val = getattr(obj, k, None)
        if val is not None:
            return val
    return default


async def _open_mcp_streamable_http(stack, url, headers):
    """Enter a streamable-HTTP MCP client on `stack`; returns (read, write). Works on mcp 1.x and 2.x.

    mcp 2.x removed `streamablehttp_client(url, headers=...)` (headers now ride on an `httpx2.AsyncClient` passed as
    `http_client=`) and yields a 2-tuple instead of 1.x's (read, write, get_session_id). mcp >=1.24 also has
    `streamable_http_client(url, http_client=...)`; older 1.x only has the deprecated `streamablehttp_client`."""
    try:
        from mcp.client.streamable_http import streamable_http_client
    except ImportError:  # older mcp 1.x
        from mcp.client.streamable_http import streamablehttp_client
        streams = await stack.enter_async_context(streamablehttp_client(url, headers=headers or None))
        return streams[0], streams[1]
    http_client = None
    if headers:
        from mcp.shared._httpx_utils import create_mcp_http_client  # httpx on 1.x, httpx2 on 2.x
        http_client = await stack.enter_async_context(create_mcp_http_client(headers=dict(headers)))
    streams = await stack.enter_async_context(streamable_http_client(url, http_client=http_client))
    return streams[0], streams[1]


def _mcp_result_parts(call_result):
    """CallToolResult -> (list of text parts, is_error). Falls back to structured content when a tool returned only that
    (mcp 2.x / fastmcp); reads `isError`/`is_error` on either major."""
    parts = []
    for c in _mcp_field(call_result, "content", None) or []:
        text = getattr(c, "text", None)
        parts.append(text if text is not None else str(c))
    if not parts:
        structured = _mcp_field(call_result, "structuredContent", None)
        if structured is not None:
            import json
            parts.append(json.dumps(structured, default=str))
    return parts, bool(_mcp_field(call_result, "isError", False))
# --- mcp-compat-helpers:end ---
