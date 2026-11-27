"""Gemini Agent Component.

Single-shot LLM agent with MCP tool support, using the Google Gen AI SDK
directly (no LiteLLM dependency). For multi-vendor support, see
LiteLLMAgentComponent.

Gemini's tool-calling shape differs from OpenAI/Anthropic:
  - Tools are `FunctionDeclaration`s wrapped in `Tool` objects.
  - Request: `client.aio.models.generate_content(model, contents, config)`.
  - Response: `candidates[0].content.parts` — each part has either `text` or
    `function_call`.
  - Tool results go back as a `user` Content with `Part.from_function_response`
    blocks.

`thinking_budget` defaults to 0 because thinking tokens come out of
`max_output_tokens` on Gemini 2.5+ and silently truncate short outputs.
For long multi-step plans you may want to raise it; see Field doc.

Output: same dict shape as the other *_agent components.
"""
from typing import Any, Dict, List, Literal, Optional
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


class MCPServerSpec(dg.Model, dg.Resolvable):
    """One MCP server connection — stdio (subprocess), http (streamable), or sse.

    Declared inline (set `name` + the transport-specific fields), or shared via
    `server_resource_key` (an MCPServerResource component's resource_key) instead --
    exactly one of the two is required per entry.
    """

    name: Optional[str] = Field(
        default=None,
        description="Short name used to prefix tool names. Required when declaring the server inline; leave unset when using `server_resource_key`. Exactly one of `name` (inline spec) or `server_resource_key` is required.",
    )
    type: str = Field(
        default="stdio",
        description="Transport: 'stdio' (subprocess), 'http' (streamable-HTTP), or 'sse' (legacy).",
    )
    command: Optional[List[str]] = Field(default=None, description="stdio: [executable, ...args].")
    url: Optional[str] = Field(default=None, description="http / sse: full URL.")
    env: Optional[Dict[str, str]] = Field(default=None, description="stdio: extra env vars.")
    headers: Optional[Dict[str, str]] = Field(default=None, description="http / sse: literal HTTP headers.")
    headers_env: Optional[Dict[str, str]] = Field(
        default=None, description="http / sse: header_name → env_var_name, value read from env."
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


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
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


def _answer_decisions(answer: Any, marker: str) -> List[str]:
    """The lines under a trailing "<marker>:" heading in an agent's final answer (the LAST occurrence, any case),
    as plain strings. "none" / an empty section yields []. Capped at 10."""
    import re
    if not marker or not isinstance(answer, str):
        return []
    idx = answer.lower().rfind(marker.lower())
    if idx < 0:
        return []
    items = []
    for line in re.sub(r"^[\s:*_#]+", "", answer[idx + len(marker):]).splitlines():
        item = re.sub(r"^\s*(?:[-*\u2022]+|\d+[.)])\s*", "", line).strip()
        if re.search(r"\w", item) and item.strip(" .*_").lower() != "none":
            items.append(item)
    return items[:10]


def _answer_summary(answer: Any, marker: str, limit: int = 700) -> str:
    """The agent's final answer up to (not including) its "<marker>" section, trimmed to `limit` characters."""
    text = answer if isinstance(answer, str) else ""
    idx = text.lower().rfind(marker.lower()) if marker else -1
    if idx >= 0:
        text = text[:idx].rstrip("#*_:\t\n ")
    text = text.strip()
    return text[:limit] + ("\u2026" if len(text) > limit else "")


def summarize_outcome_ledger(entries: List[Dict[str, Any]], answer: Any = "",
                             decisions_marker: str = "decisions for you") -> Dict[str, Any]:
    """Collapse one run's ledger entries (+ the agent's final answer) into {status, filed, would_file, refused,
    decisions_for_you, summary}. status: needs_human (a decision_needed ledger entry) > filed (anything filed) >
    practice (only would_file entries: practice mode drafted, nothing was filed) > refused > nothing_new (no entries).
    decisions_for_you = decision_needed ledger entries (objects) then lines under the answer's `decisions_marker`
    section (strings); the answer's own decisions never change `status`."""
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
    elif filed:
        status = "filed"
    elif would_file:
        status = "practice"
    elif refused:
        status = "refused"
    else:
        status = "nothing_new"
    return {
        "status": status, "filed": filed, "would_file": would_file,
        "refused": refused, "decisions_for_you": decisions_for_you + _answer_decisions(answer, decisions_marker),
        "summary": _answer_summary(answer, decisions_marker),
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


class GeminiAgentComponent(Component, Model, Resolvable):
    """Single-shot agent using the Google Gen AI SDK + MCP tools.

    Example:
        ```yaml
        type: dagster_community_components.GeminiAgentComponent
        attributes:
          asset_name: dagster_plus_gemini_agent
          prompt: "Summarize the last 5 runs in this Dagster+ deployment."
          model: gemini-2.5-flash
          api_key_env_var: GEMINI_API_KEY
          max_iterations: 8
          # Gemini-2.5: thinking tokens eat into max_output_tokens.
          # Default 0 (no thinking) is safest for short structured outputs.
          thinking_budget: 0
          mcp_servers:
            - name: dgp
              type: http
              url: https://mcp.agent.dagster.cloud/mcp/
              headers:
                Dagster-Cloud-Organization: my-org
              headers_env:
                Authorization: DAGSTER_PLUS_BEARER
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output Dagster asset name.")
    prompt: str = Field(description="User prompt sent to the model on the first turn.")
    system_prompt: Optional[str] = Field(default=None, description="System instruction.")
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
        default="gemini-2.5-flash",
        description="Gemini model id (e.g. gemini-2.5-pro, gemini-2.5-flash, gemini-2.5-flash-lite).",
    )
    api_key_env_var: str = Field(default="GEMINI_API_KEY", description="Env var with the Gemini API key.")
    temperature: float = Field(default=0.0, description="Sampling temperature.")
    max_output_tokens: int = Field(default=2048, description="Max output tokens per model call.")
    thinking_budget: Optional[int] = Field(
        default=0,
        description="Gemini 2.5+ thinking budget. Defaults to 0 — thinking tokens come out of max_output_tokens and can silently truncate short outputs. Raise (e.g. 1024) only if your prompt needs multi-step reasoning beyond what the iteration loop already provides.",
    )
    reasoning_effort: Optional[str] = Field(
        default=None,
        description=(
            "Gemini 2.5+ reasoning effort: 'low' | 'medium' | 'high'. "
            "Applied via the underlying `thinking_config.include_thoughts` "
            "+ implicit budget scaling on models that support it. Prefer "
            "explicit `thinking_budget` for fine-grained control; use "
            "`reasoning_effort` as a coarse-grained alternative."
        ),
    )
    max_iterations: int = Field(default=10, ge=1, le=100, description="Max tool-call rounds.")
    mcp_servers: List[MCPServerSpec] = Field(default_factory=list, description="MCP servers to expose as tools.")
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
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['llm', 'agent', 'gemini'].",
    )
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")
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
    decisions_marker: str = Field(
        default="decisions for you",
        description=(
            "Heading (case-insensitive) that starts the agent's 'decisions for a person' section at the end of its "
            "final answer. The lines under it fill the outcome asset's decisions_for_you (\"none\" gives nothing), "
            "and the answer text before it becomes the outcome's `summary` metadata (700 chars). Empty disables both."
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
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag in minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy deadline cron schedule.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on failure.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps', 'skill_keys'), csv=('partition_values',), tags=('asset_tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Gemini Agent', editable=True)

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        try:
            from google import genai  # noqa: F401
        except ImportError:
            import warnings
            warnings.warn(
                f"gemini_agent '{self.asset_name}': google-genai is not installed (pip install "
                "'google-genai>=0.7.0') -- dg check/defs-load succeeds, but every materialization "
                "will fail at run time with this exact error. Install it now instead of waiting "
                "for the first real run to find out.",
                stacklevel=2,
            )
        asset_name = self.asset_name
        prompt = self.prompt
        system_prompt = self.system_prompt
        context_notes = self.context_notes
        model = self.model_id
        api_key_env_var = self.api_key_env_var
        temperature = self.temperature
        max_output_tokens = self.max_output_tokens
        thinking_budget = self.thinking_budget
        reasoning_effort = self.reasoning_effort
        max_iterations = self.max_iterations
        mcp_servers = self.mcp_servers
        allowed_tools = self.allowed_tools
        required_tool_calls = self.required_tool_calls
        on_required_tool_calls_missing = self.on_required_tool_calls_missing
        skill_keys = self.skill_keys
        group_name = self.group_name
        description = self.description
        owners = self.owners or []
        outcome_ledger_path = self.outcome_ledger_path
        emit_outcome_asset = self.emit_outcome_asset
        decisions_marker = self.decisions_marker

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        _inferred_kinds = list(self.kinds or ["llm", "agent", "gemini"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""
        if self.skill_keys:
            # Dagster tag values are strict: alnum/_/-/. only, <=63 chars -- a raw
            # comma-joined list fails validation once there's more than one skill_key
            # (commas aren't allowed), so sanitize + truncate the same way this repo's
            # other tag-from-list call sites do (see data_contract._sanitize_tag_value).
            import re as _re
            _skill_tag_value = _re.sub(r"[^A-Za-z0-9_.\-]", "-", ",".join(self.skill_keys))[:63]
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

            # Resolved once per run: each entry's shared MCPServerResource config, or
            # its own inline spec -- either way the same dict shape _run_agent expects.
            resolved_mcp_servers = [
                getattr(context.resources, s.server_resource_key).model_dump()
                if s.server_resource_key
                else s.model_dump()
                for s in mcp_servers
            ]

            # Resolve each configured skill_keys entry to its registered SkillResource,
            # keyed by the skill's resolved NAME (what the model sees + what it passes
            # to load_skill/read_skill_file) -- not the Dagster resource_key. This is the
            # earliest point this component's own code can check that the key resolves to
            # a real skill resource: Dagster only binds cross-component resources at run
            # time, not at this component's own build_defs/parse time.
            skill_resources: Dict[str, Any] = {}
            for key in (skill_keys or []):
                try:
                    res = getattr(context.resources, key)
                except Exception as e:  # noqa: BLE001
                    raise ValueError(
                        f"skill_keys references {key!r} but no resource is registered under that key: {e}"
                    ) from e
                if not (hasattr(res, "describe") and hasattr(res, "load")):
                    raise ValueError(
                        f"skill_keys references {key!r}, but the resource registered under "
                        f"that key is not a SkillResource (missing describe()/load())."
                    )
                info = res.describe()
                skill_resources[info["name"]] = res

            substitutions = {"run_id": context.run_id}
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

            try:
                result = asyncio.run(
                    _run_agent(
                        log=context.log,
                        prompt=resolved_prompt,
                        system_prompt=resolved_system,
                        model=model,
                        api_key_env_var=api_key_env_var,
                        temperature=temperature,
                        max_output_tokens=max_output_tokens,
                        thinking_budget=thinking_budget,
                        reasoning_effort=reasoning_effort,
                        max_iterations=max_iterations,
                        mcp_servers=resolved_mcp_servers,
                        run_id=context.run_id,
                        skill_resources=skill_resources,
                        allowed_tools=allowed_tools,
                        required_tool_calls=required_tool_calls,
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
            if result["tool_call_details"]:
                md["tool_calls"] = MetadataValue.json(result["tool_call_details"])
            if result.get("combined_server_instructions"):
                md["mcp_server_instructions"] = MetadataValue.md(result["combined_server_instructions"])
            if "required_tools_missing" in result:
                md["required_tools_missing"] = MetadataValue.json(result["required_tools_missing"])
            if skill_keys:
                md["skills_available"] = MetadataValue.json([res.describe() for res in skill_resources.values()])
                md["skills_loaded"] = MetadataValue.json(result["skills_loaded"])
                md["skill_load_count"] = MetadataValue.int(result["skill_load_count"])
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
                outcome = summarize_outcome_ledger(entries, agent_result.get("final_answer", ""), decisions_marker)
                outcome["text"] = agent_result.get("final_answer", "")
                if "required_tools_missing" in agent_result:
                    outcome["required_tools_missing"] = agent_result["required_tools_missing"]
                md = {
                    "status": MetadataValue.text(outcome["status"]),
                    "filed_count": MetadataValue.int(len(outcome["filed"])),
                    "would_file_count": MetadataValue.int(len(outcome["would_file"])),
                    "refused_count": MetadataValue.int(len(outcome["refused"])),
                    "decisions_for_you_count": MetadataValue.int(len(outcome["decisions_for_you"])),
                    "summary": MetadataValue.md(outcome["summary"]),
                    "drafts": MetadataValue.md(_drafts_markdown(outcome["filed"], outcome["would_file"])),
                }
                if "required_tools_missing" in outcome:
                    md["required_tools_missing"] = MetadataValue.json(outcome["required_tools_missing"])
                context.add_output_metadata(md)
                return outcome

            assets_list.append(_agent_outcome_asset)

        return Definitions(assets=assets_list)


def _substitute(s: str, substitutions: Dict[str, Any]) -> str:
    """Substitute `{partition_key}` / `{partition_keys.<dim>}` / `{run_id}` in a string."""
    if "{" not in s:
        return s
    out = s
    out = out.replace("{run_id}", str(substitutions.get("run_id", "")))
    out = out.replace("{partition_key}", str(substitutions.get("partition_key", "")))
    for dim, val in (substitutions.get("partition_keys") or {}).items():
        out = out.replace("{partition_keys." + dim + "}", str(val))
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
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _axis(spec):
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
                raise ValueError("dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _vals = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _vals = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
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
        if not _vals:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_vals)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _vals or not partition_start:
            raise ValueError("partition_type='multi' requires partition_values and partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_vals),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _resolve_headers(cfg: Dict[str, Any], server_name: str) -> Dict[str, str]:
    import os

    headers: Dict[str, str] = {}
    for k, v in (cfg.get("headers") or {}).items():
        headers[k] = str(v)
    for header_name, env_var in (cfg.get("headers_env") or {}).items():
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

    A provider error raised from inside `_run_agent`'s `async with AsyncExitStack() as stack:`
    block -- which owns the MCP stdio client -- gets wrapped in a BaseExceptionGroup by
    anyio's TaskGroup during the client's teardown, burying the real error one or more
    levels deep behind an opaque "unhandled errors in a TaskGroup" message. Duck-typed on
    `.exceptions` (both the 3.11+ builtin and the 3.10 `exceptiongroup` backport expose it)
    so this needs no import of either. Cancellation noise from sibling tasks being torn
    down is skipped in favor of the real error."""
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
    """Best-effort provider/status/message extraction, duck-typed across provider SDK
    exception shapes (litellm/openai: `.llm_provider`/`.status_code`/`.message`; anthropic/
    google-genai: `.status_code`/`.code`) so this needs no import of any of them."""
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


async def _connect_mcp(stack, log, mcp_servers, allowed_tools=None):
    """Bring up every MCP server. Returns (tool_index, gemini_tools, servers_used,
    server_instructions) -- the last is name -> combined saved-config `instructions` + the
    server's own initialize() `instructions` (when either is set).

    `gemini_tools` is a list with one `types.Tool` carrying all FunctionDeclarations.
    """
    from google.genai import types as gtypes

    tool_index: Dict[str, Any] = {}
    function_decls: List[Any] = []
    servers_used: List[str] = []
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
            # Inherit the deployment's own environment (tokens, DAGSTER_* / ENG_* settings) -- the
            # stdio client's own default env is a short allowlist (PATH/HOME/...), not the parent
            # process's environment, so a config-only `env:` would otherwise silently start every
            # tool server with none of the deployment's real variables set.
            params = StdioServerParameters(command=cmd[0], args=list(cmd[1:]), env={**os.environ, **(cfg.get("env") or {})})
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
            log.info(
                f"[mcp:{name}] connecting via streamable-http: {url}"
                + (f" (headers: {sorted(headers.keys())})" if headers else "")
            )
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
            read, write = await stack.enter_async_context(sse_client(url, headers=headers or None))
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
            # Gemini accepts the JSONSchema input_schema via parameters_json_schema.
            function_decls.append(
                gtypes.FunctionDeclaration(
                    name=prefixed,
                    description=tool.description or "",
                    parameters_json_schema=_mcp_field(tool, "inputSchema", None) or {"type": "object", "properties": {}},
                )
            )
        servers_used.append(name)
        log.info(f"[mcp:{name}] discovered {len(tools_response.tools)} tools")

    gemini_tools = [gtypes.Tool(function_declarations=function_decls)] if function_decls else []
    return tool_index, gemini_tools, servers_used, server_instructions


async def _run_agent(
    log,
    prompt: str,
    system_prompt: Optional[str],
    model: str,
    api_key_env_var: str,
    temperature: float,
    max_output_tokens: int,
    thinking_budget: Optional[int],
    max_iterations: int,
    mcp_servers: List[Dict[str, Any]],
    reasoning_effort: Optional[str] = None,
    skill_resources: Optional[Dict[str, Any]] = None,
    allowed_tools: Optional[List[str]] = None,
    required_tool_calls: Optional[List[Any]] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    import os
    from contextlib import AsyncExitStack

    try:
        from google import genai
        from google.genai import types as gtypes
    except ImportError:
        raise ImportError("pip install 'google-genai>=0.7.0'")

    api_key = os.environ.get(api_key_env_var)
    if not api_key:
        raise RuntimeError(f"Gemini API key env var {api_key_env_var!r} is not set.")
    client = genai.Client(api_key=api_key)

    # A skill that needs code execution (bundled scripts, or SKILL.md text that tells the
    # model to run one) can't actually be used through load_skill/read_skill_file -- this
    # loop has no code execution at all (that's a separate, hosted-sandbox mechanism).
    # Dropped here, at the very top, so every later use of skill_resources below (the
    # listing AND the load_skill/read_skill_file dispatch) only ever sees usable skills.
    skill_resources = skill_resources or {}
    _unusable_skills = [name for name, res in skill_resources.items() if not res.describe().get("runnable_here", True)]
    for _name in _unusable_skills:
        _d = skill_resources[_name].describe()
        log.warning(
            f"skill {_name!r} needs code execution ({_d.get('code_execution_files')}) -- omitted "
            "from skill_keys (this agent's load_skill/read_skill_file loop has no code execution)."
        )
    skill_resources = {k: v for k, v in skill_resources.items() if k not in _unusable_skills}

    async with AsyncExitStack() as stack:
        tool_index, mcp_tools, servers_used, server_instructions = await _connect_mcp(
            stack, log, mcp_servers, allowed_tools
        )

        # Per-server usage guidance (saved `instructions` + whatever the server itself
        # returned in its own initialize() response) -- one heading per server, combined in
        # mcp_servers order so every agent using the same server gets the same text with no
        # copy-paste; combined text is also surfaced in run metadata (see below).
        combined_server_instructions = ""
        if server_instructions:
            _blocks = [f"## {name}\n{server_instructions[name]}" for name in servers_used if name in server_instructions]
            combined_server_instructions = "\n\n".join(_blocks)
            system_prompt = (system_prompt + "\n\n" + combined_server_instructions) if system_prompt else combined_server_instructions

        # Skills: inject the name/description listing into the system prompt (cheap,
        # up front) and add load_skill/read_skill_file as function declarations in the
        # SAME tool list the MCP tools go through -- the model can interleave calls to
        # either on any turn. The full SKILL.md body is only fetched (via load_skill)
        # once the model actually chooses a skill -- never inlined up front.
        loaded_skill_names: set = set()
        skill_load_count = 0
        skill_function_decls: List[Any] = []
        if skill_resources:
            skill_lines = "\n".join(
                f"- {name}: {res.describe()['description']}" for name, res in skill_resources.items()
            )
            skills_block = (
                "\n\nAvailable skills (call load_skill with the skill's name to load its "
                f"full instructions before using it):\n{skill_lines}"
            )
            system_prompt = (system_prompt or "") + skills_block
            skill_function_decls = [
                gtypes.FunctionDeclaration(
                    name="load_skill",
                    description=(
                        "Load the full SKILL.md instructions for a configured skill by its "
                        "display name (as shown in the skills listing), before using it."
                    ),
                    parameters_json_schema={
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "description": "The skill's display name."},
                        },
                        "required": ["name"],
                    },
                ),
                gtypes.FunctionDeclaration(
                    name="read_skill_file",
                    description=(
                        "Read one bundled file from a configured skill's folder, by its path "
                        "relative to that folder."
                    ),
                    parameters_json_schema={
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "description": "The skill's display name."},
                            "relative_path": {"type": "string", "description": "Path to the file, relative to the skill folder."},
                        },
                        "required": ["name", "relative_path"],
                    },
                ),
            ]

        function_decls: List[Any] = list(mcp_tools[0].function_declarations) if mcp_tools else []
        function_decls.extend(skill_function_decls)
        gemini_tools = [gtypes.Tool(function_declarations=function_decls)] if function_decls else []

        # Build the running `contents` list.
        contents: List[Any] = [
            gtypes.Content(role="user", parts=[gtypes.Part.from_text(text=prompt)])
        ]

        cfg_kwargs: Dict[str, Any] = {
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
        }
        if system_prompt:
            cfg_kwargs["system_instruction"] = system_prompt
        if gemini_tools:
            cfg_kwargs["tools"] = gemini_tools
        if thinking_budget is not None or reasoning_effort is not None:
            # Both fields flow through gtypes.ThinkingConfig. reasoning_effort
            # is Gemini 2.5+ only and is normalized upstream by google-genai
            # into an implicit budget; explicit thinking_budget always wins
            # when both are set.
            _tk = {}
            if thinking_budget is not None:
                _tk["thinking_budget"] = int(thinking_budget)
            if reasoning_effort is not None:
                _tk["reasoning_effort"] = reasoning_effort
            cfg_kwargs["thinking_config"] = gtypes.ThinkingConfig(**_tk)

        tool_call_details: List[Dict[str, Any]] = []
        iterations = 0
        stopped_reason = "max_iterations"
        required_tools_reminder_sent = False
        last_text_answer = ""

        for i in range(max_iterations):
            iterations = i + 1
            response = await client.aio.models.generate_content(
                model=model,
                contents=contents,
                config=gtypes.GenerateContentConfig(**cfg_kwargs),
            )

            candidate = response.candidates[0] if response.candidates else None
            if candidate is None:
                stopped_reason = "no_candidate"
                break

            # Walk parts: collect text + function_calls. Append the assistant
            # turn back to contents so the next iteration sees it.
            text_parts = []
            function_calls = []
            for part in candidate.content.parts or []:
                if getattr(part, "function_call", None):
                    function_calls.append(part.function_call)
                if getattr(part, "text", None):
                    text_parts.append(part.text)

            if text_parts:
                last_text_answer = "\n".join(t for t in text_parts if t)
            contents.append(candidate.content)

            if not function_calls:
                if required_tool_calls and not required_tools_reminder_sent:
                    _missing = _missing_required_tool_groups([d["tool"] for d in tool_call_details], required_tool_calls)
                    if _missing:
                        required_tools_reminder_sent = True
                        contents.append(gtypes.Content(role="user", parts=[gtypes.Part.from_text(text=_required_tools_reminder_text(_missing))]))
                        continue
                stopped_reason = "final_answer"
                break

            # Dispatch every function call and reply with all function_responses
            # in a single user-role Content (Gemini convention).
            response_parts = []
            for fc in function_calls:
                fn_name = fc.name
                fn_args = dict(fc.args or {})

                if fn_name == "load_skill":
                    skill_name = fn_args.get("name")
                    res = (skill_resources or {}).get(skill_name)
                    if res is None:
                        err = (
                            f"load_skill: no skill named {skill_name!r} is configured "
                            f"(available: {sorted((skill_resources or {}).keys())})"
                        )
                        log.warning(err)
                        response_parts.append(
                            gtypes.Part.from_function_response(name=fn_name, response={"error": err})
                        )
                        tool_call_details.append(
                            {"iteration": iterations, "tool": fn_name, "args": fn_args, "error": err}
                        )
                        continue

                    loaded = res.load()
                    loaded_skill_names.add(skill_name)
                    skill_load_count += 1
                    tool_output = loaded["content"]
                    log.info(f"[iter {iterations}] loaded skill {skill_name!r} ({len(loaded.get('files') or [])} bundled files)")
                    response_parts.append(
                        gtypes.Part.from_function_response(
                            name=fn_name, response={"output": tool_output, "is_error": False}
                        )
                    )
                    tool_call_details.append(
                        {
                            "iteration": iterations,
                            "tool": fn_name,
                            "args": fn_args,
                            "result_preview": tool_output[:500],
                            "is_error": False,
                        }
                    )
                    continue

                if fn_name == "read_skill_file":
                    skill_name = fn_args.get("name")
                    relative_path = fn_args.get("relative_path")
                    res = (skill_resources or {}).get(skill_name)
                    if res is None:
                        err = (
                            f"read_skill_file: no skill named {skill_name!r} is configured "
                            f"(available: {sorted((skill_resources or {}).keys())})"
                        )
                        log.warning(err)
                        response_parts.append(
                            gtypes.Part.from_function_response(name=fn_name, response={"error": err})
                        )
                        tool_call_details.append(
                            {"iteration": iterations, "tool": fn_name, "args": fn_args, "error": err}
                        )
                        continue

                    try:
                        tool_output = res.read_file(relative_path)
                        is_error = False
                    except (ValueError, FileNotFoundError) as e:
                        tool_output = str(e)
                        is_error = True
                        log.warning(f"read_skill_file({skill_name!r}, {relative_path!r}) failed: {e}")
                    response_parts.append(
                        gtypes.Part.from_function_response(
                            name=fn_name, response={"output": tool_output, "is_error": is_error}
                        )
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
                    response_parts.append(
                        gtypes.Part.from_function_response(
                            name=fn_name, response={"error": err}
                        )
                    )
                    tool_call_details.append(
                        {"iteration": iterations, "tool": fn_name, "args": fn_args, "error": err}
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
                    parts, is_error = _mcp_result_parts(call_result)
                    tool_output = "\n".join(parts) if parts else "(empty)"
                except Exception as e:  # noqa: BLE001
                    tool_output = f"Tool execution error: {e}"
                    is_error = True
                    log.warning(f"tool {fn_name} raised: {e}")

                response_parts.append(
                    gtypes.Part.from_function_response(
                        name=fn_name,
                        response={"output": tool_output, "is_error": is_error},
                    )
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

            contents.append(gtypes.Content(role="user", parts=response_parts))

        # Build a serializable transcript (strip non-pickleable SDK objects).
        transcript_serial = []
        for c in contents:
            try:
                transcript_serial.append(c.model_dump(exclude_none=True))
            except Exception:
                transcript_serial.append(str(c))

        # Reported here as data; _run_agent never raises for this on its own. The caller (the
        # asset body in build_defs, well outside this `async with` block) decides whether
        # on_required_tool_calls_missing="fail" raises -- raising from inside this function's own
        # `async with` block gets wrapped in an anyio/mcp BaseExceptionGroup by the stdio client's
        # own TaskGroup teardown, which a plain `except RuntimeError` / `pytest.raises` upstream
        # wouldn't match.
        _result = {
            "final_answer": last_text_answer,
            "iterations": iterations,
            "tool_calls_made": len(tool_call_details),
            "tool_call_details": tool_call_details,
            "transcript": transcript_serial,
            "stopped_reason": stopped_reason,
            "model": model,
            "mcp_servers": servers_used,
            "combined_server_instructions": combined_server_instructions or None,
            "skills_loaded": sorted(loaded_skill_names),
            "skill_load_count": skill_load_count,
        }
        if required_tool_calls:
            _result["required_tools_missing"] = _missing_required_tool_groups([d["tool"] for d in tool_call_details], required_tool_calls)
        return _result


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
