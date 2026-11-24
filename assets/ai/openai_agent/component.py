"""OpenAI Agent Component.

Single-shot LLM agent with MCP tool support, using the OpenAI SDK directly
(no LiteLLM dependency). For multi-vendor support via LiteLLM, see
LiteLLMAgentComponent.

Runs on the RESPONSES API (`client.responses.create`), not Chat Completions --
switched specifically so `hosted_code_execution` (OpenAI's hosted
`code_interpreter` tool) can exist at all; Chat Completions has no
provider-hosted code execution, only caller-implemented function calling.

The agent loop is the standard tool-calling shape, in Responses API terms:
  1. List tools from every configured MCP server (prefix tool names by
     server name so cross-server collisions are impossible) as flat
     `{"type": "function", "name": ..., "parameters": ...}` tool defs (the
     Responses API's tool shape has no nested `function` wrapper, unlike
     Chat Completions').
  2. Send `input` (an accumulating list of message/function_call/
     function_call_output items) + `instructions` (the system prompt) to
     OpenAI with all tool defs.
  3. If the model emits `function_call` items, dispatch each to the owning
     MCP session and append a matching `function_call_output` item, then
     loop.
  4. Stop when a turn's `output` has no `function_call` items, or
     `max_iterations` is hit.

Output is a Python dict (loaded by IO manager) carrying the final answer,
full transcript, tool-call/iteration counts, and (when `hosted_code_execution`
generated any) the files the model's code produced.
"""
from typing import Any, Dict, List, Literal, Optional
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
from pydantic import ConfigDict, Field, model_validator


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
        description="stdio only: [executable, ...args].",
    )
    url: Optional[str] = Field(
        default=None,
        description="http / sse: full URL of the MCP endpoint.",
    )
    env: Optional[Dict[str, str]] = Field(
        default=None,
        description="stdio only: extra env vars for the subprocess.",
    )
    headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="http / sse: literal HTTP headers (don't put secrets here — use `headers_env`).",
    )
    headers_env: Optional[Dict[str, str]] = Field(
        default=None,
        description="http / sse: map of header_name → env_var_name; the header value is read from the named env var at materialization time.",
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

# BEGIN report-manifest (inlined per component; keep copies identical across report_tools_server / anthropic_agent /
# openai_agent / slack_reply / teams_reply / slack_chat_tools -- see tests/test_report_manifest_drift.py)
import json as _rm_json  # noqa: E402,F401
import os as _rm_os  # noqa: E402,F401
import shutil as _rm_shutil  # noqa: E402,F401
import threading as _rm_threading  # noqa: E402,F401
import time as _rm_time  # noqa: E402,F401

_REPORT_MANIFEST_LOCK = _rm_threading.Lock()


def _report_run_dir(files_dir: str, run_id: Optional[str]) -> str:
    return _rm_os.path.join(_rm_os.path.expanduser(files_dir), run_id or "_unscoped")


def _report_manifest_path(files_dir: str, run_id: Optional[str]) -> str:
    return _rm_os.path.join(_report_run_dir(files_dir, run_id), "manifest.json")


def read_run_manifest(files_dir: str, run_id: Optional[str]) -> List[Dict[str, Any]]:
    """Every file {file_id, name, media_type, path, size_bytes, tool, created_at} report_tools_server has
    written for this run so far. Trusted, run-scoped lookup -- same reasoning as receipts_db_path's own
    receipt_ids_for_run: a caller only ever reads its OWN run's files by the run id Dagster itself put on the
    run (never an argument an agent could spoof)."""
    path = _report_manifest_path(files_dir, run_id)
    if not _rm_os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = _rm_json.load(f)
        return data if isinstance(data, list) else []
    except (OSError, ValueError):
        return []


def append_to_report_manifest(files_dir: str, run_id: Optional[str], entry: Dict[str, Any]) -> None:
    run_dir = _report_run_dir(files_dir, run_id)
    with _REPORT_MANIFEST_LOCK:
        _rm_os.makedirs(run_dir, exist_ok=True)
        existing = read_run_manifest(files_dir, run_id)
        existing.append(entry)
        manifest_path = _report_manifest_path(files_dir, run_id)
        tmp_path = manifest_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            _rm_json.dump(existing, f)
        _rm_os.replace(tmp_path, manifest_path)


def lookup_report_file(files_dir: str, run_id: Optional[str], file_id: str) -> Optional[Dict[str, Any]]:
    for entry in read_run_manifest(files_dir, run_id):
        if entry.get("file_id") == file_id:
            return entry
    return None


def sweep_old_report_runs(files_dir: str, retention_hours: float) -> int:
    """Delete run folders under files_dir whose manifest.json is older than retention_hours. Best-effort: a run
    folder with no manifest.json yet, or one that loses a race with a concurrent writer, is skipped rather than
    raised -- this runs opportunistically on every tool call, not as a scheduled job."""
    base = _rm_os.path.expanduser(files_dir)
    if not _rm_os.path.isdir(base):
        return 0
    cutoff = _rm_time.time() - max(0.0, retention_hours) * 3600
    removed = 0
    for name in list(_rm_os.listdir(base)):
        run_dir = _rm_os.path.join(base, name)
        manifest = _rm_os.path.join(run_dir, "manifest.json")
        if not _rm_os.path.isdir(run_dir) or not _rm_os.path.isfile(manifest):
            continue
        try:
            if _rm_os.path.getmtime(manifest) < cutoff:
                _rm_shutil.rmtree(run_dir, ignore_errors=True)
                removed += 1
        except OSError:
            continue
    return removed
# END report-manifest


class OpenAIAgentComponent(Component, Model, Resolvable):
    """Single-shot agent using the OpenAI SDK directly + MCP tools.

    Example:
        ```yaml
        type: dagster_community_components.OpenAIAgentComponent
        attributes:
          asset_name: dagster_plus_agent
          prompt: "Summarize the last 5 runs in this Dagster+ deployment."
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          max_iterations: 8
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
    system_prompt: Optional[str] = Field(
        default=None,
        description="System prompt prepended to the message list.",
    )
    model_id: str = Field(
        alias="model",
        default="gpt-4o-mini",
        description="OpenAI model id (e.g. gpt-4o, gpt-4o-mini, gpt-4-turbo, o1-mini).",
    )
    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Env var holding the OpenAI API key.",
    )
    base_url_env_var: Optional[str] = Field(
        default=None,
        description="Optional env var holding a custom base_url (Azure OpenAI / proxies / OpenAI-compatible endpoints).",
    )
    organization_env_var: Optional[str] = Field(
        default=None,
        description="Optional env var holding the OpenAI organization id.",
    )
    temperature: float = Field(default=0.0, description="Sampling temperature.")
    max_tokens: int = Field(default=2048, description="Max tokens per model call.")
    reasoning_effort: Optional[str] = Field(
        default=None,
        description=(
            "Reasoning-model effort level: 'low' | 'medium' | 'high'. "
            "Forwarded to OpenAI o1/o3/o4 reasoning models. Passed through "
            "unchanged for other OpenAI models — OpenAI returns 400 if the "
            "model doesn't accept it, so only set on reasoning models. "
            "Note: OpenAI applies prompt caching automatically on prompts "
            ">1024 tokens; there's no client-side flag for it."
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
        description="List of MCP servers to expose as tools.",
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
    hosted_code_execution: bool = Field(
        default=False,
        description=(
            "Enable OpenAI's hosted code_interpreter tool. A skill_keys entry that needs "
            "code execution (skill_kind='code_execution', or the auto-detected heuristic) "
            "is no longer omitted with a warning when this is true -- its SKILL.md plus "
            "bundled files are uploaded via the Files API (purpose='user_data') and "
            "attached to the code_interpreter container's file_ids, so the model can "
            "actually read and run them, instead of just being told it can't. Generated "
            "files come back in this asset's own output (`files`) and materialization "
            "metadata, and -- when hosted_code_execution_files_dir is also set -- are "
            "written there too, in the same manifest.json shape report_tools_server uses. "
            "NOT live-verified against a real OpenAI account in this repo's environment -- "
            "confirmed against OpenAI's own current API reference instead (the container/"
            "file_ids request shape, the container_file_citation annotation fields, and "
            "the container-files content-retrieval endpoint's purpose are all documented; "
            "the exact async SDK call for reading the downloaded bytes was not pinned down "
            "to one certain shape, so that one step tries a few plausible forms rather than "
            "assuming one). A container expires after 20 minutes of inactivity (OpenAI's "
            "own limit, not configurable here)."
        ),
    )
    hosted_code_execution_files_dir: Optional[str] = Field(
        default=None,
        description=(
            "Local directory root -- hosted_code_execution only. When set, every file the "
            "code_interpreter tool generates is ALSO written here (one subfolder per "
            "dagster/run_id, a manifest.json per run) in the exact same shape "
            "report_tools_server/anthropic_agent's hosted_skills_files_dir use -- so "
            "slack_reply/teams_reply's own report_files_dir (and slack_chat_tools' "
            "upload_files) deliver these files with zero changes needed there."
        ),
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['llm', 'agent', 'openai'].",
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

    # Partitions (prompt + system_prompt support {partition_key} / {partition_keys.<dim>} / {run_id}).
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
        return ComponentFormConfig(label='Open AI Agent', editable=True)

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        try:
            import openai  # noqa: F401
        except ImportError:
            import warnings
            warnings.warn(
                f"openai_agent '{self.asset_name}': the openai SDK is not installed (pip install "
                "'openai>=1.30.0') -- dg check/defs-load succeeds, but every materialization will "
                "fail at run time with this exact error. Install it now instead of waiting for the "
                "first real run to find out.",
                stacklevel=2,
            )
        asset_name = self.asset_name
        prompt = self.prompt
        system_prompt = self.system_prompt
        model = self.model_id
        api_key_env_var = self.api_key_env_var
        base_url_env_var = self.base_url_env_var
        organization_env_var = self.organization_env_var
        temperature = self.temperature
        max_tokens = self.max_tokens
        reasoning_effort = self.reasoning_effort
        max_iterations = self.max_iterations
        mcp_servers = self.mcp_servers
        allowed_tools = self.allowed_tools
        required_tool_calls = self.required_tool_calls
        on_required_tool_calls_missing = self.on_required_tool_calls_missing
        skill_keys = self.skill_keys
        hosted_code_execution = self.hosted_code_execution
        hosted_code_execution_files_dir = self.hosted_code_execution_files_dir
        group_name = self.group_name
        description = self.description
        owners = self.owners or []
        outcome_ledger_path = self.outcome_ledger_path
        emit_outcome_asset = self.emit_outcome_asset

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        _inferred_kinds = list(self.kinds or ["llm", "agent", "openai"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""
        if self.skill_keys:
            # Dagster's own strict tag-value validation only allows alpha-numeric/'_'/'-'/'.'
            # (<=63 chars) -- a literal comma-joined value would crash build_defs with
            # DagsterInvalidDefinitionError the moment 2+ skill_keys are configured, so
            # commas are swapped for '-' here (',' isn't in the allowed set).
            import re
            _skill_tag_value = re.sub(r"[^A-Za-z0-9_.\-]", "-", ",".join(self.skill_keys))[:63]
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

            # A skill that needs code execution (bundled scripts, or SKILL.md text that
            # tells the model to run one) can't actually be used through load_skill/
            # read_skill_file -- this loop has no code execution at all. Filtered out of a
            # SEPARATE dict (not `skill_resources` itself) so skills_available metadata
            # below still shows every CONFIGURED skill, including why one was excluded;
            # only the model-facing listing and _run_agent's own dispatch use the filtered
            # one below.
            _unusable_skills = [name for name, res in skill_resources.items() if not res.describe().get("runnable_here", True)]
            if hosted_code_execution:
                # Routed to the code_interpreter path inside _run_agent instead of being
                # dropped -- their files get uploaded and attached to the container.
                code_execution_skill_resources = {name: skill_resources[name] for name in _unusable_skills}
            else:
                for _name in _unusable_skills:
                    _d = skill_resources[_name].describe()
                    context.log.warning(
                        f"skill {_name!r} needs code execution ({_d.get('code_execution_files')}) -- omitted "
                        "from skill_keys (this agent's load_skill/read_skill_file loop has no code execution "
                        "unless hosted_code_execution is enabled)."
                    )
                code_execution_skill_resources = {}
            usable_skill_resources = {k: v for k, v in skill_resources.items() if k not in _unusable_skills}

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
            # `{tag.<key>}` in a stdio server's own `env` -- e.g. env: {REQUESTER: "{tag.slack/user}"}
            # -- is how a per-run identifier reaches a spawned MCP tool subprocess's environment
            # WITHOUT the model ever supplying it as a tool argument (see slack_agent_request_sensor
            # / dagster_workflow_tools' launch_job for why that distinction matters).
            for _srv in resolved_mcp_servers:
                if isinstance(_srv.get("env"), dict):
                    _srv["env"] = {k: (_substitute(v, substitutions) if isinstance(v, str) else v) for k, v in _srv["env"].items()}

            # Skills go into context up front as just name + description (cheap); the
            # full SKILL.md body only loads if the model actually calls load_skill.
            if usable_skill_resources:
                _skill_lines = "\n".join(
                    f"- {name}: {res.describe()['description']}" for name, res in usable_skill_resources.items()
                )
                _skill_listing = (
                    "\n\nAvailable skills (call load_skill with the skill's name to load "
                    f"its full instructions before using it):\n{_skill_lines}"
                )
                resolved_system = (resolved_system or "") + _skill_listing

            if code_execution_skill_resources:
                _ce_lines = "\n".join(
                    f"- {name}: {res.describe()['description']}" for name, res in code_execution_skill_resources.items()
                )
                resolved_system = (resolved_system or "") + (
                    "\n\nSkills available with code execution (their SKILL.md and bundled files "
                    f"are already in your code execution environment -- read and run them "
                    f"directly, no load_skill call needed):\n{_ce_lines}"
                )

            result = asyncio.run(
                _run_agent(
                    log=context.log,
                    prompt=resolved_prompt,
                    system_prompt=resolved_system,
                    model=model,
                    api_key_env_var=api_key_env_var,
                    base_url_env_var=base_url_env_var,
                    organization_env_var=organization_env_var,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    max_iterations=max_iterations,
                    mcp_servers=resolved_mcp_servers,
                    reasoning_effort=reasoning_effort,
                    skill_resources=usable_skill_resources,
                    allowed_tools=allowed_tools,
                    required_tool_calls=required_tool_calls,
                    hosted_code_execution=hosted_code_execution,
                    code_execution_skill_resources=code_execution_skill_resources,
                    hosted_code_execution_files_dir=hosted_code_execution_files_dir,
                    run_id=context.run_id,
                )
            )
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
                md["skills_available"] = MetadataValue.json(
                    [res.describe() for res in skill_resources.values()]
                )
                md["skills_loaded"] = MetadataValue.json(sorted(result.get("loaded_skill_names") or []))
                md["skill_load_count"] = MetadataValue.int(result.get("skill_load_count") or 0)
            if hosted_code_execution:
                md["hosted_code_execution_used"] = MetadataValue.bool(bool(result.get("files")))
                md["hosted_code_execution_files"] = MetadataValue.json([
                    {"name": f["name"], "size_bytes": len(f["bytes"]) if "bytes" in f else f.get("size_bytes")}
                    for f in result.get("files", [])
                ])
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
        import re
        out = re.sub(r"\{tag\.([^{}]+)\}", lambda m: str(tags[m.group(1)]) if m.group(1) in tags else m.group(0), out)
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
            "Set either partition_type or partition_dimensions, not both."
        )

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


async def _run_agent(
    log,
    prompt: str,
    system_prompt: Optional[str],
    model: str,
    api_key_env_var: str,
    base_url_env_var: Optional[str],
    organization_env_var: Optional[str],
    temperature: float,
    max_tokens: int,
    max_iterations: int,
    mcp_servers: List[Dict[str, Any]],
    reasoning_effort: Optional[str] = None,
    skill_resources: Optional[Dict[str, Any]] = None,
    allowed_tools: Optional[List[str]] = None,
    required_tool_calls: Optional[List[Any]] = None,
    hosted_code_execution: bool = False,
    code_execution_skill_resources: Optional[Dict[str, Any]] = None,
    hosted_code_execution_files_dir: Optional[str] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    import json
    import os
    from contextlib import AsyncExitStack
    from datetime import datetime, timezone

    try:
        from openai import AsyncOpenAI
    except ImportError:
        raise ImportError("pip install 'openai>=1.30.0'")

    api_key = os.environ.get(api_key_env_var)
    if not api_key:
        raise RuntimeError(f"OpenAI API key env var {api_key_env_var!r} is not set.")
    client_kwargs: Dict[str, Any] = {"api_key": api_key}
    if base_url_env_var:
        bu = os.environ.get(base_url_env_var)
        if bu:
            client_kwargs["base_url"] = bu
    if organization_env_var:
        org = os.environ.get(organization_env_var)
        if org:
            client_kwargs["organization"] = org

    client = AsyncOpenAI(**client_kwargs)

    async with AsyncExitStack() as stack:
        tool_index: Dict[str, Any] = {}
        openai_tools: List[Dict[str, Any]] = []
        servers_used: List[str] = []
        # name -> combined (saved config `instructions` + the server's own initialize()
        # `instructions`, if either is set) -- appended to the system prompt below, and
        # surfaced in run metadata as combined_server_instructions / mcp_server_instructions.
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
                # Inherit the deployment's own environment (tokens, DAGSTER_* / ENG_* settings) --
                # the stdio client's own default env is a short allowlist (PATH/HOME/...), not the
                # parent process's environment, so a config-only `env:` would otherwise silently
                # start every tool server with none of the deployment's real variables set.
                params = StdioServerParameters(
                    command=cmd[0], args=list(cmd[1:]), env={**os.environ, **(cfg.get("env") or {})}
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
                openai_tools.append(
                    {
                        # Responses API tool shape is FLAT (no nested "function" key, unlike
                        # Chat Completions') -- confirmed against OpenAI's own current API
                        # reference.
                        "type": "function",
                        "name": prefixed,
                        "description": tool.description or "",
                        "parameters": _mcp_field(tool, "inputSchema", None) or {"type": "object", "properties": {}},
                    }
                )
            servers_used.append(name)
            log.info(f"[mcp:{name}] discovered {len(tools_response.tools)} tools")

        loaded_skill_names: set = set()
        skill_load_count = 0
        if skill_resources:
            openai_tools.append(
                {
                    "type": "function",
                    "name": "load_skill",
                    "description": "Load the full SKILL.md instructions for a skill, by its name, before using it. Only call this once you've decided a skill is actually relevant.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "The skill's name, exactly as shown in the available-skills listing.",
                            }
                        },
                        "required": ["name"],
                    },
                }
            )
            openai_tools.append(
                {
                    "type": "function",
                    "name": "read_skill_file",
                    "description": "Read one bundled file from a skill's folder, by path relative to that folder (see the `files` list returned by load_skill).",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "The skill's name, exactly as shown in the available-skills listing.",
                            },
                            "relative_path": {
                                "type": "string",
                                "description": "Path to the file, relative to the skill's folder.",
                            },
                        },
                        "required": ["name", "relative_path"],
                    },
                }
            )

        # hosted_code_execution: upload every code-execution skill's SKILL.md + bundled files
        # (text only -- SkillResource.read_file() reads as text, so a binary bundled file is
        # skipped with a warning rather than crashing the run) to the Files API, then attach
        # them to the code_interpreter container's file_ids so the model can read/run them
        # directly -- no load_skill call needed, unlike the plain markdown-skill path above.
        if hosted_code_execution:
            uploaded_file_ids: List[str] = []
            if code_execution_skill_resources:
                for _name, _res in code_execution_skill_resources.items():
                    try:
                        _loaded = _res.load()
                    except Exception as e:  # noqa: BLE001 -- one bad skill must never fail the whole run
                        log.warning(f"hosted_code_execution: could not load skill {_name!r} for upload: {e}")
                        continue
                    try:
                        _skill_md_file = await client.files.create(
                            file=(f"{_name}_SKILL.md", _loaded["content"].encode("utf-8")), purpose="user_data",
                        )
                        uploaded_file_ids.append(_skill_md_file.id)
                    except Exception as e:  # noqa: BLE001
                        log.warning(f"hosted_code_execution: could not upload SKILL.md for skill {_name!r}: {e}")
                    for _rel in _loaded.get("files") or []:
                        try:
                            _text = _res.read_file(_rel)
                        except Exception as e:  # noqa: BLE001 -- binary bundled files aren't readable as text; skip them
                            log.warning(f"hosted_code_execution: could not read {_rel!r} from skill {_name!r}: {e}")
                            continue
                        try:
                            _f = await client.files.create(
                                file=(_rel.replace("/", "_"), _text.encode("utf-8")), purpose="user_data",
                            )
                            uploaded_file_ids.append(_f.id)
                        except Exception as e:  # noqa: BLE001
                            log.warning(f"hosted_code_execution: could not upload {_rel!r} for skill {_name!r}: {e}")
            _container_param: Dict[str, Any] = {"type": "auto"}
            if uploaded_file_ids:
                _container_param["file_ids"] = uploaded_file_ids
            openai_tools.append({"type": "code_interpreter", "container": _container_param})

        # Per-server usage guidance (saved `instructions` + whatever the server itself
        # returned in its own initialize() response) -- one heading per server, combined in
        # mcp_servers order so every agent using the same server gets the same text with no
        # copy-paste; combined text is also surfaced in run metadata (see below).
        combined_server_instructions = ""
        if server_instructions:
            _blocks = [f"## {name}\n{server_instructions[name]}" for name in servers_used if name in server_instructions]
            combined_server_instructions = "\n\n".join(_blocks)
            system_prompt = (system_prompt + "\n\n" + combined_server_instructions) if system_prompt else combined_server_instructions

        # input_list is BOTH the running conversation (resent in full every turn -- the
        # Responses API's stateless multi-turn shape, confirmed against OpenAI's own function-
        # calling guide: no previous_response_id is used here, so every item the model needs to
        # see again must already be in this list) and the returned "transcript".
        input_list: List[Dict[str, Any]] = [{"role": "user", "content": prompt}]

        tool_call_details: List[Dict[str, Any]] = []
        iterations = 0
        stopped_reason = "max_iterations"
        required_tools_reminder_sent = False
        last_assistant_content: Optional[str] = None

        for i in range(max_iterations):
            iterations = i + 1
            kwargs: Dict[str, Any] = {
                "model": model,
                "input": input_list,
                "temperature": temperature,
                "max_output_tokens": max_tokens,
            }
            if system_prompt:
                kwargs["instructions"] = system_prompt
            # Forward reasoning_effort to o1/o3/o4 models. If the caller
            # sets it on a non-reasoning model, OpenAI returns 400 — that's
            # a user config error, not something we should silently swallow.
            if reasoning_effort is not None:
                kwargs["reasoning"] = {"effort": reasoning_effort}
            if openai_tools:
                kwargs["tools"] = openai_tools

            response = await client.responses.create(**kwargs)

            output_items: List[Dict[str, Any]] = []
            for item in response.output:
                try:
                    d = item.model_dump(exclude_none=True)
                except AttributeError:
                    d = item.dict(exclude_none=True) if hasattr(item, "dict") else {"type": getattr(item, "type", "unknown")}
                output_items.append(d)
            input_list.extend(output_items)

            _output_text = getattr(response, "output_text", None)
            if _output_text:
                last_assistant_content = _output_text

            function_calls = [d for d in output_items if d.get("type") == "function_call"]
            if not function_calls:
                if required_tool_calls and not required_tools_reminder_sent:
                    _missing = _missing_required_tool_groups([d["tool"] for d in tool_call_details], required_tool_calls)
                    if _missing:
                        required_tools_reminder_sent = True
                        input_list.append({"role": "user", "content": _required_tools_reminder_text(_missing)})
                        continue
                stopped_reason = "final_answer"
                break

            for fc in function_calls:
                fn_name = fc.get("name")
                call_id = fc.get("call_id")
                fn_args_raw = fc.get("arguments") or "{}"
                try:
                    fn_args = json.loads(fn_args_raw)
                except json.JSONDecodeError as e:
                    fn_args = {}
                    log.warning(f"tool call arguments not valid JSON: {fn_args_raw!r} ({e})")

                if fn_name == "load_skill":
                    skill_name = fn_args.get("name")
                    skill_res = (skill_resources or {}).get(skill_name)
                    if skill_res is None:
                        err = f"load_skill: no skill named {skill_name!r} is available"
                        log.warning(err)
                        tool_output, is_error = f"Error: {err}", True
                    else:
                        loaded = skill_res.load()
                        tool_output, is_error = loaded["content"], False
                        loaded_skill_names.add(skill_name)
                        skill_load_count += 1
                    input_list.append({"type": "function_call_output", "call_id": call_id, "output": tool_output})
                    tool_call_details.append(
                        {
                            "iteration": iterations, "tool": fn_name, "args": fn_args,
                            "result_preview": tool_output[:500], "is_error": is_error,
                        }
                    )
                    continue

                if fn_name == "read_skill_file":
                    skill_name = fn_args.get("name")
                    relative_path = fn_args.get("relative_path")
                    skill_res = (skill_resources or {}).get(skill_name)
                    if skill_res is None:
                        err = f"read_skill_file: no skill named {skill_name!r} is available"
                        log.warning(err)
                        tool_output, is_error = f"Error: {err}", True
                    else:
                        try:
                            tool_output, is_error = skill_res.read_file(relative_path), False
                        except (ValueError, FileNotFoundError) as e:
                            tool_output, is_error = f"Error: {e}", True
                    input_list.append({"type": "function_call_output", "call_id": call_id, "output": tool_output})
                    tool_call_details.append(
                        {
                            "iteration": iterations, "tool": fn_name, "args": fn_args,
                            "result_preview": tool_output[:500], "is_error": is_error,
                        }
                    )
                    continue

                if fn_name not in tool_index:
                    err = f"tool {fn_name!r} not registered with any MCP server"
                    log.warning(err)
                    input_list.append({"type": "function_call_output", "call_id": call_id, "output": f"Error: {err}"})
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

                input_list.append({"type": "function_call_output", "call_id": call_id, "output": tool_output})
                tool_call_details.append(
                    {
                        "iteration": iterations,
                        "tool": fn_name,
                        "args": fn_args,
                        "result_preview": tool_output[:500],
                        "is_error": is_error,
                    }
                )

        downloaded_files: List[Dict[str, Any]] = []
        if hosted_code_execution:
            # Generated files are discovered from container_file_citation annotations on
            # output_text content items (confirmed against OpenAI's own code_interpreter
            # guide) -- scan every "message" item accumulated across the whole run, not just
            # the last turn's, since a file generated mid-conversation is still cited there.
            _seen_refs: set = set()
            _refs: List[tuple] = []
            for item in input_list:
                if item.get("type") != "message":
                    continue
                for _c in item.get("content") or []:
                    for _ann in (_c.get("annotations") or []):
                        if _ann.get("type") == "container_file_citation":
                            _ref = (_ann.get("container_id"), _ann.get("file_id"), _ann.get("filename") or _ann.get("file_id"))
                            if _ref not in _seen_refs:
                                _seen_refs.add(_ref)
                                _refs.append(_ref)
            for _container_id, _file_id, _filename in _refs:
                if not _container_id or not _file_id:
                    continue
                try:
                    # Exact async download shape wasn't pinned down to one certain form in
                    # OpenAI's docs at the time this was written -- try the plausible ones
                    # rather than assume a single shape (see hosted_code_execution's own
                    # field description).
                    _content_resp = client.containers.files.content.retrieve(file_id=_file_id, container_id=_container_id)
                    if hasattr(_content_resp, "__await__"):
                        _content_resp = await _content_resp
                    if hasattr(_content_resp, "aread"):
                        _data = await _content_resp.aread()
                    elif hasattr(_content_resp, "read"):
                        _r = _content_resp.read()
                        _data = await _r if hasattr(_r, "__await__") else _r
                    else:
                        _data = bytes(_content_resp)
                except Exception as e:  # noqa: BLE001 -- one bad download must never fail the whole run
                    log.warning(f"hosted_code_execution: could not download generated file {_file_id!r}: {e}")
                    continue
                _entry: Dict[str, Any] = {"file_id": _file_id, "container_id": _container_id, "name": _filename}
                if hosted_code_execution_files_dir:
                    try:
                        _run_dir = _report_run_dir(hosted_code_execution_files_dir, run_id)
                        os.makedirs(_run_dir, exist_ok=True)
                        _path = os.path.join(_run_dir, f"{_file_id}_{_filename}")
                        with open(_path, "wb") as f:
                            f.write(_data)
                        _entry["path"] = _path
                        append_to_report_manifest(hosted_code_execution_files_dir, run_id, {
                            "file_id": _file_id, "name": _filename, "media_type": "application/octet-stream",
                            "path": _path, "size_bytes": len(_data), "tool": "hosted_code_execution",
                            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        })
                    except Exception as e:  # noqa: BLE001 -- a manifest write failure must never lose the file itself
                        log.warning(f"hosted_code_execution: could not write {_file_id!r} to hosted_code_execution_files_dir: {e}")
                        _entry["bytes"] = _data
                else:
                    _entry["bytes"] = _data
                downloaded_files.append(_entry)

        # Reported here as data; _run_agent never raises for this on its own. The caller (the
        # asset body in build_defs, well outside this `async with` block) decides whether
        # on_required_tool_calls_missing="fail" raises -- raising from inside this function's own
        # `async with` block gets wrapped in an anyio/mcp BaseExceptionGroup by the stdio client's
        # own TaskGroup teardown, which a plain `except RuntimeError` / `pytest.raises` upstream
        # wouldn't match.
        _result = {
            "final_answer": last_assistant_content or "",
            "iterations": iterations,
            "tool_calls_made": len(tool_call_details),
            "tool_call_details": tool_call_details,
            "transcript": input_list,
            "stopped_reason": stopped_reason,
            "model": model,
            "mcp_servers": servers_used,
            "combined_server_instructions": combined_server_instructions or None,
            "loaded_skill_names": sorted(loaded_skill_names),
            "skill_load_count": skill_load_count,
            "files": downloaded_files,
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
