"""LinearTicketToolsComponent -- team-scoped Linear ticket tools for engineering agents, with a
duplicate-check before filing and an optional practice mode.

Same dual-mode shape as `github_pr_tools`/`dagster_instance_tools`/`dagster_workflow_tools`:
  1. An importable toolset (`LinearTicketTools`, plus the component that registers it as a Dagster
     resource) under `resource_key`.
  2. A self-contained stdio MCP server: `python -m <dotted.path.to.this.module>`.

Tools:
  find_issues(query)            -- open tickets the agents already filed (identified by a footer
                                    marker in the description, not a label -- see below) whose
                                    title matches query's main words. Call before create_issue.
  create_issue(title, body)     -- files a ticket in the one configured team.
  list_agent_issues()           -- open agent-filed tickets with their bodies, for announcing
                                    elsewhere (e.g. Slack).

Auth: a personal API key or OAuth2 access token, sent RAW in the `Authorization` header -- Linear
does NOT want a `Bearer ` prefix (same convention already established by `linear_ingestion` and
`resources/linear_resource` in this repo). Linear has no REST surface at all; GraphQL is the only
way in.

Scoped to ONE team (`team_id`) -- every query/mutation is pinned to it, so a prompt cannot point
the tools at another team. Labeling is best-effort, not load-bearing: a personal API key may lack
permission to create labels (confirmed real: Linear's own docs note label creation can require
elevated access), so `find_issues`/duplicate-detection key off a description FOOTER MARKER instead
of a label -- it works even when the key can't create or read labels at all. When label creation
does succeed, the ticket is labeled too (nice-to-have, not relied upon).
"""

import os
import sys
from typing import Any, Callable, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


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


class LinearScopeError(Exception):
    pass


_LINEAR_LABEL = "engineering-agent"
_LINEAR_MARKER = "Filed by an engineering agent."
_LINEAR_GENERIC_WORDS = frozenset({"dagster", "assets", "asset", "project", "improve", "improvements", "reliability"})


def _linear_gql(query: str, variables: Optional[Dict[str, Any]], *, api_key: str, api_base_url: str) -> Dict[str, Any]:
    import requests
    resp = requests.post(
        api_base_url, json={"query": query, "variables": variables or {}},
        headers={"Authorization": api_key, "Content-Type": "application/json"}, timeout=30,
    )
    data = resp.json()
    if resp.status_code >= 400 or data.get("errors"):
        raise LinearScopeError(f"Linear request failed: {str(data.get('errors') or data)[:300]}")
    return data["data"]


def _linear_label_id(team_id: str, *, api_key: str, api_base_url: str) -> Optional[str]:
    """The team's engineering-agent label id, created if the key is allowed to; None when it isn't
    (duplicate-check/filing both still work without it -- see module docstring)."""
    labels = _linear_gql(
        "query($t:String!){ team(id:$t){ labels(first:100){ nodes{ id name } } } }",
        {"t": team_id}, api_key=api_key, api_base_url=api_base_url,
    )["team"]["labels"]["nodes"]
    for lab in labels:
        if lab["name"] == _LINEAR_LABEL:
            return lab["id"]
    try:
        made = _linear_gql(
            "mutation($i:IssueLabelCreateInput!){ issueLabelCreate(input:$i){ issueLabel{ id } } }",
            {"i": {"name": _LINEAR_LABEL, "teamId": team_id, "color": "#1d76db"}},
            api_key=api_key, api_base_url=api_base_url,
        )
    except LinearScopeError:
        return None
    return made["issueLabelCreate"]["issueLabel"]["id"]


def _linear_find_issues(team_id: str, query: str, *, api_key: str, api_base_url: str) -> List[Dict[str, Any]]:
    """Open tickets the agents already filed (identified by the description footer marker, not a
    label) in `team_id` whose title matches `query`'s main words -- a duplicate-check before
    filing a new one. Generic project words are dropped first."""
    import re
    words = [w for w in dict.fromkeys(re.findall(r"[A-Za-z0-9]{5,}", query.lower())) if w not in _LINEAR_GENERIC_WORDS][:6]
    if not words:
        return []
    data = _linear_gql(
        """query($t:ID!,$f:[IssueFilter!]!){ issues(first:20, filter:{ team:{id:{eq:$t}},
        description:{contains:"%s"}, state:{type:{nin:["completed","canceled"]}},
        or:$f }){ nodes{ identifier title url } } }""" % _LINEAR_MARKER,
        {"t": team_id, "f": [{"title": {"containsIgnoreCase": w}} for w in words]},
        api_key=api_key, api_base_url=api_base_url,
    )
    return data["issues"]["nodes"]


def _linear_create_issue(
    team_id: str, title: str, body: str, *, api_key: str, api_base_url: str, practice_mode: bool = False,
) -> Dict[str, Any]:
    if practice_mode:
        return {"practice_mode": True, "would_open": "linear_ticket", "title": title, "url": None}
    label_id = _linear_label_id(team_id, api_key=api_key, api_base_url=api_base_url)
    issue_input: Dict[str, Any] = {"teamId": team_id, "title": title, "description": f"{body}\n\n---\n{_LINEAR_MARKER}"}
    if label_id:
        issue_input["labelIds"] = [label_id]
    data = _linear_gql(
        "mutation($i:IssueCreateInput!){ issueCreate(input:$i){ success issue{ identifier title url } } }",
        {"i": issue_input}, api_key=api_key, api_base_url=api_base_url,
    )
    return data["issueCreate"]["issue"]


def _linear_list_agent_issues(team_id: str, *, api_key: str, api_base_url: str) -> List[Dict[str, Any]]:
    data = _linear_gql(
        """query($t:ID!){ issues(first:50, filter:{ team:{id:{eq:$t}}, description:{contains:"%s"},
        state:{type:{nin:["completed","canceled"]}} }){ nodes{ identifier title url description } } }""" % _LINEAR_MARKER,
        {"t": team_id}, api_key=api_key, api_base_url=api_base_url,
    )
    return [
        {"number": i["identifier"], "title": i["title"], "url": i["url"], "body": i.get("description") or ""}
        for i in data["issues"]["nodes"]
    ]


def _linear_is_practice_mode(env_var_name: Optional[str]) -> bool:
    """Same vocabulary/rationale as github_pr_tools' own _gh_is_practice_mode -- read fresh, by
    name, at call time (never templated into a typed component field at YAML-resolve time)."""
    if not env_var_name:
        return False
    return str(os.environ.get(env_var_name, "")).strip().lower() not in ("", "0", "false", "no", "off")


_AUDIT_LOGGER_NAME = "linear_ticket_tools.audit"
ALL_TOOLS = ("find_issues", "create_issue", "list_agent_issues")

# Same outcome-ledger shape as github_pr_tools' own _LEDGER_KIND_BY_TOOL/_classify_ledger_bucket --
# only create_issue is consequential (find_issues/list_agent_issues are read-only).
_LEDGER_KIND_BY_TOOL = {"create_issue": "linear_ticket"}


def _classify_ledger_bucket(tool: str, result: Dict[str, Any]) -> Optional[str]:
    if tool not in _LEDGER_KIND_BY_TOOL:
        return None
    if result.get("practice_mode"):
        return "would_file"
    if "error" in result:
        return "refused"
    return "filed"


class LinearTicketTools:
    """`call(tool, args)` is the one safe entry point (allow-list, audit, size cap, error shaping);
    the `t_*` methods are the raw implementations, built on the Linear-scope functions above."""

    def __init__(
        self, *, team_id: str, api_key: str, api_base_url: str = "https://api.linear.app/graphql",
        practice_mode_env_var: Optional[str] = None, allow: Optional[List[str]] = None,
        max_result_chars: int = 20000, ledger_path: Optional[str] = None,
        audit: Optional[Callable[[str], None]] = None,
    ):
        import logging
        self.team_id = team_id
        self.api_key = api_key
        self.api_base_url = api_base_url
        self.practice_mode_env_var = practice_mode_env_var
        self.ledger_path = ledger_path
        if allow is None:
            allow = list(ALL_TOOLS)
        unknown = sorted(set(allow) - set(ALL_TOOLS))
        if unknown:
            raise ValueError(f"unknown tool(s) in allow: {unknown}; valid: {list(ALL_TOOLS)}")
        self.allow = frozenset(allow)
        self.max_result_chars = max(2000, int(max_result_chars))
        self._audit = audit or (lambda line: logging.getLogger(_AUDIT_LOGGER_NAME).info(line))

    def tool_names(self) -> List[str]:
        return [t for t in ALL_TOOLS if t in self.allow]

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None, *, run_id: Optional[str] = None) -> Dict[str, Any]:
        import json
        args = dict(args or {})
        try:
            self._audit(f"linear_ticket_tools call tool={tool} team_id={self.team_id} args={json.dumps(args, default=str)[:300]}")
        except Exception:
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except LinearScopeError as e:
            result = {"error": str(e)}
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {e}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"}
        bucket = _classify_ledger_bucket(tool, result)
        if bucket:
            write_outcome_ledger_entry(self.ledger_path, run_id, {
                "component": "linear_ticket_tools", "tool": tool, "kind": _LEDGER_KIND_BY_TOOL[tool], "bucket": bucket,
                **({"reason": result.get("error")} if bucket == "refused" else
                   {k: v for k, v in result.items() if k != "practice_mode"}),
            })
        return self._fit(result)

    def _fit(self, result):
        import json
        cap = self.max_result_chars
        text = json.dumps(result, default=str)
        if len(text) <= cap:
            return result
        return {"truncated": True, "preview": text[: max(0, cap - 40)]}

    # -- tools ---------------------------------------------------------------------------------------------------
    def t_find_issues(self, query: str) -> Dict[str, Any]:
        return {"issues": _linear_find_issues(self.team_id, query, api_key=self.api_key, api_base_url=self.api_base_url)}

    def t_create_issue(self, title: str, body: str) -> Dict[str, Any]:
        return _linear_create_issue(
            self.team_id, title, body, api_key=self.api_key, api_base_url=self.api_base_url,
            practice_mode=_linear_is_practice_mode(self.practice_mode_env_var),
        )

    def t_list_agent_issues(self) -> Dict[str, Any]:
        return {"issues": _linear_list_agent_issues(self.team_id, api_key=self.api_key, api_base_url=self.api_base_url)}


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>` -- same mechanism as
    github_pr_tools'/dagster_instance_tools' own copies (duplicated here, not imported, per this
    repo's self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "linear_ticket_tools's mcp_server_resource_key path must be run as `python -m <dotted.path.to.this.module>`, "
        "not `python component.py` directly -- that bare-script form still works for `python component.py` itself, "
        "just not for this path. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined here rather than imported from
    resources/mcp_server_resource (confirmed not importable across components). Same copy
    github_pr_tools/dagster_instance_tools/dagster_workflow_tools each carry."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None
    live_env_passthrough: Optional[List[str]] = None

    def model_dump(self, *args, **kwargs):
        dumped = super().model_dump(*args, **kwargs)
        passthrough = dumped.pop("live_env_passthrough", None)
        if dumped.get("type") == "stdio" and passthrough:
            env = dict(dumped.get("env") or {})
            for var_name in passthrough:
                value = os.environ.get(var_name)
                if value:
                    env[var_name] = value
            dumped["env"] = env
        return dumped


class LinearTicketToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers team-scoped Linear ticket tools as a resource (default key `linear_ticket_tools`).
    For an agent (MCP only) or Claude Code (`claude mcp add`), run THIS FILE as the stdio MCP
    server -- see README.md.

    Example:

    ```yaml
    type: dagster_community_components.LinearTicketToolsComponent
    attributes:
      team_id: "{{ env.LINEAR_TEAM_ID }}"
      api_key_env_var: LINEAR_API_KEY
      practice_mode_env_var: ENG_PRACTICE_MODE
    ```
    """

    resource_key: str = Field(default="linear_ticket_tools", description="Resource key the toolset is registered under.")
    team_id: str = Field(description="Linear team ID every query/mutation is scoped to -- a prompt cannot point the tools at another team.")
    api_key_env_var: str = Field(default="LINEAR_API_KEY", description="Env var holding a Linear personal API key or OAuth2 access token (sent RAW, no 'Bearer ' prefix -- Linear's own convention).")
    api_base_url: str = Field(default="https://api.linear.app/graphql", description="Linear GraphQL endpoint.")
    practice_mode_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var NAME (not a value templated into this field) whose value, read fresh at call "
            "time, switches create_issue into practice mode: returns "
            "{'practice_mode': True, 'would_open': 'linear_ticket', ...} instead of actually "
            "filing a ticket. Same vocabulary/rationale as github_pr_tools' own "
            "practice_mode_env_var -- scope it to a Dagster+ branch deployment to practice there "
            "and act for real after merge."
        ),
    )

    ledger_path: Optional[str] = Field(
        default=None,
        description=(
            "Directory this tool writes one JSON-lines file per Dagster run_id into, recording every "
            "create_issue call as it happens -- filed, would_file (practice mode), or refused. Same "
            "shape/rationale as github_pr_tools' own ledger_path (the run_id comes from MCP's "
            "out-of-band request _meta, not a tool argument). Point an agent component's own "
            "outcome_ledger_path at this SAME directory to read it back. Unset: no ledger is written."
        ),
    )

    allow: Optional[List[str]] = Field(default=None, description=f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}.")
    max_result_chars: int = Field(default=20000, description="Upper bound on the serialized size of any tool result.")
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "If set, ALSO registers a stdio MCP-server-shaped resource under this key (python -m this "
            "module, with LINEAR_TICKET_TOOLS_* env vars set from this component's own settings, and "
            "the real API key passed through via live env passthrough). Any mcp_servers-aware "
            "component can then reference it via server_resource_key."
        ),
    )
    mcp_extra_env_vars: Optional[List[str]] = Field(
        default=None,
        description="mcp_server_resource_key only. Extra env var NAMES (beyond the API key, which is always passed through) to copy into the spawned MCP server's env.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow", "mcp_extra_env_vars"), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Linear Ticket Tools", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        team_id = self.team_id
        api_key_env_var = self.api_key_env_var
        api_base_url = self.api_base_url
        practice_mode_env_var = self.practice_mode_env_var
        ledger_path = self.ledger_path
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(ALL_TOOLS)}")
        max_chars = int(self.max_result_chars)

        def _init(init_context):
            return LinearTicketTools(
                team_id=team_id, api_key=os.environ.get(api_key_env_var, ""), api_base_url=api_base_url,
                practice_mode_env_var=practice_mode_env_var, allow=allow, max_result_chars=max_chars,
                ledger_path=ledger_path,
            )

        res = dg.ResourceDefinition(resource_fn=_init, description=f"Team-scoped Linear ticket tools for team {team_id}.")
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {
                "LINEAR_TICKET_TOOLS_TEAM_ID": team_id, "LINEAR_TICKET_TOOLS_API_KEY_ENV_VAR": api_key_env_var,
                "LINEAR_TICKET_TOOLS_API_BASE_URL": api_base_url, "LINEAR_TICKET_TOOLS_MAX_CHARS": str(max_chars),
            }
            if allow:
                mcp_env["LINEAR_TICKET_TOOLS_ALLOW"] = ",".join(allow)
            if practice_mode_env_var:
                mcp_env["LINEAR_TICKET_TOOLS_PRACTICE_MODE_ENV_VAR"] = practice_mode_env_var
            if ledger_path:
                mcp_env["LINEAR_TICKET_TOOLS_LEDGER_PATH"] = ledger_path
            passthrough = (
                [api_key_env_var]
                + ([practice_mode_env_var] if practice_mode_env_var else [])
                + list(self.mcp_extra_env_vars or [])
            )
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key, type="stdio", command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env, live_env_passthrough=passthrough,
            )

        return dg.Definitions(resources=resources)


# ── stdio MCP server entrypoint:  python -m <this module>  (or  python component.py) ────────────────────────────────
def build_mcp_server(tools: LinearTicketTools):
    """MCP server (official `mcp` package) exposing exactly the allowed tools. Works on mcp 1.x and 2.x."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server, Context as _Context
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server, Context as _Context

    mcp = _Server("linear-ticket-tools")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__ in allowed:
            mcp.tool()(fn)
        return fn

    def _run_id_from(ctx) -> Optional[str]:
        """Same mechanism as github_pr_tools' own _run_id_from -- ctx is MCP's injected Context, not
        a tool argument; `dagster_run_id` is what an agent component's run_id threading puts in its
        out-of-band request _meta."""
        meta = getattr(getattr(ctx, "request_context", None), "meta", None)
        return (meta or {}).get("dagster_run_id") if ctx is not None else None

    @_tool
    def find_issues(query: str) -> dict:
        """Open Linear tickets the agents already filed whose title matches query's main words. Call this BEFORE create_issue to avoid filing the same problem twice."""
        return tools.call("find_issues", {"query": query})

    @_tool
    def create_issue(title: str, body: str, ctx: _Context = None) -> dict:
        """File a Linear ticket in the configured team. Check find_issues first."""
        return tools.call("create_issue", {"title": title, "body": body}, run_id=_run_id_from(ctx))

    @_tool
    def list_agent_issues() -> dict:
        """Open tickets the agents filed, with their bodies -- for announcing elsewhere (e.g. Slack)."""
        return tools.call("list_agent_issues", {})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: LINEAR_TICKET_TOOLS_TEAM_ID (required), _API_KEY_ENV_VAR (default LINEAR_API_KEY),
    _API_BASE_URL, _ALLOW (csv), _MAX_CHARS, _PRACTICE_MODE_ENV_VAR (the NAME of another env var
    to read fresh at call time -- not a value), _LEDGER_PATH (directory for the per-run_id
    outcome-ledger JSON-lines files)."""
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    team_id = os.environ.get("LINEAR_TICKET_TOOLS_TEAM_ID")
    if not team_id:
        raise RuntimeError("LINEAR_TICKET_TOOLS_TEAM_ID must be set.")
    api_key_env_var = os.environ.get("LINEAR_TICKET_TOOLS_API_KEY_ENV_VAR", "LINEAR_API_KEY")
    allow_env = os.environ.get("LINEAR_TICKET_TOOLS_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None
    tools = LinearTicketTools(
        team_id=team_id, api_key=os.environ.get(api_key_env_var, ""),
        api_base_url=os.environ.get("LINEAR_TICKET_TOOLS_API_BASE_URL", "https://api.linear.app/graphql"),
        practice_mode_env_var=os.environ.get("LINEAR_TICKET_TOOLS_PRACTICE_MODE_ENV_VAR"),
        allow=allow, max_result_chars=int(os.environ.get("LINEAR_TICKET_TOOLS_MAX_CHARS", "20000")),
        ledger_path=os.environ.get("LINEAR_TICKET_TOOLS_LEDGER_PATH"),
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
