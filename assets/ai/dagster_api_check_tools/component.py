"""DagsterApiCheckToolsComponent -- confirms Dagster names exist in the INSTALLED Dagster version (with their real
signatures), and scans Python source for removed, deprecated or superseded Dagster API usage, so an agent proposing
a code change checks against reality instead of guessing a name that doesn't exist (e.g. `AssetAutoMaterializePolicy`,
removed long ago) or suggesting an already-deprecated replacement.

Same dual-mode shape as `agentic_pipeline_authoring_tools`/`dagster_instance_tools`/`dagster_workflow_tools`:
  1. An importable toolset (`DagsterApiCheckTools`, plus the component that registers it as a Dagster resource)
     under `resource_key`.
  2. A self-contained stdio MCP server: `python -m <dotted.path.to.this.module>`.
`mcp_server_resource_key`, if set, ALSO registers a stdio MCP-server-shaped resource so any mcp_servers-aware
component can reference this server via `server_resource_key`.

Deliberately NOT coupled to any repo-hosting service: `scan_source`/`scan_sources` take source text directly (the
caller supplies it -- from `github_pr_tools.get_pull_request`'s file patches, a local read, any MCP filesystem
server, ...), rather than this component fetching files itself. One tool, reusable regardless of where the code
lives.
"""
import os
import sys
from typing import Any, Callable, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


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


def _check_dagster_api(names: List[str]) -> Dict[str, Any]:
    """Whether Dagster names exist in the installed version, with their signatures."""
    import importlib
    import inspect

    import dagster as _dagster

    result: Dict[str, Any] = {"dagster_version": _dagster.__version__, "names": {}}
    for name in names[:12]:
        parts = name.replace("dg.", "dagster.", 1).split(".")
        try:
            obj = importlib.import_module(parts[0])
            for part in parts[1:]:
                obj = getattr(obj, part)
        except (ImportError, AttributeError):
            result["names"][name] = {"exists": False}
            continue
        try:
            signature = str(inspect.signature(obj))
        except (TypeError, ValueError):
            signature = None
        result["names"][name] = {"exists": True, "signature": signature}
    return result


def _resolve_dagster(chain: List[str], aliases: Dict[str, str]):
    """Resolve a dotted name rooted at `dagster` or an alias of it, one hop at a time."""
    import importlib

    root = aliases.get(chain[0])
    if root is None:
        return None, "unrooted"
    obj = importlib.import_module(root.split(".")[0])
    for part in root.split(".")[1:] + chain[1:2]:
        if not hasattr(obj, part):
            return None, "missing"
        obj = getattr(obj, part)
    return obj, "ok"


def _scan_source(path: str, source: str) -> List[Dict[str, Any]]:
    """Removed, deprecated or superseded Dagster API usage in one file's source text."""
    import ast

    from dagster import _annotations as ann

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    aliases: Dict[str, str] = {}
    findings: List[Dict[str, Any]] = []

    def judge(obj, name: str, line: int, param: Optional[str] = None) -> None:
        info = ann.get_deprecated_info(obj) if ann.is_deprecated(obj) else None
        sup = ann.get_superseded_info(obj) if ann.is_superseded(obj) else None
        if param is not None:
            if param in (ann.get_deprecated_params(obj) if ann.has_deprecated_params(obj) else set()):
                pinfo = ann.get_deprecated_param_info(obj, param)
                findings.append({
                    "path": path, "line": line, "name": f"{name}({param}=...)", "status": "deprecated parameter",
                    "breaking_version": getattr(pinfo, "breaking_version", None),
                    "note": getattr(pinfo, "additional_warn_text", None),
                })
            return
        if info:
            findings.append({"path": path, "line": line, "name": name, "status": "deprecated", "breaking_version": info.breaking_version, "note": info.additional_warn_text})
        elif sup:
            findings.append({"path": path, "line": line, "name": name, "status": "superseded", "breaking_version": None, "note": getattr(sup, "additional_warn_text", None)})

    def chain_of(node) -> Optional[List[str]]:
        parts: List[str] = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            return [node.id, *reversed(parts)]
        return None

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name.split(".")[0] == "dagster":
                    aliases[a.asname or a.name.split(".")[0]] = "dagster"
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] == "dagster":
            for a in node.names:
                aliases[a.asname or a.name] = f"{node.module}.{a.name}"

    seen: "set[tuple[int, str]]" = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] == "dagster":
            import importlib

            try:
                module = importlib.import_module(node.module)
            except ImportError:
                continue
            for a in node.names:
                if not hasattr(module, a.name):
                    findings.append({"path": path, "line": node.lineno, "name": f"{node.module}.{a.name}", "status": "removed", "breaking_version": None, "note": "no longer exists"})
                else:
                    judge(getattr(module, a.name), f"{node.module}.{a.name}", node.lineno)
        chain = chain_of(node) if isinstance(node, ast.Attribute) else None
        if chain and chain[0] in aliases and len(chain) >= 2 and aliases[chain[0]] == "dagster":
            obj, state = _resolve_dagster(chain, aliases)
            key = (node.lineno, ".".join(chain[:2]))
            if key in seen or isinstance(node.value, ast.Attribute):
                continue
            seen.add(key)
            if state == "missing":
                findings.append({"path": path, "line": node.lineno, "name": f"dagster.{chain[1]}", "status": "removed", "breaking_version": None, "note": "no longer exists"})
            elif obj is not None:
                judge(obj, f"dagster.{chain[1]}", node.lineno)
        if isinstance(node, ast.Call):
            fchain = chain_of(node.func) if isinstance(node.func, ast.Attribute) else ([node.func.id] if isinstance(node.func, ast.Name) else None)
            if fchain:
                obj, state = _resolve_dagster(fchain, aliases) if len(fchain) > 1 else (None, "unrooted")
                if len(fchain) == 1 and fchain[0] in aliases and aliases[fchain[0]] != "dagster":
                    import importlib

                    mod_name, _, attr = aliases[fchain[0]].rpartition(".")
                    try:
                        obj, state = getattr(importlib.import_module(mod_name), attr, None), "ok"
                    except ImportError:
                        obj, state = None, "unrooted"
                if obj is not None and state == "ok":
                    for kw in node.keywords:
                        if kw.arg:
                            judge(obj, ".".join(fchain), node.lineno, param=kw.arg)
    return findings


_AUDIT_LOGGER_NAME = "dagster_api_check_tools.audit"
ALL_TOOLS = ("check_dagster_api", "scan_source", "scan_sources")


class DagsterApiCheckTools:
    """`call(tool, args)` is the one safe entry point (allow-list, audit, size cap, error shaping); the `t_*`
    methods are the raw implementations."""

    def __init__(self, *, allow: Optional[List[str]] = None, max_result_chars: int = 20000, audit: Optional[Callable[[str], None]] = None):
        import logging
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

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        import json
        args = dict(args or {})
        try:
            self._audit(f"dagster_api_check_tools call tool={tool} args={json.dumps({k: (v[:200] if isinstance(v, str) else v) for k, v in args.items()}, default=str)[:300]}")
        except Exception:  # an audit sink must never break a call
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {e}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"}
        return self._fit(result)

    def _fit(self, result):
        import json
        cap = self.max_result_chars
        text = json.dumps(result, default=str)
        if len(text) <= cap:
            return result
        return {"truncated": True, "preview": text[: max(0, cap - 40)]}

    # -- tools ---------------------------------------------------------------------------------------------------
    def t_check_dagster_api(self, names: List[str]) -> Dict[str, Any]:
        return _check_dagster_api(names)

    def t_scan_source(self, path: str, source: str) -> Dict[str, Any]:
        return {"checked_against_dagster": dg.__version__, "findings": _scan_source(path, source)}

    def t_scan_sources(self, files: List[Dict[str, str]]) -> Dict[str, Any]:
        findings: List[Dict[str, Any]] = []
        for f in files[:40]:
            findings += _scan_source(f["path"], f["source"])
        unique = {(f["path"], f["line"], f["name"]): f for f in findings}
        return {"checked_against_dagster": dg.__version__, "files_scanned": len(files[:40]), "findings": sorted(unique.values(), key=lambda f: (f["path"], f["line"]))}


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>` -- same mechanism as
    agentic_pipeline_authoring_tools'/dagster_instance_tools' own copies (duplicated here, not imported, per this
    repo's self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "dagster_api_check_tools's mcp_server_resource_key path must be run as `python -m <dotted.path.to.this.module>`, "
        "not `python component.py` directly -- that bare-script form still works for `python component.py` itself, "
        "just not for this path. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource (name/type/command/url/env/headers/headers_env) -- inlined here
    rather than imported from resources/mcp_server_resource (confirmed not importable across components). Same
    copy agentic_pipeline_authoring_tools/dagster_instance_tools/dagster_workflow_tools each carry."""

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


class DagsterApiCheckToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers Dagster-API-checking tools as a resource (default key `dagster_api_check_tools`). For an agent
    (MCP only) or Claude Code (`claude mcp add`), run THIS FILE as the stdio MCP server -- see README.md."""

    resource_key: str = Field(default="dagster_api_check_tools", description="Resource key the toolset is registered under.")
    allow: Optional[List[str]] = Field(default=None, description=f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}.")
    max_result_chars: int = Field(default=20000, description="Upper bound on the serialized size of any tool result (a large source scan can otherwise be sizable).")
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description="If set, ALSO registers a stdio MCP-server-shaped resource under this key (python -m this module, with DAGSTER_API_CHECK_TOOLS_* env vars set from allow/max_result_chars). Any mcp_servers-aware component can then reference it via server_resource_key.",
    )
    mcp_extra_env_vars: Optional[List[str]] = Field(default=None, description="mcp_server_resource_key only. Extra env var NAMES to copy into the spawned MCP server's env.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow", "mcp_extra_env_vars"), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Dagster Api Check Tools", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(ALL_TOOLS)}")
        max_chars = int(self.max_result_chars)

        def _init(init_context):
            return DagsterApiCheckTools(allow=allow, max_result_chars=max_chars)

        res = dg.ResourceDefinition(resource_fn=_init, description="Confirms Dagster names exist in the installed version, and scans source for removed/deprecated/superseded Dagster API usage.")
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {"DAGSTER_API_CHECK_TOOLS_MAX_CHARS": str(max_chars)}
            if allow:
                mcp_env["DAGSTER_API_CHECK_TOOLS_ALLOW"] = ",".join(allow)
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key, type="stdio", command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env, live_env_passthrough=list(self.mcp_extra_env_vars or []),
            )

        return dg.Definitions(resources=resources)


# ── stdio MCP server entrypoint:  python -m <this module>  (or  python component.py) ────────────────────────────────
def build_mcp_server(tools: DagsterApiCheckTools):
    """MCP server (official `mcp` package) exposing exactly the allowed tools. Works on mcp 1.x and 2.x."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    try:  # the SDK's own ToolError, not this module's -- see _mcp_call below for why that distinction matters.
        from mcp.server.mcpserver.exceptions import ToolError as _SdkToolError  # mcp 2.x (confirmed live, 2.3.0)
    except ImportError:
        from mcp.server.fastmcp.exceptions import ToolError as _SdkToolError  # mcp 1.x (stable FastMCP API, not live-verified here)

    mcp = _Server("dagster-api-check-tools")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__ in allowed:
            mcp.tool()(fn)
        return fn

    def _mcp_call(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """tools.call() never raises (see DagsterApiCheckTools.call) -- a bad-arguments or failure comes back
        as a plain {"error": ...} dict, which the MCP transport would otherwise hand back as a NORMAL
        (isError=False) tool result indistinguishable from success. Re-raising here lets the `mcp` package's
        own exception handling (both fastmcp 1.x and mcp.server.mcpserver 2.x catch any exception from a
        registered tool function and turn it into a real isError=True CallToolResult) make the failure visible
        to whatever's counting failed calls in the agent loop. MUST be the SDK's own ToolError (_SdkToolError),
        not this module's: the SDK's tool dispatch (mcp/server/mcpserver/tools/base.py `Tool.run`) only keeps a
        raised exception's own message when it recognizes the exception as ITS OWN ToolError/ResourceError --
        anything else (including a same-named ToolError class defined elsewhere) is treated as an unexpected
        crash and replaced with a generic 'Error executing tool <name>' before it reaches the client, throwing
        away the reason the agent needs to act on."""
        result = tools.call(name, args)
        if isinstance(result, dict) and "error" in result:
            raise _SdkToolError(result["error"])
        return result

    @_tool
    def check_dagster_api(names: list) -> dict:
        """Check that Dagster names exist in the installed version and get their signatures, e.g.
        ["dg.AutomationCondition.eager", "dg.FreshnessPolicy.time_window"]. Call it for every Dagster name you put
        in a code snippet; if a name does not exist, use one that does."""
        return _mcp_call("check_dagster_api", {"names": names})

    @_tool
    def scan_source(path: str, source: str) -> dict:
        """Removed, deprecated or superseded Dagster APIs used in one file's source text, with the line, the
        version where it breaks, and the replacement hint."""
        return _mcp_call("scan_source", {"path": path, "source": source})

    @_tool
    def scan_sources(files: list) -> dict:
        """Same as scan_source, batched: files is a list of {path, source}. Call it once per PR/change set."""
        return _mcp_call("scan_sources", {"files": files})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: DAGSTER_API_CHECK_TOOLS_ALLOW (csv), _MAX_CHARS (default 20000)."""
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    allow_env = os.environ.get("DAGSTER_API_CHECK_TOOLS_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None
    max_chars = int(os.environ.get("DAGSTER_API_CHECK_TOOLS_MAX_CHARS", "20000"))
    tools = DagsterApiCheckTools(allow=allow, max_result_chars=max_chars)
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
