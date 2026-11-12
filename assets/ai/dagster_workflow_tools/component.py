"""DagsterWorkflowToolsComponent -- "workflow as tool": WRITE-capable tools over a Dagster
instance for agents. An agent can trigger "materialize these assets" / "run this job" as a real,
synchronous Dagster run -- not a read-only query.

Deliberately a SEPARATE component from `dagster_instance_tools` (read-only): that component's
whole safety story is "tools only call read APIs on the instance" -- bolting a write capability
onto it would silently change that guarantee for every existing user of it. This component is
named and documented so its write capability is obvious before anyone wires it into an agent.

How agents consume it (same shape as dagster_instance_tools, verified against agentic_pipeline in
this repo): `tool_use_loop` / `mcp_call` / `agent_call` obtain tools ONLY from MCP servers (stdio /
http / sse / fastmcp). So this file is BOTH
  1. an importable toolset (`DagsterWorkflowTools`, plus the `DagsterWorkflowToolsComponent` that
     registers it as a Dagster resource), and
  2. a self-contained stdio MCP server: `python -m <dotted.path.to.this.module>` (NOT a bare
     `python component.py` -- materialize_assets/run_job need `__spec__.name`, the real
     importable module path, to reconstruct the job in a step's own subprocess; a bare script
     invocation only ever gets `__name__ == "__main__"` either way and leaves `__spec__` None.
     Reads DAGSTER_HOME.)
See README.md for the exact agentic_pipeline YAML.

Tools: materialize_assets, run_job. Both execute a REAL, synchronous Dagster run via
`dagster.execute_job` (dagster's own documented out-of-process execution entrypoint -- it does
NOT need a live code-server/workspace connection the way `instance.submit_run` does, which is why
this works from a bare script the same way `dagster_instance_tools` already does for reads) and
block until the run finishes. `allow_assets`/`allow_jobs` are hard allow-lists (materializing/
running anything outside them is refused); every triggered run is tagged with
`dagster-community/workflow-tool=true` plus any caller-supplied tags, for audit; every call writes
one audit log line (tool + args, never results, same convention as dagster_instance_tools).

Verified (direct introspection of the installed dagster version this session): `dagster.execute_job`
needs a `ReconstructableJob`, built here via `dagster._core.definitions.reconstruct.
build_reconstructable_job(module, function_name, reconstructable_kwargs=...)` pointing at this
file's own `_reconstruct_job` function -- the string-based reconstruction strategy, since the job
to run is only known at call time (not at Python-decoration time, so plain `dagster.reconstructable`
doesn't apply). An ad-hoc asset-selection job MUST be built via `dagster.define_asset_job(name=...,
selection=...).resolve(asset_graph=...)`, not `Definitions.resolve_implicit_job_def_def_for_assets`
-- the latter returns a job literally named `__ASSET_JOB` (dagster's internal reserved name for the
implicit per-repo asset job), which a real run confirmed `DagsterInvalidDefinitionError`s when
wrapped standalone for reconstruction ("Attempted to provide job called __ASSET_JOB ... reserved
name"). Confirmed working end-to-end against a real two-asset Definitions object and a real
DAGSTER_HOME instance (multiprocess executor, real ASSET_MATERIALIZATION events, RUN_SUCCESS).

Not verified / deliberately out of scope for v1: `execute_job` blocks synchronously for the whole
run's duration (steps run in subprocesses via the multiprocess executor, but the calling MCP tool
call itself waits) -- there's no built-in timeout, so a long-running job will hold the tool call
open for just as long. An async "submit and poll" mode would need `instance.submit_run`, which
needs a live `BaseWorkspaceRequestContext` this script does not have (confirmed via `instance.
submit_run`'s own signature) -- that's a real architectural gap, not something this file papers
over; see the README's "Known limitation" section.
"""

import json
import logging
import os
import re
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


_AUDIT = logging.getLogger("dagster_workflow_tools.audit")

# Same redaction convention as dagster_instance_tools (duplicated, not imported -- no shared files
# across components in this repo): tokens/passwords/Authorization headers/well-known key shapes.
_SECRET_KEY_RE = re.compile(r"(?i)^(.*_)?(password|secret|token|api_key|apikey|auth|credential)s?$")
_REDACTED = "[REDACTED]"
_BLOB_RES = [
    re.compile(r"(?i)\b(?:authorization|bearer)\s*[:=]\s*\S+"),
    re.compile(r"(?i)\b(?:password|passwd|secret|token|api[_-]?key)\s*[:=]\s*\S+"),
    re.compile(r"\b[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\b"),  # jwt-shaped
    re.compile(r"\b[A-Fa-f0-9]{32,}\b"),  # long hex blob
]


def _blob_sub(m: "re.Match[str]") -> str:
    return _REDACTED


def redact_text(text: Any) -> str:
    s = str(text)
    for pat in _BLOB_RES:
        s = pat.sub(_blob_sub, s)
    return s


def redact_obj(obj: Any, _key: Optional[str] = None) -> Any:
    if isinstance(obj, dict):
        return {k: redact_obj(v, str(k)) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [redact_obj(v, _key) for v in obj]
    if isinstance(obj, str):
        return _REDACTED if _key and _SECRET_KEY_RE.match(_key) else redact_text(obj)
    return obj


class ToolError(Exception):
    pass


def _first_line(s: Any, n: int = 300) -> str:
    line = str(s).splitlines()[0] if str(s).splitlines() else str(s)
    return redact_text(line)[:n]


ALL_TOOLS = ("materialize_assets", "run_job")


def load_definitions(spec: str) -> "dg.Definitions":
    """`pkg.module` or `pkg.module:attr` (default attr `defs`; a callable is called). Same
    convention as dagster_instance_tools' own `load_definitions` (duplicated, not imported)."""
    if not spec:
        raise ValueError("definitions_module is required for DagsterWorkflowTools (unlike the read-only dagster_instance_tools, a write tool must know which Definitions it's allowed to run).")
    import importlib
    mod_name, _, attr = spec.partition(":")
    obj = getattr(importlib.import_module(mod_name), attr or "defs")
    if callable(obj) and not isinstance(obj, dg.Definitions):
        obj = obj()
    if not isinstance(obj, dg.Definitions):
        raise ValueError(f"{spec!r} did not resolve to dagster.Definitions (got {type(obj).__name__})")
    return obj


def _reconstruct_job(definitions_module: str, job_name: Optional[str], asset_keys: Optional[List[str]]) -> "dg.JobDefinition":
    """The reconstruction target for `build_reconstructable_job` -- called fresh in the executing
    process, never directly by this component's own code. Exactly one of job_name/asset_keys is
    set (validated by the caller)."""
    defs = load_definitions(definitions_module)
    if job_name:
        return defs.resolve_job_def(job_name)
    keys = [dg.AssetKey.from_user_string(k) for k in (asset_keys or [])]
    # NOT Definitions.resolve_implicit_job_def_def_for_assets: that returns a job named
    # __ASSET_JOB, dagster's reserved name for the implicit per-repo asset job, which raises
    # DagsterInvalidDefinitionError ("... reserved name") when wrapped standalone like this.
    job_def = dg.define_asset_job(name="dagster_workflow_tools_materialize", selection=dg.AssetSelection.assets(*keys))
    return job_def.resolve(asset_graph=defs.resolve_asset_graph())


class DagsterWorkflowTools:
    """WRITE tools over a DagsterInstance: materialize_assets / run_job, each a real synchronous
    Dagster run. `call(tool, args)` is the one safe entry point (allow-list, audit, run-tag
    attribution, error shaping); the `t_*` methods are the raw implementations."""

    def __init__(
        self,
        instance: "dg.DagsterInstance",
        *,
        definitions_module: str,
        allow: Optional[List[str]] = None,
        allow_assets: Optional[List[str]] = None,
        allow_jobs: Optional[List[str]] = None,
        default_tags: Optional[Dict[str, str]] = None,
        audit: Optional[Callable[[str], None]] = None,
    ):
        self.instance = instance
        self.definitions_module = definitions_module
        if allow is None:
            allow = list(ALL_TOOLS)
        unknown = sorted(set(allow) - set(ALL_TOOLS))
        if unknown:
            raise ValueError(f"unknown tool(s) in allow: {unknown}; valid: {list(ALL_TOOLS)}")
        self.allow = frozenset(allow)
        self.allow_assets = frozenset(allow_assets) if allow_assets is not None else None
        self.allow_jobs = frozenset(allow_jobs) if allow_jobs is not None else None
        self.default_tags = dict(default_tags or {})
        self._audit = audit or (lambda line: _AUDIT.info(line))

    def tool_names(self) -> List[str]:
        return [t for t in ALL_TOOLS if t in self.allow]

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(args or {})
        try:
            self._audit(f"dagster_workflow_tools call tool={tool} args={redact_text(json.dumps(args, default=str))[:300]}")
        except Exception:  # an audit sink must never break a call
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except ToolError as e:
            return {"error": redact_text(str(e))}
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {redact_text(str(e))}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {_first_line(e)}"}
        return redact_obj(result)

    def _run_tags(self, extra: Optional[Dict[str, str]]) -> Dict[str, str]:
        tags = {"dagster-community/workflow-tool": "true", **self.default_tags}
        tags.update({str(k): str(v) for k, v in (extra or {}).items()})
        return tags

    def _execute(self, *, job_name: Optional[str], asset_keys: Optional[List[str]], run_config: Optional[dict], tags: Optional[dict], partition_key: Optional[str]) -> Dict[str, Any]:
        from dagster._core.definitions.reconstruct import build_reconstructable_job

        reconstructable_job = build_reconstructable_job(
            _this_module_name(),
            "_reconstruct_job",
            reconstructable_kwargs={
                "definitions_module": self.definitions_module,
                "job_name": job_name,
                "asset_keys": asset_keys,
            },
        )
        kwargs: Dict[str, Any] = {}
        if partition_key:
            kwargs["run_config"] = run_config
            kwargs["tags"] = {**self._run_tags(tags), "dagster/partition": partition_key}
        else:
            kwargs["run_config"] = run_config
            kwargs["tags"] = self._run_tags(tags)
        result = dg.execute_job(reconstructable_job, instance=self.instance, **kwargs)
        return {
            "success": bool(result.success),
            "run_id": result.run_id,
            "job_name": job_name or "dagster_workflow_tools_materialize",
            "asset_keys": asset_keys,
        }

    def t_materialize_assets(self, asset_keys: List[str], run_config: Optional[dict] = None, tags: Optional[dict] = None, partition_key: Optional[str] = None) -> Dict[str, Any]:
        """Trigger a real, synchronous materialization of the given asset keys. Blocks until the
        run finishes (or fails) and returns {success, run_id, asset_keys}."""
        if not asset_keys:
            raise ToolError("materialize_assets requires a non-empty asset_keys list")
        if self.allow_assets is not None:
            denied = [k for k in asset_keys if k not in self.allow_assets]
            if denied:
                raise ToolError(f"asset_key(s) not in allow_assets: {denied}")
        return self._execute(job_name=None, asset_keys=asset_keys, run_config=run_config, tags=tags, partition_key=partition_key)

    def t_run_job(self, job_name: str, run_config: Optional[dict] = None, tags: Optional[dict] = None, partition_key: Optional[str] = None) -> Dict[str, Any]:
        """Trigger a real, synchronous run of an existing named job from definitions_module.
        Blocks until the run finishes (or fails) and returns {success, run_id, job_name}."""
        if not job_name:
            raise ToolError("run_job requires job_name")
        if self.allow_jobs is not None and job_name not in self.allow_jobs:
            raise ToolError(f"job {job_name!r} is not in allow_jobs")
        return self._execute(job_name=job_name, asset_keys=None, run_config=run_config, tags=tags, partition_key=partition_key)


def _this_module_name() -> str:
    """The module name `build_reconstructable_job` needs to re-import this file in the executing
    process. `__name__` alone is unreliable here: it's literally `"__main__"` both for a bare
    `python component.py` invocation AND for `python -m dotted.path` (confirmed directly -- `-m`
    does NOT give the entry module a real `__name__`, contrary to what you might expect). `
    __spec__.name` is the fix: it holds the real dotted path under `-m` invocation, a normal
    import, AND this repo's own test convention (`importlib.util.spec_from_file_location` +
    `module_from_spec` also sets `__spec__.name` to the loader's synthetic name, confirmed
    directly) -- `__spec__` is only `None` for a truly bare `python component.py` run, which is
    the one case the stdio MCP server genuinely cannot support (see README.md: invoke it with
    `-m`, not as a bare script)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "dagster_workflow_tools's stdio MCP server must be run as "
        "`python -m <dotted.path.to.this.module>` (e.g. `python -m "
        "dagster_community_components.assets.ai.dagster_workflow_tools.component`), not "
        "`python component.py` directly -- materialize_assets/run_job need a real importable "
        "module name (available via __spec__.name) to reconstruct the job in a step's own "
        "subprocess. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource (name/type/command/url/env/headers/
    headers_env) -- inlined here rather than imported from resources/mcp_server_resource
    (confirmed not importable: `dagster_community_components.resources...` isn't a real
    submodule path -- the package's lazy __init__.py only exposes *Component classes by
    name, not plain resource/helper classes -- so subclassing it isn't actually available;
    this duck-typed copy is the working equivalent every mcp_servers-aware consumer
    already expects, since none of them import MCPServerResource either, they just call
    `.model_dump()`).

    `env`'s DAGSTER_HOME is deliberately NOT baked in at construction time (`build_defs`
    runs once, at code-location load) -- it's read fresh from the environment every time
    `model_dump()` is actually called instead, which happens at run time (when a
    consuming component resolves this resource to make the actual MCP connection). This
    matters because DAGSTER_HOME can differ between when a code location loads and when
    a run later executes."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None

    def model_dump(self, *args, **kwargs):
        dumped = super().model_dump(*args, **kwargs)
        if dumped.get("type") == "stdio":
            env = dict(dumped.get("env") or {})
            dagster_home = os.environ.get("DAGSTER_HOME")
            if dagster_home:
                env["DAGSTER_HOME"] = dagster_home
            dumped["env"] = env
        return dumped


# ── Dagster component: registers the toolset as a resource ──────────────────────────────────────────────────────────
class DagsterWorkflowToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers WRITE-capable Dagster-workflow tools as a resource (default key
    `dagster_workflow_tools`): an agent can trigger a real materialize/run-job, not just inspect.

    Use inside assets/ops: `context.resources.dagster_workflow_tools.call("materialize_assets",
    {"asset_keys": ["orders"]})`. For agentic_pipeline (MCP only) run THIS FILE as the stdio MCP
    server -- see README.md."""

    resource_key: str = Field(default="dagster_workflow_tools", description="Resource key the toolset is registered under")
    definitions_module: str = Field(
        description="Required 'pkg.module[:attr]' that yields the Definitions this tool is allowed to materialize/run (attr defaults to `defs`). Unlike dagster_instance_tools (read-only), a write tool must know its own scope -- there is no 'degrade gracefully without it' mode here.",
    )
    allow: Optional[List[str]] = Field(
        default=None,
        description=f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}.",
    )
    allow_assets: Optional[List[str]] = Field(
        default=None,
        description="Hard allow-list of asset keys materialize_assets may target (default: any asset in definitions_module). Recommended in production -- an agent should materialize specific, intended assets, not anything in the repo.",
    )
    allow_jobs: Optional[List[str]] = Field(
        default=None,
        description="Hard allow-list of job names run_job may target (default: any job in definitions_module).",
    )
    default_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Tags merged onto every triggered run, alongside dagster-community/workflow-tool=true (e.g. {'triggered_by': 'support-agent'}) -- for run-tag attribution/audit.",
    )
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/"
            "env shape as MCPServerResource) under this key, built from this component's own "
            "settings -- python -m this module, with the DAGSTER_WORKFLOW_TOOLS_* env vars set "
            "from definitions_module/allow/allow_assets/allow_jobs/default_tags. DAGSTER_HOME is "
            "read fresh from the environment at run time (when a consumer actually resolves this "
            "resource), not baked in at component load. Any mcp_servers-aware component "
            "(mcp_tool_call, mcp_tool_picker, the agent components, agentic_pipeline) can then "
            "reference it via server_resource_key instead of hand-writing an inline command+env "
            "entry. Leave unset to skip this -- existing resource_key-only behavior is unchanged."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow", "allow_assets", "allow_jobs"), csv=(), tags=("default_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dagster Workflow Tools', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(ALL_TOOLS)}")
        definitions_module = self.definitions_module
        allow_assets, allow_jobs, default_tags = self.allow_assets, self.allow_jobs, self.default_tags

        def _init(init_context):
            instance = init_context.instance or dg.DagsterInstance.get()
            return DagsterWorkflowTools(
                instance, definitions_module=definitions_module, allow=allow,
                allow_assets=allow_assets, allow_jobs=allow_jobs, default_tags=default_tags,
            )

        res = dg.ResourceDefinition(resource_fn=_init, description="WRITE-capable Dagster workflow tools for agents (materialize_assets/run_job).")
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            # DAGSTER_HOME is deliberately NOT read here (component-load time) -- see
            # _McpServerShapeResource.model_dump(), which reads it fresh at run time
            # instead. Everything else here IS this component's own config, legitimately
            # fixed at build time.
            mcp_env = {
                "DAGSTER_WORKFLOW_TOOLS_DEFINITIONS_MODULE": definitions_module,
            }
            if allow:
                mcp_env["DAGSTER_WORKFLOW_TOOLS_ALLOW"] = ",".join(allow)
            if allow_assets:
                mcp_env["DAGSTER_WORKFLOW_TOOLS_ALLOW_ASSETS"] = ",".join(allow_assets)
            if allow_jobs:
                mcp_env["DAGSTER_WORKFLOW_TOOLS_ALLOW_JOBS"] = ",".join(allow_jobs)
            if default_tags:
                mcp_env["DAGSTER_WORKFLOW_TOOLS_DEFAULT_TAGS"] = json.dumps(default_tags)
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key,
                type="stdio",
                command=["python", "-m", _this_module_name()],
                env=mcp_env,
            )

        return dg.Definitions(resources=resources)


# ── stdio MCP server entrypoint:  python -m <dotted.path.to.this.module>  (reads DAGSTER_HOME) ──────────────────────
# NOT `python component.py` directly -- see _this_module_name's docstring: a step's own tool call
# needs __spec__.name (the real importable module path) to reconstruct its job in a subprocess,
# and a bare script invocation never has that (__spec__ is None either way).
def build_mcp_server(tools: DagsterWorkflowTools):
    """MCP server (official `mcp` package) exposing exactly the allowed tools. Works on mcp 1.x and 2.x
    (same compat shim as dagster_instance_tools' own build_mcp_server)."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server("dagster-workflow-tools")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__ in allowed:
            mcp.tool()(fn)
        return fn

    @_tool
    def materialize_assets(asset_keys: List[str], run_config: Optional[dict] = None, tags: Optional[dict] = None, partition_key: Optional[str] = None) -> dict:
        """Trigger a real, synchronous materialization of the given asset keys (blocks until the run finishes). Returns {success, run_id, asset_keys}."""
        return tools.call("materialize_assets", {"asset_keys": asset_keys, "run_config": run_config, "tags": tags, "partition_key": partition_key})

    @_tool
    def run_job(job_name: str, run_config: Optional[dict] = None, tags: Optional[dict] = None, partition_key: Optional[str] = None) -> dict:
        """Trigger a real, synchronous run of an existing named job (blocks until the run finishes). Returns {success, run_id, job_name}."""
        return tools.call("run_job", {"job_name": job_name, "run_config": run_config, "tags": tags, "partition_key": partition_key})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: DAGSTER_HOME (required), DAGSTER_WORKFLOW_TOOLS_DEFINITIONS_MODULE (required),
    DAGSTER_WORKFLOW_TOOLS_ALLOW (csv), DAGSTER_WORKFLOW_TOOLS_ALLOW_ASSETS (csv),
    DAGSTER_WORKFLOW_TOOLS_ALLOW_JOBS (csv), DAGSTER_WORKFLOW_TOOLS_DEFAULT_TAGS (JSON object) --
    this last one is what DagsterWorkflowToolsComponent's mcp_server_resource_key path sets so the
    MCP-server entrypoint has the same default_tags the in-process resource_key path gets."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    if not os.environ.get("DAGSTER_HOME"):
        raise SystemExit("DAGSTER_HOME must point at the Dagster instance directory (the one with dagster.yaml)")
    definitions_module = os.environ.get("DAGSTER_WORKFLOW_TOOLS_DEFINITIONS_MODULE")
    if not definitions_module:
        raise SystemExit("DAGSTER_WORKFLOW_TOOLS_DEFINITIONS_MODULE must be set (e.g. 'my_project.defs:defs')")
    allow_env = os.environ.get("DAGSTER_WORKFLOW_TOOLS_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None
    allow_assets_env = os.environ.get("DAGSTER_WORKFLOW_TOOLS_ALLOW_ASSETS")
    allow_assets = [t.strip() for t in allow_assets_env.split(",") if t.strip()] if allow_assets_env else None
    allow_jobs_env = os.environ.get("DAGSTER_WORKFLOW_TOOLS_ALLOW_JOBS")
    allow_jobs = [t.strip() for t in allow_jobs_env.split(",") if t.strip()] if allow_jobs_env else None
    default_tags_env = os.environ.get("DAGSTER_WORKFLOW_TOOLS_DEFAULT_TAGS")
    default_tags = json.loads(default_tags_env) if default_tags_env else None
    tools = DagsterWorkflowTools(
        dg.DagsterInstance.get(),
        definitions_module=definitions_module,
        allow=allow, allow_assets=allow_assets, allow_jobs=allow_jobs, default_tags=default_tags,
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
