"""AgenticPipelineAuthoringToolsComponent -- read-only authoring/introspection tools for agentic_pipeline, so a
pipeline can be drafted and checked by Claude Code or by an agent running inside a code location, without hand-
maintaining a second copy of agentic_pipeline's own schema anywhere.

Same dual-mode shape as `dagster_instance_tools`/`dagster_workflow_tools`/`knowledge_search_server`:
  1. An importable toolset (`AgenticPipelineAuthoringTools`, plus the component that registers it as a Dagster
     resource) under `resource_key`.
  2. A self-contained stdio MCP server: `python -m <dotted.path.to.this.module>` (works unchanged from Claude Code's
     own `claude mcp add` and from inside a code location -- see README.md).
`mcp_server_resource_key`, if set, ALSO registers a stdio MCP-server-shaped resource (same convention as the other
three components' own `mcp_server_resource_key`) so any mcp_servers-aware component (agentic_pipeline's own
tool_use_loop, the agent components, ...) can reference this server via `server_resource_key` for a "draft/preview a
pipeline" step of its own.

Tools (all read-only, no file writes -- source: https://github.com/eric-thomas-dagster/dagster-component-templates):
  list_ops()                 -- every op name + one-line summary.
  describe_op(op)            -- that op's real JSON Schema (fields/types/defaults/descriptions), generated live from
                                 the real Pydantic model -- identical to assets/ai/agentic_pipeline/ops_schema.json.
  list_examples()            -- every shipped example's name + title + one-paragraph summary (from its README.md).
  get_example(name)          -- that example's pipeline.yaml + README.md, verbatim.
  validate_pipeline(yaml)    -- agentic_pipeline's OWN load-time validation (real Pydantic construction + build_defs),
                                 errors reported with step index/id/op/field where resolvable.
  preview_pipeline(yaml)     -- the assets it would create (<prefix>_<step>), the edges between steps, whether (and
                                 how) it is partitioned, and the env vars / MCP servers / skill resource keys it
                                 needs -- all WITHOUT executing a single step (build_defs() only constructs
                                 definitions; it never runs the pipeline body).

agentic_pipeline/component.py is loaded by path (sibling-relative `Path(__file__).parent.parent / "agentic_pipeline" /
"component.py"`), NOT via `dagster_community_components.assets.ai.agentic_pipeline.component` -- that dotted path
only resolves in a built/installed wheel (hatchling's `force-include` nests `assets/` under the package at BUILD
time only); a sibling-relative load works identically in this repo's own dev checkout and in an installed package,
since both keep `agentic_pipeline/` and this component's own directory as siblings under `assets/ai/`. Loaded once
and cached (the file is ~9000 lines with its own heavy imports -- litellm/sqlalchemy/jsonschema).
"""
import importlib.util
import json
import logging
import os
import sys
from pathlib import Path
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

_AUDIT = logging.getLogger("agentic_pipeline_authoring_tools.audit")

ALL_TOOLS = ("list_ops", "describe_op", "list_examples", "get_example", "validate_pipeline", "preview_pipeline")

_THIS_DIR = Path(__file__).resolve().parent
_AGENTIC_PIPELINE_PY = _THIS_DIR.parent / "agentic_pipeline" / "component.py"
_EXAMPLES_DIR = _THIS_DIR.parent / "agentic_pipeline" / "examples"


class ToolError(Exception):
    """A user-facing, safe error message (returned to the agent as {"error": ...})."""


def _first_line(s: Any, n: int = 300) -> str:
    line = (str(s or "").strip().splitlines() or [""])[0]
    return line[:n]


_ap_module_cache: Dict[str, Any] = {}


def _load_agentic_pipeline_module():
    """Load agentic_pipeline/component.py once, cached for the life of this process."""
    mod = _ap_module_cache.get("mod")
    if mod is not None:
        return mod
    if not _AGENTIC_PIPELINE_PY.exists():
        raise ToolError(
            f"agentic_pipeline/component.py not found at {_AGENTIC_PIPELINE_PY} -- "
            "is dagster-community-components installed correctly (both components ship as siblings under assets/ai/)?"
        )
    spec = importlib.util.spec_from_file_location("agentic_pipeline_component_for_authoring_tools", _AGENTIC_PIPELINE_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _ap_module_cache["mod"] = mod
    return mod


def _op_summary(model: type) -> str:
    doc = (model.__doc__ or "").strip()
    return doc.splitlines()[0] if doc else ""


def _clip_text(text: str, max_chars: int) -> Dict[str, Any]:
    if len(text) <= max_chars:
        return {"text": text, "truncated": False}
    return {"text": text[:max_chars], "truncated": True}


def _parse_example_readme(path: Path) -> "tuple[str, str]":
    """Title = the first `# ` heading; summary = the paragraph right after it (stops at the first blank line,
    code fence, or table row)."""
    title = None
    summary_lines: List[str] = []
    for line in path.read_text().splitlines():
        if title is None:
            if line.startswith("# "):
                title = line[2:].strip()
            continue
        stripped = line.strip()
        if not stripped:
            if summary_lines:
                break
            continue
        if stripped.startswith("```") or stripped.startswith("|"):
            break
        summary_lines.append(stripped)
    return title or path.parent.name, " ".join(summary_lines)


def _parse_pipeline_yaml(text: str) -> Dict[str, Any]:
    """Accepts either a full component.yaml (`type: ...` / `attributes: {...}`) or a bare attributes-only mapping --
    either is common to paste in by hand."""
    try:
        import yaml
    except ImportError as e:
        raise ToolError("validate_pipeline/preview_pipeline require pyyaml: pip install pyyaml") from e
    try:
        cfg = yaml.safe_load(text)
    except Exception as e:  # noqa: BLE001
        raise ToolError(f"invalid YAML: {e}") from e
    if not isinstance(cfg, dict):
        raise ToolError("expected a YAML mapping: either a bare attributes dict, or a full component.yaml with `type:` + `attributes:`")
    if "attributes" in cfg and isinstance(cfg["attributes"], dict):
        return cfg["attributes"]
    return cfg


def _format_pydantic_errors(exc: Any, attrs: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Maps pydantic's `.errors()` `loc` tuples onto {step_index, step_id, op, field, message} -- `loc` for a step
    field looks like ('steps', 2, 'llm_call', 'model'); the discriminator value ('llm_call') is redundant with the
    step's own `op` once resolved, so it's dropped from `field`."""
    steps = attrs.get("steps") if isinstance(attrs, dict) else None
    out: List[Dict[str, Any]] = []
    for err in exc.errors():
        loc = err.get("loc", ())
        step_index = step_id = op = field = None
        if loc and loc[0] == "steps" and len(loc) > 1 and isinstance(loc[1], int):
            step_index = loc[1]
            if isinstance(steps, list) and 0 <= step_index < len(steps) and isinstance(steps[step_index], dict):
                step_id = steps[step_index].get("id")
                op = steps[step_index].get("op")
            rest = list(loc[2:])
            if rest and isinstance(rest[0], str) and rest[0] == op:
                rest = rest[1:]
            field = ".".join(str(p) for p in rest) or None
        else:
            field = ".".join(str(p) for p in loc) or None
        out.append({"step_index": step_index, "step_id": step_id, "op": op, "field": field, "message": err.get("msg")})
    return out


def _collect_env_var_refs(obj: Any) -> "set[str]":
    """Best-effort static scan: every string value under a key ending in `_env_var`, plus `bearer_env` and the
    values of a `headers_env` dict, anywhere in the parsed attributes (source/steps/policy/personas/agents all
    included, recursively, including inside a `sub_pipeline` step's nested `steps:`). Does not account for
    `{{ env.X }}`-style Jinja templating a surrounding Dagster component.yaml might apply before this component ever
    sees the dict -- that's resolved at the YAML-loading layer, not by agentic_pipeline's own runtime code."""
    found: "set[str]" = set()

    def walk(o: Any) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                key = str(k)
                if isinstance(v, str) and v.strip() and (key.endswith("_env_var") or key == "bearer_env"):
                    found.add(v.strip())
                if key == "headers_env" and isinstance(v, dict):
                    for name in v.values():
                        if isinstance(name, str) and name.strip():
                            found.add(name.strip())
                walk(v)
        elif isinstance(o, list):
            for item in o:
                walk(item)

    walk(obj)
    return found


def _collect_server_refs(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Every MCP server entry referenced by a step (`mcp_call.server`, `tool_use_loop.mcp_servers`,
    `plan.executor.mcp_servers`), inline or via `server_resource_key`, recursing into `sub_pipeline` steps."""
    out: List[Dict[str, Any]] = []

    def add(step_id: Optional[str], entry: Any) -> None:
        if isinstance(entry, dict):
            out.append({
                "step": step_id, "name": entry.get("name"), "type": entry.get("type"),
                "server_resource_key": entry.get("server_resource_key"),
            })

    for step in steps or []:
        sid = step.get("id")
        add(sid, step.get("server"))
        for entry in (step.get("mcp_servers") or []):
            add(sid, entry)
        executor = step.get("executor")
        if isinstance(executor, dict):
            for entry in (executor.get("mcp_servers") or []):
                add(sid, entry)
        if step.get("op") == "sub_pipeline":
            out.extend(_collect_server_refs(step.get("steps") or []))
    return out


class AgenticPipelineAuthoringTools:
    """Read-only authoring/introspection tools for agentic_pipeline. `call(tool, args)` is the one safe entry point
    (allow-list, audit, size cap, error shaping); the `t_*` methods are the raw implementations."""

    def __init__(self, *, allow: Optional[List[str]] = None, max_result_chars: int = 20000, audit: Optional[Callable[[str], None]] = None):
        if allow is None:
            allow = list(ALL_TOOLS)
        unknown = sorted(set(allow) - set(ALL_TOOLS))
        if unknown:
            raise ValueError(f"unknown tool(s) in allow: {unknown}; valid: {list(ALL_TOOLS)}")
        self.allow = frozenset(allow)
        self.max_result_chars = max(2000, int(max_result_chars))
        self._audit = audit or (lambda line: _AUDIT.info(line))

    def tool_names(self) -> List[str]:
        return [t for t in ALL_TOOLS if t in self.allow]

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(args or {})
        try:
            self._audit(f"agentic_pipeline_authoring_tools call tool={tool} args={json.dumps({k: (v[:200] if isinstance(v, str) else v) for k, v in args.items()}, default=str)[:300]}")
        except Exception:  # an audit sink must never break a call
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except ToolError as e:
            return {"error": str(e)}
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {e}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {_first_line(e)}"}
        return self._fit(result)

    def _fit(self, result: Dict[str, Any]) -> Dict[str, Any]:
        cap = self.max_result_chars
        text = json.dumps(result, default=str)
        if len(text) <= cap:
            return result
        return {"truncated": True, "preview": text[: max(0, cap - 40)]}

    # -- tools ---------------------------------------------------------------------------------------------------
    def t_list_ops(self) -> Dict[str, Any]:
        mod = _load_agentic_pipeline_module()
        return {"ops": [{"op": op, "summary": _op_summary(model)} for op, model in mod._STEP_MODELS.items()]}

    def t_describe_op(self, op: Optional[str] = None) -> Dict[str, Any]:
        mod = _load_agentic_pipeline_module()
        if not op or op not in mod._STEP_MODELS:
            raise ToolError(f"unknown op {op!r}; valid: {sorted(mod._STEP_MODELS)}")
        model = mod._STEP_MODELS[op]
        return {"op": op, "summary": _op_summary(model), "schema": model.model_json_schema()}

    def t_list_examples(self) -> Dict[str, Any]:
        if not _EXAMPLES_DIR.exists():
            return {"examples": []}
        examples = []
        for d in sorted(_EXAMPLES_DIR.iterdir()):
            pipeline_yaml = d / "pipeline.yaml"
            if not d.is_dir() or not pipeline_yaml.exists():
                continue
            readme = d / "README.md"
            title, summary = _parse_example_readme(readme) if readme.exists() else (d.name, "")
            examples.append({"name": d.name, "title": title, "summary": summary})
        return {"examples": examples}

    def t_get_example(self, name: Optional[str] = None) -> Dict[str, Any]:
        if not name:
            raise ToolError("name is required")
        d = _EXAMPLES_DIR / name
        pipeline_yaml = d / "pipeline.yaml"
        if not d.is_dir() or not pipeline_yaml.exists():
            valid = sorted(p.name for p in _EXAMPLES_DIR.iterdir() if p.is_dir() and (p / "pipeline.yaml").exists()) if _EXAMPLES_DIR.exists() else []
            raise ToolError(f"unknown example {name!r}; valid: {valid}")
        out: Dict[str, Any] = {"name": name, "pipeline_yaml": _clip_text(pipeline_yaml.read_text(), self.max_result_chars)}
        readme = d / "README.md"
        if readme.exists():
            out["readme"] = _clip_text(readme.read_text(), self.max_result_chars)
        return out

    def t_validate_pipeline(self, yaml: Optional[str] = None) -> Dict[str, Any]:
        if not yaml:
            raise ToolError("yaml is required")
        attrs = _parse_pipeline_yaml(yaml)
        mod = _load_agentic_pipeline_module()
        from pydantic import ValidationError
        try:
            comp = mod.AgenticPipelineComponent(**attrs)
        except ValidationError as e:
            return {"valid": False, "errors": _format_pydantic_errors(e, attrs)}
        except (ValueError, TypeError) as e:
            return {"valid": False, "errors": [{"step_index": None, "step_id": None, "op": None, "field": None, "message": str(e), "source": "construction"}]}
        try:
            comp.build_defs(None)
        except Exception as e:  # noqa: BLE001 -- a load-time guard raising is exactly what this reports, not a server bug
            return {"valid": False, "errors": [{"step_index": None, "step_id": None, "op": None, "field": None, "message": str(e), "source": "build_defs"}]}
        return {"valid": True, "errors": []}

    def t_preview_pipeline(self, yaml: Optional[str] = None) -> Dict[str, Any]:
        if not yaml:
            raise ToolError("yaml is required")
        attrs = _parse_pipeline_yaml(yaml)
        mod = _load_agentic_pipeline_module()
        from pydantic import ValidationError
        try:
            comp = mod.AgenticPipelineComponent(**attrs)
        except ValidationError as e:
            raise ToolError("pipeline does not validate -- call validate_pipeline for structured errors: " + "; ".join(er["msg"] for er in e.errors()[:5]))
        try:
            defs = comp.build_defs(None)
        except Exception as e:  # noqa: BLE001
            raise ToolError(f"pipeline failed to build: {type(e).__name__}: {e}")

        # A YAML that references server_resource_key / skill_keys needs those resources PROVIDED to even resolve
        # the asset graph (Dagster validates every AssetsDefinition's required_resource_keys up front) -- but a
        # resource declared elsewhere in the project (a sibling MCPServerResourceComponent/SkillResourceComponent)
        # is exactly what this preview can't see in isolation. Stub in a placeholder for anything still missing so
        # preview works on a standalone YAML snippet, not just a fully-wired project.
        needed_keys: "set[str]" = set()
        for a in defs.assets or []:
            needed_keys |= set(getattr(a, "required_resource_keys", None) or set())
        missing = needed_keys - set(defs.resources or {}) - {"io_manager"}
        if missing:
            defs = dg.Definitions.merge(defs, dg.Definitions(resources={k: dg.ResourceDefinition.hardcoded_resource(None) for k in missing}))

        step_dicts = [s.model_dump(exclude_unset=True, by_alias=True) for s in comp.steps]
        graph = defs.resolve_asset_graph()
        specs_by_key = {s.key: s for s in defs.resolve_all_asset_specs()}

        assets: List[Dict[str, Any]] = []
        edges: List[Dict[str, str]] = []
        partitioned = False
        for key in sorted(graph.get_all_asset_keys(), key=lambda k: k.path):
            node = graph.get(key)
            spec = specs_by_key.get(key)
            md = {k: getattr(v, "value", v) for k, v in (spec.metadata or {}).items()} if spec is not None else {}
            name = "/".join(key.path)
            parents = sorted("/".join(p.path) for p in node.parent_keys)
            assets.append({"asset": name, "step": md.get("step"), "op": md.get("op"), "reads": md.get("reads"), "depends_on": parents})
            for p in parents:
                edges.append({"from": p, "to": name})
            if node.partitions_def is not None:
                partitioned = True

        return {
            "assets": assets,
            "edges": edges,
            "partition": {
                "partitioned": partitioned,
                "partition_type": attrs.get("partition_type"),
                "partition_dimensions": attrs.get("partition_dimensions"),
            },
            "needs": {
                "env_vars": sorted(_collect_env_var_refs(attrs)),
                "mcp_servers": _collect_server_refs(step_dicts),
                "skill_resource_keys": sorted({k for s in step_dicts if s.get("op") == "tool_use_loop" for k in (s.get("skill_keys") or [])}),
            },
        }


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>`. Same mechanism as dagster_instance_tools'/
    dagster_workflow_tools'/knowledge_search_server's own `_this_module_name` (duplicated here, not imported, per
    this repo's self-contained-component convention): `__name__` alone is `"__main__"` both for `python
    component.py` and for `python -m dotted.path`; `__spec__.name` is the real dotted path only the `-m` form sets."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "agentic_pipeline_authoring_tools's mcp_server_resource_key path must be run as `python -m <dotted.path.to.this.module>` "
        "(e.g. `python -m dagster_community_components.assets.ai.agentic_pipeline_authoring_tools.component`), not "
        "`python component.py` directly -- that bare-script form still works for `python component.py` itself, just not for this "
        "path. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource (name/type/command/url/env/headers/headers_env) -- inlined here
    rather than imported from resources/mcp_server_resource (confirmed not importable across components; same
    duck-typed copy every mcp_servers-aware consumer already expects). Structurally identical to
    dagster_instance_tools'/dagster_workflow_tools' own copies -- same reasoning: `env`'s any live_env_passthrough
    names are deliberately NOT baked in at construction time, read fresh from the environment only when
    model_dump() is actually called (run time), since that can differ from code-location load time."""

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


class AgenticPipelineAuthoringToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers read-only agentic_pipeline authoring/introspection tools as a resource (default key
    `agentic_pipeline_authoring_tools`). For agentic_pipeline (MCP only) or Claude Code (`claude mcp add`), run THIS
    FILE as the stdio MCP server -- see README.md."""

    resource_key: str = Field(default="agentic_pipeline_authoring_tools", description="Resource key the toolset is registered under")
    allow: Optional[List[str]] = Field(default=None, description=f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}.")
    max_result_chars: int = Field(default=20000, description="Upper bound on the serialized size of any tool result (an example's README/pipeline.yaml or a preview can be large).")
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/env shape as "
            "MCPServerResource) under this key, built from this component's own settings -- python -m this module, "
            "with AGENTIC_PIPELINE_AUTHORING_TOOLS_* env vars set from allow/max_result_chars. Any mcp_servers-aware "
            "component (mcp_call, tool_use_loop, the agent components, agentic_pipeline itself) can then reference "
            "it via server_resource_key instead of hand-writing an inline command+env entry. Leave unset to skip "
            "this -- existing resource_key-only behavior is unchanged."
        ),
    )
    mcp_extra_env_vars: Optional[List[str]] = Field(
        default=None,
        description=(
            "mcp_server_resource_key only. Extra env var NAMES to copy (current value, read fresh every time a "
            "consumer resolves the resource) from this process into the spawned MCP server's env. MCP's stdio "
            "client does not inherit the rest of the environment, so anything this server needs beyond its own "
            "AGENTIC_PIPELINE_AUTHORING_TOOLS_* settings must be listed here."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow", "mcp_extra_env_vars"), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Agentic Pipeline Authoring Tools", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(ALL_TOOLS)}")
        max_chars = int(self.max_result_chars)

        def _init(init_context):
            return AgenticPipelineAuthoringTools(allow=allow, max_result_chars=max_chars)

        res = dg.ResourceDefinition(resource_fn=_init, description="Read-only agentic_pipeline authoring/introspection tools for agents.")
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {}
            if allow:
                mcp_env["AGENTIC_PIPELINE_AUTHORING_TOOLS_ALLOW"] = ",".join(allow)
            mcp_env["AGENTIC_PIPELINE_AUTHORING_TOOLS_MAX_CHARS"] = str(max_chars)
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key,
                type="stdio",
                command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env,
                live_env_passthrough=list(self.mcp_extra_env_vars or []),
            )

        return dg.Definitions(resources=resources)


# ── stdio MCP server entrypoint:  python -m <this module>  (or  python component.py) ────────────────────────────────
def build_mcp_server(tools: AgenticPipelineAuthoringTools):
    """MCP server (official `mcp` package) exposing exactly the allowed tools. Works on mcp 1.x and 2.x."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server("agentic-pipeline-authoring-tools")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__ in allowed:
            mcp.tool()(fn)
        return fn

    @_tool
    def list_ops() -> dict:
        """Every agentic_pipeline op name + one-line summary."""
        return tools.call("list_ops")

    @_tool
    def describe_op(op: str) -> dict:
        """That op's real JSON Schema (fields/types/defaults/descriptions), generated live from the real model."""
        return tools.call("describe_op", {"op": op})

    @_tool
    def list_examples() -> dict:
        """Every shipped agentic_pipeline example's name + title + one-paragraph summary."""
        return tools.call("list_examples")

    @_tool
    def get_example(name: str) -> dict:
        """That example's pipeline.yaml + README.md, verbatim."""
        return tools.call("get_example", {"name": name})

    @_tool
    def validate_pipeline(yaml: str) -> dict:
        """agentic_pipeline's own load-time validation (real construction + build_defs); errors carry step index/id/op/field where resolvable."""
        return tools.call("validate_pipeline", {"yaml": yaml})

    @_tool
    def preview_pipeline(yaml: str) -> dict:
        """The assets a pipeline YAML would create, the edges between steps, its partitioning, and the env vars / MCP servers / skill keys it needs -- without running a single step."""
        return tools.call("preview_pipeline", {"yaml": yaml})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: AGENTIC_PIPELINE_AUTHORING_TOOLS_ALLOW (csv), _MAX_CHARS (default 20000)."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    allow_env = os.environ.get("AGENTIC_PIPELINE_AUTHORING_TOOLS_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None
    max_chars = int(os.environ.get("AGENTIC_PIPELINE_AUTHORING_TOOLS_MAX_CHARS", "20000"))
    tools = AgenticPipelineAuthoringTools(allow=allow, max_result_chars=max_chars)
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
