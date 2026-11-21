"""DagsterDiscoveryToolsComponent -- read-only "what exists in this deployment" tools for agents, over the
Dagster+ GraphQL API. No Dagster+ backend or MCP server changes: this is a plain client, like `ops_tools.py`
in the engineering-agents reference sandbox.

The Dagster+ MCP server does not expose components, agents, or MCP servers -- an agent has no way to ask
"what agents/tools/components exist in this deployment" except being told in a prompt. This fills that gap:
  list_locations           -- code locations + their load status/errors + library versions + schedule/sensor counts
  list_assets              -- assets across the deployment (group_name, freshness, automation, checks, partitions)
  list_components(type?)   -- every UI-created (app-managed) component in a location, with its type + YAML
  get_component(id)        -- one component's full config, by id
  list_agents              -- the agent components specifically (type/server/allowed-tools), parsed from their YAML
  list_mcp_servers         -- the MCP-server-shaped components specifically
  list_schedules_and_sensors -- schedules + sensors per repository
  list_runs(location_name) -- recent runs, ALWAYS scoped to one code location (a deployment-wide list is not
                              what "what ran recently in X" means, and the API makes that mistake easy to make)
  list_alert_policies      -- alert policies configured on the deployment

Same dual-mode shape as `dagster_instance_tools`/`dagster_workflow_tools`/`dagster_api_check_tools`:
  1. An importable toolset (`DagsterDiscoveryTools`, plus the component that registers it as a Dagster resource).
  2. A self-contained stdio MCP server: `python -m <dotted.path.to.this.module>`.

Auth + deployment-URL resolution are the SAME verified mechanism `insights_custom_metrics_sync` already uses
(inlined fresh here, not imported, per this repo's self-contained-component convention): `deployment_url: auto`
resolves to `https://<org>[.<region>].dagster.cloud/<deployment>` from `DAGSTER_CLOUD_DEPLOYMENT_NAME` + the
instance's `dagit_url` (only real in a Dagster+ code server/run worker); `token_env_var` (default
`DAGSTER_CLOUD_API_TOKEN`) is sent as the `Dagster-Cloud-Api-Token` header.

Honesty about what's verified vs. Dagster+-only and unverifiable from this OSS-only environment:
  - `workspaceOrError`/`locationEntries`/`RepositoryLocation.dagsterLibraryVersions`/`Repository.{schedules,sensors}`
    and `runsOrError(filter: RunsFilter, ...)` are REAL, OSS GraphQL fields -- confirmed by introspecting the
    installed `dagster_graphql` package's own schema (this repo has no live Dagster+ deployment to test against).
    `RunsFilter.repositoryOrigin` is a Dagster+-only extension (not in the OSS RunsFilter at all) -- used here
    exactly as supplied by Dagster Labs engineers who validated it against a real deployment.
  - Top-level `assetNodes`, `alertPolicies`, and `appManagedComponentsForLocationOrError` are Dagster+-only
    schema extensions this OSS environment cannot introspect. Rather than hand-guess their exact nested type
    names (a wrong inline-fragment type name is a hard GraphQL validation error, not a soft miss), the tools
    that touch them use `_auto_selection()`: it introspects the live deployment's `__type` for the field's
    return type at call time and builds a field-selection set from whatever is actually there (real unions
    get real `... on <PossibleType>` fragments, discovered live -- not guessed). This costs one extra
    introspection round-trip per distinct type the first time it's touched (cached on the client for the
    process lifetime), in exchange for not hard-coding a schema shape nobody here could check.
"""

import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Literal, Optional
from urllib.parse import urlparse

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


# ── deployment URL resolution (same verified mechanism as insights_custom_metrics_sync) ────────────────────────────
def validate_deployment_url(url: str) -> str:
    u = urlparse(url or "")
    loopback = u.hostname in ("localhost", "127.0.0.1", "::1")
    if not u.hostname or u.scheme not in ("https", "http") or (u.scheme == "http" and not loopback):
        raise ValueError("deployment_url must be a full https URL like https://<org>.dagster.cloud/<deployment> "
                         f"(got {url!r}; plain http is only allowed for localhost)")
    return url.rstrip("/")


_AUTO = "auto"
_DEPLOYMENT_ENV = "DAGSTER_CLOUD_DEPLOYMENT_NAME"


def resolve_deployment_url(value: str, instance: Any = None, env: Optional[Dict[str, str]] = None) -> str:
    """`deployment_url: auto` -> `https://<org>[.<region>].dagster.cloud/<deployment>`; anything else wins as given.
    Same resolution path as insights_custom_metrics_sync.resolve_deployment_url (duplicated here, not imported,
    per this repo's self-contained-component convention) -- see that component's docstring for the verified
    ground truth (DAGSTER_CLOUD_DEPLOYMENT_NAME + instance.dagit_url)."""
    if (value or "").strip().lower() != _AUTO:
        return validate_deployment_url(value)
    deployment = ((env if env is not None else os.environ).get(_DEPLOYMENT_ENV) or "").strip()
    how = "set deployment_url explicitly, e.g. https://<org>.dagster.cloud/<deployment>"
    if not deployment:
        raise ValueError(f"deployment_url: auto could not be resolved: env {_DEPLOYMENT_ENV} is not set (not running in a "
                         f"Dagster+ code server / run worker). {how}")
    try:
        base = instance.dagit_url
    except Exception as e:  # noqa: BLE001 -- plain OSS instance (no attribute) or legacy agent token (no org in the token)
        raise ValueError("deployment_url: auto could not be resolved: the Dagster instance does not expose the Dagster+ "
                         f"organization ({type(e).__name__}: {e}). {how}") from None
    u = urlparse(str(base or ""))
    if u.scheme != "https" or not u.hostname:
        raise ValueError(f"deployment_url: auto could not be resolved: instance.dagit_url is {base!r}. {how}")
    return validate_deployment_url(f"https://{u.netloc}/{deployment}")


class DiscoveryToolError(Exception):
    """A user-facing, safe error message (returned to the agent as {"error": ...})."""


# ── GraphQL client with transient-error retry + type-introspection cache ───────────────────────────────────────────
class _Client:
    def __init__(self, deployment_url: str, token: str, *, token_env_var: str = "DAGSTER_CLOUD_API_TOKEN",
                 timeout: float = 30.0, max_retries: int = 3, backoff_seconds: float = 1.0,
                 max_backoff_seconds: float = 15.0, sleep: Callable[[float], None] = time.sleep,
                 rng: Optional[random.Random] = None):
        self.endpoint = deployment_url.rstrip("/") + "/graphql"
        self._token, self.token_env_var = token, token_env_var
        self.timeout, self.max_retries = timeout, max_retries
        self.backoff, self.max_backoff = backoff_seconds, max_backoff_seconds
        self.sleep, self.rng = sleep, rng or random.Random()
        self.requests = 0
        self._type_cache: Dict[str, Any] = {}

    def _redact(self, text: str) -> str:
        return text.replace(self._token, "***") if self._token else text

    def _delay(self, attempt: int, retry_after: Optional[str]) -> float:
        if retry_after:
            try:
                return min(max(float(retry_after), 0.0), self.max_backoff)
            except ValueError:
                pass
        base = min(self.backoff * (2 ** attempt), self.max_backoff)
        return base * (0.5 + self.rng.random() / 2)

    def call(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        body = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
        for attempt in range(self.max_retries + 1):
            req = urllib.request.Request(self.endpoint, data=body, method="POST", headers={
                "Content-Type": "application/json", "Dagster-Cloud-Api-Token": self._token})
            retry_after, why = None, ""
            try:
                self.requests += 1
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                if payload.get("errors"):
                    raise DiscoveryToolError(f"Dagster+ GraphQL errors: {self._redact(json.dumps(payload['errors'])[:500])}")
                return payload.get("data") or {}
            except urllib.error.HTTPError as e:
                text = self._redact(e.read().decode("utf-8", errors="replace")[:300])
                if e.code in (401, 403):
                    raise DiscoveryToolError(
                        f"Dagster+ rejected the API token (HTTP {e.code}); check env var {self.token_env_var} holds a "
                        "token with access to this deployment") from None
                if e.code == 429 or 500 <= e.code < 600:
                    retry_after, why = e.headers.get("Retry-After") if e.headers else None, f"HTTP {e.code}"
                else:
                    raise DiscoveryToolError(f"Dagster+ GraphQL HTTP {e.code}: {text}") from None
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
                why = f"{type(e).__name__}: {self._redact(str(e))[:200]}"
            except ValueError:
                raise DiscoveryToolError("Dagster+ returned a non-JSON response; is deployment_url correct?") from None
            if attempt >= self.max_retries:
                raise DiscoveryToolError(f"Dagster+ unreachable after {self.max_retries + 1} attempts ({why})") from None
            self.sleep(self._delay(attempt, retry_after))
        raise DiscoveryToolError("unreachable")  # pragma: no cover -- loop above always returns or raises

    # -- schema introspection (for the Dagster+-only fields this OSS environment can't verify ahead of time) -----
    _INTROSPECT_TYPE = """
    query($name: String!) {
      __type(name: $name) {
        name kind
        possibleTypes { name }
        fields {
          name
          args { name type { kind ofType { kind } } }
          type { kind name ofType { kind name ofType { kind name } } }
        }
      }
    }
    """

    def introspect_type(self, type_name: str) -> Optional[Dict[str, Any]]:
        if type_name not in self._type_cache:
            self._type_cache[type_name] = self.call(self._INTROSPECT_TYPE, {"name": type_name}).get("__type")
        return self._type_cache[type_name]


_LEAF_SCALARS = {"String", "Int", "Float", "Boolean", "ID"}


def _unwrap(t: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    t = t or {}
    while t.get("kind") in ("NON_NULL", "LIST") and t.get("ofType"):
        t = t["ofType"]
    return t


def auto_selection(client: _Client, type_name: str, depth: int = 2, _seen: Optional[frozenset] = None) -> str:
    """Build a GraphQL selection set for `type_name` by introspecting the LIVE schema rather than
    hard-coding a guessed shape -- see module docstring. Selects every leaf scalar/enum field, recurses
    into object/union/interface fields up to `depth`, and skips any field that takes a required argument
    (an auto-built query has no way to supply one). Unions/interfaces get a real `... on <PossibleType>`
    per type the schema actually reports, discovered live, never assumed."""
    _seen = _seen or frozenset()
    if depth <= 0 or type_name in _seen:
        return "__typename"
    info = client.introspect_type(type_name)
    if info is None:
        return "__typename"
    seen2 = _seen | {type_name}
    if info["kind"] in ("UNION", "INTERFACE"):
        parts = ["__typename"]
        for pt in info.get("possibleTypes") or []:
            sub = auto_selection(client, pt["name"], depth, seen2)
            if sub:
                parts.append(f"... on {pt['name']} {{ {sub} }}")
        return " ".join(parts)
    if info["kind"] != "OBJECT":
        return ""
    parts = []
    for f in info.get("fields") or []:
        if any((a.get("type") or {}).get("kind") == "NON_NULL" for a in (f.get("args") or [])):
            continue  # a required argument this auto-builder can't supply
        leaf = _unwrap(f["type"])
        kind = leaf.get("kind")
        if kind in ("SCALAR", "ENUM") or leaf.get("name") in _LEAF_SCALARS:
            parts.append(f["name"])
        elif kind in ("OBJECT", "UNION", "INTERFACE") and depth > 1:
            sub = auto_selection(client, leaf["name"], depth - 1, seen2)
            if sub:
                parts.append(f"{f['name']} {{ {sub} }}")
    return " ".join(parts) or "__typename"


ALL_TOOLS = (
    "list_locations", "list_assets", "list_components", "get_component",
    "list_agents", "list_mcp_servers", "list_schedules_and_sensors",
    "list_runs", "list_alert_policies",
)

# Component-type-name substrings this registry uses for agent / MCP-server-shaped components (case-insensitive).
# Best-effort classification of appManagedComponentsForLocationOrError's generic {id, type, yaml} rows -- there is
# no dedicated "is this an agent" field on that query, so this reads the component's own `type` string, the same
# way a person skimming the Components tab would.
_AGENT_TYPE_HINTS = ("agent", "advisor", "reviewer", "bugfixer", "bug_fixer")
_MCP_SERVER_TYPE_HINTS = ("mcptools", "mcp_tools", "toolscomponent", "chattools", "chat_tools", "mcpserver", "mcp_server")


class DagsterDiscoveryTools:
    """`call(tool, args)` is the one safe entry point (allow-list, audit, size cap, error shaping); the `t_*`
    methods are the raw implementations. Every tool is read-only."""

    def __init__(self, client: _Client, *, allow: Optional[List[str]] = None, max_result_chars: int = 12000,
                 audit: Optional[Callable[[str], None]] = None):
        import logging
        self._client = client
        if allow is None:
            allow = list(ALL_TOOLS)
        unknown = sorted(set(allow) - set(ALL_TOOLS))
        if unknown:
            raise ValueError(f"unknown tool(s) in allow: {unknown}; valid: {list(ALL_TOOLS)}")
        self.allow = frozenset(allow)
        self.max_result_chars = max(1000, int(max_result_chars))
        self._audit = audit or (lambda line: logging.getLogger("dagster_discovery_tools.audit").info(line))

    def tool_names(self) -> List[str]:
        return [t for t in ALL_TOOLS if t in self.allow]

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(args or {})
        try:
            self._audit(f"dagster_discovery_tools call tool={tool} args={json.dumps(args, default=str)[:300]}")
        except Exception:
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except DiscoveryToolError as e:
            return {"error": str(e)}
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {e}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"}
        return self._fit(result)

    def _fit(self, result: Dict[str, Any]) -> Dict[str, Any]:
        cap = self.max_result_chars
        text = json.dumps(result, default=str)
        if len(text) <= cap:
            return result
        return {"truncated": True, "preview": text[: max(0, cap - 40)]}

    # -- verified-shape tools (real OSS GraphQL fields; Dagster+ serves the same ones) ----------------------------
    def t_list_locations(self) -> Dict[str, Any]:
        query = """
        query {
          workspaceOrError {
            __typename
            ... on Workspace {
              locationEntries {
                name
                loadStatus
                locationOrLoadError {
                  __typename
                  ... on RepositoryLocation {
                    dagsterLibraryVersions { name version }
                    repositories { name schedules { name } sensors { name } }
                  }
                  ... on PythonError { message }
                }
              }
            }
            ... on PythonError { message }
          }
        }
        """
        data = self._client.call(query)
        ws = data.get("workspaceOrError") or {}
        if ws.get("__typename") == "PythonError":
            raise DiscoveryToolError(f"workspaceOrError: {ws.get('message')}")
        locations = []
        for entry in ws.get("locationEntries") or []:
            loc = entry.get("locationOrLoadError") or {}
            row: Dict[str, Any] = {"name": entry.get("name"), "load_status": entry.get("loadStatus")}
            if loc.get("__typename") == "PythonError":
                row["load_error"] = loc.get("message")
            else:
                row["library_versions"] = {v["name"]: v["version"] for v in (loc.get("dagsterLibraryVersions") or [])}
                row["repositories"] = [
                    {"name": r["name"], "schedule_count": len(r.get("schedules") or []), "sensor_count": len(r.get("sensors") or [])}
                    for r in (loc.get("repositories") or [])
                ]
            locations.append(row)
        return {"count": len(locations), "locations": locations}

    def t_list_schedules_and_sensors(self, location_name: Any = None) -> Dict[str, Any]:
        query = """
        query {
          workspaceOrError {
            __typename
            ... on Workspace {
              locationEntries {
                name
                locationOrLoadError {
                  __typename
                  ... on RepositoryLocation {
                    repositories {
                      name
                      schedules { name cronSchedule description }
                      sensors { name description sensorType }
                    }
                  }
                }
              }
            }
          }
        }
        """
        data = self._client.call(query)
        ws = data.get("workspaceOrError") or {}
        rows = []
        for entry in ws.get("locationEntries") or []:
            if location_name and entry.get("name") != location_name:
                continue
            loc = entry.get("locationOrLoadError") or {}
            for repo in (loc.get("repositories") or []):
                rows.append({
                    "location_name": entry.get("name"), "repository_name": repo.get("name"),
                    "schedules": [{"name": s["name"], "cron_schedule": s.get("cronSchedule"), "description": s.get("description")}
                                  for s in (repo.get("schedules") or [])],
                    "sensors": [{"name": s["name"], "description": s.get("description"), "sensor_type": s.get("sensorType")}
                                for s in (repo.get("sensors") or [])],
                })
        return {"count": len(rows), "repositories": rows}

    def t_list_runs(self, location_name: Any = None, limit: Any = 20, statuses: Any = None) -> Dict[str, Any]:
        """ALWAYS pass location_name (a deployment can have many concurrent runs across unrelated
        code locations, and an unscoped list answers a different question than 'what ran in X')."""
        if not isinstance(location_name, str) or not location_name.strip():
            raise DiscoveryToolError("list_runs requires location_name -- a deployment-wide run list mixes "
                                      "concurrent, unrelated runs from every code location.")
        n = max(1, min(int(limit or 20), 200))
        gql_filter: Dict[str, Any] = {"repositoryOrigin": {"repositoryLocationName": location_name}}
        if statuses:
            gql_filter["statuses"] = [str(s).upper() for s in (statuses if isinstance(statuses, (list, tuple)) else [statuses])]
        query = """
        query($filter: RunsFilter, $limit: Int) {
          runsOrError(filter: $filter, limit: $limit) {
            __typename
            ... on Runs {
              results { runId jobName status startTime endTime }
            }
            ... on PythonError { message }
          }
        }
        """
        data = self._client.call(query, {"filter": gql_filter, "limit": n})
        result = data.get("runsOrError") or {}
        if result.get("__typename") == "PythonError":
            raise DiscoveryToolError(f"runsOrError: {result.get('message')}")
        runs = result.get("results") or []
        return {"location_name": location_name, "count": len(runs), "runs": runs}

    # -- Dagster+-only fields: schema discovered live via introspection, never hard-coded ---------------------------
    def t_list_assets(self, location_name: Any = None, group_name: Any = None, limit: Any = 200) -> Dict[str, Any]:
        """No confirmed server-side filter argument on Dagster+'s top-level `assetNodes` field (this OSS
        environment can't introspect the Cloud schema to check), so this fetches the field as auto-discovered
        and filters client-side by location_name/group_name -- correct either way, just not server-pruned."""
        selection = auto_selection(self._client, "AssetNode", depth=2)
        # repository.location.name is the one nested path this tool needs filterable -- make sure it's present
        # even if a shallow auto_selection missed it (AssetNode -> Repository -> RepositoryLocation is 2 hops).
        if "repository" not in selection:
            selection += " repository { name location { name } }"
        query = f"query {{ assetNodes {{ {selection} }} }}"
        data = self._client.call(query)
        nodes = data.get("assetNodes") or []
        rows = []
        for n in nodes:
            repo = n.get("repository") or {}
            loc_name = ((repo.get("location") or {}).get("name"))
            if location_name and loc_name != location_name:
                continue
            if group_name and n.get("groupName") != group_name:
                continue
            key = n.get("assetKey") or {}
            rows.append({
                "asset_key": "/".join(key.get("path") or []) if isinstance(key, dict) else key,
                "group_name": n.get("groupName"), "job_names": n.get("jobNames"),
                "freshness_policy": n.get("freshnessPolicy"), "automation_condition": n.get("automationCondition"),
                "has_asset_checks": n.get("hasAssetChecks"), "partition_stats": n.get("partitionStats"),
                "location_name": loc_name,
            })
            if len(rows) >= int(limit or 200):
                break
        return {"count": len(rows), "assets": rows}

    def _list_app_managed_components(self, location_name: str) -> List[Dict[str, Any]]:
        if not isinstance(location_name, str) or not location_name.strip():
            raise DiscoveryToolError("location_name is required (call list_locations first to find one).")
        result_type = (self._client.introspect_type("AppManagedComponentsForLocationOrError") or {})
        # Discover the real success type's name live rather than guessing it (see module docstring);
        # it's whichever possible type ISN'T the generic error type.
        success_type = next(
            (pt["name"] for pt in (result_type.get("possibleTypes") or []) if "Error" not in pt["name"]),
            None,
        )
        selection = "__typename ... on PythonError { message }"
        if success_type:
            inner = auto_selection(self._client, success_type, depth=2)
            selection += f" ... on {success_type} {{ {inner} }}"
        query = f"query($locationName: String!) {{ appManagedComponentsForLocationOrError(locationName: $locationName) {{ {selection} }} }}"
        data = self._client.call(query, {"locationName": location_name})
        result = data.get("appManagedComponentsForLocationOrError") or {}
        if result.get("__typename") == "PythonError":
            raise DiscoveryToolError(f"appManagedComponentsForLocationOrError({location_name!r}): {result.get('message')}")
        # The success payload's own list field is named differently across schema versions ('components',
        # 'results', ...) -- take the first list-of-objects field found rather than hard-coding one name.
        for v in result.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        return []

    def t_list_components(self, location_name: Any = None, type: Any = None) -> Dict[str, Any]:
        rows = self._list_app_managed_components(location_name)
        if type:
            type_lower = str(type).lower()
            rows = [r for r in rows if type_lower in str(r.get("type", "")).lower()]
        return {"location_name": location_name, "count": len(rows), "components": rows}

    def t_get_component(self, location_name: Any = None, id: Any = None) -> Dict[str, Any]:
        if not id:
            raise DiscoveryToolError("get_component requires id (from list_components).")
        rows = self._list_app_managed_components(location_name)
        for r in rows:
            if str(r.get("id")) == str(id):
                return r
        raise DiscoveryToolError(f"no component with id {id!r} in location {location_name!r}")

    def t_list_agents(self, location_name: Any = None) -> Dict[str, Any]:
        """Agent components, identified by their `type` string (see _AGENT_TYPE_HINTS) -- with their
        server/allowed-tools config, read straight from the component's own YAML. 'last outcome' is NOT
        included here: that lives on the agent's own outcome asset's materialization metadata (see the
        ledger/outcomes pattern), not on the component's static config -- combine with dagster_instance_tools'
        asset_status on the agent's outcome asset key for that."""
        rows = self._list_app_managed_components(location_name)
        agents = [r for r in rows if any(h in str(r.get("type", "")).lower() for h in _AGENT_TYPE_HINTS)]
        return {"location_name": location_name, "count": len(agents), "agents": agents}

    def t_list_mcp_servers(self, location_name: Any = None) -> Dict[str, Any]:
        rows = self._list_app_managed_components(location_name)
        servers = [r for r in rows if any(h in str(r.get("type", "")).lower().replace(" ", "") for h in _MCP_SERVER_TYPE_HINTS)]
        return {"location_name": location_name, "count": len(servers), "mcp_servers": servers}

    def t_list_alert_policies(self) -> Dict[str, Any]:
        selection = auto_selection(self._client, "AlertPolicy", depth=1)
        query = f"query {{ alertPolicies {{ {selection} }} }}"
        data = self._client.call(query)
        policies = data.get("alertPolicies") or []
        return {"count": len(policies), "alert_policies": policies}


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>` -- same mechanism as
    dagster_api_check_tools'/dagster_instance_tools' own copies (duplicated here, not imported, per this
    repo's self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "dagster_discovery_tools's mcp_server_resource_key path must be run as `python -m <dotted.path.to.this.module>`, "
        "not `python component.py` directly -- that bare-script form still works for `python component.py` itself, "
        "just not for this path. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined here rather than imported from
    resources/mcp_server_resource (confirmed not importable across components). Same copy
    dagster_api_check_tools/dagster_instance_tools/dagster_workflow_tools each carry."""

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


class DagsterDiscoveryToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers read-only Dagster+ discovery tools as a resource (default key `dagster_discovery_tools`).
    For an agent (MCP only), run THIS FILE as the stdio MCP server -- see README.md.

    Example:

    ```yaml
    type: dagster_community_components.DagsterDiscoveryToolsComponent
    attributes:
      deployment_url: auto
      token_env_var: DAGSTER_CLOUD_API_TOKEN
      mcp_server_resource_key: discovery_tools_server
    ```
    """

    resource_key: str = Field(default="dagster_discovery_tools", description="Resource key the toolset is registered under.")
    deployment_url: str = Field(
        default=_AUTO,
        description="Full Dagster+ deployment URL, e.g. https://acme.dagster.cloud/prod. 'auto' derives it at run "
                     "time from DAGSTER_CLOUD_DEPLOYMENT_NAME + the instance's Dagster+ organization -- resolvable "
                     "for the plain resource (it sees the real DagsterInstance), but NOT for mcp_server_resource_key "
                     "(that spawns a separate subprocess with no DagsterInstance at all): set deployment_url "
                     "explicitly whenever mcp_server_resource_key is also set. An explicit value always wins.",
    )
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN", description="Env var holding a Dagster+ API token with read access to this deployment.")
    allow: Optional[List[str]] = Field(default=None, description=f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}.")
    max_result_chars: int = Field(default=12000, description="Upper bound on the serialized size of any tool result.")
    timeout_seconds: float = Field(default=30.0, description="HTTP timeout per GraphQL call.")
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description="If set, ALSO registers a stdio MCP-server-shaped resource under this key (python -m this "
                     "module, with DAGSTER_DISCOVERY_TOOLS_* env vars set from this component's own config). Any "
                     "mcp_servers-aware component can then reference it via server_resource_key.",
    )
    mcp_extra_env_vars: Optional[List[str]] = Field(default=None, description="mcp_server_resource_key only. Extra env var NAMES to copy into the spawned MCP server's env.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow", "mcp_extra_env_vars"), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Dagster Discovery Tools", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(ALL_TOOLS)}")
        max_chars = int(self.max_result_chars)
        deployment_url, token_env_var, timeout = self.deployment_url, self.token_env_var, self.timeout_seconds

        def _init(init_context):
            url = resolve_deployment_url(deployment_url, init_context.instance)
            token = os.environ.get(token_env_var, "")
            if not token:
                raise ValueError(f"DagsterDiscoveryToolsComponent: env var {token_env_var!r} is empty or unset.")
            client = _Client(url, token, token_env_var=token_env_var, timeout=timeout)
            return DagsterDiscoveryTools(client, allow=allow, max_result_chars=max_chars)

        res = dg.ResourceDefinition(
            resource_fn=_init,
            description="Read-only 'what exists in this deployment' tools over the Dagster+ GraphQL API.",
        )
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {
                "DAGSTER_DISCOVERY_TOOLS_MAX_CHARS": str(max_chars),
                "DAGSTER_DISCOVERY_TOOLS_DEPLOYMENT_URL": deployment_url,
                "DAGSTER_DISCOVERY_TOOLS_TOKEN_ENV_VAR": token_env_var,
            }
            if allow:
                mcp_env["DAGSTER_DISCOVERY_TOOLS_ALLOW"] = ",".join(allow)
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key, type="stdio", command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env, live_env_passthrough=list(self.mcp_extra_env_vars or []) + [token_env_var, _DEPLOYMENT_ENV],
            )

        return dg.Definitions(resources=resources)


# ── stdio MCP server entrypoint:  python -m <this module>  ──────────────────────────────────────────────────────────
def build_mcp_server(tools: DagsterDiscoveryTools):
    """MCP server (official `mcp` package) exposing exactly the allowed tools. Works on mcp 1.x and 2.x."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server("dagster-discovery-tools")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__ in allowed:
            mcp.tool()(fn)
        return fn

    @_tool
    def list_locations() -> dict:
        """Every code location in this deployment: load status, load errors, Dagster library versions,
        and per-repository schedule/sensor counts. Call this first -- most other tools need a location_name."""
        return tools.call("list_locations")

    @_tool
    def list_assets(location_name: str = None, group_name: str = None, limit: int = 200) -> dict:
        """Assets across the deployment: group_name, freshness policy, automation condition, whether it has
        asset checks, partition stats. Optionally filter by location_name and/or group_name."""
        return tools.call("list_assets", {"location_name": location_name, "group_name": group_name, "limit": limit})

    @_tool
    def list_components(location_name: str, type: str = None) -> dict:
        """Every UI-created (app-managed) component in location_name, with its id, type, and YAML config.
        Optionally filter to types whose name contains `type` (case-insensitive substring)."""
        return tools.call("list_components", {"location_name": location_name, "type": type})

    @_tool
    def get_component(location_name: str, id: str) -> dict:
        """One component's full config by id (from list_components)."""
        return tools.call("get_component", {"location_name": location_name, "id": id})

    @_tool
    def list_agents(location_name: str = None) -> dict:
        """The agent components in location_name (or every location if omitted), with their configured
        model/server/allowed-tools. Does NOT include 'last outcome' -- read that from the agent's own
        outcome asset's materialization metadata via dagster_instance_tools.asset_status."""
        return tools.call("list_agents", {"location_name": location_name})

    @_tool
    def list_mcp_servers(location_name: str = None) -> dict:
        """The MCP-server-shaped components in location_name (or every location if omitted)."""
        return tools.call("list_mcp_servers", {"location_name": location_name})

    @_tool
    def list_schedules_and_sensors(location_name: str = None) -> dict:
        """Schedules and sensors per repository, optionally filtered to one code location."""
        return tools.call("list_schedules_and_sensors", {"location_name": location_name})

    @_tool
    def list_runs(location_name: str, limit: int = 20, statuses: list = None) -> dict:
        """Recent runs in ONE code location (location_name is required -- a deployment-wide list mixes
        concurrent, unrelated runs). Optional statuses, e.g. ['FAILURE']."""
        return tools.call("list_runs", {"location_name": location_name, "limit": limit, "statuses": statuses})

    @_tool
    def list_alert_policies() -> dict:
        """Alert policies configured on this deployment."""
        return tools.call("list_alert_policies")

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: DAGSTER_DISCOVERY_TOOLS_DEPLOYMENT_URL (default 'auto'), _TOKEN_ENV_VAR (default
    DAGSTER_CLOUD_API_TOKEN), _ALLOW (csv), _MAX_CHARS (default 12000)."""
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel

    deployment_url = os.environ.get("DAGSTER_DISCOVERY_TOOLS_DEPLOYMENT_URL", _AUTO)
    token_env_var = os.environ.get("DAGSTER_DISCOVERY_TOOLS_TOKEN_ENV_VAR", "DAGSTER_CLOUD_API_TOKEN")
    allow_env = os.environ.get("DAGSTER_DISCOVERY_TOOLS_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None
    max_chars = int(os.environ.get("DAGSTER_DISCOVERY_TOOLS_MAX_CHARS", "12000"))

    if deployment_url.strip().lower() == _AUTO:
        # 'auto' needs instance.dagit_url for the org/region -- unavailable in THIS process: the
        # mcp_server_resource_key path spawns `python -m this module` as a separate subprocess with no
        # DagsterInstance at all (confirmed: Dagster+ run workers set DAGSTER_CLOUD_DEPLOYMENT_NAME but
        # never an org-bearing env var -- see resolve_deployment_url's docstring). Fail with a message
        # that names the actual limitation, not the generic "not running in Dagster+" one.
        print("ERROR: deployment_url: auto cannot be resolved for the stdio MCP server process -- it has no "
              "DagsterInstance (unlike the plain resource path, which resolves 'auto' fine). Set deployment_url "
              "explicitly on DagsterDiscoveryToolsComponent when using mcp_server_resource_key, e.g. "
              "https://<org>.dagster.cloud/<deployment>.", file=sys.stderr)
        sys.exit(1)
    url = validate_deployment_url(deployment_url)
    token = os.environ.get(token_env_var, "")
    if not token:
        print(f"ERROR: env var {token_env_var!r} is empty or unset", file=sys.stderr)
        sys.exit(1)
    client = _Client(url, token, token_env_var=token_env_var)
    tools = DagsterDiscoveryTools(client, allow=allow, max_result_chars=max_chars)
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
