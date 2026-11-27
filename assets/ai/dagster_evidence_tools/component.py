"""DagsterEvidenceToolsComponent -- read-only deployment-HEALTH evidence tools for an "improvement advisor"
agent, over the Dagster+ GraphQL API, GitHub, PyPI and OSV.dev. Ported from `ops_tools.py` in an internal
engineering-agents reference sandbox (the hand-written Python a real production deployment runs these
agents from today), through THIS repo's own conventions -- same dual-mode shape, GraphQL-client/auth/
error-shaping style, and allow-list/audit/size-cap dispatcher as its sibling `dagster_discovery_tools`.

`dagster_discovery_tools` answers "what exists in this deployment". This component answers "is it
healthy, and what changed" -- three tools:
  project_overview(location)  -- for ONE code location: every asset's freshness/automation/schedules/
                                  repository-file refs, steps that retried in the last N days (error +
                                  whether the retry ultimately succeeded), partition + asset-check health
                                  (pass/fail/stale counts), slower-than-before runs, pinned-dependency
                                  versions + known OSV vulnerabilities (with a fixed-in version when OSV
                                  has one), failures in the last 24h, and a `repo_notes` passthrough for
                                  what's deliberate in this deployment (same idea as the `context_notes`
                                  the *_agent components take -- see litellm_agent/README.md).
  deprecation_report(max_files) -- deprecated/superseded/removed Dagster API usage across the repository's
                                  Python files, via `dagster._annotations` (ast-scanned, not a guess).
  check_dagster_api(names)    -- whether Dagster names exist in the installed Dagster + their real signatures.
  latest_dagster_release()    -- the newest published `dagster` version (PyPI) + its GitHub release notes.

Same dual-mode shape as `dagster_discovery_tools`/`dagster_instance_tools`/`dagster_workflow_tools`:
  1. An importable toolset (`DagsterEvidenceTools`, plus the component that registers it as a Dagster
     resource).
  2. A self-contained stdio MCP server: `python -m <dotted.path.to.this.module>`.

Honesty about what's verified vs. best-effort:
  - `assetNodes` (with `freshnessPolicy`, `automationCondition`, `hasAssetChecks`, `partitionStats`,
    `assetChecksOrError { ... on AssetChecks { checks { executionForLatestMaterialization { status
    timestamp } } } }`, `metadataEntries { ... on CodeReferencesMetadataEntry { codeReferences { ... on
    LocalFileCodeReference { filePath lineNumber } ... on UrlCodeReference { url } } } }`), `workspaceOrError
    { ... on Workspace { locationEntries { locationOrLoadError { ... on RepositoryLocation {
    dagsterLibraryVersions, repositories { schedules } } } } } }`, `runsOrError(limit)` with
    `Run.repositoryOrigin.repositoryLocationName`, and `logsForRun` with `ExecutionStepUpForRetryEvent`/
    `ExecutionStepFailureEvent` are confirmed REAL, OSS GraphQL fields -- verified directly by introspecting
    the installed `dagster_graphql` package's own schema classes (`GrapheneAssetNode`, `GrapheneRun`,
    `GrapheneRepositoryOrigin`, `GrapheneFreshnessPolicy`, `dagster_graphql.schema.asset_checks`,
    `dagster_graphql.schema.logs.events`), not guessed and not merely assumed from the reference code.
    Every query this component sends is also validated against a subset of the real Dagster+ schema
    (`tests/fixtures/dagster_plus_schema_subset.graphql`, generated from dagster-cloud's own schema by
    `tests/fixtures/make_dagster_plus_schema_subset.py`), because Dagster+ is stricter than OSS: e.g. it
    rejects `... on PythonError` under `assetChecksOrError`, whose union is only `AssetChecks |
    AssetCheckNeedsMigrationError | AssetCheckNeedsUserCodeUpgrade`, while `dagster_graphql` accepts it.
    `alertPoliciesForCodeLocation(codeLocationName)` is Dagster+-only and is checked the same way.
  - `RunsFilter` has NO `repositoryOrigin` filter argument in the OSS schema (confirmed: absent from
    `GrapheneRunsFilter`) -- so, like the reference code, this component fetches `runsOrError(limit)`
    UNFILTERED and filters client-side on each run's own `repositoryOrigin.repositoryLocationName`,
    rather than relying on a Dagster+-only filter extension.
  - PyPI's `https://pypi.org/pypi/<project>/json` and OSV.dev's `POST https://api.osv.dev/v1/query`
    (`{"package": {"name", "ecosystem": "PyPI"}, "version"}` -> `{"vulns": [{"id", "affected": [{"ranges":
    [{"events": [{"introduced"|"fixed": "..."}]}]}]}]}`) were both confirmed live against the real APIs
    while building this component -- the `fixed` event is a real, present field (not in the reference
    code's own `dependency_report`, which only surfaces vulnerability ids; this component additionally
    extracts the fixed-in version since OSV's real response already carries it).
  - "Package version changes in the last N days" (a historical, over-time diff of installed library
    versions) is NOT implemented -- neither the reference code nor Dagster+'s GraphQL schema exposes a
    historical snapshot of `dagsterLibraryVersions` to diff against. `project_overview` instead reports
    the CURRENT running version per declared dependency (cross-referenced against `dagsterLibraryVersions`,
    which only covers dagster-family packages, not arbitrary third-party ones -- see `dependency_report`'s
    docstring) alongside the latest published `dagster` release, matching the one time-comparison the
    reference code (and this component) can actually make.
  - "Incidents in the last 24h" is runsOrError(status=FAILURE) in the last 24h + the failing step's error,
    exactly what the reference code's own `incidents_last_24h` does -- NOT alert-policy-fired events.
    The alert policies that apply to the location are reported separately (`alert_policies`/`alert_policy_count`).
  - `deprecation_report` reads Python files from GitHub (via `repo`/`base_branch`, the same auth/traversal-
    safety conventions `github_pr_tools` already established in this repo -- `_gh_call`/`_gh_safe_path`/
    `_gh_path_matches_any`, inlined here per this repo's self-contained-component convention) and scans them
    with `dagster._annotations.{is_deprecated,get_deprecated_info,is_superseded,get_superseded_info,
    has_deprecated_params,get_deprecated_params,get_deprecated_param_info}` -- all confirmed present on the
    installed `dagster` package. It needs `repo` configured; without it, it raises a clear error rather than
    silently returning nothing.
"""
import ast
import importlib
import os
import re
import sys
import time
try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]
import urllib.error
import urllib.request
from collections import defaultdict
from typing import Any, Callable, Dict, List, Literal, Optional
from urllib.parse import urlparse

import dagster as dg
import requests
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


# ── deployment URL resolution (same verified mechanism as dagster_discovery_tools/insights_custom_metrics_sync) ────
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
    Same resolution path as dagster_discovery_tools.resolve_deployment_url (duplicated here, not imported, per
    this repo's self-contained-component convention)."""
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


class EvidenceToolError(Exception):
    """A user-facing, safe error message (returned to the agent as {"error": ...})."""


# ── GraphQL client with transient-error retry (verbatim copy of dagster_discovery_tools._Client -- same
# proven mechanism, inlined per this repo's self-contained-component convention) ────────────────────────────────────
class _Client:
    def __init__(self, deployment_url: str, token: str, *, token_env_var: str = "DAGSTER_CLOUD_API_TOKEN",
                 timeout: float = 30.0, max_retries: int = 3, backoff_seconds: float = 1.0,
                 max_backoff_seconds: float = 15.0, sleep: Callable[[float], None] = time.sleep):
        import random
        self.endpoint = deployment_url.rstrip("/") + "/graphql"
        self._token, self.token_env_var = token, token_env_var
        self.timeout, self.max_retries = timeout, max_retries
        self.backoff, self.max_backoff = backoff_seconds, max_backoff_seconds
        self.sleep, self.rng = sleep, random.Random()
        self.requests = 0

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
        import json
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
                    raise EvidenceToolError(f"Dagster+ GraphQL errors: {self._redact(json.dumps(payload['errors'])[:500])}")
                return payload.get("data") or {}
            except urllib.error.HTTPError as e:
                text = self._redact(e.read().decode("utf-8", errors="replace")[:300])
                if e.code in (401, 403):
                    raise EvidenceToolError(
                        f"Dagster+ rejected the API token (HTTP {e.code}); check env var {self.token_env_var} holds a "
                        "token with access to this deployment") from None
                if e.code == 429 or 500 <= e.code < 600:
                    retry_after, why = e.headers.get("Retry-After") if e.headers else None, f"HTTP {e.code}"
                else:
                    raise EvidenceToolError(f"Dagster+ GraphQL HTTP {e.code}: {text}") from None
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
                why = f"{type(e).__name__}: {self._redact(str(e))[:200]}"
            except ValueError:
                raise EvidenceToolError("Dagster+ returned a non-JSON response; is deployment_url correct?") from None
            if attempt >= self.max_retries:
                raise EvidenceToolError(f"Dagster+ unreachable after {self.max_retries + 1} attempts ({why})") from None
            self.sleep(self._delay(attempt, retry_after))
        raise EvidenceToolError("unreachable")  # pragma: no cover -- loop above always returns or raises


# ── GitHub client (same conventions as github_pr_tools: _gh_call/_gh_safe_path/_gh_path_matches(_any),
# inlined here rather than imported, per this repo's self-contained-component convention) ───────────────────────────
class GithubScopeError(EvidenceToolError):
    pass


def _gh_safe_path(path: str) -> str:
    path = path.strip().lstrip("/")
    if ".." in path.split("/") or path.startswith(".git/"):
        raise GithubScopeError(f"Invalid path: {path!r}")
    return path


def _gh_path_matches(path: str, pattern: str) -> bool:
    """Same matching convention as github_pr_tools' protected_paths/exclude_paths (ported verbatim): a
    directory-like pattern (ends with '/') matches that run of segments anywhere in the path's directory
    portion, not just at the root; a filename-like pattern (no trailing '/') matches the file's own basename
    at any depth, or a root-level prefix."""
    segments = path.split("/")
    if pattern.endswith("/"):
        pat_segments = pattern.rstrip("/").split("/")
        dir_segments = segments[:-1]
        n = len(pat_segments)
        return any(dir_segments[i:i + n] == pat_segments for i in range(len(dir_segments) - n + 1))
    basename = segments[-1]
    return basename.startswith(pattern) or path.startswith(pattern)


def _gh_path_matches_any(path: str, patterns) -> bool:
    return any(_gh_path_matches(path, p) for p in patterns)


def _gh_call(method: str, repo: str, path: str, *, token: str, api_base_url: str, **kwargs) -> Any:
    resp = requests.request(
        method,
        f"{api_base_url}/repos/{repo}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=30,
        **kwargs,
    )
    if resp.status_code >= 400:
        raise GithubScopeError(f"GitHub {method} {path} failed ({resp.status_code}): {resp.text[:300]}")
    return resp.json() if resp.content else {}


def _gh_read_file(repo: str, path: str, ref: str, *, token: str, api_base_url: str) -> str:
    import base64
    item = _gh_call("GET", repo, f"/contents/{_gh_safe_path(path)}", token=token, api_base_url=api_base_url, params={"ref": ref})
    return base64.b64decode(item["content"]).decode("utf-8")


def _gh_list_tree(repo: str, ref: str, *, token: str, api_base_url: str, exclude_paths=()) -> List[Dict[str, Any]]:
    """Every file (git calls it a 'blob') in the repo at `ref`, via GitHub's recursive git-trees API
    (`GET /git/trees/{ref}?recursive=1`) -- the same endpoint the reference code's `github_scope` uses for
    `deprecation_report`/`dependency_report`'s repo-wide file listing. Directories ('tree' entries) and any
    path matching exclude_paths are dropped."""
    data = _gh_call("GET", repo, f"/git/trees/{ref}", token=token, api_base_url=api_base_url, params={"recursive": "1"})
    return [
        {"path": t["path"], "type": t["type"]}
        for t in (data.get("tree") or [])
        if t.get("type") == "blob" and not _gh_path_matches_any(t["path"], exclude_paths)
    ]


# ── deprecated-API static analysis (ported verbatim from the reference code's _scan_source/_resolve_dagster;
# depends only on the installed `dagster` package, confirmed present: dagster._annotations.{is_deprecated,
# get_deprecated_info, is_superseded, get_superseded_info, has_deprecated_params, get_deprecated_params,
# get_deprecated_param_info}) ─────────────────────────────────────────────────────────────────────────────────────
def _resolve_dagster(chain: List[str], aliases: Dict[str, str]):
    """Resolve a dotted name rooted at `dagster` or an alias of it, one hop at a time."""
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
                findings.append(
                    {"path": path, "line": line, "name": f"{name}({param}=...)", "status": "deprecated parameter",
                     "breaking_version": getattr(pinfo, "breaking_version", None),
                     "note": getattr(pinfo, "additional_warn_text", None)}
                )
            return
        if info:
            findings.append(
                {"path": path, "line": line, "name": name, "status": "deprecated",
                 "breaking_version": info.breaking_version, "note": info.additional_warn_text}
            )
        elif sup:
            findings.append(
                {"path": path, "line": line, "name": name, "status": "superseded",
                 "breaking_version": None, "note": getattr(sup, "additional_warn_text", None)}
            )

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
    seen: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] == "dagster":
            try:
                module = importlib.import_module(node.module)
            except ImportError:
                continue
            for a in node.names:
                if not hasattr(module, a.name):
                    findings.append({"path": path, "line": node.lineno, "name": f"{node.module}.{a.name}",
                                     "status": "removed", "breaking_version": None, "note": "no longer exists"})
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
                findings.append({"path": path, "line": node.lineno, "name": f"dagster.{chain[1]}",
                                 "status": "removed", "breaking_version": None, "note": "no longer exists"})
            elif obj is not None:
                judge(obj, f"dagster.{chain[1]}", node.lineno)
        if isinstance(node, ast.Call):
            fchain = chain_of(node.func) if isinstance(node.func, ast.Attribute) else (
                [node.func.id] if isinstance(node.func, ast.Name) else None)
            if fchain:
                obj, state = _resolve_dagster(fchain, aliases) if len(fchain) > 1 else (None, "unrooted")
                if len(fchain) == 1 and fchain[0] in aliases and aliases[fchain[0]] != "dagster":
                    mod, _, attr = aliases[fchain[0]].rpartition(".")
                    try:
                        obj, state = getattr(importlib.import_module(mod), attr, None), "ok"
                    except ImportError:
                        obj, state = None, "unrooted"
                if obj is not None and state == "ok":
                    for kw in node.keywords:
                        if kw.arg:
                            judge(obj, ".".join(fchain), node.lineno, param=kw.arg)
    return findings


def _median(values: List[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


def _last_line(message: Optional[str], cap: int = 240) -> str:
    msg = (message or "").strip()
    lines = msg.splitlines()
    return lines[-1][:cap] if lines else ""


def _error_message(error: Optional[Dict[str, Any]], cap: int = 240) -> str:
    """Deepest cause message of a GraphQL PythonError payload ({message, cause{message}, causes[{message}]}),
    same precedence the reference code uses: the last entry of `causes` if present, else `cause`, else the
    error's own `message`."""
    error = error or {}
    causes = error.get("causes") or []
    if causes:
        return _last_line(causes[-1].get("message"), cap)
    cause = error.get("cause")
    if cause:
        return _last_line(cause.get("message"), cap)
    return _last_line(error.get("message"), cap)


def _code_references(node: Dict[str, Any], limit: int = 5) -> List[Dict[str, Any]]:
    """Repository files that define an asset, from its `CodeReferencesMetadataEntry` -- a `LocalFileCodeReference`
    is used directly (filePath/lineNumber); a `UrlCodeReference` (the Dagster+ GitHub link) is parsed the same
    way the reference code's `_code_paths` does."""
    found: List[Dict[str, Any]] = []
    for entry in node.get("metadataEntries") or []:
        if entry.get("__typename") != "CodeReferencesMetadataEntry":
            continue
        for ref in entry.get("codeReferences") or []:
            item = None
            if ref.get("__typename") == "LocalFileCodeReference" and ref.get("filePath"):
                item = {"path": ref["filePath"], "line": int(ref.get("lineNumber") or 0)}
            elif ref.get("__typename") == "UrlCodeReference":
                m = re.search(r"/(?:tree|blob)/[0-9a-f]{7,40}/(?P<path>[^#?]+)(?:#L(?P<line>\d+))?", ref.get("url") or "")
                if m:
                    item = {"path": m["path"], "line": int(m["line"] or 0)}
            if item and item not in found:
                found.append(item)
    return found[:limit]


def _check_health(node: Dict[str, Any]) -> Dict[str, Any]:
    """Pass/fail/stale counts for one asset's checks, from `assetChecksOrError`. 'Stale' means the check has
    never run against the asset's latest materialization (executionForLatestMaterialization is null) or is
    still IN_PROGRESS -- not a guess at a staleness threshold, since Dagster+ doesn't expose one."""
    result = node.get("assetChecksOrError") or {}
    if result.get("__typename") != "AssetChecks":
        return {"passing": 0, "failing": 0, "stale": 0, "failing_checks": []}
    passing = failing = stale = 0
    failing_checks = []
    for c in result.get("checks") or []:
        execution = c.get("executionForLatestMaterialization")
        status = (execution or {}).get("status")
        if not execution or status == "IN_PROGRESS":
            stale += 1
        elif status == "SUCCEEDED":
            passing += 1
        elif status in ("FAILED", "EXECUTION_FAILED"):
            failing += 1
            failing_checks.append({"name": c.get("name"), "status": status})
        else:
            stale += 1
    return {"passing": passing, "failing": failing, "stale": stale, "failing_checks": failing_checks}


def _partition_health(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    stats = node.get("partitionStats")
    if not stats:
        return None
    missing = stats["numPartitions"] - stats["numMaterialized"] - stats["numFailed"] - stats["numMaterializing"]
    if not (stats["numFailed"] or missing):
        return None
    return {"partitions": stats["numPartitions"], "materialized": stats["numMaterialized"],
            "failed": stats["numFailed"], "missing": missing}


# ── GraphQL queries (every field below is confirmed present in the installed dagster_graphql package's own
# schema classes -- see module docstring) ───────────────────────────────────────────────────────────────────────────
_ASSETS_QUERY = """
query {
  assetNodes {
    assetKey { path }
    groupName
    jobNames
    freshnessPolicy { maximumLagMinutes cronSchedule }
    automationCondition { label }
    hasAssetChecks
    partitionStats { numMaterialized numPartitions numFailed numMaterializing }
    metadataEntries {
      __typename
      ... on CodeReferencesMetadataEntry {
        codeReferences {
          __typename
          ... on LocalFileCodeReference { filePath lineNumber }
          ... on UrlCodeReference { url }
        }
      }
    }
    assetChecksOrError {
      __typename
      ... on AssetChecks { checks { name executionForLatestMaterialization { status timestamp } } }
    }
    repository { location { name } }
  }
}
"""

# Dagster+-only field (the policies that apply to one code location); the component already targets Dagster+ only.
_ALERT_POLICIES_QUERY = """
query($location: String!) {
  alertPoliciesForCodeLocation(codeLocationName: $location) { id name description enabled eventTypes }
}
"""

_WORKSPACE_QUERY = """
query {
  workspaceOrError {
    __typename
    ... on Workspace {
      locationEntries {
        name
        locationOrLoadError {
          __typename
          ... on RepositoryLocation {
            dagsterLibraryVersions { name version }
            repositories { schedules { name cronSchedule pipelineName scheduleState { status } } }
          }
          ... on PythonError { message }
        }
      }
    }
    ... on PythonError { message }
  }
}
"""

_RUNS_QUERY = """
query($limit: Int) {
  runsOrError(limit: $limit) {
    __typename
    ... on Runs {
      results {
        runId status pipelineName startTime endTime
        assetSelection { path }
        repositoryOrigin { repositoryLocationName }
      }
    }
    ... on PythonError { message }
  }
}
"""

_RETRY_EVENTS_QUERY = """
query($id: ID!) {
  logsForRun(runId: $id) {
    __typename
    ... on EventConnection {
      events {
        __typename
        ... on ExecutionStepUpForRetryEvent { stepKey error { message cause { message } causes { message } } }
      }
    }
    ... on PythonError { message }
  }
}
"""

_FAILURE_EVENTS_QUERY = """
query($id: ID!) {
  logsForRun(runId: $id) {
    __typename
    ... on EventConnection {
      events {
        __typename
        ... on ExecutionStepFailureEvent { stepKey error { message cause { message } causes { message } } }
      }
    }
    ... on PythonError { message }
  }
}
"""


# Inlined verbatim from dagster_api_check_tools (self-contained-component convention); a drift test keeps the two equal.
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


ALL_TOOLS = ("project_overview", "deprecation_report", "latest_dagster_release", "check_dagster_api")


class DagsterEvidenceTools:
    """`call(tool, args)` is the one safe entry point (allow-list, audit, size cap, error shaping); the `t_*`
    methods are the raw implementations. Every tool is read-only."""

    def __init__(
        self, client: _Client, *,
        deployment_url: str = "",
        repo: Optional[str] = None, github_token: str = "", github_api_base_url: str = "https://api.github.com",
        base_branch: str = "main", exclude_paths: Optional[List[str]] = None,
        exclude_locations: Optional[List[str]] = None, include_locations: Optional[List[str]] = None, repo_notes: str = "",
        allow: Optional[List[str]] = None, max_result_chars: int = 12000,
        retry_window_days: int = 7, runtime_change_window_days: int = 14, incident_window_hours: int = 24,
        max_runs_scanned: int = 100, max_files_scanned: int = 40, osv_max_packages: int = 25,
        audit: Optional[Callable[[str], None]] = None, now: Callable[[], float] = time.time,
    ):
        import logging
        self._client = client
        self._deployment_url = deployment_url
        self._repo = repo
        self._github_token, self._github_api_base_url, self._base_branch = github_token, github_api_base_url, base_branch
        self._exclude_paths = list(exclude_paths or ())
        self._exclude_locations = frozenset(exclude_locations or ())
        self._include_locations = frozenset(include_locations or ())  # empty = every location; exclude wins
        self._repo_notes = repo_notes or ""
        self._retry_window_days = max(1, int(retry_window_days))
        self._runtime_change_window_days = max(1, int(runtime_change_window_days))
        self._incident_window_hours = max(1, int(incident_window_hours))
        self._max_runs_scanned = max(1, int(max_runs_scanned))
        self._max_files_scanned = max(1, int(max_files_scanned))
        self._osv_max_packages = max(1, int(osv_max_packages))
        self._now = now
        if allow is None:
            allow = list(ALL_TOOLS)
        unknown = sorted(set(allow) - set(ALL_TOOLS))
        if unknown:
            raise ValueError(f"unknown tool(s) in allow: {unknown}; valid: {list(ALL_TOOLS)}")
        self.allow = frozenset(allow)
        self.max_result_chars = max(1000, int(max_result_chars))
        self._audit = audit or (lambda line: logging.getLogger("dagster_evidence_tools.audit").info(line))

    def tool_names(self) -> List[str]:
        return [t for t in ALL_TOOLS if t in self.allow]

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        import json
        args = dict(args or {})
        try:
            self._audit(f"dagster_evidence_tools call tool={tool} args={json.dumps(args, default=str)[:300]}")
        except Exception:
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except EvidenceToolError as e:
            return {"error": str(e)}
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {e}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"}
        return self._fit(result)

    def _fit(self, result: Dict[str, Any]) -> Dict[str, Any]:
        import json
        cap = self.max_result_chars
        text = json.dumps(result, default=str)
        if len(text) <= cap:
            return result
        return {"truncated": True, "preview": text[: max(0, cap - 40)]}

    def _run_url(self, run_id: str) -> str:
        return f"{self._deployment_url}/runs/{run_id}" if self._deployment_url else run_id

    # -- check_dagster_api --------------------------------------------------------------------------------------
    def t_check_dagster_api(self, names: List[str]) -> Dict[str, Any]:
        """Whether each dotted Dagster name (`dg.load_from_defs_folder`, `dagster.Definitions`, ...) exists in the
        Dagster installed where this server runs, and its real signature -- so an agent can check an API while it
        reasons, not only after it has written an issue body. At most 12 names per call."""
        if isinstance(names, str):
            names = [names]
        if not names or not all(isinstance(n, str) and n.strip() for n in names):
            raise EvidenceToolError("check_dagster_api requires `names`: a non-empty list of dotted Dagster names.")
        return _check_dagster_api([n.strip() for n in names])

    # -- latest_dagster_release ---------------------------------------------------------------------------------
    def t_latest_dagster_release(self) -> Dict[str, Any]:
        """The newest published `dagster` version, via PyPI's documented JSON API, plus its GitHub release
        notes (best-effort -- the GitHub call is unauthenticated and public-rate-limited; a failure there
        still returns the PyPI version)."""
        resp = requests.get("https://pypi.org/pypi/dagster/json", timeout=30)
        resp.raise_for_status()
        version = resp.json()["info"]["version"]
        notes, url = "", f"https://github.com/dagster-io/dagster/releases/tag/{version}"
        try:
            gh = requests.get(
                f"https://api.github.com/repos/dagster-io/dagster/releases/tags/{version}",
                headers={"Accept": "application/vnd.github+json"}, timeout=30,
            )
            if gh.ok:
                body = gh.json()
                notes = (body.get("body") or "")[:6000]
                url = body.get("html_url", url)
        except requests.RequestException:
            pass
        return {"latest_version": version, "notes": notes, "url": url}

    # -- deprecation_report --------------------------------------------------------------------------------------
    def t_deprecation_report(self, max_files: Optional[int] = None) -> Dict[str, Any]:
        if not self._repo:
            raise EvidenceToolError("deprecation_report requires `repo` to be configured ('owner/repo' to scan).")
        cap = max(1, int(max_files)) if max_files else self._max_files_scanned
        tree = _gh_list_tree(self._repo, self._base_branch, token=self._github_token,
                              api_base_url=self._github_api_base_url, exclude_paths=self._exclude_paths)
        paths = [t["path"] for t in tree if t["path"].endswith(".py")][:cap]
        findings: List[Dict[str, Any]] = []
        for path in paths:
            content = _gh_read_file(self._repo, path, self._base_branch, token=self._github_token,
                                     api_base_url=self._github_api_base_url)
            findings += _scan_source(path, content)
        unique = {(f["path"], f["line"], f["name"]): f for f in findings}
        return {
            "checked_against_dagster": dg.__version__,
            "files_scanned": len(paths),
            "findings": sorted(unique.values(), key=lambda f: (f["path"], f["line"])),
        }

    # -- project_overview and its sub-reports --------------------------------------------------------------------
    def _fetch_runs_in_location(self, location: str) -> List[Dict[str, Any]]:
        data = self._client.call(_RUNS_QUERY, {"limit": self._max_runs_scanned})
        result = data.get("runsOrError") or {}
        if result.get("__typename") == "PythonError":
            raise EvidenceToolError(f"runsOrError: {result.get('message')}")
        runs = result.get("results") or []
        return [r for r in runs if ((r.get("repositoryOrigin") or {}).get("repositoryLocationName")) == location]

    def _retry_report(self, runs: List[Dict[str, Any]]) -> Dict[str, Any]:
        cutoff = self._now() - self._retry_window_days * 86400
        steps: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"runs_with_retries": 0, "retry_events": 0, "errors": {}, "run_ids": [], "outcomes": defaultdict(int)}
        )
        scanned = 0
        for run in runs:
            start = run.get("startTime")
            if start is None or start < cutoff:
                continue
            scanned += 1
            logs = (self._client.call(_RETRY_EVENTS_QUERY, {"id": run["runId"]}).get("logsForRun") or {})
            if logs.get("__typename") == "PythonError":
                continue  # one run's log fetch failing shouldn't blow up the whole report
            per_step: Dict[str, int] = defaultdict(int)
            for e in logs.get("events") or []:
                if e.get("__typename") == "ExecutionStepUpForRetryEvent":
                    step = e["stepKey"]
                    per_step[step] += 1
                    msg = _error_message(e.get("error"))
                    steps[step]["errors"][msg] = steps[step]["errors"].get(msg, 0) + 1
            for step, n in per_step.items():
                rec = steps[step]
                rec["runs_with_retries"] += 1
                rec["retry_events"] += n
                rec["outcomes"][run["status"]] += 1
                rec["run_ids"].append(self._run_url(run["runId"]))
        return {
            "window_days": self._retry_window_days,
            "runs_scanned": scanned,
            "steps": [
                {
                    "step": step,
                    "runs_with_retries": r["runs_with_retries"],
                    "retry_events": r["retry_events"],
                    "final_run_status": dict(r["outcomes"]),
                    "errors": [{"message": m, "count": c} for m, c in sorted(r["errors"].items(), key=lambda x: -x[1])[:3]],
                    "example_runs": r["run_ids"][:3],
                }
                for step, r in sorted(steps.items(), key=lambda x: -x[1]["retry_events"])
            ],
        }

    def _incidents(self, runs: List[Dict[str, Any]], max_incidents: int = 8) -> Dict[str, Any]:
        cutoff = self._now() - self._incident_window_hours * 3600
        failed = [r for r in runs if r.get("status") == "FAILURE" and (r.get("startTime") or 0) >= cutoff][:max_incidents]
        out = []
        for r in failed:
            logs = (self._client.call(_FAILURE_EVENTS_QUERY, {"id": r["runId"]}).get("logsForRun") or {})
            events = (logs.get("events") or []) if logs.get("__typename") != "PythonError" else []
            step_key, message = None, ""
            for e in events:
                if e.get("__typename") == "ExecutionStepFailureEvent" and e.get("error"):
                    step_key, message = e.get("stepKey"), _error_message(e.get("error"), cap=200)
                    break
            out.append({
                "run": self._run_url(r["runId"]), "job": r.get("pipelineName"),
                "assets": ["/".join(a["path"]) for a in (r.get("assetSelection") or [])],
                "failed_step": step_key, "error": message,
            })
        return {"window_hours": self._incident_window_hours, "failed_runs": len(failed), "incidents": out}

    def _runtime_changes(self, runs: List[Dict[str, Any]]) -> Dict[str, Any]:
        cutoff = self._now() - self._runtime_change_window_days * 86400
        groups: Dict[str, List[tuple]] = defaultdict(list)
        for r in runs:
            if (r.get("status") != "SUCCESS" or not r.get("startTime") or not r.get("endTime")
                    or r["startTime"] < cutoff):
                continue
            key = ", ".join(sorted("/".join(a["path"]) for a in (r.get("assetSelection") or []))) or r.get("pipelineName")
            groups[key].append((r["startTime"], r["endTime"] - r["startTime"]))
        slower = []
        for key, rows in groups.items():
            rows.sort()
            if len(rows) < 4:
                continue
            half = len(rows) // 2
            before, after = _median([d for _, d in rows[:half]]), _median([d for _, d in rows[half:]])
            if after >= before * 1.5 and after - before >= 30:
                slower.append({"runs_of": key, "median_seconds_before": round(before), "median_seconds_after": round(after)})
        return {"window_days": self._runtime_change_window_days, "runs_considered": sum(len(v) for v in groups.values()), "slower": slower}

    def _dependency_report(self, libraries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Declared dependencies from pyproject.toml (pinned/range/unconstrained) cross-referenced against
        CURRENT running versions -- but only for dagster-family packages, since `dagsterLibraryVersions` is
        the only source this tool has for "what version is actually running", and it only covers those (NOT
        arbitrary third-party packages like `pandas`/`boto3` -- Dagster+'s GraphQL schema does not expose the
        full site-packages list). OSV.dev is queried only for packages with a known running version, for the
        same reason -- a vulnerability check against a version this tool can't confirm is running would be a
        guess, not evidence. `fixed_in` collects every `fixed` version OSV's real response reports."""
        if not self._repo:
            return {"configured": False, "note": "`repo` not configured -- skipping dependency/package-risk scan."}
        lib_versions = {lib["name"]: lib["version"] for lib in (libraries or [])}

        def _running_version(name: str) -> Optional[str]:
            return lib_versions.get(name) or lib_versions.get(name.replace("_", "-")) or lib_versions.get(name.replace("-", "_"))

        tree = _gh_list_tree(self._repo, self._base_branch, token=self._github_token,
                              api_base_url=self._github_api_base_url, exclude_paths=self._exclude_paths)
        names = {t["path"] for t in tree}
        lockfiles = sorted(n for n in names if n.split("/")[-1] in ("uv.lock", "poetry.lock", "Pipfile.lock", "requirements.txt"))
        deps: List[Dict[str, Any]] = []
        if "pyproject.toml" in names:
            content = _gh_read_file(self._repo, "pyproject.toml", self._base_branch, token=self._github_token,
                                     api_base_url=self._github_api_base_url)
            project = tomllib.loads(content).get("project", {})
            for spec in project.get("dependencies", []) or []:
                name = re.split(r"[<>=!~\[ ;]", spec, maxsplit=1)[0]
                kind = "pinned" if "==" in spec else "range" if re.search(r"[<>~]", spec) else "unconstrained"
                deps.append({"name": name, "spec": spec, "kind": kind, "running_version": _running_version(name)})
        vulnerable = []
        checked = [d for d in deps if d["running_version"]][: self._osv_max_packages]
        for d in checked:
            try:
                resp = requests.post(
                    "https://api.osv.dev/v1/query",
                    json={"package": {"name": d["name"], "ecosystem": "PyPI"}, "version": d["running_version"]},
                    timeout=15,
                )
                resp.raise_for_status()
                payload = resp.json()
            except requests.RequestException:
                continue
            vulns = payload.get("vulns") or []
            if not vulns:
                continue
            advisories, fixed_in = [], set()
            for v in vulns[:5]:
                advisories.append(v.get("id"))
                for affected in v.get("affected") or []:
                    for rng in affected.get("ranges") or []:
                        for event in rng.get("events") or []:
                            if "fixed" in event:
                                fixed_in.add(event["fixed"])
            vulnerable.append({
                "package": d["name"], "running_version": d["running_version"], "advisories": advisories,
                "fixed_in": sorted(fixed_in) or None,
            })
        return {
            "configured": True,
            "declared_dependencies": deps,
            "lock_files": lockfiles,
            "has_lock_file": bool(lockfiles),
            "running_packages_checked_against_osv": len(checked),
            "running_packages_with_known_vulnerabilities": vulnerable,
        }

    def t_project_overview(self, location: str = "") -> Dict[str, Any]:
        if not location or not str(location).strip():
            raise EvidenceToolError("project_overview requires `location` (a code location name).")
        location = str(location).strip()
        if location not in self._exclude_locations and self._include_locations and location not in self._include_locations:
            raise EvidenceToolError(f"location {location!r} is not in include_locations {sorted(self._include_locations)}.")
        if location in self._exclude_locations:
            raise EvidenceToolError(
                f"location {location!r} is excluded (exclude_locations) -- an agent may not review its own code location.")

        assets_data = self._client.call(_ASSETS_QUERY)
        nodes = [
            n for n in (assets_data.get("assetNodes") or [])
            if ((n.get("repository") or {}).get("location") or {}).get("name") == location
        ]

        ws = self._client.call(_WORKSPACE_QUERY)
        wso = ws.get("workspaceOrError") or {}
        if wso.get("__typename") == "PythonError":
            raise EvidenceToolError(f"workspaceOrError: {wso.get('message')}")
        schedules: List[Dict[str, Any]] = []
        libraries: List[Dict[str, Any]] = []
        for entry in wso.get("locationEntries") or []:
            if entry.get("name") != location:
                continue
            loc = entry.get("locationOrLoadError") or {}
            if loc.get("__typename") == "PythonError":
                raise EvidenceToolError(f"code location {location!r} failed to load: {loc.get('message')}")
            libraries = loc.get("dagsterLibraryVersions") or []
            for repo in loc.get("repositories") or []:
                for s in repo.get("schedules") or []:
                    schedules.append({
                        "name": s["name"], "cron": s.get("cronSchedule"), "job": s.get("pipelineName"),
                        "status": (s.get("scheduleState") or {}).get("status"),
                    })
            break

        assets = []
        for node in nodes:
            jobs = [j for j in (node.get("jobNames") or []) if not j.startswith("__")]
            assets.append({
                "asset_key": "/".join(node["assetKey"]["path"]),
                "group": node.get("groupName"),
                "freshness_policy": node.get("freshnessPolicy"),
                "automation_condition": (node.get("automationCondition") or {}).get("label"),
                "scheduled_by": [s["name"] for s in schedules if s["job"] in jobs],
                "code_references": _code_references(node),
                "asset_checks": _check_health(node),
                "partition_health": _partition_health(node),
            })

        running_dagster_version = next((lib["version"] for lib in libraries if lib["name"] == "dagster"), None)
        runs = self._fetch_runs_in_location(location)

        partitions_needing_attention = [
            {"asset_key": a["asset_key"], **a["partition_health"]} for a in assets if a["partition_health"]
        ]
        partition_and_check_health = {
            "partitions_needing_attention": partitions_needing_attention,
            "checks_passing": sum(a["asset_checks"]["passing"] for a in assets),
            "checks_failing": sum(a["asset_checks"]["failing"] for a in assets),
            "checks_stale": sum(a["asset_checks"]["stale"] for a in assets),
            "assets_without_checks": [
                a["asset_key"] for a in assets
                if not (a["asset_checks"]["passing"] or a["asset_checks"]["failing"] or a["asset_checks"]["stale"])
            ],
            "assets_with_failing_checks": [
                {"asset_key": a["asset_key"], "failing_checks": a["asset_checks"]["failing_checks"]}
                for a in assets if a["asset_checks"]["failing"]
            ],
        }

        alert_policies, alert_policies_error = None, None
        try:
            alert_policies = [
                {"id": p.get("id"), "name": p.get("name"), "description": p.get("description"),
                 "enabled": p.get("enabled"), "event_types": p.get("eventTypes") or []}
                for p in self._client.call(_ALERT_POLICIES_QUERY, {"location": location}).get("alertPoliciesForCodeLocation") or []
            ]
        except EvidenceToolError as e:  # e.g. a token without alert-policy read access must not sink the whole overview
            alert_policies_error = str(e)[:300]

        dependencies = self._dependency_report(libraries)
        dagster_dependency = next(
            (d["spec"] for d in dependencies.get("declared_dependencies", []) if d["name"] == "dagster"), None
        )
        latest = self.t_latest_dagster_release()

        return {
            "code_location": location,
            "repo_notes": self._repo_notes,
            "assets": assets,
            "schedules": schedules,
            "running_dagster_version": running_dagster_version,
            "latest_dagster_version": latest["latest_version"],
            "dagster_dependency": dagster_dependency,
            "retries": self._retry_report(runs),
            "partition_and_check_health": partition_and_check_health,
            "runtime_changes": self._runtime_changes(runs),
            "dependencies": dependencies,
            "incidents": self._incidents(runs),
            "alert_policies": alert_policies,
            "alert_policy_count": None if alert_policies is None else len(alert_policies),
            **({"alert_policies_error": alert_policies_error} if alert_policies_error else {}),
        }


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>` -- same mechanism as dagster_discovery_tools'/
    dagster_api_check_tools'/dagster_instance_tools' own copies (duplicated here, not imported, per this
    repo's self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "dagster_evidence_tools's mcp_server_resource_key path must be run as `python -m <dotted.path.to.this.module>`, "
        "not `python component.py` directly. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined here rather than imported from
    resources/mcp_server_resource (confirmed not importable across components). Same copy
    dagster_discovery_tools/dagster_api_check_tools/dagster_instance_tools/dagster_workflow_tools each carry."""

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


class DagsterEvidenceToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers read-only Dagster-deployment-health evidence tools as a resource (default key
    `dagster_evidence_tools`). For an agent (MCP only), run THIS FILE as the stdio MCP server -- see README.md.

    Example:

    ```yaml
    type: dagster_community_components.DagsterEvidenceToolsComponent
    attributes:
      deployment_url: auto
      token_env_var: DAGSTER_CLOUD_API_TOKEN
      repo: acme/hooli-data-eng-pipelines
      exclude_locations: [improvement_advisor]
      repo_notes: |
        This is a demo repository. The flakiness in retry_handler.py is deliberate.
    ```
    """

    resource_key: str = Field(default="dagster_evidence_tools", description="Resource key the toolset is registered under.")
    deployment_url: str = Field(
        default=_AUTO,
        description="Full Dagster+ deployment URL, e.g. https://acme.dagster.cloud/prod. 'auto' derives it at run "
                     "time from DAGSTER_CLOUD_DEPLOYMENT_NAME + the instance's Dagster+ organization -- resolvable "
                     "for the plain resource (it sees the real DagsterInstance), but NOT for mcp_server_resource_key "
                     "(that spawns a separate subprocess with no DagsterInstance at all): set deployment_url "
                     "explicitly whenever mcp_server_resource_key is also set. An explicit value always wins.",
    )
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN", description="Env var holding a Dagster+ API token with read access to this deployment.")
    repo: Optional[str] = Field(
        default=None,
        description="GitHub repository 'owner/repo' this deployment's code lives in -- required for "
                     "deprecation_report and for project_overview's dependency/package-risk scan (pyproject.toml, "
                     "lock files). Without it, deprecation_report refuses with a clear error and project_overview's "
                     "dependency section is skipped with a note, rather than guessing.",
    )
    github_token_env_var: str = Field(default="GITHUB_TOKEN", description="Env var holding a GitHub token with read access to `repo`.")
    github_api_base_url: str = Field(default="https://api.github.com", description="GitHub API base URL. Change for GitHub Enterprise Server.")
    base_branch: str = Field(default="main", description="Branch deprecation_report/the dependency scan read `repo` from.")
    exclude_paths: Optional[List[str]] = Field(
        default=None,
        description="Path prefixes deprecation_report's file scan and the dependency scan never read -- e.g. "
                     "vendored code or generated files. Same directory-like ('src/')/filename-like ('Dockerfile') "
                     "matching convention as github_pr_tools' exclude_paths/protected_paths.",
    )
    include_locations: Optional[List[str]] = Field(
        default=None,
        description=(
            "Allow-list of code location names: when set, ONLY these locations are visible to the tools (everything "
            "else is hidden or refused). `exclude_locations` still applies on top and takes precedence, so a name in "
            "both is excluded. Use it for 'review only these locations' / 'watch only these'."
        ),
    )
    exclude_locations: Optional[List[str]] = Field(
        default=None,
        description=(
            "Code location names project_overview refuses to review -- typically the improvement-advisor "
            "agent's OWN code location, so it never reviews or reacts to itself. Same name/convention as "
            "dagster_discovery_tools' own exclude_locations."
        ),
    )
    repo_notes: Optional[str] = Field(
        default=None,
        description="Free text passed through project_overview's output verbatim, under `repo_notes` -- for what's "
                     "deliberate in this deployment (a demo's artificial flakiness, a known limitation that isn't "
                     "a bug). Same idea as the *_agent components' context_notes -- see litellm_agent/README.md's "
                     "'Telling the agent what's deliberate' section.",
    )
    allow: Optional[List[str]] = Field(default=None, description=f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}.")
    max_result_chars: int = Field(default=12000, description="Upper bound on the serialized size of any tool result.")
    timeout_seconds: float = Field(default=30.0, description="HTTP timeout per GraphQL call.")
    retry_window_days: int = Field(default=7, description="project_overview's retried-steps lookback window, in days.")
    runtime_change_window_days: int = Field(default=14, description="project_overview's slower-than-before-runs lookback window, in days.")
    incident_window_hours: int = Field(default=24, description="project_overview's recent-incidents lookback window, in hours.")
    max_runs_scanned: int = Field(default=100, description="Upper bound on runs fetched per project_overview call (retries/incidents/runtime-changes all derive from this one fetch).")
    max_files_scanned: int = Field(default=40, description="Default cap on .py files deprecation_report scans (override per call with max_files).")
    osv_max_packages: int = Field(default=25, description="Upper bound on packages checked against OSV.dev per project_overview call.")
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description="If set, ALSO registers a stdio MCP-server-shaped resource under this key (python -m this "
                     "module, with DAGSTER_EVIDENCE_TOOLS_* env vars set from this component's own config). Any "
                     "mcp_servers-aware component can then reference it via server_resource_key.",
    )
    mcp_extra_env_vars: Optional[List[str]] = Field(default=None, description="mcp_server_resource_key only. Extra env var NAMES to copy into the spawned MCP server's env.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow", "mcp_extra_env_vars", "exclude_locations", "include_locations", "exclude_paths"), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Dagster Evidence Tools", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(ALL_TOOLS)}")
        max_chars = int(self.max_result_chars)
        deployment_url, token_env_var, timeout = self.deployment_url, self.token_env_var, self.timeout_seconds
        repo, github_token_env_var, github_api_base_url, base_branch = self.repo, self.github_token_env_var, self.github_api_base_url, self.base_branch
        exclude_paths, exclude_locations, repo_notes = list(self.exclude_paths or []), list(self.exclude_locations or []), self.repo_notes or ""
        include_locations = list(self.include_locations or [])
        retry_window_days, runtime_change_window_days, incident_window_hours = self.retry_window_days, self.runtime_change_window_days, self.incident_window_hours
        max_runs_scanned, max_files_scanned, osv_max_packages = self.max_runs_scanned, self.max_files_scanned, self.osv_max_packages

        def _init(init_context):
            url = resolve_deployment_url(deployment_url, init_context.instance)
            token = os.environ.get(token_env_var, "")
            if not token:
                raise ValueError(f"DagsterEvidenceToolsComponent: env var {token_env_var!r} is empty or unset.")
            client = _Client(url, token, token_env_var=token_env_var, timeout=timeout)
            return DagsterEvidenceTools(
                client, deployment_url=url, repo=repo, github_token=os.environ.get(github_token_env_var, ""),
                github_api_base_url=github_api_base_url, base_branch=base_branch, exclude_paths=exclude_paths,
                exclude_locations=exclude_locations, include_locations=include_locations, repo_notes=repo_notes, allow=allow, max_result_chars=max_chars,
                retry_window_days=retry_window_days, runtime_change_window_days=runtime_change_window_days,
                incident_window_hours=incident_window_hours, max_runs_scanned=max_runs_scanned,
                max_files_scanned=max_files_scanned, osv_max_packages=osv_max_packages,
            )

        res = dg.ResourceDefinition(
            resource_fn=_init,
            description="Read-only deployment-health evidence tools (project overview, deprecated-API scan, "
                         "latest Dagster release) over the Dagster+ GraphQL API, GitHub, PyPI and OSV.dev.",
        )
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {
                "DAGSTER_EVIDENCE_TOOLS_DEPLOYMENT_URL": deployment_url,
                "DAGSTER_EVIDENCE_TOOLS_TOKEN_ENV_VAR": token_env_var,
                "DAGSTER_EVIDENCE_TOOLS_MAX_CHARS": str(max_chars),
                "DAGSTER_EVIDENCE_TOOLS_GITHUB_TOKEN_ENV_VAR": github_token_env_var,
                "DAGSTER_EVIDENCE_TOOLS_GITHUB_API_BASE_URL": github_api_base_url,
                "DAGSTER_EVIDENCE_TOOLS_BASE_BRANCH": base_branch,
                "DAGSTER_EVIDENCE_TOOLS_RETRY_WINDOW_DAYS": str(retry_window_days),
                "DAGSTER_EVIDENCE_TOOLS_RUNTIME_CHANGE_WINDOW_DAYS": str(runtime_change_window_days),
                "DAGSTER_EVIDENCE_TOOLS_INCIDENT_WINDOW_HOURS": str(incident_window_hours),
                "DAGSTER_EVIDENCE_TOOLS_MAX_RUNS_SCANNED": str(max_runs_scanned),
                "DAGSTER_EVIDENCE_TOOLS_MAX_FILES_SCANNED": str(max_files_scanned),
                "DAGSTER_EVIDENCE_TOOLS_OSV_MAX_PACKAGES": str(osv_max_packages),
            }
            if repo:
                mcp_env["DAGSTER_EVIDENCE_TOOLS_REPO"] = repo
            if repo_notes:
                mcp_env["DAGSTER_EVIDENCE_TOOLS_REPO_NOTES"] = repo_notes
            if allow:
                mcp_env["DAGSTER_EVIDENCE_TOOLS_ALLOW"] = ",".join(allow)
            if exclude_locations:
                mcp_env["DAGSTER_EVIDENCE_TOOLS_EXCLUDE_LOCATIONS"] = ",".join(exclude_locations)
            if include_locations:
                mcp_env["DAGSTER_EVIDENCE_TOOLS_INCLUDE_LOCATIONS"] = ",".join(include_locations)
            if exclude_paths:
                mcp_env["DAGSTER_EVIDENCE_TOOLS_EXCLUDE_PATHS"] = ",".join(exclude_paths)
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key, type="stdio", command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env, live_env_passthrough=list(self.mcp_extra_env_vars or []) + [token_env_var, github_token_env_var, _DEPLOYMENT_ENV],
            )

        return dg.Definitions(resources=resources)


# ── stdio MCP server entrypoint:  python -m <this module>  ──────────────────────────────────────────────────────────
def build_mcp_server(tools: DagsterEvidenceTools):
    """MCP server (official `mcp` package) exposing exactly the allowed tools. Works on mcp 1.x and 2.x."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    try:  # the SDK's own ToolError, not this module's EvidenceToolError -- see _mcp_call below for why that distinction matters.
        from mcp.server.mcpserver.exceptions import ToolError as _SdkToolError  # mcp 2.x (confirmed live, 2.3.0)
    except ImportError:
        from mcp.server.fastmcp.exceptions import ToolError as _SdkToolError  # mcp 1.x (stable FastMCP API, not live-verified here)

    mcp = _Server("dagster-evidence-tools")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__ in allowed:
            mcp.tool()(fn)
        return fn

    def _mcp_call(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """tools.call() never raises (see module docstring) -- a bad-arguments or failure comes back as a plain
        {"error": ...} dict, which the MCP transport would otherwise hand back as a NORMAL (isError=False) tool
        result indistinguishable from success. Re-raising here lets the `mcp` package's own exception handling
        (both fastmcp 1.x and mcp.server.mcpserver 2.x catch any exception from a registered tool function and
        turn it into a real isError=True CallToolResult) make the failure visible to whatever's counting failed
        calls in the agent loop. MUST be the SDK's own ToolError (_SdkToolError), not this module's
        EvidenceToolError: the SDK's tool dispatch (mcp/server/mcpserver/tools/base.py `Tool.run`) only keeps a
        raised exception's own message when it recognizes the exception as ITS OWN ToolError/ResourceError --
        anything else (including a same-named ToolError class defined elsewhere) is treated as an unexpected
        crash and replaced with a generic 'Error executing tool <name>' before it reaches the client, throwing
        away the reason the agent needs to act on."""
        result = tools.call(name, args)
        if isinstance(result, dict) and "error" in result:
            raise _SdkToolError(result["error"])
        return result

    @_tool
    def project_overview(location: str) -> dict:
        """For ONE code location: every asset's freshness/automation/schedules/repository-file refs, steps
        that retried recently (error + whether the retry ultimately succeeded), partition + asset-check
        health, slower-than-before runs, pinned-dependency versions + known OSV vulnerabilities, failures in
        the last 24h, and repo_notes (what's deliberate here). Refuses an excluded location."""
        return _mcp_call("project_overview", {"location": location})

    @_tool
    def deprecation_report(max_files: int = None) -> dict:
        """Deprecated, superseded or removed Dagster API usage across the repository's Python files."""
        return _mcp_call("deprecation_report", {"max_files": max_files})

    @_tool
    def check_dagster_api(names: List[str]) -> dict:
        """Whether Dagster names (e.g. `dg.load_from_defs_folder`) exist in the installed Dagster, with their real
        signatures. Use it while reasoning, before relying on an API. At most 12 names."""
        return _mcp_call("check_dagster_api", {"names": names})

    @_tool
    def latest_dagster_release() -> dict:
        """The newest published `dagster` version and its GitHub release notes."""
        return _mcp_call("latest_dagster_release", {})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: DAGSTER_EVIDENCE_TOOLS_DEPLOYMENT_URL (default 'auto'), _TOKEN_ENV_VAR (default
    DAGSTER_CLOUD_API_TOKEN), _REPO, _GITHUB_TOKEN_ENV_VAR (default GITHUB_TOKEN), _GITHUB_API_BASE_URL,
    _BASE_BRANCH, _EXCLUDE_LOCATIONS (csv), _INCLUDE_LOCATIONS (csv), _EXCLUDE_PATHS (csv), _REPO_NOTES, _ALLOW (csv), _MAX_CHARS,
    _RETRY_WINDOW_DAYS, _RUNTIME_CHANGE_WINDOW_DAYS, _INCIDENT_WINDOW_HOURS, _MAX_RUNS_SCANNED,
    _MAX_FILES_SCANNED, _OSV_MAX_PACKAGES."""
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel

    env = os.environ
    deployment_url = env.get("DAGSTER_EVIDENCE_TOOLS_DEPLOYMENT_URL", _AUTO)
    token_env_var = env.get("DAGSTER_EVIDENCE_TOOLS_TOKEN_ENV_VAR", "DAGSTER_CLOUD_API_TOKEN")
    github_token_env_var = env.get("DAGSTER_EVIDENCE_TOOLS_GITHUB_TOKEN_ENV_VAR", "GITHUB_TOKEN")
    allow_env = env.get("DAGSTER_EVIDENCE_TOOLS_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None
    exclude_env = env.get("DAGSTER_EVIDENCE_TOOLS_EXCLUDE_LOCATIONS")
    exclude_locations = [t.strip() for t in exclude_env.split(",") if t.strip()] if exclude_env else None
    include_env = env.get("DAGSTER_EVIDENCE_TOOLS_INCLUDE_LOCATIONS")
    include_locations = [t.strip() for t in include_env.split(",") if t.strip()] if include_env else None
    exclude_paths_env = env.get("DAGSTER_EVIDENCE_TOOLS_EXCLUDE_PATHS")
    exclude_paths = [t.strip() for t in exclude_paths_env.split(",") if t.strip()] if exclude_paths_env else None

    if deployment_url.strip().lower() == _AUTO:
        # 'auto' needs instance.dagit_url for the org/region -- unavailable in THIS process: the
        # mcp_server_resource_key path spawns `python -m this module` as a separate subprocess with no
        # DagsterInstance at all.
        print("ERROR: deployment_url: auto cannot be resolved for the stdio MCP server process -- it has no "
              "DagsterInstance (unlike the plain resource path, which resolves 'auto' fine). Set deployment_url "
              "explicitly on DagsterEvidenceToolsComponent when using mcp_server_resource_key, e.g. "
              "https://<org>.dagster.cloud/<deployment>.", file=sys.stderr)
        sys.exit(1)
    url = validate_deployment_url(deployment_url)
    token = env.get(token_env_var, "")
    if not token:
        print(f"ERROR: env var {token_env_var!r} is empty or unset", file=sys.stderr)
        sys.exit(1)
    client = _Client(url, token, token_env_var=token_env_var)
    tools = DagsterEvidenceTools(
        client, deployment_url=url, repo=env.get("DAGSTER_EVIDENCE_TOOLS_REPO"),
        github_token=env.get(github_token_env_var, ""),
        github_api_base_url=env.get("DAGSTER_EVIDENCE_TOOLS_GITHUB_API_BASE_URL", "https://api.github.com"),
        base_branch=env.get("DAGSTER_EVIDENCE_TOOLS_BASE_BRANCH", "main"),
        exclude_paths=exclude_paths, exclude_locations=exclude_locations, include_locations=include_locations,
        repo_notes=env.get("DAGSTER_EVIDENCE_TOOLS_REPO_NOTES", ""),
        allow=allow, max_result_chars=int(env.get("DAGSTER_EVIDENCE_TOOLS_MAX_CHARS", "12000")),
        retry_window_days=int(env.get("DAGSTER_EVIDENCE_TOOLS_RETRY_WINDOW_DAYS", "7")),
        runtime_change_window_days=int(env.get("DAGSTER_EVIDENCE_TOOLS_RUNTIME_CHANGE_WINDOW_DAYS", "14")),
        incident_window_hours=int(env.get("DAGSTER_EVIDENCE_TOOLS_INCIDENT_WINDOW_HOURS", "24")),
        max_runs_scanned=int(env.get("DAGSTER_EVIDENCE_TOOLS_MAX_RUNS_SCANNED", "100")),
        max_files_scanned=int(env.get("DAGSTER_EVIDENCE_TOOLS_MAX_FILES_SCANNED", "40")),
        osv_max_packages=int(env.get("DAGSTER_EVIDENCE_TOOLS_OSV_MAX_PACKAGES", "25")),
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
