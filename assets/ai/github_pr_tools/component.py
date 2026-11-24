"""GithubPrToolsComponent -- repo-scoped GitHub pull-request tools for engineering agents (a bug fixer, a PR
reviewer), with the review rules enforced in code, not in the prompt.

Same dual-mode shape as `agentic_pipeline_authoring_tools`/`dagster_instance_tools`/`dagster_workflow_tools`:
  1. An importable toolset (`GithubPrTools`, plus the component that registers it as a Dagster resource) under
     `resource_key`.
  2. A self-contained stdio MCP server: `python -m <dotted.path.to.this.module>`.
`mcp_server_resource_key`, if set, ALSO registers a stdio MCP-server-shaped resource so any mcp_servers-aware
component can reference this server via `server_resource_key`.

Tools:
  get_pull_request(number)            -- title, body, author, state, changed files + patches, CI check state.
  list_open_pull_requests()           -- number, head sha, title, author, url -- to avoid proposing a change twice.
  assess_pull_request(number)         -- the FIXED risk assessment (level 'low'/'needs_human' + reasons), from
                                          changed paths, size and checks -- a model may describe it but cannot
                                          change it. review_pull_request enforces this; it is not advisory only.
  propose_fix(branch, title, body, files, note="") -- opens a PR. `files` items are {path, old, new} (replace one
                                          exact piece of text -- it must appear once -- leaving the rest of the file
                                          untouched) or {path, content} (new files only). CI/dependency/packaging
                                          paths are protected; refuses if an already-open PR touches the same file.
  review_pull_request(number, decision, body) -- decision is COMMENT or APPROVE. APPROVE is refused unless the PR
                                          is low risk, and is posted under the SECOND identity (reviewer_token_env_var)
                                          -- GitHub blocks an author approving their own PR.
  list_files(path="")                 -- lists files/directories under `path` in the configured repo (GitHub's
                                          Contents API, pinned to base_branch). An entry matching exclude_paths
                                          never appears in the listing at all.
  read_file(path, ref=None)           -- reads one file's content (ref defaults to base_branch; pass a branch,
                                          commit SHA or tag to read a different one). Refuses a path matching
                                          exclude_paths.

Both list_files and read_file refuse a path containing '..' before doing anything else, same as every other
path this component touches (_gh_safe_path) -- ported from the engineering-agents reference's
github_scope.list_files/read_file/is_excluded/_safe_path, through this component's own GitHub-calling and
error-shaping conventions rather than a second style.

Deliberately NOT a tool: merging. `merge_if_ready` (same GitHub-PR-scope block, copied into
`sensors/slack_pr_approval_watcher`) is a human-reaction-triggered sensor action only, never something an agent
can call directly.
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


# BEGIN github PR scope (inlined per component; keep copies identical -- see tests/test_github_pr_scope_drift.py)
# Repo-scoped GitHub pull-request operations: read, propose a targeted-edit fix, assess risk deterministically,
# review (COMMENT always, APPROVE only when low risk and under a second identity), and merge when ready. Every
# function takes its config explicitly (repo/token/api_base_url/...) rather than reading a global settings object,
# so it's a pure, independently testable unit -- copied verbatim into assets/ai/github_pr_tools,
# sensors/slack_pr_approval_watcher, and sensors/github_pull_request_agent_sensor (whichever of these functions
# each file actually needs).
class GithubPrScopeError(Exception):
    pass


def _gh_safe_path(path: str) -> str:
    path = path.strip().lstrip("/")
    if ".." in path.split("/") or path.startswith(".git/"):
        raise GithubPrScopeError(f"Invalid path: {path!r}")
    return path


def _gh_call(method: str, repo: str, path: str, *, token: str, api_base_url: str, **kwargs) -> Any:
    import requests
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
        raise GithubPrScopeError(f"GitHub {method} {path} failed ({resp.status_code}): {resp.text[:300]}")
    return resp.json() if resp.content else {}


def _gh_checks(repo: str, sha: str, *, token: str, api_base_url: str) -> Dict[str, Any]:
    runs = _gh_call("GET", repo, f"/commits/{sha}/check-runs", token=token, api_base_url=api_base_url)["check_runs"]
    statuses = {r["name"]: (r["conclusion"] or r["status"]) for r in runs}
    if not statuses:
        return {"state": "none", "runs": {}}
    if any(v in ("failure", "cancelled", "timed_out") for v in statuses.values()):
        state = "failing"
    elif all(v == "success" for v in statuses.values()):
        state = "passing"
    else:
        state = "pending"
    return {"state": state, "runs": statuses}


_GH_CLOUD_URL_RE = r"https://[A-Za-z0-9.-]*dagster\.cloud/[^\s)>\]\"']+"


def _gh_find_branch_deployment_url(texts: List[str]) -> Optional[str]:
    """The Dagster+ branch deployment link in texts the Dagster Cloud GitHub Action posts, if any. A link that
    names a deployment is preferred over a generic one (docs, the org home page)."""
    import re
    found = []
    for text in texts:
        for match in re.findall(_GH_CLOUD_URL_RE, text or ""):
            url = match.rstrip(".,;")
            if "/docs" in url or "/settings" in url:
                continue
            found.append(url)
    for url in found:
        if re.search(r"/[^/]+--[^/]+|branch|/prod/|/deployment", url):
            return url
    return found[0] if found else None


def _gh_branch_deployment_url(repo: str, number: int, head_sha: str, *, token: str, api_base_url: str) -> Optional[str]:
    """Look in the PR's comments, check runs and commit statuses for the branch deployment link the Dagster Cloud
    GitHub Action posts -- so an agent (or a person reading its review comment) can jump straight to the branch
    deployment a PR is running against, without a separate 'post the link' step: it's just part of what
    get_pull_request returns."""
    texts: List[str] = []
    for c in _gh_call("GET", repo, f"/issues/{int(number)}/comments", token=token, api_base_url=api_base_url, params={"per_page": 100}):
        texts.append(c.get("body") or "")
    for run in _gh_call("GET", repo, f"/commits/{head_sha}/check-runs", token=token, api_base_url=api_base_url)["check_runs"]:
        out = run.get("output") or {}
        texts += [run.get("details_url") or "", out.get("summary") or "", out.get("text") or ""]
    for st in _gh_call("GET", repo, f"/commits/{head_sha}/statuses", token=token, api_base_url=api_base_url):
        texts += [st.get("target_url") or "", st.get("description") or ""]
    return _gh_find_branch_deployment_url(texts)


def _gh_path_matches(path: str, pattern: str) -> bool:
    """A path-like pattern (ends with '/', one or more segments, e.g. 'src/' or
    'assets/ai/github_pr_tools/') matches if those segments appear anywhere as a CONTIGUOUS run
    of directory segments in the path, not just at the root -- a monorepo's projects commonly
    live in a subfolder (e.g. 'hooli-basics/src/orders.py'), where the original
    root-anchored-only path.startswith(pattern) matched nothing. A filename-like pattern (no
    trailing '/') matches the file's own basename (so 'Dockerfile'/'README' match at any depth
    too) as well as a root-level prefix (keeps 'requirements' matching 'requirements.txt' at the
    repo root, 'pyproject.toml' matching itself)."""
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


def _gh_get_pull_request(repo: str, number: int, *, token: str, api_base_url: str, required_branch_prefix: Optional[str] = None, exclude_paths=()) -> Dict[str, Any]:
    pr = _gh_call("GET", repo, f"/pulls/{int(number)}", token=token, api_base_url=api_base_url)
    if required_branch_prefix and not pr["head"]["ref"].startswith(required_branch_prefix):
        raise GithubPrScopeError(f"PR #{number} is on {pr['head']['ref']!r}, not a {required_branch_prefix!r} branch; refusing.")
    files = _gh_call("GET", repo, f"/pulls/{int(number)}/files", token=token, api_base_url=api_base_url, params={"per_page": 100})
    return {
        "number": pr["number"],
        "url": pr["html_url"],
        "title": pr["title"],
        "body": pr["body"],
        "author": pr["user"]["login"],
        "state": pr["state"],
        "merged": bool(pr.get("merged")),
        "head_sha": pr["head"]["sha"],
        "head_ref": pr["head"]["ref"],
        "files": [
            {
                "path": f["filename"], "status": f["status"], "additions": f["additions"],
                "deletions": f["deletions"], "patch": (f.get("patch") or "")[:6000],
            }
            for f in files
            if not _gh_path_matches_any(f["filename"], exclude_paths)
        ],
        "checks": _gh_checks(repo, pr["head"]["sha"], token=token, api_base_url=api_base_url),
        "branch_deployment_url": _gh_branch_deployment_url(repo, number, pr["head"]["sha"], token=token, api_base_url=api_base_url),
    }


def _gh_assess_risk(pr: Dict[str, Any], *, protected_paths, low_risk_paths, low_risk_max_files: int, low_risk_max_lines: int) -> Dict[str, Any]:
    """Deterministic risk level for a PR; a model may describe it but cannot change it."""
    reasons: List[str] = []
    files = pr["files"]
    changed = sum(f["additions"] + f["deletions"] for f in files)
    if len(files) > low_risk_max_files:
        reasons.append(f"changes {len(files)} files (limit {low_risk_max_files})")
    if changed > low_risk_max_lines:
        reasons.append(f"changes {changed} lines (limit {low_risk_max_lines})")
    for f in files:
        if _gh_path_matches_any(f["path"], protected_paths):
            reasons.append(f"touches protected path {f['path']}")
        elif not _gh_path_matches_any(f["path"], low_risk_paths):
            reasons.append(f"touches {f['path']}, outside the configured low-risk paths")
    if pr["checks"]["state"] != "passing":
        reasons.append(f"checks are {pr['checks']['state']}")
    if "Kind: improvement" in (pr.get("body") or ""):
        reasons.append("it is an improvement proposal, which changes behavior, so a person decides")
    return {"level": "low" if not reasons else "needs_human", "reasons": reasons, "changed_lines": changed}


def _gh_read_file(repo: str, path: str, ref: str, *, token: str, api_base_url: str) -> str:
    import base64
    item = _gh_call("GET", repo, f"/contents/{_gh_safe_path(path)}", token=token, api_base_url=api_base_url, params={"ref": ref})
    return base64.b64decode(item["content"]).decode("utf-8")


def _gh_apply_edit(repo: str, path: str, edit: Dict[str, Any], *, ref: str, token: str, api_base_url: str) -> str:
    """Replace one exact piece of the current file, so formatting everywhere else is untouched -- a whole-file
    rewrite drops a blank line or fails a formatter on a one-line fix."""
    current = _gh_read_file(repo, path, ref, token=token, api_base_url=api_base_url)
    old, new = edit.get("old"), edit.get("new")
    if not old or new is None:
        raise GithubPrScopeError(f"{path}: give either content, or old and new text.")
    if current.count(old) != 1:
        raise GithubPrScopeError(f"{path}: the old text must appear exactly once (found {current.count(old)}).")
    return current.replace(old, new)


def _gh_propose_fix(
    repo: str, branch: str, title: str, body: str, files: List[Dict[str, Any]], *,
    base_branch: str, branch_prefix: str, protected_paths, max_files_per_fix: int,
    token: str, api_base_url: str, note: str = "", kind: str = "fix", practice_mode: bool = False,
    exclude_paths=(),
) -> Dict[str, Any]:
    import base64
    if not files or len(files) > max_files_per_fix:
        raise GithubPrScopeError(f"A fix changes between 1 and {max_files_per_fix} files.")
    for f in files:
        p = _gh_safe_path(f["path"])
        if _gh_path_matches_any(p, protected_paths):
            raise GithubPrScopeError(f"{p} is protected; agents may not change it.")
        if _gh_path_matches_any(p, exclude_paths):
            raise GithubPrScopeError(f"{p} is excluded (exclude_paths); agents may not change it.")
    wanted = {_gh_safe_path(f["path"]) for f in files}
    for open_pr in _gh_call("GET", repo, "/pulls", token=token, api_base_url=api_base_url, params={"state": "open", "per_page": 50}):
        touched = {
            f["filename"]
            for f in _gh_call("GET", repo, f"/pulls/{open_pr['number']}/files", token=token, api_base_url=api_base_url, params={"per_page": 100})
        }
        if wanted & touched:
            raise GithubPrScopeError(
                f"PR #{open_pr['number']} ({open_pr['title']}) already changes {min(wanted & touched)}; "
                "do not open another for the same file."
            )
    if practice_mode:
        # Every validation above (file count, protected paths, duplicate-PR-file check) still runs --
        # practice mode only withholds the real branch/commit/PR creation, not the checks that would
        # have refused it anyway.
        return {"practice_mode": True, "would_open": "pull_request", "title": title, "files": sorted(wanted)}
    slug = "".join(c if c.isalnum() or c in "-_" else "-" for c in branch)[:60].strip("-") or "fix"
    name = branch_prefix + slug
    base_sha = _gh_call("GET", repo, f"/git/ref/heads/{base_branch}", token=token, api_base_url=api_base_url)["object"]["sha"]
    _gh_call("POST", repo, "/git/refs", token=token, api_base_url=api_base_url, json={"ref": f"refs/heads/{name}", "sha": base_sha})
    for f in files:
        p = _gh_safe_path(f["path"])
        try:
            existing = _gh_call("GET", repo, f"/contents/{p}", token=token, api_base_url=api_base_url, params={"ref": base_branch})["sha"]
        except GithubPrScopeError:
            existing = None
        content = f["content"] if "content" in f else _gh_apply_edit(repo, p, f, ref=base_branch, token=token, api_base_url=api_base_url)
        payload: Dict[str, Any] = {"message": title, "content": base64.b64encode(content.encode()).decode(), "branch": name}
        if existing:
            payload["sha"] = existing
        _gh_call("PUT", repo, f"/contents/{p}", token=token, api_base_url=api_base_url, json=payload)
    footer = f"\n\n---\nOpened by an engineering agent. Kind: {kind}. {note}".rstrip()
    pr = _gh_call(
        "POST", repo, "/pulls", token=token, api_base_url=api_base_url,
        json={"title": title, "body": body + footer, "head": name, "base": base_branch},
    )
    return {"number": pr["number"], "url": pr["html_url"], "branch": name}


def _gh_review_pull_request(
    repo: str, number: int, decision: str, body: str, *,
    token: str, reviewer_token: Optional[str], api_base_url: str,
    protected_paths, low_risk_paths, low_risk_max_files: int, low_risk_max_lines: int,
    required_branch_prefix: Optional[str] = None, exclude_paths=(),
) -> Dict[str, Any]:
    """COMMENT is always allowed. APPROVE only passes the deterministic risk rules, and is posted under the
    reviewer (second) identity -- GitHub blocks an author approving their own PR."""
    pr = _gh_get_pull_request(repo, number, token=token, api_base_url=api_base_url, required_branch_prefix=required_branch_prefix, exclude_paths=exclude_paths)
    risk = _gh_assess_risk(pr, protected_paths=protected_paths, low_risk_paths=low_risk_paths, low_risk_max_files=low_risk_max_files, low_risk_max_lines=low_risk_max_lines)
    decision = decision.upper()
    if decision not in ("COMMENT", "APPROVE"):
        raise GithubPrScopeError("decision must be COMMENT or APPROVE.")
    if decision == "APPROVE" and risk["level"] != "low":
        raise GithubPrScopeError("Not approved; a person must approve this one: " + "; ".join(risk["reasons"]))
    if decision == "APPROVE" and not reviewer_token:
        raise GithubPrScopeError("APPROVE requires a second GitHub identity (reviewer_token_env_var) -- GitHub blocks approving your own PR.")
    use_token = reviewer_token if decision == "APPROVE" else token
    _gh_call(
        "POST", repo, f"/pulls/{int(number)}/reviews", token=use_token, api_base_url=api_base_url,
        json={"event": decision, "body": body, "commit_id": pr["head_sha"]},
    )
    return {"number": number, "decision": decision, "risk": risk}


def _gh_approvals(repo: str, number: int, *, token: str, api_base_url: str) -> List[str]:
    reviews = _gh_call("GET", repo, f"/pulls/{int(number)}/reviews", token=token, api_base_url=api_base_url, params={"per_page": 100})
    latest: Dict[str, str] = {}
    for r in reviews:
        latest[r["user"]["login"]] = r["state"]
    return [u for u, state in latest.items() if state == "APPROVED"]


def _gh_human_decision(repo: str, number: int, approve: bool, by: str, *, token: str, reviewer_token: Optional[str], api_base_url: str) -> Dict[str, Any]:
    """Record a person's decision (e.g. made via a Slack reaction). Approval is refused while CI is failing, and
    always posted under the reviewer (second) identity."""
    pr = _gh_get_pull_request(repo, number, token=token, api_base_url=api_base_url)
    if pr["state"] != "open":
        raise GithubPrScopeError(f"PR #{number} is {pr['state']}.")
    if approve and pr["checks"]["state"] == "failing":
        raise GithubPrScopeError("CI is failing, so this was not approved.")
    if not reviewer_token:
        raise GithubPrScopeError("Recording a human decision requires a second GitHub identity (reviewer_token_env_var) -- GitHub blocks approving your own PR.")
    event = "APPROVE" if approve else "REQUEST_CHANGES"
    review_body = f"{'Approved' if approve else 'Changes requested'} by {by} in Slack."
    _gh_call(
        "POST", repo, f"/pulls/{int(number)}/reviews", token=reviewer_token, api_base_url=api_base_url,
        json={"event": event, "body": review_body, "commit_id": pr["head_sha"]},
    )
    return {"number": number, "decision": event}


def _gh_merge_if_ready(repo: str, number: int, *, branch_prefix: str, reviewer_login: Optional[str], token: str, api_base_url: str) -> Dict[str, Any]:
    """Squash-merge an agent PR that the second (reviewer) identity approved and whose checks pass. Only ever
    merges branches under `branch_prefix` -- never a human's own branch."""
    pr = _gh_get_pull_request(repo, number, token=token, api_base_url=api_base_url)
    if pr["state"] != "open":
        raise GithubPrScopeError(f"PR #{number} is {pr['state']}.")
    if not pr["head_ref"].startswith(branch_prefix):
        raise GithubPrScopeError("Only agent branches are merged automatically.")
    if not reviewer_login or reviewer_login not in _gh_approvals(repo, number, token=token, api_base_url=api_base_url):
        raise GithubPrScopeError("Not approved by the reviewer yet.")
    if pr["checks"]["state"] != "passing":
        raise GithubPrScopeError(f"Checks are {pr['checks']['state']}.")
    _gh_call("PUT", repo, f"/pulls/{int(number)}/merge", token=token, api_base_url=api_base_url, json={"merge_method": "squash", "sha": pr["head_sha"]})
    try:
        _gh_call("DELETE", repo, f"/git/refs/heads/{pr['head_ref']}", token=token, api_base_url=api_base_url)
    except GithubPrScopeError:
        pass
    return {"number": number, "merged": True}


def _gh_open_pull_requests(repo: str, *, token: str, api_base_url: str, branch_prefix: Optional[str] = None) -> List[Dict[str, Any]]:
    prs = _gh_call("GET", repo, "/pulls", token=token, api_base_url=api_base_url, params={"state": "open", "per_page": 50})
    return [
        {"number": p["number"], "head_sha": p["head"]["sha"], "head_ref": p["head"]["ref"], "title": p["title"], "url": p["html_url"], "author": p["user"]["login"]}
        for p in prs
        if not branch_prefix or p["head"]["ref"].startswith(branch_prefix)
    ]


def _gh_refresh_open_agent_prs(repo: str, *, branch_prefix: str, token: str, api_base_url: str) -> List[int]:
    """Bring every open agent-branch PR up to date with base_branch (GitHub's update-branch API, a merge-base-branch-
    into-head-branch operation) after a merge, so CI runs against the just-merged code instead of a stale base --
    never touches a human's own branch (branch_prefix-only)."""
    updated: List[int] = []
    for pr in _gh_call("GET", repo, "/pulls", token=token, api_base_url=api_base_url, params={"state": "open", "per_page": 50}):
        if not pr["head"]["ref"].startswith(branch_prefix):
            continue
        try:
            _gh_call(
                "PUT", repo, f"/pulls/{pr['number']}/update-branch", token=token, api_base_url=api_base_url,
                json={"expected_head_sha": pr["head"]["sha"]},
            )
            updated.append(pr["number"])
        except GithubPrScopeError:
            pass
    return updated
# END github PR scope


def _gh_is_practice_mode(env_var_name: Optional[str]) -> bool:
    """Same vocabulary as the engineering-agents reference's settings.shadow(): any value except
    '' / '0' / 'false' / 'no' / 'off' (case-insensitive) turns practice mode on. Deliberately NOT a
    typed (e.g. bool) component field templated from an env var at YAML-resolve time -- a numeric-
    looking env value coerced into a typed field at config-validation time is exactly the failure
    mode that broke a whole code location in the reference sandbox (an env value like '1' got
    stored as the integer 1). Reading the env var fresh, by NAME, inside the tool call avoids that
    class of bug entirely and matches how a Dagster+ branch deployment actually scopes env vars --
    the same component YAML, a different env var value per deployment."""
    if not env_var_name:
        return False
    import os
    return str(os.environ.get(env_var_name, "")).strip().lower() not in ("", "0", "false", "no", "off")


# ── GitHub issue filing, with a duplicate-check (same pattern as the engineering-agents reference's
# github_scope.find_issues/open_issue, ported here -- not part of the shared "github PR scope" block
# above since neither slack_pr_approval_watcher nor github_pull_request_agent_sensor needs issue
# filing, only PR review/merge) ─────────────────────────────────────────────────────────────────────
_GH_ISSUE_LABEL = "engineering-agent"
_GH_ISSUE_GENERIC_WORDS = frozenset({"dagster", "assets", "asset", "project", "improve", "improvements", "reliability", "update"})


def _gh_find_issues(
    repo: str, query: str, *, label: str, token: str, api_base_url: str, lookback_days: int = 30,
) -> List[Dict[str, Any]]:
    """Issues the agents already filed (carrying `label`) whose title has any of `query`'s main
    words -- a duplicate-check before filing a new one. Generic words are dropped first so a
    query full of common project vocabulary ('improve this Dagster asset's reliability') doesn't
    "match" nearly every prior issue by coincidence.

    Includes every still-open issue PLUS one closed within the last `lookback_days`. A person
    closing a bad finding is a signal not to re-file it, not something to ignore once the issue
    drops out of an open-only search -- client-side-filtered on `state`/`closed_at` rather than
    an `is:open OR closed:>date` search qualifier, since that compound-boolean search syntax is
    confirmed for GitHub's website search but not confirmed to parse identically through this
    REST `/search/issues` endpoint."""
    import re
    import requests
    from datetime import datetime, timedelta, timezone

    words = [w for w in dict.fromkeys(re.findall(r"[A-Za-z0-9_]{5,}", query.lower())) if w not in _GH_ISSUE_GENERIC_WORDS][:5]
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    found: Dict[int, Dict[str, Any]] = {}
    for word in words:
        q = f"repo:{repo} is:issue label:{label} {word} in:title"
        resp = requests.get(
            f"{api_base_url}/search/issues", params={"q": q, "per_page": 10},
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}, timeout=30,
        )
        if resp.status_code >= 400:
            raise GithubPrScopeError(f"Issue search failed ({resp.status_code}): {resp.text[:200]}")
        for i in resp.json()["items"]:
            state = i.get("state", "open")
            if state == "closed":
                closed_at = i.get("closed_at")
                if not closed_at or datetime.fromisoformat(closed_at.replace("Z", "+00:00")) < cutoff:
                    continue
            found[i["number"]] = {
                "number": i["number"], "title": i["title"], "url": i["html_url"],
                "state": state, "closed_at": i.get("closed_at"),
            }
    return list(found.values())


# BEGIN python snippet check (ported from the engineering-agents reference's snippet_check.py;
# checks Python/diff code blocks in an issue body against the INSTALLED dagster, catching a
# suggestion that calls a renamed/removed API, an attribute that doesn't exist, or a call whose
# arguments don't match the real signature -- before it's ever filed, not after a person tries
# the snippet and it fails.)
import re as _gh_snippet_re
_GH_SNIPPET_FENCE = _gh_snippet_re.compile(r"```([a-zA-Z]*)\n(.*?)```", _gh_snippet_re.DOTALL)
_GH_SNIPPET_IMPORT_LINE = _gh_snippet_re.compile(r"^\s*(from\s+dagster[\w.]*\s+import\s+.+|import\s+dagster[\w.]*(\s+as\s+\w+)?)\s*$")


def _gh_snippet_new_code(block: str) -> str:
    """A unified-diff block's added/context lines, with the +/- prefix stripped -- a plain
    (non-diff) Python block passes through unchanged (no line starts with + or -)."""
    lines = block.splitlines()
    if not any(line.startswith(("+", "-")) for line in lines):
        return block
    return "\n".join(line[1:] for line in lines if line.startswith(("+", " ")) and not line.startswith("+++"))


def _gh_snippet_resolve(root: str, chain: List[str]):
    import importlib
    obj = importlib.import_module(root.split(".")[0])
    for part in [*root.split(".")[1:], *chain]:
        if hasattr(obj, part):
            obj = getattr(obj, part)
        else:
            try:
                obj = importlib.import_module(f"{obj.__name__}.{part}")
            except (ImportError, AttributeError):
                return None
    return obj


def _gh_snippet_chain(node) -> Optional[List[str]]:
    import ast
    parts: List[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    return [node.id, *reversed(parts)] if isinstance(node, ast.Name) else None


def _gh_snippet_check_tree(tree, problems: List[str], aliases: Optional[Dict[str, str]] = None) -> None:
    import ast
    import importlib
    import inspect
    aliases = {} if aliases is None else aliases
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name.split(".")[0] == "dagster":
                    aliases[a.asname or a.name.split(".")[0]] = a.name
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] == "dagster":
            try:
                module = importlib.import_module(node.module)
            except ImportError:
                problems.append(f"line {node.lineno}: module {node.module} does not exist")
                continue
            for a in node.names:
                if a.name != "*" and not hasattr(module, a.name):
                    problems.append(f"line {node.lineno}: {node.module} has no {a.name}")
                aliases[a.asname or a.name] = f"{node.module}.{a.name}"
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            chain = _gh_snippet_chain(node)
            if chain and chain[0] in aliases and aliases[chain[0]] == "dagster" and len(chain) == 2:
                if _gh_snippet_resolve("dagster", chain[1:]) is None:
                    problems.append(f"line {node.lineno}: dagster has no {chain[1]}")
        if not isinstance(node, ast.Call):
            continue
        chain = _gh_snippet_chain(node.func)
        if not chain or chain[0] not in aliases:
            continue
        obj = _gh_snippet_resolve(aliases[chain[0]], chain[1:])
        if obj is None or any(isinstance(a, ast.Starred) for a in node.args) or any(k.arg is None for k in node.keywords):
            continue
        try:
            signature = inspect.signature(obj)
        except (TypeError, ValueError):
            continue
        try:
            signature.bind(*[object()] * len(node.args), **{k.arg: object() for k in node.keywords})
        except TypeError as e:
            problems.append(f"line {node.lineno}: {'.'.join(chain)}{signature} does not accept this call ({e})")


def _gh_snippet_check_by_line(code: str, problems: List[str]) -> None:
    """For a fragment that doesn't parse as a whole (a bare diff hunk, a partial snippet): check
    its imports first, then each remaining line on its own, rather than giving up entirely."""
    import ast
    import contextlib
    aliases: Dict[str, str] = {}
    lines = [line.strip() for line in code.splitlines() if line.strip()]
    for line in lines:
        if _GH_SNIPPET_IMPORT_LINE.match(line):
            with contextlib.suppress(SyntaxError):
                _gh_snippet_check_tree(ast.parse(line), problems, aliases)
    for line in lines:
        if not _GH_SNIPPET_IMPORT_LINE.match(line):
            with contextlib.suppress(SyntaxError):
                _gh_snippet_check_tree(ast.parse(line), problems, aliases)


def _gh_snippet_problems(body: str) -> List[str]:
    """Problems found in the Python and diff code blocks of an issue body against the INSTALLED
    dagster; empty when every block is sound (or there are no Python/diff blocks at all)."""
    import ast
    problems: List[str] = []
    for lang, block in _GH_SNIPPET_FENCE.findall(body or ""):
        if lang.lower() not in ("", "python", "py", "diff"):
            continue
        code = _gh_snippet_new_code(block)
        try:
            _gh_snippet_check_tree(ast.parse(code), problems)
        except SyntaxError:
            _gh_snippet_check_by_line(code, problems)
    return problems
# END python snippet check


def _gh_open_issue(
    repo: str, title: str, body: str, labels: Optional[List[str]], *,
    label: str, token: str, api_base_url: str, practice_mode: bool = False,
    validate_python_snippets: bool = False,
) -> Dict[str, Any]:
    if validate_python_snippets:
        problems = _gh_snippet_problems(body)
        if problems:
            raise GithubPrScopeError("Python snippet(s) in the body don't match the installed dagster: " + "; ".join(problems))
    if practice_mode:
        return {"practice_mode": True, "would_open": "github_issue", "title": title, "url": None}
    try:
        _gh_call("POST", repo, "/labels", token=token, api_base_url=api_base_url, json={"name": label, "color": "1d76db"})
    except GithubPrScopeError:
        pass  # already exists
    extra = [x for x in (labels or []) if x and x != label][:3]
    issue = _gh_call(
        "POST", repo, "/issues", token=token, api_base_url=api_base_url,
        json={"title": title, "body": body + "\n\n---\nFiled by an engineering agent.", "labels": [label, *extra]},
    )
    return {"number": issue["number"], "url": issue["html_url"]}


# ── Repository reading tools (list_files/read_file), ported from the engineering-agents reference's
# github_scope.list_files/read_file/is_excluded/_safe_path -- same traversal-safety (_gh_safe_path, shared
# block above) and exclude_paths matching (_gh_path_matches_any, same function get_pull_request/propose_fix
# already use -- not a second matching function) as the rest of this component, not the reference's own
# settings-object/is_excluded style. Not part of the shared "github PR scope" block above for the same
# reason find_issues/open_issue aren't: neither slack_pr_approval_watcher nor
# github_pull_request_agent_sensor needs repo-browsing, only PR review/merge ─────────────────────────────
def _gh_list_files(
    repo: str, path: str, *, ref: str, token: str, api_base_url: str, exclude_paths=(),
) -> List[Dict[str, Any]]:
    """List files/directories under `path` (GitHub's Contents API: an array of entries for a directory,
    one entry for a file) filtered through the SAME exclude_paths matching get_pull_request/propose_fix
    already use -- an excluded entry never appears in the listing at all, not just refused if asked
    directly."""
    p = _gh_safe_path(path)
    items = _gh_call("GET", repo, f"/contents/{p}", token=token, api_base_url=api_base_url, params={"ref": ref})
    items = items if isinstance(items, list) else [items]

    def _excluded(i: Dict[str, Any]) -> bool:
        # _gh_path_matches_any treats a path's LAST segment as a filename and checks the directory
        # segments before it against a trailing-slash pattern (built for a PR's changed-file paths,
        # which are always files) -- a directory ENTRY's own path has no such trailing filename, so
        # 'assets/ai/github_pr_tools' (the directory itself) wouldn't match 'assets/ai/github_pr_tools/'
        # without this. Appending a placeholder segment makes a directory entry checked the same way a
        # file nested inside it already is, with no second matching function.
        check_path = i["path"] + "/." if i["type"] == "dir" else i["path"]
        return _gh_path_matches_any(check_path, exclude_paths)

    return [{"path": i["path"], "type": i["type"], "size": i.get("size")} for i in items if not _excluded(i)]


def _gh_read_file_tool(
    repo: str, path: str, ref: str, *, token: str, api_base_url: str, exclude_paths=(),
) -> str:
    """read_file tool: refuses a path matching exclude_paths (_gh_read_file above has no reason to do this
    itself -- its one caller, _gh_apply_edit, only ever reaches a file propose_fix has already checked
    against exclude_paths before calling it), then reuses _gh_read_file for the actual fetch + traversal
    check (_gh_safe_path) + base64 decode."""
    p = _gh_safe_path(path)
    if _gh_path_matches_any(p, exclude_paths):
        raise GithubPrScopeError(f"{p!r} is excluded (exclude_paths); agents may not read it.")
    return _gh_read_file(repo, p, ref, token=token, api_base_url=api_base_url)


_AUDIT_LOGGER_NAME = "github_pr_tools.audit"
ALL_TOOLS = (
    "get_pull_request", "list_open_pull_requests", "assess_pull_request", "propose_fix", "review_pull_request",
    "refresh_open_agent_prs", "find_issues", "open_issue", "list_files", "read_file",
)

# Which tools write an outcome-ledger entry, and what kind of action each is -- see the "agent outcome
# ledger" block above. Read-only tools (get_pull_request, list_open_pull_requests, assess_pull_request,
# refresh_open_agent_prs, find_issues, list_files, read_file) never write one.
_LEDGER_KIND_BY_TOOL = {"propose_fix": "pull_request", "open_issue": "github_issue", "review_pull_request": "review"}


def _classify_ledger_bucket(tool: str, result: Dict[str, Any]) -> Optional[str]:
    if tool not in _LEDGER_KIND_BY_TOOL:
        return None
    if result.get("practice_mode"):
        return "would_file"
    if "error" in result:
        # review_pull_request's APPROVE path refuses with "Not approved; a person must approve this
        # one: ..." when the deterministic risk rules say so (e.g. "Kind: improvement") -- that is a
        # person needing to decide, not a failure. Every other error (bad args, a duplicate-file
        # refusal, a protected path) is a guardrail actually declining the action.
        if tool == "review_pull_request" and "Not approved" in str(result["error"]):
            return "decision_needed"
        return "refused"
    return "filed"


class GithubPrTools:
    """Repo-scoped GitHub PR tools. `call(tool, args)` is the one safe entry point (allow-list, audit, size cap,
    error shaping); the `t_*` methods are the raw implementations, built on the github-PR-scope functions above."""

    def __init__(
        self, *, repo: str, token: str, api_base_url: str = "https://api.github.com",
        reviewer_token: Optional[str] = None, base_branch: str = "main", branch_prefix: str = "agent/",
        restrict_to_branch_prefix: bool = False,
        protected_paths=(".github/", "pyproject.toml", "requirements", "Dockerfile", "setup."),
        exclude_paths=(),
        low_risk_paths=("src/", "tests/", "docs/", "README"), low_risk_max_files: int = 2, low_risk_max_lines: int = 30,
        max_files_per_fix: int = 5, allow: Optional[List[str]] = None, max_result_chars: int = 20000,
        practice_mode_env_var: Optional[str] = None, ledger_path: Optional[str] = None,
        duplicate_check_lookback_days: int = 30, validate_python_snippets: bool = False,
        audit: Optional[Callable[[str], None]] = None,
    ):
        import logging
        self.repo = repo
        self.token = token
        self.reviewer_token = reviewer_token
        self.api_base_url = api_base_url
        self.base_branch = base_branch
        self.branch_prefix = branch_prefix
        self.restrict_to_branch_prefix = restrict_to_branch_prefix
        self.protected_paths = tuple(protected_paths)
        self.exclude_paths = tuple(exclude_paths)
        self.low_risk_paths = tuple(low_risk_paths)
        self.low_risk_max_files = low_risk_max_files
        self.low_risk_max_lines = low_risk_max_lines
        self.max_files_per_fix = max_files_per_fix
        self.practice_mode_env_var = practice_mode_env_var
        self.ledger_path = ledger_path
        self.duplicate_check_lookback_days = duplicate_check_lookback_days
        self.validate_python_snippets = validate_python_snippets
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
            self._audit(f"github_pr_tools call tool={tool} repo={self.repo} args={json.dumps({k: (v[:200] if isinstance(v, str) else v) for k, v in args.items()}, default=str)[:300]}")
        except Exception:  # an audit sink must never break a call
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except GithubPrScopeError as e:
            result = {"error": str(e)}
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {e}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"}
        bucket = _classify_ledger_bucket(tool, result)
        if bucket:
            write_outcome_ledger_entry(self.ledger_path, run_id, {
                "component": "github_pr_tools", "tool": tool, "kind": _LEDGER_KIND_BY_TOOL[tool], "bucket": bucket,
                **({"reason": result.get("error")} if bucket in ("refused", "decision_needed") else
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
    def _required_branch_prefix(self) -> Optional[str]:
        return self.branch_prefix if self.restrict_to_branch_prefix else None

    def t_get_pull_request(self, number: int) -> Dict[str, Any]:
        return _gh_get_pull_request(self.repo, number, token=self.token, api_base_url=self.api_base_url, required_branch_prefix=self._required_branch_prefix(), exclude_paths=self.exclude_paths)

    def t_list_open_pull_requests(self) -> Dict[str, Any]:
        return {"pull_requests": _gh_open_pull_requests(self.repo, token=self.token, api_base_url=self.api_base_url, branch_prefix=self._required_branch_prefix())}

    def t_assess_pull_request(self, number: int) -> Dict[str, Any]:
        pr = _gh_get_pull_request(self.repo, number, token=self.token, api_base_url=self.api_base_url, required_branch_prefix=self._required_branch_prefix(), exclude_paths=self.exclude_paths)
        return _gh_assess_risk(
            pr, protected_paths=self.protected_paths, low_risk_paths=self.low_risk_paths,
            low_risk_max_files=self.low_risk_max_files, low_risk_max_lines=self.low_risk_max_lines,
        )

    def t_propose_fix(self, branch: str, title: str, body: str, files: List[Dict[str, Any]], note: str = "") -> Dict[str, Any]:
        return _gh_propose_fix(
            self.repo, branch, title, body, files, base_branch=self.base_branch, branch_prefix=self.branch_prefix,
            protected_paths=self.protected_paths, max_files_per_fix=self.max_files_per_fix,
            exclude_paths=self.exclude_paths,
            token=self.token, api_base_url=self.api_base_url, note=note,
            practice_mode=_gh_is_practice_mode(self.practice_mode_env_var),
        )

    def t_refresh_open_agent_prs(self) -> Dict[str, Any]:
        """Bring open agent-branch PRs up to date with base_branch after a merge, so CI runs
        against the just-merged code instead of a stale base."""
        return {"updated": _gh_refresh_open_agent_prs(self.repo, branch_prefix=self.branch_prefix, token=self.token, api_base_url=self.api_base_url)}

    def t_find_issues(self, query: str) -> Dict[str, Any]:
        """Open (or recently-closed, within duplicate_check_lookback_days) issues the agents
        already filed whose title matches query's main words -- call this BEFORE open_issue to
        avoid filing the same problem twice. A recently-closed match is still returned (with
        state/closed_at) since a person closing a bad finding is a signal not to re-file it."""
        return {"issues": _gh_find_issues(
            self.repo, query, label=_GH_ISSUE_LABEL, token=self.token, api_base_url=self.api_base_url,
            lookback_days=self.duplicate_check_lookback_days,
        )}

    def t_open_issue(self, title: str, body: str, labels: Optional[List[str]] = None) -> Dict[str, Any]:
        return _gh_open_issue(
            self.repo, title, body, labels, label=_GH_ISSUE_LABEL, token=self.token, api_base_url=self.api_base_url,
            practice_mode=_gh_is_practice_mode(self.practice_mode_env_var),
            validate_python_snippets=self.validate_python_snippets,
        )

    def t_list_files(self, path: str = "") -> Dict[str, Any]:
        """Files/directories under `path` in the configured repo, pinned to base_branch. An entry
        matching exclude_paths never appears in the listing at all."""
        return {"path": path, "entries": _gh_list_files(
            self.repo, path, ref=self.base_branch, token=self.token, api_base_url=self.api_base_url,
            exclude_paths=self.exclude_paths,
        )}

    def t_read_file(self, path: str, ref: Optional[str] = None) -> Dict[str, Any]:
        """One file's content. ref defaults to base_branch; pass a branch, commit SHA or tag to read a
        different one. Refuses a path matching exclude_paths."""
        used_ref = ref or self.base_branch
        content = _gh_read_file_tool(
            self.repo, path, used_ref, token=self.token, api_base_url=self.api_base_url,
            exclude_paths=self.exclude_paths,
        )
        return {"path": path, "ref": used_ref, "content": content}

    def t_review_pull_request(self, number: int, decision: str, body: str) -> Dict[str, Any]:
        return _gh_review_pull_request(
            self.repo, number, decision, body, token=self.token, reviewer_token=self.reviewer_token,
            api_base_url=self.api_base_url, protected_paths=self.protected_paths, low_risk_paths=self.low_risk_paths,
            low_risk_max_files=self.low_risk_max_files, low_risk_max_lines=self.low_risk_max_lines,
            required_branch_prefix=self._required_branch_prefix(), exclude_paths=self.exclude_paths,
        )


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>` -- `__name__` alone is `"__main__"` both for `python
    component.py` and for `python -m dotted.path`; `__spec__.name` is the real dotted path only the `-m` form sets.
    Same mechanism as agentic_pipeline_authoring_tools'/dagster_instance_tools' own copies (duplicated here, not
    imported, per this repo's self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "github_pr_tools's mcp_server_resource_key path must be run as `python -m <dotted.path.to.this.module>`, "
        "not `python component.py` directly -- that bare-script form still works for `python component.py` itself, "
        "just not for this path. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource (name/type/command/url/env/headers/headers_env) -- inlined here
    rather than imported from resources/mcp_server_resource (confirmed not importable across components). Same copy
    agentic_pipeline_authoring_tools/dagster_instance_tools/dagster_workflow_tools each carry."""

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


class GithubPrToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers repo-scoped GitHub pull-request tools as a resource (default key `github_pr_tools`). For an
    agent (MCP only) or Claude Code (`claude mcp add`), run THIS FILE as the stdio MCP server -- see README.md.
    """

    resource_key: str = Field(default="github_pr_tools", description="Resource key the toolset is registered under.")
    repo: str = Field(description="GitHub repository the tools are scoped to, 'owner/repo'. Every call is pinned to this repo.")
    token_env_var: str = Field(default="GITHUB_TOKEN", description="Env var holding the primary GitHub token (reads, COMMENT reviews, propose_fix, merge).")
    reviewer_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding a SECOND GitHub token (a distinct bot/user identity with write access) -- required for "
            "an APPROVE review. GitHub blocks a PR's own author (or a single bot identity that opened it) from "
            "approving it, so APPROVE always needs a genuinely different identity's token."
        ),
    )
    api_base_url: str = Field(default="https://api.github.com", description="API base URL. Change for GitHub Enterprise Server.")
    base_branch: str = Field(default="main", description="Base branch for propose_fix's pull requests.")
    branch_prefix: str = Field(default="agent/", description="Prefix propose_fix uses for the branches it creates.")
    restrict_to_branch_prefix: bool = Field(
        default=False,
        description=(
            "When true, get_pull_request/list_open_pull_requests/assess_pull_request/review_pull_request all "
            "refuse (GithubPrScopeError) to operate on a PR whose head branch doesn't start with branch_prefix -- "
            "never a human's or a bot's (e.g. dependabot's) PR. Off by default, since this toolset also works as "
            "a general-purpose PR reviewer for any PR. Turn this on when pairing with "
            "sensors/github_pull_request_agent_sensor (which already only ever launches runs for branch_prefix "
            "PRs) -- a second layer of defense covering a manually-invoked chat agent or a hallucinated PR number."
        ),
    )
    protected_paths: List[str] = Field(
        default_factory=lambda: [".github/", "pyproject.toml", "requirements", "Dockerfile", "setup."],
        description=(
            "Path prefixes propose_fix refuses to touch (CI, dependencies, packaging). A "
            "directory-like entry (ends with '/', e.g. '.github/') matches that segment anywhere "
            "in the path, not just at the repo root -- a monorepo's projects commonly live in a "
            "subfolder (e.g. 'hooli-basics/.github/workflows/ci.yml'). A filename-like entry (no "
            "trailing '/', e.g. 'Dockerfile'/'pyproject.toml') matches the file's own basename at "
            "any depth, as well as a root-level prefix."
        ),
    )
    exclude_paths: List[str] = Field(
        default_factory=list,
        description=(
            "Path prefixes the agent may neither READ nor change -- get_pull_request/"
            "assess_pull_request/review_pull_request never include a matching file in the PR's "
            "file list at all (not just refuse to approve touching it), propose_fix refuses "
            "to change one, list_files never includes a matching entry in its listing at all, "
            "and read_file refuses to read one. For when the repository that hosts these agent components is the "
            "SAME repository they review (e.g. 'assets/ai/github_pr_tools/' for this repo's own "
            "copy) -- without this, a review agent spends its review on its own source instead "
            "of the PR it's actually meant to look at. Same directory-like/filename-like "
            "matching convention as protected_paths."
        ),
    )
    low_risk_paths: List[str] = Field(
        default_factory=lambda: ["src/", "tests/", "docs/", "README"],
        description=(
            "Path prefixes assess_pull_request/review_pull_request treat as low-risk when "
            "nothing else disqualifies the PR. Same directory-like/filename-like matching "
            "convention as protected_paths -- 'src/' matches a 'src' segment anywhere in the "
            "path (e.g. 'hooli-basics/src/orders.py'), not only at the repo root."
        ),
    )
    low_risk_max_files: int = Field(default=2, description="assess_pull_request: max changed files for a 'low' risk level.")
    low_risk_max_lines: int = Field(default=30, description="assess_pull_request: max changed lines (additions + deletions) for a 'low' risk level.")
    max_files_per_fix: int = Field(default=5, description="propose_fix: max files a single fix may change.")
    practice_mode_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var NAME (not a value templated into this field) whose value, read fresh at call "
            "time, switches propose_fix into practice mode: every validation (protected paths, "
            "duplicate-PR-file check, file count) still runs, but no branch/commit/PR is actually "
            "created -- {'practice_mode': True, 'would_open': 'pull_request', ...} is returned "
            "instead. Same vocabulary as the engineering-agents reference's settings.shadow(): any "
            "value except '', '0', 'false', 'no', 'off' (case-insensitive) is ON. Scope this to a "
            "Dagster+ branch deployment (set the env var there, leave it unset in the full "
            "deployment) to practice in a PR's own branch deployment and act for real after merge."
        ),
    )

    ledger_path: Optional[str] = Field(
        default=None,
        description=(
            "Directory this tool writes one JSON-lines file per Dagster run_id into, recording every "
            "consequential action (propose_fix, open_issue, an APPROVE review) as it happens -- filed, "
            "would_file (practice mode), refused (a guardrail declined it), or decision_needed (e.g. an "
            "'Kind: improvement' PR an APPROVE rule always refers to a person). The run_id comes from "
            "MCP's out-of-band request _meta (see each agent component's own run_id threading), NOT a "
            "tool argument -- a prompt cannot spoof which run an entry belongs to. Point an agent "
            "component's own outcome_ledger_path at this SAME directory to read it back. Unset: no "
            "ledger is written (zero behavior change)."
        ),
    )
    duplicate_check_lookback_days: int = Field(
        default=30,
        description=(
            "find_issues includes an issue closed within this many days, not just still-open "
            "ones. A person closing a bad finding (wontfix, already handled) is a signal not to "
            "re-file it -- without this, that signal is lost the moment the issue drops out of "
            "an open-only search, and an advisor agent re-files the same thing it was already "
            "told no about."
        ),
    )
    validate_python_snippets: bool = Field(
        default=False,
        description=(
            "open_issue parses every Python/diff code block in the body and refuses to file (with "
            "the specific reason) if a Dagster import, attribute, or call doesn't match the "
            "INSTALLED dagster -- e.g. a renamed/removed function, or a call whose arguments don't "
            "match the real signature. Catches a wrong suggestion before it's filed, not after a "
            "person tries it and it fails. Best-effort: a snippet that doesn't parse as Python at "
            "all is checked line by line instead of being skipped."
        ),
    )

    allow: Optional[List[str]] = Field(default=None, description=f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}.")
    max_result_chars: int = Field(default=20000, description="Upper bound on the serialized size of any tool result.")
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "If set, ALSO registers a stdio MCP-server-shaped resource under this key (python -m this module, with "
            "GITHUB_PR_TOOLS_* env vars set from this component's own settings, and the real token(s) passed through "
            "via live env passthrough). Any mcp_servers-aware component can then reference it via server_resource_key."
        ),
    )
    mcp_extra_env_vars: Optional[List[str]] = Field(
        default=None,
        description="mcp_server_resource_key only. Extra env var NAMES (beyond the token env vars, which are always passed through) to copy into the spawned MCP server's env.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("protected_paths", "exclude_paths", "low_risk_paths", "allow", "mcp_extra_env_vars"), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Github Pr Tools", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        repo = self.repo
        token_env_var = self.token_env_var
        reviewer_token_env_var = self.reviewer_token_env_var
        api_base_url = self.api_base_url
        base_branch = self.base_branch
        branch_prefix = self.branch_prefix
        restrict_to_branch_prefix = self.restrict_to_branch_prefix
        protected_paths = list(self.protected_paths)
        exclude_paths = list(self.exclude_paths)
        low_risk_paths = list(self.low_risk_paths)
        low_risk_max_files = self.low_risk_max_files
        low_risk_max_lines = self.low_risk_max_lines
        max_files_per_fix = self.max_files_per_fix
        practice_mode_env_var = self.practice_mode_env_var
        ledger_path = self.ledger_path
        duplicate_check_lookback_days = self.duplicate_check_lookback_days
        validate_python_snippets = self.validate_python_snippets
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(ALL_TOOLS)}")
        max_chars = int(self.max_result_chars)

        def _init(init_context):
            return GithubPrTools(
                repo=repo, token=os.environ.get(token_env_var, ""), reviewer_token=(os.environ.get(reviewer_token_env_var) if reviewer_token_env_var else None),
                api_base_url=api_base_url, base_branch=base_branch, branch_prefix=branch_prefix,
                restrict_to_branch_prefix=restrict_to_branch_prefix,
                protected_paths=protected_paths, exclude_paths=exclude_paths, low_risk_paths=low_risk_paths, low_risk_max_files=low_risk_max_files,
                low_risk_max_lines=low_risk_max_lines, max_files_per_fix=max_files_per_fix, allow=allow, max_result_chars=max_chars,
                practice_mode_env_var=practice_mode_env_var, ledger_path=ledger_path,
                duplicate_check_lookback_days=duplicate_check_lookback_days,
                validate_python_snippets=validate_python_snippets,
            )

        res = dg.ResourceDefinition(resource_fn=_init, description=f"Repo-scoped GitHub PR tools for {repo}.")
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {
                "GITHUB_PR_TOOLS_REPO": repo, "GITHUB_PR_TOOLS_TOKEN_ENV_VAR": token_env_var,
                "GITHUB_PR_TOOLS_API_BASE_URL": api_base_url, "GITHUB_PR_TOOLS_BASE_BRANCH": base_branch,
                "GITHUB_PR_TOOLS_BRANCH_PREFIX": branch_prefix,
                "GITHUB_PR_TOOLS_RESTRICT_TO_BRANCH_PREFIX": "1" if restrict_to_branch_prefix else "",
                "GITHUB_PR_TOOLS_PROTECTED_PATHS": ",".join(protected_paths),
                "GITHUB_PR_TOOLS_EXCLUDE_PATHS": ",".join(exclude_paths),
                "GITHUB_PR_TOOLS_LOW_RISK_PATHS": ",".join(low_risk_paths),
                "GITHUB_PR_TOOLS_LOW_RISK_MAX_FILES": str(low_risk_max_files),
                "GITHUB_PR_TOOLS_LOW_RISK_MAX_LINES": str(low_risk_max_lines),
                "GITHUB_PR_TOOLS_MAX_FILES_PER_FIX": str(max_files_per_fix),
                "GITHUB_PR_TOOLS_MAX_CHARS": str(max_chars),
                "GITHUB_PR_TOOLS_DUPLICATE_CHECK_LOOKBACK_DAYS": str(duplicate_check_lookback_days),
                "GITHUB_PR_TOOLS_VALIDATE_PYTHON_SNIPPETS": "1" if validate_python_snippets else "",
            }
            if reviewer_token_env_var:
                mcp_env["GITHUB_PR_TOOLS_REVIEWER_TOKEN_ENV_VAR"] = reviewer_token_env_var
            if allow:
                mcp_env["GITHUB_PR_TOOLS_ALLOW"] = ",".join(allow)
            if practice_mode_env_var:
                mcp_env["GITHUB_PR_TOOLS_PRACTICE_MODE_ENV_VAR"] = practice_mode_env_var
            if ledger_path:
                mcp_env["GITHUB_PR_TOOLS_LEDGER_PATH"] = ledger_path
            passthrough = (
                [token_env_var]
                + ([reviewer_token_env_var] if reviewer_token_env_var else [])
                + ([practice_mode_env_var] if practice_mode_env_var else [])
                + list(self.mcp_extra_env_vars or [])
            )
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key, type="stdio", command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env, live_env_passthrough=passthrough,
            )

        return dg.Definitions(resources=resources)


# ── stdio MCP server entrypoint:  python -m <this module>  (or  python component.py) ────────────────────────────────
def build_mcp_server(tools: GithubPrTools):
    """MCP server (official `mcp` package) exposing exactly the allowed tools. Works on mcp 1.x and 2.x."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server, Context as _Context
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server, Context as _Context

    mcp = _Server("github-pr-tools")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__ in allowed:
            mcp.tool()(fn)
        return fn

    def _run_id_from(ctx) -> Optional[str]:
        """ctx (MCP's Context, type-annotated so the framework injects it -- not a tool argument a
        prompt supplies) carries this call's out-of-band request _meta; `dagster_run_id` is what each
        agent component's own run_id threading puts there."""
        meta = getattr(getattr(ctx, "request_context", None), "meta", None)
        return (meta or {}).get("dagster_run_id") if ctx is not None else None

    @_tool
    def get_pull_request(number: int) -> dict:
        """A pull request's title, description, changed files with patches, and CI check state."""
        return tools.call("get_pull_request", {"number": number})

    @_tool
    def list_open_pull_requests() -> dict:
        """Open pull requests (number, title, branch, author), to avoid proposing a change twice."""
        return tools.call("list_open_pull_requests")

    @_tool
    def assess_pull_request(number: int) -> dict:
        """The fixed risk assessment for a PR: level 'low' or 'needs_human', and the reasons. review_pull_request enforces this; it is not advisory only."""
        return tools.call("assess_pull_request", {"number": number})

    @_tool
    def propose_fix(branch: str, title: str, body: str, files: list, note: str = "", ctx: _Context = None) -> dict:
        """Open a pull request that changes files. `files` is a list where each item is either {path, old, new},
        which replaces that exact text (it must appear once) and leaves the rest of the file untouched, or
        {path, content} with the FULL new content (only for new files). The branch is created for you. CI,
        dependencies and packaging files are protected. Never merges."""
        return tools.call("propose_fix", {"branch": branch, "title": title, "body": body, "files": files, "note": note}, run_id=_run_id_from(ctx))

    @_tool
    def review_pull_request(number: int, decision: str, body: str, ctx: _Context = None) -> dict:
        """Review a PR. decision is COMMENT or APPROVE. APPROVE is refused unless the PR is low risk, and is posted under a second GitHub identity."""
        return tools.call("review_pull_request", {"number": number, "decision": decision, "body": body}, run_id=_run_id_from(ctx))

    @_tool
    def refresh_open_agent_prs() -> dict:
        """Bring every open agent-branch PR up to date with the base branch (e.g. after a merge), so CI runs against the just-merged code instead of a stale base."""
        return tools.call("refresh_open_agent_prs", {})

    @_tool
    def find_issues(query: str) -> dict:
        """Open issues the agents already filed whose title matches query's main words. Call this BEFORE open_issue to avoid filing the same problem twice."""
        return tools.call("find_issues", {"query": query})

    @_tool
    def open_issue(title: str, body: str, labels: list = None, ctx: _Context = None) -> dict:
        """File a GitHub issue (labeled 'engineering-agent'). Check find_issues first."""
        return tools.call("open_issue", {"title": title, "body": body, "labels": labels}, run_id=_run_id_from(ctx))

    @_tool
    def list_files(path: str = "") -> dict:
        """List files/directories under `path` in the configured repo (pinned to base_branch). An
        entry matching exclude_paths never appears in the listing at all."""
        return tools.call("list_files", {"path": path})

    @_tool
    def read_file(path: str, ref: str = None) -> dict:
        """Read one file's content from the configured repo. ref defaults to base_branch; pass a
        branch, commit SHA or tag to read a different one. Refuses a path matching exclude_paths."""
        return tools.call("read_file", {"path": path, "ref": ref})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: GITHUB_PR_TOOLS_REPO (required), _TOKEN_ENV_VAR (default GITHUB_TOKEN), _REVIEWER_TOKEN_ENV_VAR,
    _API_BASE_URL, _BASE_BRANCH, _BRANCH_PREFIX, _RESTRICT_TO_BRANCH_PREFIX (any non-empty value is ON),
    _PROTECTED_PATHS (csv), _EXCLUDE_PATHS (csv), _LOW_RISK_PATHS (csv),
    _LOW_RISK_MAX_FILES, _LOW_RISK_MAX_LINES, _MAX_FILES_PER_FIX, _ALLOW (csv), _MAX_CHARS,
    _PRACTICE_MODE_ENV_VAR (the NAME of another env var to read fresh at call time -- not a value),
    _LEDGER_PATH (directory for the per-run_id outcome-ledger JSON-lines files)."""
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    repo = os.environ.get("GITHUB_PR_TOOLS_REPO")
    if not repo:
        raise RuntimeError("GITHUB_PR_TOOLS_REPO must be set.")
    token_env_var = os.environ.get("GITHUB_PR_TOOLS_TOKEN_ENV_VAR", "GITHUB_TOKEN")
    reviewer_token_env_var = os.environ.get("GITHUB_PR_TOOLS_REVIEWER_TOKEN_ENV_VAR")
    allow_env = os.environ.get("GITHUB_PR_TOOLS_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None
    protected_env = os.environ.get("GITHUB_PR_TOOLS_PROTECTED_PATHS")
    exclude_env = os.environ.get("GITHUB_PR_TOOLS_EXCLUDE_PATHS")
    low_risk_env = os.environ.get("GITHUB_PR_TOOLS_LOW_RISK_PATHS")
    tools = GithubPrTools(
        repo=repo, token=os.environ.get(token_env_var, ""),
        reviewer_token=(os.environ.get(reviewer_token_env_var) if reviewer_token_env_var else None),
        api_base_url=os.environ.get("GITHUB_PR_TOOLS_API_BASE_URL", "https://api.github.com"),
        base_branch=os.environ.get("GITHUB_PR_TOOLS_BASE_BRANCH", "main"),
        branch_prefix=os.environ.get("GITHUB_PR_TOOLS_BRANCH_PREFIX", "agent/"),
        restrict_to_branch_prefix=bool(os.environ.get("GITHUB_PR_TOOLS_RESTRICT_TO_BRANCH_PREFIX")),
        protected_paths=[p.strip() for p in protected_env.split(",") if p.strip()] if protected_env else (".github/", "pyproject.toml", "requirements", "Dockerfile", "setup."),
        exclude_paths=[p.strip() for p in exclude_env.split(",") if p.strip()] if exclude_env else (),
        low_risk_paths=[p.strip() for p in low_risk_env.split(",") if p.strip()] if low_risk_env else ("src/", "tests/", "docs/", "README"),
        low_risk_max_files=int(os.environ.get("GITHUB_PR_TOOLS_LOW_RISK_MAX_FILES", "2")),
        low_risk_max_lines=int(os.environ.get("GITHUB_PR_TOOLS_LOW_RISK_MAX_LINES", "30")),
        max_files_per_fix=int(os.environ.get("GITHUB_PR_TOOLS_MAX_FILES_PER_FIX", "5")),
        allow=allow, max_result_chars=int(os.environ.get("GITHUB_PR_TOOLS_MAX_CHARS", "20000")),
        practice_mode_env_var=os.environ.get("GITHUB_PR_TOOLS_PRACTICE_MODE_ENV_VAR"),
        ledger_path=os.environ.get("GITHUB_PR_TOOLS_LEDGER_PATH"),
        duplicate_check_lookback_days=int(os.environ.get("GITHUB_PR_TOOLS_DUPLICATE_CHECK_LOOKBACK_DAYS", "30")),
        validate_python_snippets=bool(os.environ.get("GITHUB_PR_TOOLS_VALIDATE_PYTHON_SNIPPETS")),
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
