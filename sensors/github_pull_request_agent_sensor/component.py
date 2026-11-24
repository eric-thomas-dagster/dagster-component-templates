"""GithubPullRequestAgentSensorComponent.

On each new pull request or new head commit, launches an agent asset selection with the PR number on the run's
tags, and announces it in Slack with approve/reject reactions seeded -- the thread stays open for replies
(`slack_reply` answers in it) and for a vote (`sensors/slack_pr_approval_watcher` reads the reaction and records a
GitHub review).

A plain `@dg.sensor` (not `@dg.run_failure_sensor`, which only accepts `request_job`, not an asset selection) --
this one fires `RunRequest(asset_selection=...)` directly, the same way `linear_issue_sensor`/`github_event_sensor`
already do in this repo.

No shared state file with `slack_pr_approval_watcher`: this component's own Dagster-native sensor cursor holds the
dedup-by-head-sha map (one entry per open PR number), and the Slack correlation ("which message is PR #42's review
thread") rides inside the posted message's own hidden metadata (`chat.postMessage`'s `metadata` param, same
mechanism `slack_approval_gate`'s `message_metadata` correlation mode uses) -- not a document two sensors could race
on rewriting.
"""
import json
import os
from typing import Any, Dict, List, Optional

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


# BEGIN slack PR review metadata (inlined per component; keep copies identical -- see tests/test_slack_pr_review_metadata_drift.py)
# Posting + discovering PR-review Slack messages, with no shared file between the posting sensor
# (github_pull_request_agent_sensor) and the watching sensor (slack_pr_approval_watcher) -- the correlation (which
# PR a message is reviewing, and who's allowed to vote) rides inside the message's own hidden metadata
# (chat.postMessage's own `metadata` param -- invisible in the UI, read back via
# conversations.history(include_all_metadata=true)), same mechanism slack_approval_gate's own message_metadata
# correlation mode uses.
_PR_REVIEW_EVENT_TYPE = "dagster_pr_review_v1"


def _get_slack_client(bot_token_env_var: str):
    try:
        from slack_sdk import WebClient
    except ImportError as e:
        raise ImportError("requires slack_sdk: pip install 'slack_sdk>=3.0'") from e
    token = os.environ.get(bot_token_env_var)
    if not token:
        raise RuntimeError(f"Slack bot token env var {bot_token_env_var!r} not set.")
    return WebClient(token=token)


def _lookup_bot_user_id(client) -> Optional[str]:
    try:
        auth = client.auth_test()
        return auth.get("user_id")
    except Exception:  # noqa: BLE001
        return None


def _post_pr_review_message(client, channel: str, text: str, approve_emoji: str, reject_emoji: str, ping_users: List[str], event_payload: Dict[str, Any]) -> Dict[str, Any]:
    pings = " ".join(f"<@{u}>" for u in ping_users) + " " if ping_users else ""
    body = f"{pings}{text}"
    resp = client.chat_postMessage(channel=channel, text=body, metadata={"event_type": _PR_REVIEW_EVENT_TYPE, "event_payload": event_payload})
    if not resp.get("ok"):
        raise RuntimeError(f"Slack chat_postMessage failed: {resp.get('error')}")
    message_ts, channel_id = resp["ts"], resp["channel"]
    for emoji in (approve_emoji, reject_emoji):
        try:
            client.reactions_add(channel=channel_id, timestamp=message_ts, name=emoji)
        except Exception:  # noqa: BLE001
            pass
    return {"message_ts": message_ts, "channel_id": channel_id}


def _iter_pr_review_messages(client, channel: str, lookback_hours: float):
    """Yields {message_ts, payload} for every dagster_pr_review_v1-tagged message in `channel` within the last
    `lookback_hours`, paginated via conversations.history's own cursor."""
    import time as _time
    oldest = str(_time.time() - lookback_hours * 3600)
    cursor = None
    while True:
        kwargs: Dict[str, Any] = {"channel": channel, "oldest": oldest, "include_all_metadata": True, "limit": 200}
        if cursor:
            kwargs["cursor"] = cursor
        resp = client.conversations_history(**kwargs)
        if not resp.get("ok"):
            raise RuntimeError(f"Slack conversations_history failed: {resp.get('error')}")
        for m in resp.get("messages", []) or []:
            meta = m.get("metadata") or {}
            if meta.get("event_type") != _PR_REVIEW_EVENT_TYPE:
                continue
            yield {"message_ts": m["ts"], "payload": meta.get("event_payload") or {}}
        cursor = (resp.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            return


def _fetch_reactions(client, channel_id: str, message_ts: str) -> Dict[str, List[str]]:
    resp = client.reactions_get(channel=channel_id, timestamp=message_ts)
    if not resp.get("ok"):
        raise RuntimeError(f"Slack reactions_get failed: {resp.get('error')}")
    reactions = (resp.get("message", {}) or {}).get("reactions", []) or []
    return {r["name"]: r.get("users", []) for r in reactions}
# END slack PR review metadata


class GithubPullRequestAgentSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """On each new pull request or new head commit, launches `asset_keys` with the PR number on the run's tags,
    and announces it in Slack with approve/reject reactions seeded. Pair with
    `sensors/slack_pr_approval_watcher` to turn a reaction into a recorded GitHub review (and optionally a merge)."""

    sensor_name: str = Field(default="github_pull_request_agent_sensor", description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Agent asset(s) to launch for each new/updated pull request (e.g. a PR-reviewer asset + its Slack-reply asset).")
    repo: str = Field(description="GitHub repository to watch, 'owner/repo'.")
    token_env_var: str = Field(default="GITHUB_TOKEN", description="Env var holding a GitHub token with read access to pull requests.")
    api_base_url: str = Field(default="https://api.github.com", description="API base URL. Change for GitHub Enterprise Server.")
    branch_prefix: str = Field(
        default="agent/",
        description=(
            "Only pull requests whose head branch starts with this prefix are ever announced in Slack or "
            "launched as a review run -- never a human's PR or a bot's (e.g. dependabot's). Matches the "
            "branch_prefix assets/ai/github_pr_tools' propose_fix uses by default, so a paired reviewer agent "
            "only ever reviews its own fixer's PRs. Set to '' to watch every open PR (not recommended -- a "
            "review run can comment on or approve a PR)."
        ),
    )
    announcements_per_tick: int = Field(
        default=5,
        description=(
            "Max new/updated PRs announced (Slack post + RunRequest) in one sensor tick. A Dagster+ sensor tick "
            "times out at 60 seconds; a backlog of many PRs (e.g. the first tick after turning this sensor on) "
            "posts one Slack message each and can blow through that. Unannounced PRs stay in the backlog (the "
            "cursor isn't advanced for them) and are picked up on a later tick."
        ),
    )
    skip_in_branch_deployments: bool = Field(
        default=True,
        description=(
            "Skip every tick while running inside a Dagster+ branch deployment "
            "(DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT=1). A branch deployment runs its own copy of every sensor "
            "alongside the full deployment's -- without this, a branch deployment double-announces PRs in "
            "Slack and double-launches review runs."
        ),
    )

    slack_channel: str = Field(description="Channel to announce new/updated pull requests in (either `C123ABC456` id or `#channel-name`).")
    slack_bot_token_env_var: str = Field(default="SLACK_BOT_TOKEN", description="Env var holding the bot's `xoxb-...` token. Needs `chat:write` + `reactions:read` + `reactions:write` + `channels:history`/`groups:history`.")
    approve_emoji: str = Field(default="white_check_mark", description="Emoji name (no colons) a reaction-based approval uses.")
    reject_emoji: str = Field(default="x", description="Emoji name (no colons) a reaction-based rejection uses.")
    approver_allowlist: Optional[List[str]] = Field(
        default=None,
        description="Slack user IDs (`U123...`) allowed to vote on the posted thread (enforced by sensors/slack_pr_approval_watcher, which reads this from the message's own metadata). Leave unset to allow anyone in the channel to vote.",
    )
    message_template: Optional[str] = Field(
        default=None,
        description="Optional custom message template. Placeholders: {number}, {title}, {url}, {author}, {approve_emoji}, {reject_emoji}. Default: ':mag: Pull request <{url}|#{number} {title}> by {author}. Reviewing now. React :{approve_emoji}: to approve or :{reject_emoji}: to request changes.'",
    )
    ping_users_on_post: Optional[List[str]] = Field(default=None, description="Optional Slack user IDs to @-mention in the initial post.")

    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("asset_keys", "approver_allowlist", "ping_users_on_post"), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Github Pull Request Agent Sensor", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        targets = [dg.AssetKey.from_user_string(k) for k in self.asset_keys]
        repo = self.repo
        token_env_var = self.token_env_var
        api_base_url = self.api_base_url
        branch_prefix = self.branch_prefix or None
        announcements_per_tick = max(1, int(self.announcements_per_tick))
        skip_in_branch_deployments = self.skip_in_branch_deployments
        slack_channel = self.slack_channel
        slack_bot_token_env_var = self.slack_bot_token_env_var
        approve_emoji = self.approve_emoji
        reject_emoji = self.reject_emoji
        approver_allowlist = list(self.approver_allowlist) if self.approver_allowlist else None
        ping_users = list(self.ping_users_on_post or [])
        message_template = self.message_template or (
            ":mag: Pull request <{url}|#{number} {title}> by {author}. Reviewing now. "
            "React :{approve_emoji}: to approve or :{reject_emoji}: to request changes."
        )

        def sensor_fn(context: dg.SensorEvaluationContext, **_resources):
            if skip_in_branch_deployments and os.environ.get("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1":
                return dg.SkipReason("branch deployment -- skipping (skip_in_branch_deployments=True)")
            token = os.environ.get(token_env_var, "")
            if not token:
                return dg.SkipReason(f"{token_env_var} not set")
            try:
                prs = _gh_open_pull_requests(repo, token=token, api_base_url=api_base_url, branch_prefix=branch_prefix)
            except GithubPrScopeError as e:
                return dg.SkipReason(f"GitHub error: {e}")

            try:
                seen = json.loads(context.cursor) if context.cursor else {}
            except Exception:  # noqa: BLE001
                seen = {}

            new_or_updated = [pr for pr in prs if seen.get(str(pr["number"])) != pr["head_sha"]]
            if not new_or_updated:
                # Prune entries for PRs that are no longer open, so the cursor doesn't grow forever.
                open_numbers = {str(pr["number"]) for pr in prs}
                pruned = {k: v for k, v in seen.items() if k in open_numbers}
                return dg.SensorResult(skip_reason="no new or updated pull requests", cursor=json.dumps(pruned))

            # A Dagster+ sensor tick times out at 60s -- a large backlog (e.g. the first tick after turning this
            # sensor on) posts one Slack message per PR and can blow through that. Cap this tick's work; whatever's
            # left over isn't in `seen` yet, so it's picked up again next tick.
            deferred_count = max(0, len(new_or_updated) - announcements_per_tick)
            new_or_updated = new_or_updated[:announcements_per_tick]
            if deferred_count:
                context.log.info(f"{deferred_count} more pull request(s) deferred to a later tick (announcements_per_tick={announcements_per_tick})")

            client = _get_slack_client(slack_bot_token_env_var)
            run_requests: List[dg.RunRequest] = []
            problems: List[str] = []
            for pr in new_or_updated:
                body = message_template.format(
                    number=pr["number"], title=pr["title"], url=pr["url"], author=pr["author"],
                    approve_emoji=approve_emoji, reject_emoji=reject_emoji,
                )
                event_payload: Dict[str, Any] = {"pr_number": pr["number"], "head_sha": pr["head_sha"], "approve_emoji": approve_emoji, "reject_emoji": reject_emoji}
                if approver_allowlist:
                    event_payload["approver_allowlist"] = approver_allowlist
                try:
                    posted = _post_pr_review_message(client, slack_channel, body, approve_emoji, reject_emoji, ping_users, event_payload)
                except Exception as e:  # noqa: BLE001
                    problems.append(f"PR #{pr['number']}: Slack post failed: {e}")
                    continue
                seen[str(pr["number"])] = pr["head_sha"]
                run_requests.append(dg.RunRequest(
                    run_key=f"pr-{pr['number']}-{pr['head_sha'][:7]}",
                    asset_selection=targets,
                    tags={"eng/pr_number": str(pr["number"]), "slack/channel": posted["channel_id"], "slack/thread_ts": posted["message_ts"]},
                ))

            new_cursor = json.dumps(seen)
            if not run_requests:
                return dg.SensorResult(skip_reason="; ".join(problems) or "no runs launched", cursor=new_cursor)
            return dg.SensorResult(run_requests=run_requests, cursor=new_cursor)

        sensor_def = dg.SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=dg.DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else dg.DefaultSensorStatus.STOPPED,
            asset_selection=dg.AssetSelection.assets(*targets),
        )
        return dg.Definitions(sensors=[sensor_def])
