"""SlackPrApprovalWatcherComponent.

The reaction half of what `slack_approval_gate` does for an upstream asset, but for pull requests: watches the
Slack threads `sensors/github_pull_request_agent_sensor` posts, and when an allowlisted person reacts, records a
GitHub review through a SECOND identity (GitHub blocks a PR's own author approving it), refuses to approve while
checks are failing, and -- only for branches under `branch_prefix`, and only once the reviewer has approved and
checks pass -- optionally merges.

No shared state file: pending threads are discovered by scanning the Slack channel for
`dagster_pr_review_v1`-tagged messages (the same hidden-metadata mechanism `github_pull_request_agent_sensor` posts
with, and `slack_approval_gate`'s own `message_metadata` correlation mode uses), and already-resolved threads /
already-shown errors are tracked in this sensor's own Dagster-native cursor, not a document two sensors could race
on rewriting.
"""
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


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
        "base_ref": (pr.get("base") or {}).get("ref"),
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


# BEGIN post-merge verification (inlined per component; this sensor's own addition, not shared
# with another component -- no drift test needed).
import re as _re_verify  # noqa: E402
_FAILED_RUN_IN_BODY = _re_verify.compile(
    r"(?:Fixes failed run|Run ID)[^0-9a-f]*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    _re_verify.IGNORECASE,
)


def _dagster_plus_gql(deployment_url: str, token: str, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    """Minimal Dagster+ GraphQL client -- same endpoint-suffix/auth-header convention as
    dagster_discovery_tools' own _Client (Dagster-Cloud-Api-Token header, POST {query, variables}
    to <deployment_url>/graphql), without that component's retry/backoff machinery -- this is an
    occasional, one-shot call (launch a re-execution, poll its status), not a high-volume tool."""
    import requests
    resp = requests.post(
        deployment_url.rstrip("/") + "/graphql",
        json={"query": query, "variables": variables},
        headers={"Dagster-Cloud-Api-Token": token},
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errors"):
        raise RuntimeError(f"Dagster+ GraphQL errors: {json.dumps(payload['errors'])[:500]}")
    return payload["data"]


def _start_verification(deployment_url: str, token: str, failed_run_id: str, post_merge_command: Optional[str]) -> Dict[str, Any]:
    """After a merge: optionally run a deploy hook, then re-execute the run the PR says it fixes
    (Dagster's own launchRunReexecution mutation, ALL_STEPS strategy -- a full re-run of the
    failed run's job, now against the just-merged code)."""
    import subprocess as _subprocess
    if post_merge_command:
        import shlex as _shlex
        cmd = _shlex.split(post_merge_command) if isinstance(post_merge_command, str) else post_merge_command
        done = _subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=False)
        if done.returncode:
            return {"ok": False, "message": f"the deploy step failed, so I did not verify: {done.stderr.strip()[:200]}"}
    data = _dagster_plus_gql(
        deployment_url, token,
        "mutation($p:ReexecutionParams!){ launchRunReexecution(reexecutionParams:$p){ __typename "
        "... on LaunchRunSuccess { run { runId } } ... on PythonError { message } } }",
        {"p": {"parentRunId": failed_run_id, "strategy": "ALL_STEPS"}},
    )["launchRunReexecution"]
    if data["__typename"] != "LaunchRunSuccess":
        return {"ok": False, "message": f"I could not re-run {failed_run_id[:8]}: {data.get('message', data['__typename'])[:200]}"}
    return {"ok": True, "run_id": data["run"]["runId"]}


def _check_verification(deployment_url: str, token: str, entry: Dict[str, Any], timeout_minutes: int) -> Optional[Dict[str, Any]]:
    """Poll one in-flight verification re-run. Returns None while still running, or
    {"result": "fixed"|"still_failing"|"timed_out", "text": "..."} once it's resolved."""
    import time as _time_verify
    run_id = entry["run_id"]
    status = _dagster_plus_gql(
        deployment_url, token, "query($id:ID!){ runOrError(runId:$id){ ... on Run { status } } }", {"id": run_id},
    )["runOrError"].get("status")
    if status == "SUCCESS":
        return {"result": "fixed", "text": f":white_check_mark: Verified: the re-run of {entry['failed_run'][:8]} succeeded, so the fix works."}
    if status in ("FAILURE", "CANCELED"):
        return {"result": "still_failing", "text": f":x: Still failing after the fix: re-run of {entry['failed_run'][:8]} {status.lower()}. A person needs to look."}
    if _time_verify.time() - entry["started"] > timeout_minutes * 60:
        return {"result": "timed_out", "text": f":hourglass: I could not confirm the fix: the re-run of {entry['failed_run'][:8]} did not finish in {timeout_minutes} minutes."}
    return None
# END post-merge verification


def _classify_pr_votes(reactions: Dict[str, List[str]], approve_emoji: str, reject_emoji: str, allowlist: Optional[List[str]], bot_user_id: Optional[str]) -> Dict[str, List[str]]:
    """Filter reactions to allowlisted users (anyone, if no allowlist is configured). Bot's own seed reactions
    don't count as votes."""
    allowset = set(allowlist) if allowlist else None
    exclude = {bot_user_id} if bot_user_id else set()

    def ok(u):
        return u not in exclude and (allowset is None or u in allowset)

    approvers = [u for u in reactions.get(approve_emoji, []) if ok(u)]
    rejecters = [u for u in reactions.get(reject_emoji, []) if ok(u)]
    return {"approve": approvers, "reject": rejecters}


def _display_name(client, user_id: str) -> str:
    try:
        profile = client.users_info(user=user_id)["user"]["profile"]
        return profile.get("display_name") or profile.get("real_name") or user_id
    except Exception:  # noqa: BLE001
        return user_id


class SlackPrApprovalWatcherComponent(dg.Component, dg.Model, dg.Resolvable):
    """Watches `sensors/github_pull_request_agent_sensor`'s posted threads for a reaction; when an allowlisted
    person reacts, records a GitHub review through a second identity, refuses to approve while checks fail, and
    (only for `branch_prefix` branches, only once approved and checks pass) optionally merges."""

    sensor_name: str = Field(default="slack_pr_approval_watcher", description="Unique sensor name.")
    repo: str = Field(description="GitHub repository to act on, 'owner/repo'. Must match the repo sensors/github_pull_request_agent_sensor watches.")
    token_env_var: str = Field(default="GITHUB_TOKEN", description="Env var holding the primary GitHub token (reads, checks CI).")
    reviewer_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a SECOND GitHub token (a distinct bot/user identity with write access) -- required to actually record an approval. GitHub blocks a PR's own author approving it.",
    )
    reviewer_login: Optional[str] = Field(default=None, description="The reviewer (second) identity's GitHub username -- checked before merging, to confirm THAT identity (not just anyone) approved.")
    api_base_url: str = Field(default="https://api.github.com", description="API base URL. Change for GitHub Enterprise Server.")
    branch_prefix: str = Field(default="agent/", description="Only branches with this prefix are ever merged automatically.")

    slack_channel: str = Field(description="The SAME channel sensors/github_pull_request_agent_sensor posts to.")
    slack_bot_token_env_var: str = Field(default="SLACK_BOT_TOKEN", description="Env var holding the bot's `xoxb-...` token. Needs `reactions:read` + `chat:write` + `channels:history`/`groups:history`.")
    history_lookback_hours: float = Field(default=168.0, description="How far back (hours) to scan the channel for still-open PR-review threads.")
    post_branch_deployment_link: bool = Field(
        default=True,
        description=(
            "Post ':test_tube: A branch deployment of this change is ready to test: <link>' once in a PR's Slack thread, "
            "when the PR has a Dagster+ branch deployment link and is NOT low risk (per protected_paths / low_risk_paths / "
            "low_risk_max_files / low_risk_max_lines below), so a person approving in Slack can try the change first."
        ),
    )
    protected_paths: List[str] = Field(
        default_factory=lambda: [".github/", "pyproject.toml", "requirements", "Dockerfile", "setup."],
        description="Risk assessment for post_branch_deployment_link: a PR touching one of these paths is not low risk. Same matching convention and defaults as github_pr_tools' protected_paths.",
    )
    low_risk_paths: List[str] = Field(
        default_factory=lambda: ["src/", "tests/", "docs/", "README"],
        description="Risk assessment for post_branch_deployment_link: a PR changing only these paths (within the size limits, checks passing) is low risk and gets no link. Same convention and defaults as github_pr_tools' low_risk_paths.",
    )
    low_risk_max_files: int = Field(default=2, description="Risk assessment for post_branch_deployment_link: max changed files for a low-risk PR.")
    low_risk_max_lines: int = Field(default=30, description="Risk assessment for post_branch_deployment_link: max changed lines (additions + deletions) for a low-risk PR.")
    auto_merge: bool = Field(default=False, description="When True, squash-merge automatically once the reviewer identity has approved and checks pass (still gated by branch_prefix).")
    refresh_other_agent_prs_after_merge: bool = Field(
        default=True,
        description=(
            "After auto_merge successfully merges a PR, bring every OTHER open branch_prefix PR up to date "
            "with the new base_branch (GitHub's update-branch API) so their CI runs against the just-merged "
            "code instead of a stale base, the same reconciliation github_pr_tools' refresh_open_agent_prs "
            "tool does manually. Best-effort -- a PR that can't be updated (e.g. a real merge conflict) is "
            "skipped, never raised. Ignored when auto_merge is false."
        ),
    )
    skip_in_branch_deployments: bool = Field(
        default=True,
        description=(
            "Skip every tick while running inside a Dagster+ branch deployment "
            "(DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT=1). A branch deployment runs its own copy of every sensor "
            "alongside the full deployment's -- without this, a branch deployment double-posts Slack replies "
            "and double-records GitHub reviews/merges for the same vote."
        ),
    )

    verify_after_merge: bool = Field(
        default=False,
        description=(
            "After auto_merge merges a PR, look for 'Fixes failed run <uuid>' or 'Run ID: <uuid>' in the "
            "PR body (the same convention failed_run_agent_sensor's launched fixer asset should put there) "
            "and re-execute that failed run (Dagster's launchRunReexecution, ALL_STEPS strategy) against the "
            "just-merged code, then post pass/fail/timeout in the PR's Slack thread once it resolves -- "
            "'the fix worked' is then something this sensor confirms, not something it assumes. Requires "
            "dagster_url and dagster_cloud_api_token_env_var. Ignored when auto_merge is false, since there's "
            "nothing to verify until a merge actually happens."
        ),
    )
    post_merge_command: Optional[str] = Field(
        default=None,
        description=(
            "Only used when verify_after_merge is true. An optional shell command run once, right before "
            "launching the verification re-run -- e.g. a deploy/redeploy step so the re-run actually executes "
            "against the just-merged code instead of whatever was already running. A non-zero exit skips "
            "verification and reports the failure in the thread instead."
        ),
    )
    dagster_url: Optional[str] = Field(
        default=None,
        description="Base Dagster(+) URL (e.g. 'https://org.dagster.cloud/prod'). Required when verify_after_merge is true -- both for the GraphQL endpoint (<dagster_url>/graphql) and for building run links in Slack messages.",
    )
    dagster_cloud_api_token_env_var: str = Field(
        default="DAGSTER_CLOUD_API_TOKEN",
        description="Only used when verify_after_merge is true. Env var holding a Dagster+ API token with permission to launch runs in this deployment. Same convention as dagster_discovery_tools' own token_env_var.",
    )
    verify_timeout_minutes: int = Field(
        default=30,
        description="Only used when verify_after_merge is true. Give up waiting for the verification re-run after this many minutes and report a timeout instead of a pass/fail.",
    )

    minimum_interval_seconds: int = Field(default=30, description="Sensor cadence -- how often to poll Slack + GitHub.")
    default_status: str = Field(default="RUNNING", description="'RUNNING' or 'STOPPED'.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Slack Pr Approval Watcher", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        repo = self.repo
        token_env_var = self.token_env_var
        reviewer_token_env_var = self.reviewer_token_env_var
        reviewer_login = self.reviewer_login
        api_base_url = self.api_base_url
        branch_prefix = self.branch_prefix
        slack_channel = self.slack_channel
        slack_bot_token_env_var = self.slack_bot_token_env_var
        history_lookback_hours = self.history_lookback_hours
        auto_merge = self.auto_merge
        refresh_other_agent_prs_after_merge = self.refresh_other_agent_prs_after_merge
        skip_in_branch_deployments = self.skip_in_branch_deployments
        verify_after_merge = self.verify_after_merge
        post_merge_command = self.post_merge_command
        dagster_url = self.dagster_url
        dagster_cloud_api_token_env_var = self.dagster_cloud_api_token_env_var
        verify_timeout_minutes = self.verify_timeout_minutes
        post_branch_deployment_link = self.post_branch_deployment_link
        risk_config = dict(
            protected_paths=list(self.protected_paths), low_risk_paths=list(self.low_risk_paths),
            low_risk_max_files=self.low_risk_max_files, low_risk_max_lines=self.low_risk_max_lines,
        )

        def sensor_fn(context: dg.SensorEvaluationContext, **_resources):
            if skip_in_branch_deployments and os.environ.get("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1":
                return dg.SkipReason("branch deployment -- skipping (skip_in_branch_deployments=True)")
            token = os.environ.get(token_env_var, "")
            reviewer_token = os.environ.get(reviewer_token_env_var) if reviewer_token_env_var else None
            if not token:
                return dg.SkipReason(f"{token_env_var} not set")

            try:
                cur = json.loads(context.cursor) if context.cursor else {}
            except Exception:  # noqa: BLE001
                cur = {}
            resolved_ts = set(cur.get("resolved_ts") or [])
            last_error = dict(cur.get("last_error") or {})
            verifying = dict(cur.get("verifying") or {})
            branch_links = list(cur.get("branch_links") or [])  # thread ts values whose branch-deployment link was already posted

            client = _get_slack_client(slack_bot_token_env_var)
            actions: List[str] = []

            def say(ts: str, text: str) -> bool:
                try:
                    client.chat_postMessage(channel=slack_channel, thread_ts=ts, text=text)
                    return True
                except Exception:  # noqa: BLE001
                    return False

            # Poll every in-flight post-merge verification FIRST, regardless of whether this tick
            # finds any new pending PR-review threads -- a verification re-run can easily outlive
            # the window a merged PR's own thread stays "pending" in.
            verification_resolved: List[str] = []
            if verifying:
                for ts in list(verifying.keys()):
                    try:
                        outcome = _check_verification(dagster_url, os.environ.get(dagster_cloud_api_token_env_var, ""), verifying[ts], verify_timeout_minutes)
                    except Exception as e:  # noqa: BLE001
                        outcome = None
                        if last_error.get(ts) != str(e):
                            last_error[ts] = str(e)
                            say(ts, f":warning: Could not check the verification re-run: {e}")
                    if outcome is not None:
                        say(ts, outcome["text"])
                        actions.append(f"verification {outcome['result']}")
                        del verifying[ts]
                        verification_resolved.append(ts)

            try:
                pending = [item for item in _iter_pr_review_messages(client, slack_channel, history_lookback_hours) if item["message_ts"] not in resolved_ts]
            except Exception as e:  # noqa: BLE001
                pending = []
                actions.append(f"Slack channel scan failed: {e}")

            if not pending:
                resolved_ts = resolved_ts | set(verification_resolved)
                new_cursor = json.dumps({
                    "resolved_ts": sorted(resolved_ts)[-500:],
                    "last_error": {k: v for k, v in last_error.items() if k not in resolved_ts},
                    "verifying": verifying,
                    "branch_links": branch_links[-500:],
                })
                return dg.SensorResult(
                    skip_reason="; ".join(actions) if actions else "no open PR-review threads in the scanned window",
                    cursor=new_cursor,
                )

            bot_user_id = _lookup_bot_user_id(client)
            newly_resolved: List[str] = []

            for item in pending:
                ts, payload = item["message_ts"], item["payload"]
                number = payload.get("pr_number")
                if number is None:
                    continue
                try:
                    pr = _gh_get_pull_request(repo, number, token=token, api_base_url=api_base_url)
                except GithubPrScopeError as e:
                    actions.append(f"PR #{number}: GitHub error: {e}")
                    continue

                if pr["state"] != "open":
                    newly_resolved.append(ts)
                    actions.append(f"PR #{number}: {'merged' if pr.get('merged') else 'closed'}")
                    continue

                if post_branch_deployment_link and pr.get("branch_deployment_url") and ts not in branch_links:
                    if _gh_assess_risk(pr, **risk_config)["level"] != "low":
                        # recorded only once the post actually landed, so a Slack hiccup retries next tick
                        if say(ts, f":test_tube: A branch deployment of this change is ready to test: <{pr['branch_deployment_url']}>"):
                            branch_links.append(ts)
                            actions.append(f"PR #{number}: posted the branch deployment link")

                approved_by_reviewer = bool(reviewer_login) and reviewer_login in _gh_approvals(repo, number, token=token, api_base_url=api_base_url)

                if not approved_by_reviewer:
                    try:
                        reactions = _fetch_reactions(client, payload.get("channel_id", slack_channel), ts)
                    except Exception as e:  # noqa: BLE001
                        actions.append(f"PR #{number}: Slack fetch failed: {e}")
                        continue
                    votes = _classify_pr_votes(reactions, payload.get("approve_emoji", "white_check_mark"), payload.get("reject_emoji", "x"), payload.get("approver_allowlist"), bot_user_id)
                    decided = None
                    if votes["approve"]:
                        decided = (True, votes["approve"][0])
                    elif votes["reject"]:
                        decided = (False, votes["reject"][0])
                    if decided is not None:
                        approve, who = decided
                        by = _display_name(client, who)
                        try:
                            _gh_human_decision(repo, number, approve, by, token=token, reviewer_token=reviewer_token, api_base_url=api_base_url)
                        except GithubPrScopeError as e:
                            if last_error.get(ts) != str(e):
                                last_error[ts] = str(e)
                                say(ts, f":warning: {e}")
                            continue
                        approved_by_reviewer = approve
                        actions.append(f"PR #{number}: {'approved' if approve else 'changes requested'} for {by}")
                        say(ts, f":white_check_mark: Approved on GitHub for <@{who}>." if approve else f":x: Changes requested on GitHub for <@{who}>.")
                        if not approve:
                            newly_resolved.append(ts)
                            continue

                if approved_by_reviewer and auto_merge:
                    try:
                        _gh_merge_if_ready(repo, number, branch_prefix=branch_prefix, reviewer_login=reviewer_login, token=token, api_base_url=api_base_url)
                    except GithubPrScopeError as e:
                        if "pending" not in str(e) and last_error.get(ts) != str(e):
                            last_error[ts] = str(e)
                            say(ts, f":warning: Not merged: {e}")
                    else:
                        actions.append(f"PR #{number}: merged")
                        say(ts, f":rocket: Merged #{number} into {pr.get('base_ref') or 'the base branch'}.")
                        if refresh_other_agent_prs_after_merge:
                            try:
                                _gh_refresh_open_agent_prs(repo, branch_prefix=branch_prefix, token=token, api_base_url=api_base_url)
                            except GithubPrScopeError:
                                pass
                        started_verification = False
                        if verify_after_merge:
                            found = _FAILED_RUN_IN_BODY.search(pr.get("body") or "")
                            if not found:
                                say(ts, ":grey_question: Merged, but the pull request does not name the failed run, so I could not verify it.")
                            elif not dagster_url:
                                say(ts, ":warning: Merged, but dagster_url is not set, so I could not verify it.")
                            else:
                                failed_run_id = found.group(1)
                                try:
                                    result = _start_verification(dagster_url, os.environ.get(dagster_cloud_api_token_env_var, ""), failed_run_id, post_merge_command)
                                except Exception as e:  # noqa: BLE001
                                    result = {"ok": False, "message": str(e)}
                                if result["ok"]:
                                    verifying[ts] = {"failed_run": failed_run_id, "run_id": result["run_id"], "started": time.time()}
                                    say(ts, f":arrows_counterclockwise: Re-running the failed run <{dagster_url.rstrip('/')}/runs/{failed_run_id}|{failed_run_id[:8]}> to check the fix.")
                                    started_verification = True
                                else:
                                    say(ts, f":warning: Merged, but {result['message']}")
                        if not started_verification:
                            newly_resolved.append(ts)

            resolved_ts = resolved_ts | set(newly_resolved) | set(verification_resolved)
            new_cursor = json.dumps({
                "resolved_ts": sorted(resolved_ts)[-500:],
                "last_error": {k: v for k, v in last_error.items() if k not in resolved_ts},
                "verifying": verifying,
                "branch_links": branch_links[-500:],
            })
            if not actions:
                return dg.SensorResult(skip_reason=f"scanned {len(pending)} open PR-review thread(s); no changes", cursor=new_cursor)
            return dg.SensorResult(skip_reason="; ".join(actions), cursor=new_cursor)

        sensor_def = dg.SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=dg.DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else dg.DefaultSensorStatus.STOPPED,
        )
        return dg.Definitions(sensors=[sensor_def])
