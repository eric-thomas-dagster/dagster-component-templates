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


def _gh_get_pull_request(repo: str, number: int, *, token: str, api_base_url: str) -> Dict[str, Any]:
    pr = _gh_call("GET", repo, f"/pulls/{int(number)}", token=token, api_base_url=api_base_url)
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
        ],
        "checks": _gh_checks(repo, pr["head"]["sha"], token=token, api_base_url=api_base_url),
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
    protected = tuple(protected_paths)
    low_risk = tuple(low_risk_paths)
    for f in files:
        if f["path"].startswith(protected):
            reasons.append(f"touches protected path {f['path']}")
        elif not f["path"].startswith(low_risk):
            reasons.append(f"touches {f['path']}, outside the configured low-risk paths")
    if pr["checks"]["state"] != "passing":
        reasons.append(f"checks are {pr['checks']['state']}")
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
    token: str, api_base_url: str, note: str = "", kind: str = "fix",
) -> Dict[str, Any]:
    import base64
    if not files or len(files) > max_files_per_fix:
        raise GithubPrScopeError(f"A fix changes between 1 and {max_files_per_fix} files.")
    protected = tuple(protected_paths)
    for f in files:
        p = _gh_safe_path(f["path"])
        if p.startswith(protected):
            raise GithubPrScopeError(f"{p} is protected; agents may not change it.")
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
) -> Dict[str, Any]:
    """COMMENT is always allowed. APPROVE only passes the deterministic risk rules, and is posted under the
    reviewer (second) identity -- GitHub blocks an author approving their own PR."""
    pr = _gh_get_pull_request(repo, number, token=token, api_base_url=api_base_url)
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


def _gh_open_pull_requests(repo: str, *, token: str, api_base_url: str) -> List[Dict[str, Any]]:
    prs = _gh_call("GET", repo, "/pulls", token=token, api_base_url=api_base_url, params={"state": "open", "per_page": 50})
    return [
        {"number": p["number"], "head_sha": p["head"]["sha"], "head_ref": p["head"]["ref"], "title": p["title"], "url": p["html_url"], "author": p["user"]["login"]}
        for p in prs
    ]
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
    auto_merge: bool = Field(default=False, description="When True, squash-merge automatically once the reviewer identity has approved and checks pass (still gated by branch_prefix).")

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

        def sensor_fn(context: dg.SensorEvaluationContext, **_resources):
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

            client = _get_slack_client(slack_bot_token_env_var)
            try:
                pending = [item for item in _iter_pr_review_messages(client, slack_channel, history_lookback_hours) if item["message_ts"] not in resolved_ts]
            except Exception as e:  # noqa: BLE001
                return dg.SkipReason(f"Slack channel scan failed: {e}")

            if not pending:
                return dg.SkipReason("no open PR-review threads in the scanned window")

            bot_user_id = _lookup_bot_user_id(client)
            actions: List[str] = []
            newly_resolved: List[str] = []

            def say(ts: str, text: str) -> None:
                try:
                    client.chat_postMessage(channel=slack_channel, thread_ts=ts, text=text)
                except Exception:  # noqa: BLE001
                    pass

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
                        newly_resolved.append(ts)
                        actions.append(f"PR #{number}: merged")
                        say(ts, f":rocket: Merged #{number} into main.")

            new_cursor = json.dumps({
                "resolved_ts": sorted(resolved_ts | set(newly_resolved))[-500:],
                "last_error": {k: v for k, v in last_error.items() if k not in resolved_ts and k not in newly_resolved},
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
