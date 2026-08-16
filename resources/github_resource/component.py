"""GitHub Resource component.

PAT-based (personal access token) resource with ergonomic read + write
convenience methods so Dagster assets/ops can call
`context.resources.github.list_issues("owner/repo")` without reaching for
the raw HTTP API.

Drop to `.get_client()` for anything not covered — returns an authenticated
`requests.Session` pointed at the API base URL. Supports GitHub Enterprise
Server via `api_base_url`.

For GitHub App auth (installation tokens, higher rate limits, org-scale
usage), use the official `dagster_github.GithubResource` alongside this one
via a different resource key.
"""
from typing import Iterator, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class GitHubResource(ConfigurableResource):
    """Dagster resource wrapping the GitHub REST API with convenience methods.

    Covers issues, pull requests, repos, workflows, releases, and comments.
    For anything not covered, use `.get_client()` to get an authenticated
    `requests.Session`.
    """

    token: str = Field(description="GitHub personal access token (PAT).")
    api_base_url: str = Field(
        default="https://api.github.com",
        description="API base URL. Change for GitHub Enterprise Server (e.g. https://ghe.example.com/api/v3).",
    )
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch for anything
        the convenience methods don't cover."""
        import requests
        session = requests.Session()
        session.verify = self.verify_ssl
        session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        return session

    def _url(self, path: str) -> str:
        return f"{self.api_base_url}{path}"

    def _get(self, path: str, **params) -> dict:
        r = self.get_client().get(self._url(path), params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _get_list(self, path: str, **params) -> List[dict]:
        result = self._get(path, **params)
        return result if isinstance(result, list) else result.get("items", [])

    def _iter_paginated(self, path: str, per_page: int = 100, **params) -> Iterator[dict]:
        """Auto-paginate a GET endpoint via Link headers."""
        session = self.get_client()
        url = self._url(path)
        params = {**params, "per_page": per_page}
        while url:
            r = session.get(url, params=params, timeout=60)
            r.raise_for_status()
            body = r.json()
            items = body if isinstance(body, list) else body.get("items", [])
            for it in items:
                yield it
            # GitHub's Link header signals pagination
            link = r.headers.get("Link", "")
            next_url = None
            for part in link.split(","):
                if 'rel="next"' in part:
                    next_url = part.split(";")[0].strip().strip("<>")
                    break
            url = next_url
            params = {}  # subsequent pages already carry query in the URL

    # ------------------------------------------------------------------ reads

    def get_repo(self, repo: str) -> dict:
        """Retrieve a repository. `repo` is in `owner/name` form."""
        return self._get(f"/repos/{repo}")

    def list_issues(
        self,
        repo: str,
        state: str = "open",
        labels: Optional[List[str]] = None,
        assignee: Optional[str] = None,
        per_page: int = 100,
    ) -> List[dict]:
        """List issues in a repo. Returns first page — use `iter_issues` for full pagination.

        Note: GitHub's REST API returns both issues AND pull requests from this endpoint;
        each result has a `pull_request` key on PRs. Filter downstream if needed.
        """
        params: dict = {"state": state, "per_page": per_page}
        if labels:
            params["labels"] = ",".join(labels)
        if assignee:
            params["assignee"] = assignee
        return self._get_list(f"/repos/{repo}/issues", **params)

    def iter_issues(
        self,
        repo: str,
        state: str = "open",
        labels: Optional[List[str]] = None,
        assignee: Optional[str] = None,
        per_page: int = 100,
    ) -> Iterator[dict]:
        """Auto-paginated variant of `list_issues`."""
        params: dict = {"state": state}
        if labels:
            params["labels"] = ",".join(labels)
        if assignee:
            params["assignee"] = assignee
        yield from self._iter_paginated(f"/repos/{repo}/issues", per_page=per_page, **params)

    def get_issue(self, repo: str, number: int) -> dict:
        """Retrieve a single issue by number."""
        return self._get(f"/repos/{repo}/issues/{number}")

    def list_pull_requests(
        self,
        repo: str,
        state: str = "open",
        per_page: int = 100,
    ) -> List[dict]:
        """List pull requests. Returns first page — use `iter_pull_requests` for full pagination."""
        return self._get_list(f"/repos/{repo}/pulls", state=state, per_page=per_page)

    def iter_pull_requests(
        self,
        repo: str,
        state: str = "open",
        per_page: int = 100,
    ) -> Iterator[dict]:
        """Auto-paginated variant of `list_pull_requests`."""
        yield from self._iter_paginated(f"/repos/{repo}/pulls", per_page=per_page, state=state)

    def get_pull_request(self, repo: str, number: int) -> dict:
        """Retrieve a single pull request by number."""
        return self._get(f"/repos/{repo}/pulls/{number}")

    def list_commits(self, repo: str, sha: Optional[str] = None, per_page: int = 100) -> List[dict]:
        """List commits. `sha` picks a branch or commit reference."""
        params: dict = {"per_page": per_page}
        if sha:
            params["sha"] = sha
        return self._get_list(f"/repos/{repo}/commits", **params)

    def list_workflow_runs(
        self,
        repo: str,
        workflow_id: Optional[str] = None,
        status: Optional[str] = None,
        per_page: int = 100,
    ) -> List[dict]:
        """List Actions workflow runs.

        `workflow_id` is either the numeric ID or the workflow filename (e.g. 'ci.yml').
        If omitted, returns runs across all workflows in the repo.
        """
        base = f"/repos/{repo}/actions/workflows/{workflow_id}/runs" if workflow_id else f"/repos/{repo}/actions/runs"
        params: dict = {"per_page": per_page}
        if status:
            params["status"] = status
        result = self._get(base, **params)
        return result.get("workflow_runs", [])

    def get_workflow_run(self, repo: str, run_id: int) -> dict:
        """Retrieve a single workflow run by ID."""
        return self._get(f"/repos/{repo}/actions/runs/{run_id}")

    def list_releases(self, repo: str, per_page: int = 100) -> List[dict]:
        """List releases."""
        return self._get_list(f"/repos/{repo}/releases", per_page=per_page)

    def list_labels(self, repo: str) -> List[dict]:
        """List labels defined on the repo."""
        return self._get_list(f"/repos/{repo}/labels", per_page=100)

    def whoami(self) -> dict:
        """Return the authenticated user."""
        return self._get("/user")

    # ----------------------------------------------------------------- writes

    def create_issue(
        self,
        repo: str,
        title: str,
        body: str = "",
        labels: Optional[List[str]] = None,
        assignees: Optional[List[str]] = None,
        milestone: Optional[int] = None,
    ) -> dict:
        """Create an issue."""
        payload: dict = {"title": title, "body": body}
        if labels:
            payload["labels"] = labels
        if assignees:
            payload["assignees"] = assignees
        if milestone is not None:
            payload["milestone"] = milestone
        r = self.get_client().post(self._url(f"/repos/{repo}/issues"), json=payload, timeout=60)
        r.raise_for_status()
        return r.json()

    def update_issue(
        self,
        repo: str,
        number: int,
        title: Optional[str] = None,
        body: Optional[str] = None,
        state: Optional[str] = None,
        labels: Optional[List[str]] = None,
        assignees: Optional[List[str]] = None,
    ) -> dict:
        """Patch an issue's fields."""
        payload: dict = {}
        if title is not None:
            payload["title"] = title
        if body is not None:
            payload["body"] = body
        if state is not None:
            payload["state"] = state
        if labels is not None:
            payload["labels"] = labels
        if assignees is not None:
            payload["assignees"] = assignees
        r = self.get_client().patch(self._url(f"/repos/{repo}/issues/{number}"), json=payload, timeout=60)
        r.raise_for_status()
        return r.json()

    def close_issue(self, repo: str, number: int, reason: str = "completed") -> dict:
        """Close an issue. `reason` is 'completed' or 'not_planned'."""
        payload = {"state": "closed", "state_reason": reason}
        r = self.get_client().patch(self._url(f"/repos/{repo}/issues/{number}"), json=payload, timeout=60)
        r.raise_for_status()
        return r.json()

    def add_issue_comment(self, repo: str, number: int, body: str) -> dict:
        """Add a comment to an issue or PR (they share the /issues/{n}/comments endpoint)."""
        r = self.get_client().post(
            self._url(f"/repos/{repo}/issues/{number}/comments"),
            json={"body": body},
            timeout=60,
        )
        r.raise_for_status()
        return r.json()

    def create_release(
        self,
        repo: str,
        tag_name: str,
        name: Optional[str] = None,
        body: str = "",
        draft: bool = False,
        prerelease: bool = False,
        target_commitish: Optional[str] = None,
    ) -> dict:
        """Create a release. Fails if the tag doesn't exist AND target_commitish isn't given."""
        payload: dict = {
            "tag_name": tag_name,
            "name": name or tag_name,
            "body": body,
            "draft": draft,
            "prerelease": prerelease,
        }
        if target_commitish:
            payload["target_commitish"] = target_commitish
        r = self.get_client().post(self._url(f"/repos/{repo}/releases"), json=payload, timeout=60)
        r.raise_for_status()
        return r.json()

    def dispatch_workflow(
        self,
        repo: str,
        workflow_id: str,
        ref: str = "main",
        inputs: Optional[dict] = None,
    ) -> None:
        """Fire an Actions workflow. `workflow_id` is the filename (e.g. 'ci.yml') or numeric ID.

        Returns None (GitHub returns 204 with no body). Poll `list_workflow_runs`
        with the same ref if you need to observe the run.
        """
        payload: dict = {"ref": ref}
        if inputs:
            payload["inputs"] = inputs
        r = self.get_client().post(
            self._url(f"/repos/{repo}/actions/workflows/{workflow_id}/dispatches"),
            json=payload,
            timeout=60,
        )
        r.raise_for_status()

    def set_labels(self, repo: str, number: int, labels: List[str]) -> List[dict]:
        """Replace the labels on an issue/PR. Pass [] to clear."""
        r = self.get_client().put(
            self._url(f"/repos/{repo}/issues/{number}/labels"),
            json={"labels": labels},
            timeout=60,
        )
        r.raise_for_status()
        return r.json()


class GithubResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a GitHubResource for use by other components.

    Example:
        ```yaml
        type: dagster_community_components.GithubResourceComponent
        attributes:
          resource_key: github
          token_env_var: GITHUB_TOKEN
        ```
    """

    resource_key: str = Field(
        default="github",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    token_env_var: str = Field(
        default="GITHUB_TOKEN",
        description="Env var holding a GitHub personal access token.",
    )
    api_base_url: str = Field(
        default="https://api.github.com",
        description="API base URL. Change for GitHub Enterprise Server (e.g. https://ghe.example.com/api/v3).",
    )
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = GitHubResource(
            token=dg.EnvVar(self.token_env_var),
            api_base_url=self.api_base_url,
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
