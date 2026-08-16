"""Jira Resource component.

Email + API-token (Basic auth) wrapper over the Jira Cloud REST v3 API with
ergonomic read + write convenience methods. Works against Jira Cloud
(*.atlassian.net) and Jira Data Center — pass the right `base_url`.

Drop to `.get_client()` for anything not covered — returns an authenticated
`requests.Session`. For OAuth 2.0 / installed-app auth, use a custom
resource and pass the resulting token into the session yourself.
"""
from typing import Iterator, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class JiraResource(ConfigurableResource):
    """Dagster resource wrapping the Jira Cloud REST API with convenience methods.

    Covers issues, comments, transitions, projects, and users. For anything
    not covered, use `.get_client()` to get an authenticated `requests.Session`.
    """

    email: str = Field(description="Atlassian account email (username for Basic auth).")
    api_token: str = Field(description="Jira API token from id.atlassian.com/manage-profile/security/api-tokens.")
    base_url: str = Field(
        description="Jira instance URL, e.g. https://<workspace>.atlassian.net (no trailing slash).",
    )
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch."""
        import requests
        from requests.auth import HTTPBasicAuth
        session = requests.Session()
        session.verify = self.verify_ssl
        session.auth = HTTPBasicAuth(self.email, self.api_token)
        session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        return session

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}{path}"

    def _get(self, path: str, **params):
        r = self.get_client().get(self._url(path), params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------ reads

    def get_issue(self, key: str, fields: Optional[List[str]] = None) -> dict:
        """Retrieve an issue by key (e.g. 'PROJ-123')."""
        params = {"fields": ",".join(fields)} if fields else {}
        return self._get(f"/rest/api/3/issue/{key}", **params)

    def search_issues(
        self,
        jql: str,
        fields: Optional[List[str]] = None,
        max_results: int = 100,
    ) -> List[dict]:
        """Run a JQL search. Returns the first page of issues.

        Use `iter_search_issues` for full pagination.

        Example: `search_issues('project = SCRATCH AND labels = "dagster-managed"')`.

        Uses Atlassian's `POST /rest/api/3/search/jql` (the old GET /search
        was removed in 2025).
        """
        body: dict = {"jql": jql, "maxResults": max_results}
        if fields:
            body["fields"] = fields
        r = self.get_client().post(self._url("/rest/api/3/search/jql"), json=body, timeout=60)
        r.raise_for_status()
        return r.json().get("issues", [])

    def iter_search_issues(
        self,
        jql: str,
        fields: Optional[List[str]] = None,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Auto-paginated variant of `search_issues`.

        Uses token-based pagination via `nextPageToken` (Atlassian's new /search/jql).
        """
        session = self.get_client()
        next_page_token: Optional[str] = None
        while True:
            body: dict = {"jql": jql, "maxResults": page_size}
            if fields:
                body["fields"] = fields
            if next_page_token:
                body["nextPageToken"] = next_page_token
            r = session.post(self._url("/rest/api/3/search/jql"), json=body, timeout=60)
            r.raise_for_status()
            resp = r.json()
            issues = resp.get("issues", [])
            for issue in issues:
                yield issue
            if resp.get("isLast", True) or not issues:
                return
            next_page_token = resp.get("nextPageToken")
            if not next_page_token:
                return

    def get_project(self, project_key: str) -> dict:
        """Retrieve a project by key (e.g. 'PROJ')."""
        return self._get(f"/rest/api/3/project/{project_key}")

    def list_projects(self) -> List[dict]:
        """List all projects visible to the authenticated user."""
        return self._get("/rest/api/3/project")

    def list_transitions(self, key: str) -> List[dict]:
        """List available workflow transitions for an issue."""
        return self._get(f"/rest/api/3/issue/{key}/transitions").get("transitions", [])

    def get_comments(self, key: str) -> List[dict]:
        """List comments on an issue."""
        return self._get(f"/rest/api/3/issue/{key}/comment").get("comments", [])

    def whoami(self) -> dict:
        """Retrieve the authenticated user."""
        return self._get("/rest/api/3/myself")

    def get_issue_types(self, project_key: str) -> List[dict]:
        """Retrieve valid issue types for a project."""
        proj = self.get_project(project_key)
        return proj.get("issueTypes", [])

    # ----------------------------------------------------------------- writes

    def create_issue(
        self,
        project_key: str,
        summary: str,
        description: str = "",
        issue_type: str = "Task",
        labels: Optional[List[str]] = None,
        assignee_account_id: Optional[str] = None,
        priority: Optional[str] = None,
        extra_fields: Optional[dict] = None,
    ) -> dict:
        """Create an issue.

        `description` is plain text and gets auto-wrapped into Jira's Atlassian
        Document Format (ADF). `extra_fields` merges into `fields` for custom
        fields (Story Points, epics, etc.).
        """
        fields: dict = {
            "project": {"key": project_key},
            "summary": summary,
            "issuetype": {"name": issue_type},
        }
        if description:
            fields["description"] = _plain_to_adf(description)
        if labels:
            fields["labels"] = labels
        if assignee_account_id:
            fields["assignee"] = {"accountId": assignee_account_id}
        if priority:
            fields["priority"] = {"name": priority}
        if extra_fields:
            fields.update(extra_fields)
        r = self.get_client().post(self._url("/rest/api/3/issue"), json={"fields": fields}, timeout=60)
        r.raise_for_status()
        return r.json()

    def update_issue(
        self,
        key: str,
        summary: Optional[str] = None,
        description: Optional[str] = None,
        labels: Optional[List[str]] = None,
        assignee_account_id: Optional[str] = None,
        priority: Optional[str] = None,
        extra_fields: Optional[dict] = None,
    ) -> None:
        """Patch an issue's fields. Returns None (Jira returns 204)."""
        fields: dict = {}
        if summary is not None:
            fields["summary"] = summary
        if description is not None:
            fields["description"] = _plain_to_adf(description)
        if labels is not None:
            fields["labels"] = labels
        if assignee_account_id is not None:
            fields["assignee"] = {"accountId": assignee_account_id} if assignee_account_id else None
        if priority is not None:
            fields["priority"] = {"name": priority}
        if extra_fields:
            fields.update(extra_fields)
        if not fields:
            return
        r = self.get_client().put(
            self._url(f"/rest/api/3/issue/{key}"),
            json={"fields": fields},
            timeout=60,
        )
        r.raise_for_status()

    def transition_issue(self, key: str, transition_name: str, comment: str = "") -> None:
        """Move an issue through a workflow transition (e.g. 'Done', 'In Progress').

        Resolves the transition ID by name against the issue's current state.
        Optional inline `comment` is added as part of the transition.
        """
        transitions = self.list_transitions(key)
        match = next(
            (t for t in transitions if t.get("name", "").lower() == transition_name.lower()),
            None,
        )
        if not match:
            available = [t.get("name") for t in transitions]
            raise ValueError(
                f"Transition {transition_name!r} not available from current state of {key}. "
                f"Available: {available}"
            )
        payload: dict = {"transition": {"id": match["id"]}}
        if comment:
            payload["update"] = {"comment": [{"add": {"body": _plain_to_adf(comment)}}]}
        r = self.get_client().post(
            self._url(f"/rest/api/3/issue/{key}/transitions"),
            json=payload,
            timeout=60,
        )
        r.raise_for_status()

    def add_comment(self, key: str, body: str) -> dict:
        """Add a comment to an issue."""
        r = self.get_client().post(
            self._url(f"/rest/api/3/issue/{key}/comment"),
            json={"body": _plain_to_adf(body)},
            timeout=60,
        )
        r.raise_for_status()
        return r.json()

    def assign_issue(self, key: str, account_id: Optional[str]) -> None:
        """Assign an issue. Pass `None` to unassign."""
        r = self.get_client().put(
            self._url(f"/rest/api/3/issue/{key}/assignee"),
            json={"accountId": account_id},
            timeout=60,
        )
        r.raise_for_status()

    def delete_issue(self, key: str, delete_subtasks: bool = False) -> None:
        """Delete an issue permanently. Prefer `transition_issue` to a 'Done' state."""
        params = {"deleteSubtasks": str(delete_subtasks).lower()}
        r = self.get_client().delete(
            self._url(f"/rest/api/3/issue/{key}"),
            params=params,
            timeout=60,
        )
        r.raise_for_status()


def _plain_to_adf(text: str) -> dict:
    """Convert plain text to a minimal Atlassian Document Format document.

    Jira Cloud REST v3 requires ADF for description + comment bodies. Users
    typically pass plain strings; we wrap them here so callers don't need
    to build ADF trees themselves.
    """
    if not text:
        return {"type": "doc", "version": 1, "content": []}
    paragraphs = text.split("\n\n")
    return {
        "type": "doc",
        "version": 1,
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": p}]}
            for p in paragraphs if p
        ],
    }


class JiraResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a JiraResource for use by other components.

    Example:
        ```yaml
        type: dagster_community_components.JiraResourceComponent
        attributes:
          resource_key: jira
          email_env_var: JIRA_EMAIL
          api_token_env_var: JIRA_API_TOKEN
          base_url: https://mycompany.atlassian.net
        ```
    """

    resource_key: str = Field(
        default="jira",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    email_env_var: str = Field(
        default="JIRA_EMAIL",
        description="Env var holding the Atlassian account email (Basic auth username).",
    )
    api_token_env_var: str = Field(
        default="JIRA_API_TOKEN",
        description="Env var holding a Jira API token (from id.atlassian.com/manage-profile/security/api-tokens).",
    )
    base_url: str = Field(
        description="Jira instance URL, e.g. https://<workspace>.atlassian.net (no trailing slash).",
    )
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = JiraResource(
            email=dg.EnvVar(self.email_env_var),
            api_token=dg.EnvVar(self.api_token_env_var),
            base_url=self.base_url,
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
