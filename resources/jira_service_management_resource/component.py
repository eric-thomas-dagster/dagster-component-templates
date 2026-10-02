"""Jira Service Management Resource component.

Email + API-token (Basic auth) wrapper over the Jira Service Management
(JSM) `/rest/servicedeskapi/` surface, plus the slice of the CORE Jira
Cloud REST v3 API (`/rest/api/3/...`) that JSM itself has no equivalent
for.

Why this is a SEPARATE resource from `jira_resource` (not an overlap):
  - `jira_resource` is a wrapper over the core Jira Cloud REST v3 API
    (`/rest/api/3/...` — issues, JQL search, comments, transitions,
    projects). Every one of its methods builds a URL under that base path.
    It has ZERO methods touching `/rest/servicedeskapi/` and cannot create
    a customer request: `POST /rest/api/3/issue` creates a bare Jira issue,
    it does NOT set up the request-type-specific customer-portal plumbing
    (SLA clocks, portal visibility, approval workflows, organization
    sharing) that `POST /rest/servicedeskapi/request` does when given a
    `serviceDeskId` + `requestTypeId`.
  - Conversely, Jira Service Management's own API has NO general "update a
    request's fields" endpoint under `/rest/servicedeskapi/`. Field updates
    on an existing request go through the CORE API:
    `PUT /rest/api/3/issue/{issueIdOrKey}` — because under the hood, a
    service desk request IS a Jira issue. So this resource duplicates a
    small slice of core-API logic (JQL search + field PUT) that
    `jira_resource` already has, by design and per this repo's convention
    of never importing across component/resource boundaries — every
    resource is self-contained.

Net effect: JSM request *creation* and request *comments* use
`/rest/servicedeskapi/`; JSM request *field updates* and *search* use
`/rest/api/3/`. Both surfaces live here, under one auth/session.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class JiraServiceManagementResource(ConfigurableResource):
    """Dagster resource wrapping the Jira Service Management REST API.

    Covers customer-request creation (`/rest/servicedeskapi/request`),
    request comments (`/rest/servicedeskapi/request/{id}/comment`), and the
    core-API slice JSM has no equivalent for: JQL search
    (`/rest/api/3/search/jql`) and field updates
    (`PUT /rest/api/3/issue/{id}`). See the module docstring for why this
    split exists instead of reusing `jira_resource`.

    Drop to `.get_client()` for anything not covered — returns an
    authenticated `requests.Session`.
    """

    email: str = Field(description="Atlassian account email (username for Basic auth).")
    api_token: str = Field(description="Atlassian API token from id.atlassian.com/manage-profile/security/api-tokens.")
    site_domain: str = Field(
        description='Atlassian site subdomain only, e.g. "mysite" for mysite.atlassian.net (no https://, no trailing slash).',
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
        return f"https://{self.site_domain}.atlassian.net{path}"

    # ------------------------------------------------- servicedeskapi surface

    def create_request(
        self,
        service_desk_id: str,
        request_type_id: str,
        request_field_values: Dict[str, Any],
        raise_on_behalf_of: Optional[str] = None,
    ) -> dict:
        """Create a customer request via `POST /rest/servicedeskapi/request`.

        `request_field_values` maps Jira field IDs (`summary`,
        `description`, or `customfield_XXXXX`) to values. The response JSON
        includes `issueId` and `issueKey` for the newly created request.
        """
        body: Dict[str, Any] = {
            "serviceDeskId": service_desk_id,
            "requestTypeId": request_type_id,
            "requestFieldValues": request_field_values,
        }
        if raise_on_behalf_of:
            body["raiseOnBehalfOf"] = raise_on_behalf_of
        r = self.get_client().post(self._url("/rest/servicedeskapi/request"), json=body, timeout=60)
        r.raise_for_status()
        return r.json()

    def add_request_comment(self, issue_id_or_key: str, body_text: str, public: bool = True) -> dict:
        """Add a comment to a request via `POST /rest/servicedeskapi/request/{id}/comment`.

        `public=False` posts an internal/agent-only comment, invisible on
        the customer portal.
        """
        r = self.get_client().post(
            self._url(f"/rest/servicedeskapi/request/{issue_id_or_key}/comment"),
            json={"body": body_text, "public": public},
            timeout=60,
        )
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------- core-API slice

    def search_issues_jql(
        self,
        jql: str,
        fields: Optional[List[str]] = None,
        max_results: int = 50,
    ) -> List[dict]:
        """Run a JQL search via `POST /rest/api/3/search/jql`.

        JSM has no servicedesk-scoped search endpoint, so matching an
        existing request requires going through the core JQL surface —
        scope with `project = "..."` in the JQL itself. Uses the modern
        Atlassian search endpoint (the old `GET /search` was removed in
        2025).

        Quirk (confirmed, trips people up constantly): to filter on a
        custom field by its numeric ID, JQL requires `cf[10050] = "value"`
        syntax, NOT `customfield_10050 = "value"` (the latter does not
        parse). Built-in fields use their plain name, e.g. `summary ~ "x"`.
        """
        body: Dict[str, Any] = {"jql": jql, "maxResults": max_results}
        if fields:
            body["fields"] = fields
        r = self.get_client().post(self._url("/rest/api/3/search/jql"), json=body, timeout=60)
        r.raise_for_status()
        return r.json().get("issues", [])

    def update_issue_fields(self, issue_id_or_key: str, fields: Dict[str, Any]) -> None:
        """Update a request's fields via `PUT /rest/api/3/issue/{id}`.

        JSM has no `/rest/servicedeskapi/` endpoint for updating a request's
        fields after creation — field updates go through the core Jira API,
        since a service desk request IS a Jira issue under the hood.
        Returns None (Jira returns 204 No Content on success).
        """
        r = self.get_client().put(
            self._url(f"/rest/api/3/issue/{issue_id_or_key}"),
            json={"fields": fields},
            timeout=60,
        )
        r.raise_for_status()


class JiraServiceManagementResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a JiraServiceManagementResource for use by other components.

    Example:
        ```yaml
        type: dagster_community_components.JiraServiceManagementResourceComponent
        attributes:
          resource_key: jira_service_management_resource
          email_env_var: JSM_EMAIL
          api_token_env_var: JSM_API_TOKEN
          site_domain: mysite
        ```
    """

    resource_key: str = Field(
        default="jira_service_management_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    email_env_var: str = Field(
        default="JSM_EMAIL",
        description="Env var holding the Atlassian account email (Basic auth username).",
    )
    api_token_env_var: str = Field(
        default="JSM_API_TOKEN",
        description="Env var holding an Atlassian API token (from id.atlassian.com/manage-profile/security/api-tokens).",
    )
    site_domain: str = Field(
        description='Atlassian site subdomain only, e.g. "mysite" for mysite.atlassian.net.',
    )
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = JiraServiceManagementResource(
            email=dg.EnvVar(self.email_env_var),
            api_token=dg.EnvVar(self.api_token_env_var),
            site_domain=self.site_domain,
            verify_ssl=self.verify_ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
