"""Confluence Resource component.

Self-contained Confluence Cloud REST v2 API workhorse using HTTP Basic auth
(email + API token). Provides read + write convenience methods for
downstream sinks (`confluence_page_upsert`) and custom Dagster asset code.

Why this is a SEPARATE component from `jira_resource`, not a reuse of it
----------------------------------------------------------------------
Confluence and Jira are both Atlassian Cloud products that share the same
authentication mechanism (Atlassian account email + API token, HTTP Basic
auth) and the same site host (`https://<site>.atlassian.net`). It would be
tempting to import `JiraResource` here and just point it at a different
base path. We deliberately did NOT do that.

`JiraResource` (see `resources/jira_resource/component.py`) is purpose-built
around Jira's issue-tracking domain: its convenience methods
(`get_issue`, `search_issues`, `create_issue`, `transition_issue`, ...) are
scoped entirely to `/rest/api/3/issue/...` and `/rest/api/3/search/...`,
and its `resource_key` defaults to `"jira"`. Confluence lives under a
completely different API surface (`/wiki/api/v2/pages`, `/wiki/api/v2/spaces`)
with entirely different resources (pages and spaces, not issues) and
entirely different write semantics (optimistic-concurrency page versioning
vs. Jira's issue-field PATCH/transition model). Bolting Confluence page
methods onto `JiraResource`, or importing `JiraResource` and monkeypatching
it, would violate this repo's hard rule that every component is fully
self-contained: helper code is duplicated per-component, never imported
across component directories.

So `ConfluenceResource` duplicates only the ~10-line Basic-auth-session
boilerplate (email + API token -> `requests.auth.HTTPBasicAuth`) that
`JiraResource` also happens to use (both are Atlassian Cloud Basic auth),
and otherwise owns Confluence's own page/space API end-to-end as its own
independent component. This mirrors the same base URL shape used by the
existing read-side `confluence_ingestion` component
(`https://{site_domain}.atlassian.net/wiki/api/v2`) for consistency.

Confluence v2 API mechanics (developer.atlassian.com/cloud/confluence/rest/v2)
-------------------------------------------------------------------------
- `POST /wiki/api/v2/pages` creates a page. No version number needed or
  accepted on create.
- `PUT /wiki/api/v2/pages/{id}` updates a page. The version number is
  REQUIRED and must be exactly one higher than the page's current version
  (optimistic-concurrency protection) -- get this wrong and the API
  rejects the write with a 409/400. Callers must first GET the page to
  read `version.number`, then PUT with `version.number = current + 1`.
- `GET /wiki/api/v2/pages` supports filtering directly via `space-id` and
  `title` query parameters (confirmed against Atlassian's own API
  reference), so "find a page by title within a space" is a single API
  call -- no need to page through every page in a space and match
  client-side.
- Page bodies use Confluence's "storage" representation (an XHTML-based
  wiki markup format). This resource does not attempt to convert
  Markdown/rich text into storage format -- callers pass pre-built storage
  XHTML, or plain text/simple HTML wrapped in `<p>` tags for simple cases.
  Rich formatting (tables, macros, layouts) requires hand-built storage
  XML; that's out of scope here.
"""
import time
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class ConfluenceResource(dg.ConfigurableResource):
    """Confluence Cloud REST v2 API workhorse: HTTP Basic auth + page read/write methods."""

    site_domain: str  # e.g. 'yoursite' (for yoursite.atlassian.net)
    email: str
    api_token: str
    request_timeout_seconds: int = 60
    max_retries: int = 3

    @property
    def base_url(self) -> str:
        return f"https://{self.site_domain}.atlassian.net/wiki/api/v2"

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch for anything not covered."""
        import requests
        from requests.auth import HTTPBasicAuth
        session = requests.Session()
        session.auth = HTTPBasicAuth(self.email, self.api_token)
        session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        return session

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    # ── HTTP wrapper with retry on 429 (honors Retry-After) / 5xx ─────
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Any:
        session = self.get_client()
        url = self._url(path)
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = session.request(
                    method, url,
                    params=params or {},
                    json=json_body,
                    timeout=self.request_timeout_seconds,
                )
            except Exception as e:  # noqa: BLE001 - requests.RequestException
                last_exc = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code == 429:
                retry_after = float(r.headers.get("Retry-After", "2"))
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(max(retry_after, 1.0), 30.0))
                continue
            if r.status_code in (500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            r.raise_for_status()
            if r.status_code == 204 or not r.content:
                return {}
            try:
                return r.json()
            except ValueError:
                return {"raw": r.text}
        if last_exc:
            raise last_exc
        return {}

    def _get(self, path: str, **params) -> Dict[str, Any]:
        return self._request("GET", path, params=params) or {}

    def _post(self, path: str, body: Any) -> Dict[str, Any]:
        return self._request("POST", path, json_body=body) or {}

    def _put(self, path: str, body: Any) -> Dict[str, Any]:
        return self._request("PUT", path, json_body=body) or {}

    # ── Page methods ───────────────────────────────────────────────
    def find_page_by_title(self, space_id: str, title: str) -> Optional[Dict[str, Any]]:
        """Look up a page by exact title within a space.

        Uses `GET /pages?space-id={space_id}&title={title}&status=current` --
        Confluence's v2 API filters by `space-id` and `title` directly, so
        this is a single API call, not a client-side scan of every page in
        the space.

        Returns `None` if no match.
        """
        payload = self._get(
            "/pages",
            **{"space-id": space_id, "title": title, "status": "current"},
        )
        results = payload.get("results") or []
        return results[0] if results else None

    def get_page(self, page_id: str) -> Dict[str, Any]:
        """Fetch a page by id, including its storage-format body and current version."""
        return self._get(f"/pages/{page_id}", **{"body-format": "storage"})

    def create_page(
        self,
        space_id: str,
        title: str,
        body_storage: str,
        status: str = "current",
    ) -> Dict[str, Any]:
        """POST /pages -- creates a new page. No version number needed on create."""
        body = {
            "spaceId": space_id,
            "status": status,
            "title": title,
            "body": {"representation": "storage", "value": body_storage},
        }
        return self._post("/pages", body)

    def update_page(
        self,
        page_id: str,
        title: str,
        body_storage: str,
        current_version: int,
        status: str = "current",
    ) -> Dict[str, Any]:
        """PUT /pages/{id} -- updates an existing page.

        `current_version` must be the page's CURRENT version number (e.g.
        from `get_page(...)["version"]["number"]`). This method sends
        `version.number = current_version + 1` -- Confluence's v2 API
        requires the new version number to be exactly one higher than the
        page's current version (optimistic-concurrency protection); a
        stale or skipped version number is rejected.
        """
        body = {
            "id": page_id,
            "status": status,
            "title": title,
            "body": {"representation": "storage", "value": body_storage},
            "version": {"number": current_version + 1},
        }
        return self._put(f"/pages/{page_id}", body)

    def upsert_page_by_title(
        self,
        space_id: str,
        title: str,
        body_storage: str,
    ) -> Dict[str, Any]:
        """Search-then-write. Returns `{'action': 'created'|'updated', 'page': {...}}`.

        Confluence has no native upsert endpoint on pages -- this method
        looks up the page by `(space_id, title)` and either PUTs (with the
        version-plus-one bump, after re-fetching the page to get its
        current version) or POSTs (if not found).
        """
        existing = self.find_page_by_title(space_id, title)
        if existing:
            current = self.get_page(existing["id"])
            current_version = current.get("version", {}).get("number", 0)
            page = self.update_page(
                page_id=existing["id"],
                title=title,
                body_storage=body_storage,
                current_version=current_version,
            )
            return {"action": "updated", "page": page}
        page = self.create_page(space_id=space_id, title=title, body_storage=body_storage)
        return {"action": "created", "page": page}


class ConfluenceResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a ConfluenceResource for use by other components.

    Uses an Atlassian API token (long-lived, no OAuth):
      id.atlassian.com/manage-profile/security/api-tokens -- create a token
      for the same Atlassian account email that has access to the target
      Confluence site.

    This is a dedicated component, NOT a reuse of `jira_resource` -- see
    the module docstring in `component.py` for the full rationale. Both
    share the same Atlassian Basic-auth mechanics but own entirely
    separate API surfaces (issues vs. pages/spaces), so each is
    implemented as its own fully self-contained component.

    Pairs with:
      - `confluence_ingestion` -- bulk pull from Confluence (dlt-backed).
      - `confluence_page_upsert` -- reverse-ETL sink (search-by-title upsert).

    Example:

        ```yaml
        type: dagster_component_templates.ConfluenceResourceComponent
        attributes:
          resource_key: confluence
          site_domain: yoursite
          email_env_var: CONFLUENCE_EMAIL
          api_token_env_var: CONFLUENCE_API_TOKEN
        ```
    """

    resource_key: str = Field(
        default="confluence",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    site_domain: str = Field(
        description="Your Atlassian site subdomain (for yoursite.atlassian.net, pass 'yoursite').",
    )
    email_env_var: str = Field(
        default="CONFLUENCE_EMAIL",
        description="Env var holding the Atlassian account email (Basic auth username).",
    )
    api_token_env_var: str = Field(
        default="CONFLUENCE_API_TOKEN",
        description="Env var holding an Atlassian API token (from id.atlassian.com/manage-profile/security/api-tokens).",
    )
    request_timeout_seconds: int = Field(
        default=60,
        description="Per-request timeout in seconds.",
    )
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 (honors Retry-After) / 5xx.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        resource = ConfluenceResource(
            site_domain=self.site_domain,
            email=os.environ.get(self.email_env_var, ""),
            api_token=os.environ.get(self.api_token_env_var, ""),
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
