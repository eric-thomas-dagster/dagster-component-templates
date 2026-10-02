"""Copper CRM REST API resource.

Self-contained Copper workhorse: header-based API-key auth + generic
read/write/search convenience methods for downstream sink / read
components (`copper_record_upsert`, custom asset code, etc.).

Auth (verified against developer.copper.com/introduction/requests.html --
ALL FOUR headers are required on every request, not just the API key):

    X-PW-AccessToken: <api_key>       -- the Copper API key
    X-PW-Application: developer_api   -- literal constant, always this value
    X-PW-UserEmail: <user_email>      -- email of the API key's owner
    Content-Type: application/json

Base URL: https://api.copper.com/developer_api/v1

Rate limits (per Copper's own docs): 180 requests/minute overall,
evaluated on a rolling window, plus a separate 3 requests/second cap on
Bulk endpoints. Both return 429 on exceed. `_request()` retries 429 / 5xx
with exponential backoff (honoring a `Retry-After` response header when
Copper sends one) up to `max_retries` attempts, logging a warning on each
retry so sustained throttling is visible in run logs rather than silently
eating wall-clock time.

Copper has NO native atomic "upsert" endpoint for People / Leads /
Companies -- unlike Salesforce's External-ID PATCH, there is no single
call that creates-or-updates. `.upsert()` emulates one via
search-then-write: POST `/{object_type}/search` to look for an existing
match, then PUT the match (if found) or POST a new record (if not).

    *** This is NOT race-condition-safe. *** Between the search read and
    the write, another concurrent writer (another Dagster run, a human in
    the Copper UI, a different integration) can create a record matching
    the same search filter, and this method will not see it -- the result
    is a duplicate record, not a corrupted one, but a duplicate
    nonetheless. Safe for single-writer / sequential reverse-ETL jobs;
    NOT safe if multiple processes may upsert the same object_type
    concurrently. See `copper_record_upsert`'s README for more detail.

Convenience methods:

    get(path, params)                                  # GET
    post(path, json_body)                              # POST
    put(path, json_body)                                # PUT
    search(object_type, filter_body)                   # POST /{object_type}/search -> list
    upsert(object_type, search_filter, create_body, update_body=None)
                                                        # search-then-write
"""
import os
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class CopperResource(dg.ConfigurableResource):
    """Copper CRM REST API workhorse: header API-key auth + read/write/search."""

    api_key_env_var: str = Field(
        description="Env var holding the Copper API key (sent as X-PW-AccessToken)."
    )
    user_email: str = Field(
        description=(
            "Email address of the Copper user the API key belongs to. "
            "Required by Copper on every request as X-PW-UserEmail."
        ),
    )
    base_url: str = Field(
        default="https://api.copper.com/developer_api/v1",
        description="Copper API base URL.",
    )
    request_timeout_seconds: int = Field(
        default=60, description="Per-request timeout in seconds."
    )
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 / 5xx (exponential backoff, honors Retry-After).",
    )

    # ── HTTP plumbing ──────────────────────────────────────────────
    def _headers(self) -> Dict[str, str]:
        api_key = os.environ.get(self.api_key_env_var, "")
        return {
            "X-PW-AccessToken": api_key,
            "X-PW-Application": "developer_api",
            "X-PW-UserEmail": self.user_email,
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return self.base_url.rstrip("/") + path

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Any:
        """Execute one request with retry on 429 / 5xx.

        Dispatches to `requests.get` / `requests.post` / `requests.put`
        (module-level functions, not a Session) so tests can monkeypatch
        each verb independently -- mirrors this repo's resource-test
        convention.
        """
        import requests

        method = method.upper()
        verb_fn = {"GET": requests.get, "POST": requests.post, "PUT": requests.put}.get(method)
        if verb_fn is None:
            raise ValueError(f"CopperResource._request: unsupported method {method!r}")

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            kwargs: Dict[str, Any] = {
                "headers": self._headers(),
                "timeout": self.request_timeout_seconds,
            }
            if method == "GET":
                kwargs["params"] = params or {}
            else:
                kwargs["json"] = json_body
            try:
                r = verb_fn(self._url(path), **kwargs)
            except requests.RequestException as e:
                last_exc = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2 ** attempt, 30))
                continue

            if r.status_code == 429 or r.status_code >= 500:
                if attempt >= self.max_retries:
                    r.raise_for_status()
                retry_after = r.headers.get("Retry-After") if hasattr(r, "headers") else None
                delay = float(retry_after) if retry_after else min(2 ** attempt, 30)
                import logging
                logging.getLogger("copper_resource").warning(
                    f"Copper API {r.status_code} on {method} {path} -- "
                    f"retrying in {delay:.1f}s (attempt {attempt}/{self.max_retries})."
                )
                time.sleep(delay)
                continue

            r.raise_for_status()
            if not getattr(r, "content", None):
                return None
            try:
                return r.json()
            except ValueError:
                return {"raw": r.text}

        if last_exc:
            raise last_exc
        return None

    # ── Generic verbs ──────────────────────────────────────────────
    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params)

    def post(self, path: str, json_body: Optional[Any] = None) -> Any:
        return self._request("POST", path, json_body=json_body)

    def put(self, path: str, json_body: Optional[Any] = None) -> Any:
        return self._request("PUT", path, json_body=json_body)

    # ── Search + upsert (search-then-write) ────────────────────────
    def search(self, object_type: str, filter_body: Dict[str, Any]) -> List[Dict[str, Any]]:
        """POST /{object_type}/search. Always returns a list (possibly empty)
        -- Copper's search endpoints never return a single bare object."""
        result = self.post(f"/{object_type}/search", json_body=filter_body or {})
        if isinstance(result, list):
            return result
        return []

    def upsert(
        self,
        object_type: str,
        search_filter: Dict[str, Any],
        create_body: Dict[str, Any],
        update_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Find-then-create-or-update. Returns {'action': 'created'|'updated', 'id': ...}.

        Copper has no atomic upsert -- see module docstring for the
        concurrency caveat. `search_filter` and `create_body`/`update_body`
        must already be shaped correctly for `object_type` (people use
        `emails: [...]`, leads use a singular `email: {...}`, etc. --
        `copper_record_upsert` builds these; this method is a dumb
        find-then-write primitive, not object-type aware itself).
        """
        matches = self.search(object_type, search_filter)
        if matches:
            existing_id = matches[0].get("id")
            body = update_body if update_body is not None else create_body
            self.put(f"/{object_type}/{existing_id}", json_body=body)
            return {"action": "updated", "id": existing_id}
        created = self.post(f"/{object_type}", json_body=create_body) or {}
        return {"action": "created", "id": created.get("id")}


class CopperResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Copper CRM resource for use by other components.

    Pairs with:
      - `copper_ingestion` -- READ-side bulk pull (dlt-backed, separate config).
      - `copper_record_upsert` -- reverse-ETL sink (search-then-write).

    Example:
        ```yaml
        type: dagster_component_templates.CopperResourceComponent
        attributes:
          resource_key: copper
          api_key_env_var: COPPER_API_KEY
          user_email: you@company.com
        ```
    """

    resource_key: str = Field(
        default="copper",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        description="Env var holding the Copper API key (sent as X-PW-AccessToken)."
    )
    user_email: str = Field(
        description=(
            "Email address of the Copper user the API key belongs to. "
            "Required by Copper on every request as X-PW-UserEmail."
        ),
    )
    base_url: str = Field(
        default="https://api.copper.com/developer_api/v1",
        description="Copper API base URL.",
    )
    request_timeout_seconds: int = Field(
        default=60, description="Per-request timeout in seconds."
    )
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 / 5xx (exponential backoff, honors Retry-After).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = CopperResource(
            api_key_env_var=self.api_key_env_var,
            user_email=self.user_email,
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
