"""Insightly Resource.

Wraps Insightly CRM's REST API v3.1 (https://api.insightly.com/v3.1/Help).

Auth: **HTTP Basic**, with the API key as the username and a **blank
password**. The API key is per-user (Insightly Settings -> API). This
mirrors the convention already used by this repo's ``insightly_ingestion``
component:

    auth = HTTPBasicAuth(api_key, "")

Base URL is **pod-specific** -- every Insightly account lives on a region
"pod" (visible in your account's API Settings page, right under your API
key, e.g. ``na1``), and all API calls go to:

    https://api.{pod}.insightly.com/v3.1

Rate limits (per Insightly's own docs): max **10 requests/second** per
instance, plus a **daily** quota that varies by plan (1,000/day on the
free Gratis tier up to 60,000+/day on Professional/Enterprise, rolling
24h window). Exceeding either returns **HTTP 429**. Responses carry
``X-RateLimit-Limit`` / ``X-RateLimit-Remaining`` headers -- this resource
logs them on a 429 so operators can see how close they are to quota.

No native upsert. Insightly's v3.1 REST API is plain create-or-update by
numeric internal id:

    POST /{ObjectType}            -- create
    PUT  /{ObjectType}/{id}       -- update (id in the URL path, matching
                                      the official insightly-python SDK's
                                      request construction)

There is **no server-side dedupe-by-external-field** the way Salesforce's
External-ID upsert or HubSpot's batch/upsert work. The closest thing is
the generic search endpoint:

    GET /{ObjectType}/Search?field_name={field}&field_value={value}

which Insightly documents as working against "any standard or custom
field" (e.g. ``field_name=EMAIL_ADDRESS`` for Contacts). ``.upsert()``
below composes search-then-create-or-update out of these primitives.
**This is NOT atomic** -- see the method docstring for the race-condition
caveat.

Object shapes: Contacts store email/phone inside a nested ``CONTACTINFOS``
array rather than flat fields, e.g.::

    {
      "CONTACT_ID": 123,
      "FIRST_NAME": "Jane",
      "LAST_NAME": "Doe",
      "CONTACTINFOS": [
        {"TYPE": "EMAIL", "LABEL": "Work", "DETAIL": "jane@example.com"},
        {"TYPE": "PHONE", "LABEL": "Work", "DETAIL": "+14155551234"}
      ]
    }

Leads and Organisations are flatter (e.g. Leads expose a flat ``EMAIL`` /
``PHONE`` field directly) -- this resource doesn't special-case that; it's
the caller's (``insightly_record_upsert``'s) job to shape the body
correctly per object type.
"""
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _derive_id_field(object_type: str) -> str:
    """Best-effort derivation of an object's numeric-id field name.

    Insightly's convention is SCREAMING_SNAKE_CASE singular + `_ID`:
    Contacts -> CONTACT_ID, Leads -> LEAD_ID, Organisations -> ORGANISATION_ID.
    Stripping a trailing 's' handles all three (and most other v3.1
    object types: Opportunities -> Opportunitie... -- NOTE this simple
    plural-strip is imperfect for words ending 'ies'/'es'; pass an
    explicit `id_field` to `.search()` / `.upsert()` for those.
    """
    singular = object_type[:-1] if object_type.endswith("s") else object_type
    return f"{singular.upper()}_ID"


class InsightlyResource(dg.ConfigurableResource):
    """Insightly CRM REST API v3.1 client wrapper (HTTP Basic auth, pod-based host)."""

    api_key_env_var: str = Field(description="Env var holding the Insightly API key (used as HTTP Basic username).")
    pod: str = Field(
        default="na1",
        description=(
            "Insightly pod identifier (from your Insightly account's API Settings "
            "page, directly under your API key), e.g. 'na1'."
        ),
    )
    request_timeout_seconds: int = Field(default=60, description="Per-request timeout in seconds.")
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 / 5xx (exponential backoff, capped at 10s).",
    )

    def _base_url(self) -> str:
        return f"https://api.{self.pod}.insightly.com/v3.1"

    def _api_key(self) -> str:
        import os
        key = os.environ.get(self.api_key_env_var, "")
        if not key:
            raise RuntimeError(
                f"InsightlyResource: env var {self.api_key_env_var!r} is unset or empty."
            )
        return key

    def _url(self, path: str) -> str:
        path = path if path.startswith("/") else f"/{path}"
        return self._base_url() + path

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Any:
        """Execute one Insightly API call with retry on 429 / 5xx."""
        import requests
        from requests.auth import HTTPBasicAuth

        auth = HTTPBasicAuth(self._api_key(), "")
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.request(
                    method,
                    self._url(path),
                    auth=auth,
                    params=params or {},
                    json=json_body,
                    timeout=self.request_timeout_seconds,
                )
            except requests.RequestException as e:
                last_exc = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code == 429:
                limit = r.headers.get("X-RateLimit-Limit")
                remaining = r.headers.get("X-RateLimit-Remaining")
                import logging
                logging.getLogger("insightly_resource").warning(
                    f"Insightly API 429 (rate limited) on {method} {path} — "
                    f"X-RateLimit-Limit={limit} X-RateLimit-Remaining={remaining} "
                    f"(attempt {attempt}/{self.max_retries})"
                )
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code in (500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            if not r.content:
                return None
            try:
                return r.json()
            except ValueError:
                return {"raw": r.text}
        if last_exc:
            raise last_exc
        return None

    # ── Generic HTTP methods ─────────────────────────────────────
    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """GET a path relative to the pod base URL (e.g. 'Contacts', 'Leads/123')."""
        return self._request("GET", path, params=params)

    def post(self, path: str, json_body: Optional[Dict[str, Any]] = None) -> Any:
        """POST — creates a new record at the collection endpoint (e.g. 'Contacts')."""
        return self._request("POST", path, json_body=json_body)

    def put(self, path: str, json_body: Optional[Dict[str, Any]] = None) -> Any:
        """PUT — updates an existing record. `path` must include the numeric id
        (e.g. 'Contacts/123'), matching Insightly's own Python SDK convention."""
        return self._request("PUT", path, json_body=json_body)

    # ── Search ────────────────────────────────────────────────────
    def search(self, object_type: str, field_name: str, field_value: Any) -> List[Dict[str, Any]]:
        """GET /{object_type}/Search?field_name=X&field_value=Y.

        Insightly documents this as working against "any standard or
        custom field" of the object. Returns a list of matching records
        (empty list if none). This is the only documented field-based
        lookup Insightly's v3.1 API offers -- there's no separate
        "find-by-unique-key" endpoint.
        """
        result = self._request(
            "GET",
            f"/{object_type}/Search",
            params={"field_name": field_name, "field_value": field_value},
        )
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return [result]

    # ── Upsert (emulated — NOT atomic) ───────────────────────────
    def upsert(
        self,
        object_type: str,
        search_field_name: str,
        search_field_value: Any,
        body: Dict[str, Any],
        *,
        id_field: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Find-then-create-or-update. Insightly has no native upsert.

        Flow:
          1. GET /{object_type}/Search?field_name=search_field_name&field_value=search_field_value
          2. If a match is found: PUT /{object_type}/{id} with `body` (merging
             the existing record's id in).
          3. If no match: POST /{object_type} with `body`.

        Returns ``{"action": "created" | "updated", "id": <numeric id>}``
        (mirrors ``salesforce_resource.upsert_record``'s return shape).

        **Race condition**: steps 1 and 2/3 are two separate HTTP calls, not
        one atomic server-side operation. If two upserts for the same
        search key run concurrently, both can see "no match" and both
        POST, producing a duplicate record. Safe for sequential / batch
        reverse-ETL runs; NOT safe for high-concurrency writers hammering
        the same dedupe key.
        """
        resolved_id_field = id_field or _derive_id_field(object_type)
        matches = self.search(object_type, search_field_name, search_field_value)
        if matches:
            existing = matches[0]
            record_id = existing.get(resolved_id_field)
            if record_id is None:
                # Fall back to a generic 'ID' key some Insightly responses use.
                record_id = existing.get("ID") or existing.get("Id")
            updated = self.put(f"/{object_type}/{record_id}", body)
            if isinstance(updated, dict) and updated.get(resolved_id_field) is not None:
                record_id = updated.get(resolved_id_field)
            return {"action": "updated", "id": record_id}

        created = self.post(f"/{object_type}", body) or {}
        return {"action": "created", "id": created.get(resolved_id_field)}


class InsightlyResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an InsightlyResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.InsightlyResourceComponent
        attributes:
          resource_key: insightly
          api_key_env_var: INSIGHTLY_API_KEY
          pod: "na1"
        ```

    Pairs with:
      - `insightly_record_upsert` — reverse-ETL sink (search-then-write
        emulated upsert; see that component's docs for the race-condition
        caveat).
      - `insightly_ingestion` — the READ-side counterpart (dlt-backed bulk
        pull; uses its own inline auth config, doesn't consume this
        resource).
    """

    resource_key: str = Field(
        default="insightly",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        description="Env var holding the Insightly API key (used as HTTP Basic username, blank password).",
    )
    pod: str = Field(
        default="na1",
        description=(
            "Insightly pod identifier, from your account's API Settings page "
            "(directly under your API key), e.g. 'na1'."
        ),
    )
    request_timeout_seconds: int = Field(default=60, description="Per-request timeout in seconds.")
    max_retries: int = Field(
        default=3,
        description="Retry attempts on 429 / 5xx (exponential backoff, capped at 10s).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = InsightlyResource(
            api_key_env_var=self.api_key_env_var,
            pod=self.pod,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
