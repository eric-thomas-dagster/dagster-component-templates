"""Close CRM Resource.

Self-contained Close CRM REST API workhorse (base ``https://api.close.com/api/v1``).

Auth (verified against developer.close.com/api/overview/api-key-authentication):
HTTP Basic, using the Close API key as the **username** with a **blank
password** -- NOT OAuth, NOT a bearer token:

    curl https://api.close.com/api/v1/me/ -u <api_key>:

(Note the trailing colon -- the key is sent as the username, password
empty.) This matches the auth already used by this repo's
``close_crm_ingestion`` component for the read side.

Close has **no native atomic "upsert" endpoint** (unlike Salesforce's
External-ID upsert or HubSpot's batch/upsert). This resource emulates
upsert via **search-then-write**:

  1. ``POST /api/v1/data/search/`` -- Close's "Advanced Filtering" API,
     the current/documented search mechanism (verified against
     developer.close.com/api/resources/advanced-filtering). This repo
     checked for the older ``GET /lead/?query=...`` free-text query-string
     syntax (e.g. ``email:"foo@bar.com"``) sometimes referenced in
     community Close client libraries, but it is **not** documented
     anywhere in Close's current API reference (the ``List Leads`` page
     only documents ``_limit`` / ``_skip`` / ``_fields`` params) -- so this
     resource uses Advanced Filtering exclusively, matching Close's
     presently-recommended surface.
  2. ``PUT /api/v1/lead/{id}/`` if a match was found, else
     ``POST /api/v1/lead/`` to create.

This is **NOT race-condition-safe**: two concurrent writers upserting the
same dedupe value can both miss each other's in-flight create and produce
duplicate Leads. There is no idempotency key or conditional-write
mechanism in Close's REST API to close this window. See this component's
README.md and ``close_crm_lead_upsert``'s README/docstring for the full
discussion -- do not run concurrent writers against the same dedupe value
without an external lock.

Dedupe fields supported by ``find_lead_by_query`` / ``upsert_lead``:
  - ``"email"``       -- matches a contact's email via a
                          ``has_related`` / ``contact_email`` field_condition.
  - ``"phone"``        -- matches a contact's phone via
                          ``has_related`` / ``contact_phone``.
  - ``"name"``         -- matches the Lead's own ``name`` field.
  - ``"custom.cf_xxx"`` -- matches a Lead custom field by
                          ``custom_field_id`` (the ``cf_xxx`` id, exact
                          term match).

Lead body shape (verified against
developer.close.com/api/resources/leads/create):

    {
      "name": "Bluth Company",
      "contacts": [
        {
          "name": "Gob",
          "emails": [{"email": "gob@example.com", "type": "office"}],
          "phones": [{"phone": "8004445555", "type": "office"}]
        }
      ],
      "custom.cf_FSYEbxYJFsnY9tN1OTAPIF33j7Sw5Lb7Eawll7JzoNh": "Segway"
    }

Custom fields are **flat top-level keys** of the form ``"custom.cf_xxx"``
(not nested under a ``"custom"`` object) on both create and update bodies.

Rate limits (verified against developer.close.com/api/overview/rate-limits):
Close enforces a sliding-window limit per API key AND per organization
(org limit = 3x the per-key limit for the same endpoint group), surfaced
via a single combined ``RateLimit`` response header, e.g.
``RateLimit: limit=100, remaining=50, reset=5`` -- Close deprecated the
older separate ``X-Rate-Limit-Limit`` / ``X-Rate-Limit-Remaining``
headers in favor of this combined one, so this resource parses the
combined header (and falls back to checking ``retry-after`` and finally
plain exponential backoff if neither is present). On 429, Close
guarantees both the ``ratelimit`` and ``retry-after`` headers are set;
Close's own docs recommend sleeping for the ``reset`` seconds from
``RateLimit`` rather than ``retry-after`` for a more accurate wait.
"""
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class CloseCrmResource(dg.ConfigurableResource):
    """Close CRM REST API workhorse: HTTP Basic (API-key) auth + read/write/search-upsert methods."""

    api_key: str
    base_url: str = "https://api.close.com/api/v1"
    request_timeout_seconds: int = 30
    max_retries: int = 3

    # ── HTTP core ─────────────────────────────────────────────────
    def _url(self, path: str) -> str:
        if path.startswith("http"):
            return path
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

    @staticmethod
    def _parse_rate_limit_header(headers: Any) -> Optional[Dict[str, float]]:
        """Parse Close's combined ``RateLimit: limit=100, remaining=50, reset=5``
        response header. Returns {'limit', 'remaining', 'reset'} (floats)
        or None if the header is absent / unparseable."""
        raw = headers.get("RateLimit") or headers.get("ratelimit")
        if not raw:
            return None
        parts: Dict[str, float] = {}
        for chunk in raw.split(","):
            if "=" not in chunk:
                continue
            k, _, v = chunk.strip().partition("=")
            try:
                parts[k.strip()] = float(v.strip())
            except ValueError:
                continue
        return parts or None

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Any:
        """Execute a request with retry on 429 (sliding-window rate limit,
        back off using the RateLimit `reset` value / retry-after) and 5xx."""
        import requests

        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.request(
                    method,
                    self._url(path),
                    auth=(self.api_key, ""),
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
                if attempt >= self.max_retries:
                    r.raise_for_status()
                rl = self._parse_rate_limit_header(r.headers)
                wait = (rl or {}).get("reset")
                if wait is None:
                    retry_after = r.headers.get("retry-after") or r.headers.get("Retry-After")
                    try:
                        wait = float(retry_after) if retry_after else None
                    except ValueError:
                        wait = None
                if wait is None:
                    wait = min(2 ** attempt, 10)
                time.sleep(max(wait, 0.5))
                continue

            if r.status_code in (500, 502, 503, 504):
                if attempt >= self.max_retries:
                    r.raise_for_status()
                time.sleep(min(2 ** attempt, 10))
                continue

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

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params)

    def post(self, path: str, json_body: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("POST", path, json_body=json_body)

    def put(self, path: str, json_body: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("PUT", path, json_body=json_body)

    # ── Search (Advanced Filtering, POST /data/search/) ─────────────
    def _build_dedupe_query(self, dedupe_field: str, value: Any) -> Dict[str, Any]:
        """Build the Advanced Filtering query DSL for one of the supported
        dedupe field shapes: 'email', 'phone', 'name', or 'custom.<id>'."""
        value = str(value)
        if dedupe_field == "email":
            inner: Dict[str, Any] = {
                "type": "has_related",
                "this_object_type": "lead",
                "related_object_type": "contact_email",
                "related_query": {
                    "type": "field_condition",
                    "field": {
                        "type": "regular_field",
                        "object_type": "contact_email",
                        "field_name": "email",
                    },
                    "condition": {"type": "text", "value": value, "mode": "phrase"},
                },
            }
        elif dedupe_field == "phone":
            inner = {
                "type": "has_related",
                "this_object_type": "lead",
                "related_object_type": "contact_phone",
                "related_query": {
                    "type": "field_condition",
                    "field": {
                        "type": "regular_field",
                        "object_type": "contact_phone",
                        "field_name": "phone",
                    },
                    "condition": {"type": "text", "value": value, "mode": "phrase"},
                },
            }
        elif dedupe_field == "name":
            inner = {
                "type": "field_condition",
                "field": {
                    "type": "regular_field",
                    "object_type": "lead",
                    "field_name": "name",
                },
                "condition": {"type": "text", "value": value, "mode": "phrase"},
            }
        elif dedupe_field.startswith("custom."):
            custom_field_id = dedupe_field[len("custom."):]
            inner = {
                "type": "field_condition",
                "field": {"type": "custom_field", "custom_field_id": custom_field_id},
                "condition": {"type": "term", "values": [value]},
            }
        else:
            raise ValueError(
                f"CloseCrmResource: unsupported dedupe_field={dedupe_field!r} "
                f"(supported: 'email', 'phone', 'name', or 'custom.<field_id>')."
            )
        return {
            "type": "and",
            "queries": [{"type": "object_type", "object_type": "lead"}, inner],
        }

    def find_lead_by_query(self, dedupe_field: str, value: Any) -> Optional[Dict[str, Any]]:
        """Look up a single Lead by a dedupe field via the Advanced Filtering
        search API. Returns the first match -- by default Close's search
        only returns {'id', '__object_type'}, which is all find-then-write
        needs -- or None if nothing matched (or `value` is empty).

        NOT race-condition-safe: see module docstring.
        """
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        query = self._build_dedupe_query(dedupe_field, value)
        result = self.post("data/search/", {"query": query, "_limit": 1}) or {}
        matches = result.get("data") or []
        return matches[0] if matches else None

    # ── Lead CRUD ────────────────────────────────────────────────────
    def create_lead(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """POST /lead/ -- create a new Lead. Returns the created Lead object."""
        return self.post("lead/", body) or {}

    def update_lead(self, lead_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """PUT /lead/{id}/ -- partial update of an existing Lead. Returns
        the updated Lead object."""
        return self.put(f"lead/{lead_id}/", body) or {}

    def upsert_lead(
        self,
        dedupe_field: str,
        dedupe_value: Any,
        create_body: Dict[str, Any],
        update_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Search-then-write emulated upsert (Close has no native upsert
        endpoint -- see module docstring for the race-condition caveat).

        1. ``find_lead_by_query(dedupe_field, dedupe_value)``.
        2. If found: ``PUT /lead/{id}/`` with `update_body` (falls back to
           `create_body` if `update_body` is None).
        3. Else: ``POST /lead/`` with `create_body`.

        Returns ``{"action": "created"|"updated", "id": <lead_id>}``,
        mirroring this repo's ``salesforce_resource.upsert_record`` shape.
        """
        existing = self.find_lead_by_query(dedupe_field, dedupe_value)
        if existing and existing.get("id"):
            body = update_body if update_body is not None else create_body
            updated = self.update_lead(existing["id"], body)
            return {"action": "updated", "id": updated.get("id") or existing["id"]}
        created = self.create_lead(create_body)
        return {"action": "created", "id": created.get("id")}


class CloseCrmResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a CloseCrmResource for use by other components.

    Pairs with:
      - ``close_crm_ingestion`` -- read side (dlt generic REST API source).
      - ``close_crm_lead_upsert`` -- reverse-ETL sink (search-then-write
        emulated upsert; this resource owns the find/create/update calls).

    Example:

        ```yaml
        type: dagster_component_templates.CloseCrmResourceComponent
        attributes:
          resource_key: close_crm
          api_key_env_var: CLOSE_API_KEY
          base_url: https://api.close.com/api/v1
          request_timeout_seconds: 30
          max_retries: 3
        ```
    """

    resource_key: str = Field(
        default="close_crm",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        description=(
            "Env var holding the Close API key. Sent as the HTTP Basic "
            "username with a blank password (Close's standard REST auth)."
        ),
    )
    base_url: str = Field(
        default="https://api.close.com/api/v1",
        description="Close REST API base URL.",
    )
    request_timeout_seconds: int = Field(
        default=30, description="Per-request timeout in seconds."
    )
    max_retries: int = Field(
        default=3,
        description=(
            "Retry attempts on 429 (sliding-window rate limit -- backs off "
            "using the `RateLimit` response header's `reset` value, falling "
            "back to `retry-after`, then exponential backoff) and on 5xx."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        api_key = os.environ.get(self.api_key_env_var, "")
        resource = CloseCrmResource(
            api_key=api_key,
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
            max_retries=self.max_retries,
        )
        return dg.Definitions(resources={self.resource_key: resource})
