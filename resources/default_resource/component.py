"""Default Resource component.

Default (default.com) is a real GTM/revenue-operations platform --
workflow automation, lead routing, and scheduling -- funded by 8VC/Craft
(Series A). This wraps its public REST API.

Confirmed against the real docs (not assumed) via
https://docs.os.default.com/llms.txt and the specific API reference pages
under https://docs.os.default.com/api-reference/:

  Base URL:   https://api.default.com
  Auth:       Authorization: Bearer <api_key>  (plain API-key Bearer auth,
              NOT OAuth2). Keys carry scopes -- e.g. triggers:write,
              triggers:read, scheduling:read, scheduling:write -- and
              Default returns 403 MISSING_SCOPE if a key lacks the scope an
              endpoint needs.

Default surfaces structured error codes in the response body on failure
(e.g. INVALID_API_KEY, MISSING_SCOPE, WORK_EMAIL_REQUIRED, RESERVATION_
EXPIRED, RATE_LIMITED) -- `_request` below folds the code into the raised
error message when present, rather than just re-raising a bare HTTP status.

Drop to `.get_client()` for anything not covered -- returns an
authenticated `requests.Session`.
"""
import os
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class DefaultResource(ConfigurableResource):
    """Dagster resource wrapping the Default (default.com) REST API."""

    api_key_env_var: str = Field(description="Env var holding the Default API key.")
    base_url: str = Field(
        default="https://api.default.com",
        description="Default API base URL.",
    )

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch for
        anything not covered by the convenience methods below."""
        import requests

        api_key = os.environ.get(self.api_key_env_var)
        if not api_key:
            raise RuntimeError(
                f"Env var {self.api_key_env_var!r} is unset -- cannot authenticate to Default."
            )
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
        )
        return session

    def _request(self, method: str, path: str, **kwargs: Any) -> dict:
        resp = self.get_client().request(
            method, f"{self.base_url.rstrip('/')}{path}", timeout=30, **kwargs
        )
        if resp.status_code >= 400:
            code = None
            message = resp.text
            try:
                body = resp.json()
                code = body.get("code") or body.get("error")
                message = body.get("message") or message
            except ValueError:
                pass
            raise RuntimeError(
                f"Default API error: HTTP {resp.status_code}"
                + (f" ({code})" if code else "")
                + f" for {method} {path}: {message}"
            )
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()

    def list_triggers(self) -> List[dict]:
        """GET /v1/triggers -- the API triggers available to fire, each with
        its form field schema (name/label/type/required/options). Requires
        the `triggers:read` scope."""
        return self._request("GET", "/v1/triggers").get("triggers", [])

    def fire_trigger(
        self,
        trigger_id: str,
        email: str,
        responses: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """POST /v1/triggers/{trigger_id} -- runs the workflow wired to this
        API trigger with the submitted form responses.

        `email` is a REQUIRED, separate top-level field -- Default's docs
        state identity "always comes from this field", distinct from the
        `responses` form-field payload. `context` optionally carries web
        attribution (utmParams, gclid, pageUrl, referrer, userAgent,
        ipAddress). Requires the `triggers:write` scope.
        """
        body: Dict[str, Any] = {"email": email}
        if responses:
            body["responses"] = responses
        if context:
            body["context"] = context
        return self._request("POST", f"/v1/triggers/{trigger_id}", json=body)

    def get_available_slots(
        self, event: str, start: str, end: str, reservation_id: Optional[str] = None
    ) -> dict:
        """POST /v1/scheduling/events/{event}/slots -- bookable times for an
        event or scheduling link within [start, end), returning a
        `reservationId` that `book_meeting` then consumes. Requires the
        `scheduling:read` scope."""
        body: Dict[str, Any] = {"start": start, "end": end}
        if reservation_id:
            body["reservationId"] = reservation_id
        return self._request("POST", f"/v1/scheduling/events/{event}/slots", json=body)

    def book_meeting(
        self,
        event: str,
        start_time: str,
        reservation_id: str,
        person_email: str,
        **optional_fields: Any,
    ) -> dict:
        """POST /v1/scheduling/meetings -- books the previously-reserved slot
        (`reservation_id` from `get_available_slots`). Optional fields
        (passed as kwargs): guestFirstName, guestLastName, guestPhone,
        guestEmails, leadTimezone, workflowExecutionId, responses,
        fieldResponses. Requires the `scheduling:write` scope.
        """
        body: Dict[str, Any] = {
            "event": event,
            "startTime": start_time,
            "reservationId": reservation_id,
            "personEmail": person_email,
        }
        for key in (
            "guestFirstName",
            "guestLastName",
            "guestPhone",
            "guestEmails",
            "leadTimezone",
            "workflowExecutionId",
            "responses",
            "fieldResponses",
        ):
            value = optional_fields.get(key)
            if value is not None:
                body[key] = value
        return self._request("POST", "/v1/scheduling/meetings", json=body)


class DefaultResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a DefaultResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.DefaultResourceComponent
        attributes:
          resource_key: default_resource
          api_key_env_var: DEFAULT_API_KEY
        ```
    """

    resource_key: str = Field(
        default="default_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key_env_var: str = Field(
        description="Env var holding the Default API key.",
    )
    base_url: str = Field(
        default="https://api.default.com",
        description="Default API base URL.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = DefaultResource(
            api_key_env_var=self.api_key_env_var,
            base_url=self.base_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
