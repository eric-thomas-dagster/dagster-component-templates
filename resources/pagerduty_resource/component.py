"""PagerDuty Resource component.

REST API v2 wrapper with ergonomic read + write convenience methods so
Dagster assets can call `context.resources.pd.list_incidents(...)` or
`.create_incident(service_id, title, ...)` without touching HTTP.

Also exposes `send_alert(...)` for the Events API v2 (routing-key based) —
handy for the "monitoring system → PagerDuty alert" pattern.

Drop to `.get_client()` for anything not covered — returns an authenticated
`requests.Session` for the REST API.
"""
from typing import Iterator, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class PagerDutyResource(ConfigurableResource):
    """Dagster resource wrapping PagerDuty's REST API v2 + Events API v2.

    Covers incidents, services, users, teams, schedules, escalation policies,
    oncall lookups, and alert events. For anything not covered, use
    `.get_client()` to get an authenticated `requests.Session`.
    """

    api_token: str = Field(description="PagerDuty REST API token (general-access).")
    from_email: str = Field(
        description=(
            "Email of a user in the account. Used as the `From:` header on incident "
            "writes (PagerDuty requires this on incidents/notes/updates). Usually the "
            "email of the user who created the token."
        ),
    )
    events_routing_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional Events API v2 routing key (integration key) for `send_alert`. "
            "Get it from a service's Integrations tab in the PagerDuty UI."
        ),
    )
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    _BASE: str = "https://api.pagerduty.com"
    _EVENTS: str = "https://events.pagerduty.com/v2/enqueue"

    def get_client(self):
        """Return an authenticated `requests.Session` for the REST API."""
        import requests
        session = requests.Session()
        session.verify = self.verify_ssl
        session.headers.update({
            "Authorization": f"Token token={self.api_token}",
            "Accept": "application/vnd.pagerduty+json;version=2",
            "Content-Type": "application/json",
        })
        return session

    def _url(self, path: str) -> str:
        return f"{self._BASE}{path}"

    def _get(self, path: str, **params) -> dict:
        r = self.get_client().get(self._url(path), params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _iter_paginated(self, path: str, key: str, page_size: int = 100, **params) -> Iterator[dict]:
        """Auto-paginate a PagerDuty REST list endpoint (offset-based)."""
        session = self.get_client()
        offset = 0
        while True:
            resp_params = {**params, "limit": page_size, "offset": offset}
            r = session.get(self._url(path), params=resp_params, timeout=60)
            r.raise_for_status()
            body = r.json()
            items = body.get(key, [])
            for it in items:
                yield it
            if not body.get("more") or not items:
                return
            offset += len(items)

    def _write_headers(self) -> dict:
        return {"From": self.from_email}

    # ------------------------------------------------------------------ reads

    def whoami(self) -> dict:
        """Return the authenticated user (the owner of the token)."""
        return self._get("/users/me").get("user", {})

    def list_services(
        self,
        query: Optional[str] = None,
        team_ids: Optional[List[str]] = None,
        page_size: int = 100,
    ) -> List[dict]:
        """List services. First page only — use `iter_services` for full pagination."""
        params: dict = {"limit": page_size}
        if query:
            params["query"] = query
        if team_ids:
            params["team_ids[]"] = team_ids
        return self._get("/services", **params).get("services", [])

    def iter_services(
        self,
        query: Optional[str] = None,
        team_ids: Optional[List[str]] = None,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Auto-paginated variant of `list_services`."""
        params: dict = {}
        if query:
            params["query"] = query
        if team_ids:
            params["team_ids[]"] = team_ids
        yield from self._iter_paginated("/services", "services", page_size=page_size, **params)

    def get_service(self, service_id: str) -> dict:
        """Retrieve a service by ID."""
        return self._get(f"/services/{service_id}").get("service", {})

    def list_incidents(
        self,
        service_ids: Optional[List[str]] = None,
        statuses: Optional[List[str]] = None,
        incident_key: Optional[str] = None,
        page_size: int = 100,
    ) -> List[dict]:
        """List incidents. First page only — use `iter_incidents` for full pagination.

        `statuses` = list of `triggered` / `acknowledged` / `resolved`.
        """
        params: dict = {"limit": page_size}
        if service_ids:
            params["service_ids[]"] = service_ids
        if statuses:
            params["statuses[]"] = statuses
        if incident_key:
            params["incident_key"] = incident_key
        return self._get("/incidents", **params).get("incidents", [])

    def iter_incidents(
        self,
        service_ids: Optional[List[str]] = None,
        statuses: Optional[List[str]] = None,
        incident_key: Optional[str] = None,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Auto-paginated variant of `list_incidents`."""
        params: dict = {}
        if service_ids:
            params["service_ids[]"] = service_ids
        if statuses:
            params["statuses[]"] = statuses
        if incident_key:
            params["incident_key"] = incident_key
        yield from self._iter_paginated("/incidents", "incidents", page_size=page_size, **params)

    def get_incident(self, incident_id: str) -> dict:
        """Retrieve a single incident by ID."""
        return self._get(f"/incidents/{incident_id}").get("incident", {})

    def list_users(
        self, team_ids: Optional[List[str]] = None, page_size: int = 100
    ) -> List[dict]:
        """List users. First page only."""
        params: dict = {"limit": page_size}
        if team_ids:
            params["team_ids[]"] = team_ids
        return self._get("/users", **params).get("users", [])

    def list_teams(self, page_size: int = 100) -> List[dict]:
        """List teams. First page only."""
        return self._get("/teams", limit=page_size).get("teams", [])

    def list_schedules(self, page_size: int = 100) -> List[dict]:
        """List on-call schedules. First page only."""
        return self._get("/schedules", limit=page_size).get("schedules", [])

    def list_escalation_policies(self, page_size: int = 100) -> List[dict]:
        """List escalation policies. First page only."""
        return self._get("/escalation_policies", limit=page_size).get("escalation_policies", [])

    def list_oncalls(
        self,
        schedule_ids: Optional[List[str]] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[dict]:
        """List who is currently on-call. Optionally filter by schedule IDs / time window."""
        params: dict = {}
        if schedule_ids:
            params["schedule_ids[]"] = schedule_ids
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        return self._get("/oncalls", **params).get("oncalls", [])

    # ----------------------------------------------------------------- writes

    def create_incident(
        self,
        service_id: str,
        title: str,
        details: str = "",
        incident_key: Optional[str] = None,
        urgency: str = "high",
        priority_id: Optional[str] = None,
        escalation_policy_id: Optional[str] = None,
        assignee_user_id: Optional[str] = None,
    ) -> dict:
        """Create an incident. `incident_key` provides client-side dedup — same key
        submitted twice returns the existing open incident on the second call.

        `urgency` = `high` | `low`.
        """
        incident: dict = {
            "type": "incident",
            "title": title,
            "service": {"id": service_id, "type": "service_reference"},
            "urgency": urgency,
        }
        if details:
            incident["body"] = {"type": "incident_body", "details": details}
        if incident_key:
            incident["incident_key"] = incident_key
        if priority_id:
            incident["priority"] = {"id": priority_id, "type": "priority_reference"}
        if escalation_policy_id:
            incident["escalation_policy"] = {"id": escalation_policy_id, "type": "escalation_policy_reference"}
        if assignee_user_id:
            incident["assignments"] = [{"assignee": {"id": assignee_user_id, "type": "user_reference"}}]
        session = self.get_client()
        session.headers.update(self._write_headers())
        r = session.post(self._url("/incidents"), json={"incident": incident}, timeout=60)
        r.raise_for_status()
        return r.json().get("incident", {})

    def update_incident(
        self,
        incident_id: str,
        status: Optional[str] = None,
        title: Optional[str] = None,
        urgency: Optional[str] = None,
        priority_id: Optional[str] = None,
        resolution: Optional[str] = None,
    ) -> dict:
        """Patch an incident. `status` = `acknowledged` | `resolved`."""
        incident: dict = {"type": "incident_reference"}
        if status:
            incident["status"] = status
        if title:
            incident["title"] = title
        if urgency:
            incident["urgency"] = urgency
        if priority_id:
            incident["priority"] = {"id": priority_id, "type": "priority_reference"}
        if resolution:
            incident["resolution"] = resolution
        session = self.get_client()
        session.headers.update(self._write_headers())
        r = session.put(self._url(f"/incidents/{incident_id}"), json={"incident": incident}, timeout=60)
        r.raise_for_status()
        return r.json().get("incident", {})

    def acknowledge_incident(self, incident_id: str) -> dict:
        """Move an incident to `acknowledged`."""
        return self.update_incident(incident_id, status="acknowledged")

    def resolve_incident(self, incident_id: str, resolution: str = "") -> dict:
        """Move an incident to `resolved`."""
        return self.update_incident(incident_id, status="resolved", resolution=resolution)

    def add_incident_note(self, incident_id: str, content: str) -> dict:
        """Add a note to an incident."""
        session = self.get_client()
        session.headers.update(self._write_headers())
        r = session.post(
            self._url(f"/incidents/{incident_id}/notes"),
            json={"note": {"content": content}},
            timeout=60,
        )
        r.raise_for_status()
        return r.json().get("note", {})

    def send_alert(
        self,
        summary: str,
        source: str,
        severity: str = "error",
        dedup_key: Optional[str] = None,
        custom_details: Optional[dict] = None,
        component: Optional[str] = None,
        group: Optional[str] = None,
        cls: Optional[str] = None,
    ) -> dict:
        """Fire an alert via the Events API v2. Requires `events_routing_key`.

        `severity` = `critical` | `error` | `warning` | `info`.
        `dedup_key` provides idempotent alerting — same key = same alert (updates
        instead of creating).
        """
        if not self.events_routing_key:
            raise ValueError("events_routing_key not set — cannot send alerts.")
        payload: dict = {
            "routing_key": self.events_routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": summary,
                "source": source,
                "severity": severity,
            },
        }
        if custom_details:
            payload["payload"]["custom_details"] = custom_details
        if component:
            payload["payload"]["component"] = component
        if group:
            payload["payload"]["group"] = group
        if cls:
            payload["payload"]["class"] = cls
        if dedup_key:
            payload["dedup_key"] = dedup_key
        import requests
        r = requests.post(self._EVENTS, json=payload, timeout=60, verify=self.verify_ssl)
        r.raise_for_status()
        return r.json()

    def resolve_alert(self, dedup_key: str) -> dict:
        """Resolve an alert previously fired via `send_alert` with the same `dedup_key`."""
        if not self.events_routing_key:
            raise ValueError("events_routing_key not set — cannot resolve alerts.")
        payload: dict = {
            "routing_key": self.events_routing_key,
            "event_action": "resolve",
            "dedup_key": dedup_key,
        }
        import requests
        r = requests.post(self._EVENTS, json=payload, timeout=60, verify=self.verify_ssl)
        r.raise_for_status()
        return r.json()


class PagerDutyResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PagerDutyResource for use by other components.

    Example:
        ```yaml
        type: dagster_community_components.PagerDutyResourceComponent
        attributes:
          resource_key: pd
          api_token_env_var: PAGERDUTY_API_TOKEN
          from_email_env_var: PAGERDUTY_FROM_EMAIL
          # events_routing_key_env_var: PAGERDUTY_ROUTING_KEY  # optional
        ```
    """

    resource_key: str = Field(
        default="pd",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_token_env_var: str = Field(
        default="PAGERDUTY_API_TOKEN",
        description="Env var holding a PagerDuty REST API token.",
    )
    from_email_env_var: str = Field(
        default="PAGERDUTY_FROM_EMAIL",
        description="Env var holding the email of a valid PagerDuty user (used as `From:` header on writes).",
    )
    events_routing_key_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Optional env var holding a PagerDuty Events API v2 routing key "
            "(integration key). Needed only for `send_alert` / `resolve_alert`."
        ),
    )
    verify_ssl: bool = Field(default=True, description="TLS cert verification.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        kwargs: dict = {
            "api_token": dg.EnvVar(self.api_token_env_var),
            "from_email": dg.EnvVar(self.from_email_env_var),
            "verify_ssl": self.verify_ssl,
        }
        if self.events_routing_key_env_var:
            kwargs["events_routing_key"] = dg.EnvVar(self.events_routing_key_env_var)
        return dg.Definitions(resources={self.resource_key: PagerDutyResource(**kwargs)})
