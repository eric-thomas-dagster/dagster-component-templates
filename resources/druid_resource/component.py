"""Druid Resource — Apache Druid broker SQL HTTP API connection.

Apache Druid (like Trino/Pinot/QuestDB) is reached over its broker's real
SQL HTTP API, not a wire-protocol driver: request/response shape verified
directly against the official Druid SQL API reference
(druid.apache.org/docs/latest/api-reference/sql-api), not from memory --
`POST /druid/v2/sql` with a JSON body `{"query": ..., "resultFormat":
"object"}` (Content-Type: application/json) returns a JSON array of
objects, one per row, when `resultFormat: "object"` (the default here).

This resource owns the broker URL + basic-auth credentials + extra headers
and exposes `query_url` (the base broker URL) plus `execute_query(sql)`,
which issues the exact same HTTP call `druid_sql_query` makes inline --
so setting `resource_key` on that (or any other) component is a drop-in
replacement, not a rewrite.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class DruidResource(dg.ConfigurableResource):
    """Apache Druid broker SQL HTTP API resource."""

    broker_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    request_headers: Optional[Dict[str, str]] = None
    request_timeout_seconds: int = 120

    @property
    def query_url(self) -> str:
        """Base URL of the Druid broker's SQL HTTP endpoint (e.g. 'http://druid-broker:8082')."""
        return self.broker_url

    @property
    def auth(self):
        """HTTP basic auth tuple, or None if no username is configured."""
        return (self.username, self.password or "") if self.username else None

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """POST query to /druid/v2/sql and return the JSON array of row objects."""
        import requests

        resp = requests.post(
            f"{self.broker_url}/druid/v2/sql",
            json={"query": query, "resultFormat": "object"},
            auth=self.auth,
            headers=dict(self.request_headers or {}),
            timeout=self.request_timeout_seconds,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Druid SQL query failed: HTTP {resp.status_code} {resp.text[:1000]!r}")
        return resp.json()


class DruidResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Apache Druid broker SQL HTTP API resource for use by other components.

    Pairs with ``druid_sql_query`` (set its ``resource_key`` field) and any
    other component that wants to run SQL against the same Druid broker
    without redeclaring the broker URL/auth inline.

    Example:

        ```yaml
        type: dagster_community_components.DruidResourceComponent
        attributes:
          resource_key: druid_resource
          broker_url: http://druid-broker:8082
          username: admin
          password_env_var: DRUID_PASSWORD
        ```
    """

    resource_key: str = Field(
        default="druid_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    broker_url: str = Field(description="Base URL of the Druid broker, e.g. 'http://druid-broker:8082'.")
    username: Optional[str] = Field(default=None, description="Basic auth username, if the broker requires it.")
    password_env_var: Optional[str] = Field(default=None, description="Env var holding the basic auth password.")
    request_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Extra HTTP headers, e.g. a bearer token if Druid sits behind an authenticating proxy.",
    )
    request_timeout_seconds: int = Field(default=120, description="HTTP request timeout.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Druid Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = DruidResource(
            broker_url=self.broker_url,
            username=self.username,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else None,
            request_headers=self.request_headers,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
