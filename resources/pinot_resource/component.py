"""Pinot Resource — Apache Pinot broker SQL HTTP API connection.

Apache Pinot (like Trino/Druid/QuestDB) is reached over its broker's real
SQL HTTP API, not a wire-protocol driver: request/response shape verified
directly against the official Pinot Broker Query API reference
(docs.pinot.apache.org/reference/api-reference/query-api), not from memory
-- `POST /query/sql` on the broker (default port 8099) with a JSON body
`{"sql": "..."}` returns `{"resultTable": {"dataSchema": {"columnNames":
[...], "columnDataTypes": [...]}, "rows": [[...], ...]}, "exceptions":
[...], ...}`. A non-empty `exceptions` array is Pinot's own way of
reporting a query error -- it can come back alongside HTTP 200, not just a
4xx/5xx status, so this checks both.

This resource owns the broker URL + basic-auth credentials + extra headers
and exposes `query_url` (the base broker URL) plus `execute_query(sql)`,
which issues the exact same HTTP call `pinot_sql_query` makes inline and
returns a pandas DataFrame -- so setting `resource_key` on that (or any
other) component is a drop-in replacement, not a rewrite.
"""
from typing import Dict, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class PinotResource(dg.ConfigurableResource):
    """Apache Pinot broker SQL HTTP API resource."""

    broker_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    request_headers: Optional[Dict[str, str]] = None
    request_timeout_seconds: int = 120

    @property
    def query_url(self) -> str:
        """Base URL of the Pinot broker's SQL HTTP endpoint (e.g. 'http://pinot-broker:8099')."""
        return self.broker_url

    @property
    def auth(self):
        """HTTP basic auth tuple, or None if no username is configured."""
        return (self.username, self.password or "") if self.username else None

    def execute_query(self, sql: str) -> pd.DataFrame:
        """POST sql to /query/sql and return the result as a pandas DataFrame."""
        import requests

        resp = requests.post(
            f"{self.broker_url}/query/sql",
            json={"sql": sql},
            auth=self.auth,
            headers=dict(self.request_headers or {}),
            timeout=self.request_timeout_seconds,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Pinot SQL query failed: HTTP {resp.status_code} {resp.text[:1000]!r}")
        body = resp.json()
        exceptions = body.get("exceptions") or []
        if exceptions:
            # Pinot reports query errors inline, often alongside HTTP 200 -- not just via status code.
            raise RuntimeError(f"Pinot SQL query returned exceptions: {exceptions}")
        result_table = body.get("resultTable") or {}
        schema = result_table.get("dataSchema") or {}
        columns = schema.get("columnNames") or []
        rows = result_table.get("rows") or []
        return pd.DataFrame(rows, columns=columns)


class PinotResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Apache Pinot broker SQL HTTP API resource for use by other components.

    Pairs with ``pinot_sql_query`` (set its ``resource_key`` field) and any
    other component that wants to run SQL against the same Pinot broker
    without redeclaring the broker URL/auth inline.

    Example:

        ```yaml
        type: dagster_community_components.PinotResourceComponent
        attributes:
          resource_key: pinot_resource
          broker_url: http://pinot-broker:8099
          username: admin
          password_env_var: PINOT_PASSWORD
        ```
    """

    resource_key: str = Field(
        default="pinot_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    broker_url: str = Field(description="Base URL of the Pinot broker, e.g. 'http://pinot-broker:8099'.")
    username: Optional[str] = Field(default=None, description="Basic auth username, if the broker requires it.")
    password_env_var: Optional[str] = Field(default=None, description="Env var holding the basic auth password.")
    request_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Extra HTTP headers, e.g. a bearer token if Pinot sits behind an authenticating proxy.",
    )
    request_timeout_seconds: int = Field(default=120, description="HTTP request timeout.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Pinot Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = PinotResource(
            broker_url=self.broker_url,
            username=self.username,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else None,
            request_headers=self.request_headers,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
