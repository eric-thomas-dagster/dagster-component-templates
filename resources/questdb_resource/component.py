"""QuestDB Resource — QuestDB HTTP REST API connection.

QuestDB (like Trino/Druid/Pinot) is reached over its real HTTP REST API,
not a wire-protocol driver: request/response shape verified directly
against the official QuestDB REST API reference
(questdb.com/docs/reference/api/rest/), not from memory -- `GET
/api/v1/sql/execute` (the newer name for `/exec`, same parameters and
response) with the SQL in the `query` URL parameter -- POST is not
supported there and returns 405. A successful SELECT returns `{"query":
..., "columns": [{"name", "type"}, ...], "dataset": [[v1, v2, ...], ...],
"timestamp": N, "count": N}` -- row values are positional (column order),
not keyed by name, so they're zipped against `columns` here.

This resource owns the host URL + basic-auth credentials + extra headers
and exposes `query_url` (the base host URL) plus `execute_query(sql)`,
which issues the exact same HTTP call `questdb_sql_query` makes inline and
returns a pandas DataFrame -- so setting `resource_key` on that (or any
other) component is a drop-in replacement, not a rewrite.
"""
from typing import Dict, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class QuestdbResource(dg.ConfigurableResource):
    """QuestDB HTTP REST API resource."""

    host_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    request_headers: Optional[Dict[str, str]] = None
    request_timeout_seconds: int = 120

    @property
    def query_url(self) -> str:
        """Base URL of the QuestDB HTTP REST API (e.g. 'http://questdb:9000')."""
        return self.host_url

    @property
    def auth(self):
        """HTTP basic auth tuple, or None if no username is configured."""
        return (self.username, self.password or "") if self.username else None

    def execute_query(self, query: str) -> pd.DataFrame:
        """GET /api/v1/sql/execute with the SQL as a query param and return a pandas DataFrame."""
        import requests

        resp = requests.get(
            f"{self.host_url}/api/v1/sql/execute",
            params={"query": query},
            auth=self.auth,
            headers=dict(self.request_headers or {}),
            timeout=self.request_timeout_seconds,
        )
        if resp.status_code >= 400:
            try:
                body = resp.json()
                message = body.get("error", resp.text[:1000])
            except ValueError:
                message = resp.text[:1000]
            raise RuntimeError(f"QuestDB query failed: HTTP {resp.status_code} {message!r}")
        payload = resp.json()
        columns = [c["name"] for c in payload.get("columns", [])]
        dataset = payload.get("dataset", [])
        return pd.DataFrame(dataset, columns=columns)


class QuestdbResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a QuestDB HTTP REST API resource for use by other components.

    Pairs with ``questdb_sql_query`` (set its ``resource_key`` field) and
    any other component that wants to run SQL against the same QuestDB
    instance without redeclaring the host URL/auth inline.

    Example:

        ```yaml
        type: dagster_community_components.QuestdbResourceComponent
        attributes:
          resource_key: questdb_resource
          host_url: http://questdb:9000
          username: admin
          password_env_var: QUESTDB_PASSWORD
        ```
    """

    resource_key: str = Field(
        default="questdb_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    host_url: str = Field(description="Base URL of the QuestDB HTTP REST API, e.g. 'http://questdb:9000'.")
    username: Optional[str] = Field(default=None, description="HTTP basic auth username, if QuestDB's built-in auth is enabled.")
    password_env_var: Optional[str] = Field(default=None, description="Env var holding the basic auth password.")
    request_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Extra HTTP headers, e.g. a bearer token if QuestDB sits behind an authenticating proxy.",
    )
    request_timeout_seconds: int = Field(default=120, description="HTTP request timeout.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Questdb Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = QuestdbResource(
            host_url=self.host_url,
            username=self.username,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else None,
            request_headers=self.request_headers,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
