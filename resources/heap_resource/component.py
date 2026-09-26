"""Heap Resource component.

Heap's public API is write-only (track / identify / add_user_properties) --
there is no bulk read/list endpoint, so Heap has no ingestion counterpart
in this repo. This resource exists purely to support reverse-ETL: pushing
computed user properties from a warehouse INTO Heap for analysis
segmentation.

Drop to `.get_client()` for anything not covered -- returns an
authenticated `requests.Session`.
"""
from typing import Any, Dict

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class HeapResource(ConfigurableResource):
    """Dagster resource wrapping Heap's write-only Data API."""

    app_id: str = Field(description="Heap environment ID (App ID), included in every request body.")

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch."""
        import requests
        session = requests.Session()
        session.headers.update({"Content-Type": "application/json"})
        return session

    def add_user_properties(self, identity: str, properties: Dict[str, Any]) -> None:
        """POST https://heapanalytics.com/api/add_user_properties --
        attaches/overwrites custom properties on a user identified by
        `identity` (whatever value your Heap `heap.identify()` calls use
        -- typically an email or internal user ID).
        """
        session = self.get_client()
        resp = session.post(
            "https://heapanalytics.com/api/add_user_properties",
            json={"app_id": self.app_id, "identity": identity, "properties": properties},
            timeout=60,
        )
        resp.raise_for_status()


class HeapResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a HeapResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.HeapResourceComponent
        attributes:
          resource_key: heap
          app_id_env_var: HEAP_APP_ID
        ```
    """

    resource_key: str = Field(
        default="heap",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    app_id_env_var: str = Field(
        default="HEAP_APP_ID",
        description="Env var holding your Heap environment ID (App ID).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = HeapResource(app_id=dg.EnvVar(self.app_id_env_var))
        return dg.Definitions(resources={self.resource_key: resource})
