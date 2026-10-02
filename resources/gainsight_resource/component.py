"""Gainsight Resource.

Wraps Gainsight NXT's REST API v1. Gainsight's "Query" endpoints (used to
read Company / Relationship / custom objects) are POST-based -- you send a
JSON body describing select/where/orderBy/limit/offset, rather than GET with
query-string filters. See:
  https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/Company_and_Relationship_API/Company_API_Documentation

Auth: a single org-wide Access Key, generated under Administration ->
Connectors 2.0 -> Create Connection -> "Gainsight API" -> Access_Key. It is
passed as the `accesskey` request header (not Bearer, not Basic -- Gainsight
uses its own header name). The key does not expire but is a single org-wide
secret -- there is no per-user scoping.
"""
import os
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class GainsightResource(dg.ConfigurableResource):
    """Gainsight NXT REST API v1 client wrapper (access-key auth)."""

    base_url: str = Field(
        description="Your Gainsight tenant's API base URL, e.g. 'https://yourcompany.gainsightcloud.com'"
    )
    access_key_env_var: str = Field(
        default="GAINSIGHT_ACCESS_KEY",
        description="Env var holding the org-wide Gainsight Access Key",
    )

    def post(self, path: str, json: Optional[Dict[str, Any]] = None) -> dict:
        """POST to a Gainsight v1 endpoint (e.g. 'v1/data/objects/query/Company') and return parsed JSON."""
        import requests

        access_key = os.environ.get(self.access_key_env_var)
        if not access_key:
            raise RuntimeError(
                f"Missing Gainsight access key: env var {self.access_key_env_var!r} is not set"
            )
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        resp = requests.post(
            url,
            json=json or {},
            headers={"accesskey": access_key, "Content-Type": "application/json"},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class GainsightResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a `GainsightResource` (Gainsight NXT REST API v1 client) for use by other components.

    Other components reference this resource by the value of `resource_key`.
    """

    resource_key: str = Field(
        default="gainsight_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    base_url: str = Field(
        description="Your Gainsight tenant's API base URL, e.g. 'https://yourcompany.gainsightcloud.com'"
    )
    access_key_env_var: str = Field(
        default="GAINSIGHT_ACCESS_KEY",
        description="Env var holding the org-wide Gainsight Access Key (Administration -> Connectors 2.0 -> Gainsight API -> Access_Key)",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = GainsightResource(
            base_url=self.base_url,
            access_key_env_var=self.access_key_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
