"""LaunchDarkly Resource component.

API token wrapper over the LaunchDarkly REST API with an ergonomic write
convenience method so Dagster assets can call
`context.resources.launchdarkly.add_segment_targets(...)` without
touching HTTP.

LaunchDarkly segment membership updates use a "semantic patch" --
`PATCH /api/v2/segments/{projKey}/{envKey}/{segmentKey}` with a
`addContextTargets`/`removeContextTargets` instruction and a special
content type -- rather than a plain JSON Merge Patch.

Drop to `.get_client()` for anything not covered -- returns an
authenticated `requests.Session`.
"""
from typing import List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class LaunchDarklyResource(ConfigurableResource):
    """Dagster resource wrapping the LaunchDarkly REST API."""

    api_token: str = Field(description="LaunchDarkly API access token, sent raw in the Authorization header (no 'Bearer ' prefix).")

    def get_client(self):
        """Return an authenticated `requests.Session`. Escape hatch."""
        import requests
        session = requests.Session()
        session.headers.update({"Authorization": self.api_token, "Content-Type": "application/json"})
        return session

    def add_segment_targets(
        self,
        project_key: str,
        env_key: str,
        segment_key: str,
        add_keys: Optional[List[str]] = None,
        remove_keys: Optional[List[str]] = None,
        context_kind: str = "user",
        comment: Optional[str] = None,
    ) -> dict:
        """Add/remove context keys from a segment's target list via a
        semantic patch. Requires the special
        `application/json; domain-model=launchdarkly.semanticpatch`
        content type -- a plain JSON Merge Patch is rejected by this
        endpoint.
        """
        instructions = []
        if add_keys:
            instructions.append({"kind": "addContextTargets", "contextKind": context_kind, "values": add_keys})
        if remove_keys:
            instructions.append({"kind": "removeContextTargets", "contextKind": context_kind, "values": remove_keys})
        if not instructions:
            raise ValueError("add_segment_targets requires add_keys and/or remove_keys.")

        body = {"instructions": instructions}
        if comment:
            body["comment"] = comment

        session = self.get_client()
        session.headers["Content-Type"] = "application/json; domain-model=launchdarkly.semanticpatch"
        resp = session.patch(
            f"https://app.launchdarkly.com/api/v2/segments/{project_key}/{env_key}/{segment_key}",
            json=body,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class LaunchDarklyResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a LaunchDarklyResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.LaunchDarklyResourceComponent
        attributes:
          resource_key: launchdarkly
          api_token_env_var: LAUNCHDARKLY_API_TOKEN
        ```
    """

    resource_key: str = Field(
        default="launchdarkly",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_token_env_var: str = Field(
        default="LAUNCHDARKLY_API_TOKEN",
        description="Env var holding a LaunchDarkly API access token.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = LaunchDarklyResource(api_token=dg.EnvVar(self.api_token_env_var))
        return dg.Definitions(resources={self.resource_key: resource})
