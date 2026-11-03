"""SharePoint Resource component.

Shared Microsoft Graph connection (Azure AD app, client-credentials OAuth)
with a convenience method for listing a document library's items, so
`sharepoint_monitor` and any future SharePoint component can share one
credential instead of each doing its own OAuth dance. The Graph calls here
are the same ones `sharepoint_monitor`'s own inline (non-resource_key) path
already performs -- duplicated per this repo's self-contained-component
convention, not imported.

Requires an Azure AD app registration with Sites.Read.All (application)
permission, admin-consented.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class SharePointResource(dg.ConfigurableResource):
    """Microsoft Graph workhorse for SharePoint: OAuth + document-library listing."""

    tenant_id: str
    client_id: str
    client_secret: str
    request_timeout_seconds: int = 30

    def _get_access_token(self) -> str:
        import requests

        resp = requests.post(
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "https://graph.microsoft.com/.default",
            },
            timeout=self.request_timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._get_access_token()}"}

    def resolve_site_id(self, site_url: str) -> str:
        """Resolve a SharePoint site URL to its Graph site ID."""
        import requests

        hostname = site_url.split("/")[2]
        site_path = "/" + "/".join(site_url.split("/")[3:])
        resp = requests.get(
            f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}",
            headers=self._headers(), timeout=self.request_timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()["id"]

    def list_library_items(
        self, site_url: str, library_name: str, folder_path: str = "", top: int = 200,
    ) -> List[Dict[str, Any]]:
        """Resolve the site + document library, then list items in `folder_path` (non-recursive)."""
        import requests

        headers = self._headers()
        site_id = self.resolve_site_id(site_url)

        drive_resp = requests.get(
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives",
            headers=headers, timeout=self.request_timeout_seconds,
        )
        drive_resp.raise_for_status()
        drives = drive_resp.json().get("value", [])
        drive = next((d for d in drives if d["name"] == library_name), None)
        if not drive:
            raise ValueError(f"Library '{library_name}' not found at site {site_url!r}")

        folder = folder_path.strip("/")
        path_segment = f":/{folder}:" if folder else ""
        items_resp = requests.get(
            f"https://graph.microsoft.com/v1.0/drives/{drive['id']}/root{path_segment}/children",
            headers=headers, params={"$top": top}, timeout=self.request_timeout_seconds,
        )
        items_resp.raise_for_status()
        return items_resp.json().get("value", [])


class SharePointResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SharePoint (Microsoft Graph) resource for use by other components.

    Pairs with `sharepoint_monitor` to trigger on new/modified files.
    """

    resource_key: str = Field(
        default="sharepoint_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    tenant_id_env_var: str = Field(description="Env var with Azure tenant ID.")
    client_id_env_var: str = Field(description="Env var with Azure app client ID.")
    client_secret_env_var: str = Field(description="Env var with Azure app client secret.")
    request_timeout_seconds: int = Field(default=30, description="Per-request timeout in seconds.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="SharePoint Resource", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = SharePointResource(
            tenant_id=dg.EnvVar(self.tenant_id_env_var),
            client_id=dg.EnvVar(self.client_id_env_var),
            client_secret=dg.EnvVar(self.client_secret_env_var),
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
