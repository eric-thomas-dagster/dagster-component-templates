"""PandaDoc Resource.

Wraps PandaDoc's REST API (`https://api.pandadoc.com/public/v1`) for
document creation and sending. Auth is a static API Key passed as an
`Authorization: API-Key {key}` header -- the same scheme this repo's
`pandadoc_ingestion` component already uses for read access. This
resource is the write-side counterpart: create a document from a
template (optionally prefilling `tokens` / `fields`), then send it.

Document creation is asynchronous on PandaDoc's side: a freshly created
document starts out in a transient `document.uploaded` status while
PandaDoc builds it from the template server-side, and only a document
that has reached `document.draft` can be sent. `wait_until_draft()`
polls `GET /documents/{id}` until the document reaches `document.draft`
(or a terminal error status) before `send_document()` is safe to call.
"""
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class PandaDocResource(dg.ConfigurableResource):
    """PandaDoc REST API client wrapper (static API Key auth)."""

    api_key: str = Field(description="PandaDoc API key.")
    base_url: str = Field(
        default="https://api.pandadoc.com/public/v1",
        description="PandaDoc API base URL.",
    )

    def _headers(self) -> dict:
        return {
            "Authorization": f"API-Key {self.api_key}",
            "Content-Type": "application/json",
        }

    def create_document(
        self,
        name: str,
        template_uuid: str,
        recipients: List[Dict[str, Any]],
        tokens: Optional[List[Dict[str, str]]] = None,
        fields: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """`POST /documents` -- create a document from a template.
        Returns the creation response (`{'id', 'status', ...}`); the
        document is NOT yet sendable (PandaDoc builds it from the
        template asynchronously -- see `wait_until_draft`)."""
        import requests

        body: Dict[str, Any] = {
            "name": name,
            "template_uuid": template_uuid,
            "recipients": recipients,
        }
        if tokens:
            body["tokens"] = tokens
        if fields:
            body["fields"] = fields
        resp = requests.post(
            f"{self.base_url}/documents",
            json=body,
            headers=self._headers(),
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def get_document_status(self, document_id: str) -> str:
        """`GET /documents/{id}` -- returns the document's current
        status string (e.g. `document.uploaded`, `document.draft`,
        `document.sent`, `document.error_processing`)."""
        import requests

        resp = requests.get(
            f"{self.base_url}/documents/{document_id}",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("status", "")

    def wait_until_draft(
        self,
        document_id: str,
        poll_interval_seconds: float = 2.0,
        timeout_seconds: float = 120.0,
    ) -> str:
        """Poll `get_document_status` until the document reaches
        `document.draft` (sendable), a terminal error status, or
        `timeout_seconds` elapses."""
        deadline = time.time() + timeout_seconds
        status = self.get_document_status(document_id)
        while status != "document.draft":
            if status in ("document.error_processing", "document.error_sending"):
                raise RuntimeError(
                    f"PandaDoc document {document_id} entered error status: {status}"
                )
            if time.time() >= deadline:
                raise TimeoutError(
                    f"PandaDoc document {document_id} did not reach 'document.draft' "
                    f"within {timeout_seconds}s (last status: {status!r})."
                )
            time.sleep(poll_interval_seconds)
            status = self.get_document_status(document_id)
        return status

    def send_document(
        self, document_id: str, message: Optional[str] = None, silent: bool = False
    ) -> dict:
        """`POST /documents/{id}/send` -- send a drafted document to its
        recipients. Requires the document to already be in
        `document.draft` status."""
        import requests

        body: Dict[str, Any] = {"silent": silent}
        if message:
            body["message"] = message
        resp = requests.post(
            f"{self.base_url}/documents/{document_id}/send",
            json=body,
            headers=self._headers(),
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()


class PandaDocResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PandaDocResource for use by other components.

    Example:
        ```yaml
        type: dagster_component_templates.PandaDocResourceComponent
        attributes:
          resource_key: pandadoc_resource
          api_key: "{{ env('PANDADOC_API_KEY') }}"
        ```
    """

    resource_key: str = Field(
        default="pandadoc_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    api_key: str = Field(description="PandaDoc API key.")
    base_url: str = Field(
        default="https://api.pandadoc.com/public/v1",
        description="PandaDoc API base URL.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = PandaDocResource(api_key=self.api_key, base_url=self.base_url)
        return dg.Definitions(resources={self.resource_key: resource})
