"""SendGrid Resource component.

Registers a ``SendGridResource`` with typed batch methods for
downstream SendGrid components (``dataframe_to_sendgrid``, custom sinks).

Owns the SendGrid Marketing wire protocol:
- Auth: ``Authorization: Bearer <api_key>`` header
- Bulk contact upsert via ``PUT /v3/marketing/contacts`` (async import
  job, up to 30,000 contacts per request). Returns 202 Accepted with
  ``job_id``; poll ``GET /v3/marketing/contacts/imports/{id}`` to
  confirm completion.

Docs: https://docs.sendgrid.com/api-reference/contacts/add-or-update-a-contact
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_MAX_CONTACTS_PER_JOB = 30000


class SendGridResource(dg.ConfigurableResource):
    """SendGrid Marketing REST API workhorse — auth + typed batch operations."""

    api_key_env_var: str = Field(default="SENDGRID_API_KEY",
        description="Env var holding the SendGrid API key (starts with `SG.`).")
    base_url: str = Field(default="https://api.sendgrid.com",
        description="SendGrid API base URL. Use https://api.eu.sendgrid.com for the EU regional subuser.")
    request_timeout_seconds: int = Field(default=60)

    def _token(self) -> str:
        import os
        t = os.environ.get(self.api_key_env_var, "")
        if not t: raise dg.Failure(f"env var {self.api_key_env_var!r} is unset — set your SendGrid API key.")
        return t

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _put_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.put(url, json=json_body, headers=self._headers(),
                            timeout=self.request_timeout_seconds)

    def _post_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.post(url, json=json_body, headers=self._headers(),
                             timeout=self.request_timeout_seconds)

    def post(self, path: str, json_body: Any) -> Any:
        """Raw escape hatch — POST + parse JSON. Raises Failure on non-2xx."""
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            raise dg.Failure(f"SendGrid POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def upsert_contacts_bulk(
        self, contacts: List[Dict[str, Any]],
        list_ids: Optional[List[str]] = None,
        batch_size: int = _MAX_CONTACTS_PER_JOB,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-upsert SendGrid contacts via PUT /v3/marketing/contacts.

        Args:
            contacts: List of contact dicts. Each MUST have ``email``.
                Optional reserved fields: first_name, last_name,
                address_line_1, city, state_province_region, country,
                postal_code, phone_number_id, whatsapp, line, facebook,
                unique_name. Custom fields go under ``custom_fields:
                {field_id: value}`` (SendGrid uses numeric field IDs).
            list_ids: Optional SendGrid marketing list ids to add contacts to.
            batch_size: Max 30,000 per SendGrid docs.

        Returns: {sent, failed, batches, job_ids}
            SendGrid processes imports asynchronously; poll via
            ``GET /v3/marketing/contacts/imports/{job_id}``.
        """
        total = len(contacts)
        sent = failed = batches = 0
        job_ids: List[str] = []
        if total == 0:
            if logger is not None: logger.warning("SendGrid /v3/marketing/contacts: no contacts.")
            return {"sent": 0, "failed": 0, "batches": 0, "job_ids": []}
        batch_size = min(batch_size, _MAX_CONTACTS_PER_JOB)

        for start in range(0, total, batch_size):
            chunk = contacts[start : start + batch_size]
            body: Dict[str, Any] = {"contacts": chunk}
            if list_ids:
                body["list_ids"] = list_ids
            if dry_run:
                if logger is not None: logger.info(f"[dry_run] Would PUT {len(chunk)} contacts to SendGrid /v3/marketing/contacts")
                sent += len(chunk); batches += 1; continue
            resp = self._put_raw("/v3/marketing/contacts", body)
            batches += 1
            if resp.status_code in (200, 201, 202):
                sent += len(chunk)
                try:
                    j = resp.json()
                    jid = j.get("job_id") if isinstance(j, dict) else None
                    if jid:
                        job_ids.append(jid)
                except Exception:
                    pass
            else:
                failed += len(chunk)
                if logger is not None:
                    logger.warning(f"SendGrid PUT failed: HTTP {resp.status_code} body={(resp.text or '')[:400]}")

        return {"sent": sent, "failed": failed, "batches": batches, "job_ids": job_ids}


class SendGridResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SendGridResource under a resource key for downstream SendGrid components."""

    resource_key: str = Field(default="sendgrid")
    api_key_env_var: str = Field(default="SENDGRID_API_KEY")
    base_url: str = Field(default="https://api.sendgrid.com")
    request_timeout_seconds: int = Field(default=60)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        r = SendGridResource(
            api_key_env_var=self.api_key_env_var,
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: r})
