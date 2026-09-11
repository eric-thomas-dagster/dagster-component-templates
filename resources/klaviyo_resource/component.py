"""Klaviyo Resource component.

Registers a ``KlaviyoResource`` with typed batch methods for
downstream Klaviyo components (``dataframe_to_klaviyo``, custom sinks).

Owns the Klaviyo wire protocol:
- Auth: ``Authorization: Klaviyo-API-Key <key>`` header + ``revision`` header
- Bulk profile upsert via async job (``POST /api/profile-bulk-import-jobs``,
  up to 10,000 profiles per job)
- Bulk event creation via async job (``POST /api/event-bulk-create-jobs``)
- Response parsing (202 Accepted → job id)

Docs:
- https://developers.klaviyo.com/en/reference/spawn_bulk_profile_import_job
- https://developers.klaviyo.com/en/reference/bulk_create_events
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_DEFAULT_REVISION = "2024-10-15"
_MAX_PROFILES_PER_JOB = 10000


class KlaviyoResource(dg.ConfigurableResource):
    """Klaviyo REST API workhorse — auth + async bulk operations."""

    api_key_env_var: str = Field(
        default="KLAVIYO_API_KEY",
        description="Env var holding the Klaviyo private API key (starts with `pk_`).",
    )
    api_revision: str = Field(
        default=_DEFAULT_REVISION,
        description=(
            "Klaviyo API revision date sent in the `revision` header. "
            "Klaviyo pins API behavior per date; bump this when new "
            "revisions add fields you need."
        ),
    )
    base_url: str = Field(
        default="https://a.klaviyo.com",
        description="Klaviyo API base URL (rarely overridden).",
    )
    request_timeout_seconds: int = Field(default=30)

    def _token(self) -> str:
        import os
        token = os.environ.get(self.api_key_env_var)
        if not token:
            raise dg.Failure(f"env var {self.api_key_env_var!r} is unset — set your Klaviyo API key.")
        return token

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Klaviyo-API-Key {self._token()}",
            "revision": self.api_revision,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _post_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.post(url, json=json_body, headers=self._headers(),
                             timeout=self.request_timeout_seconds)

    def post(self, path: str, json_body: Any) -> Any:
        """Raw escape hatch — POST + parse JSON. Raises Failure on non-2xx."""
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            raise dg.Failure(f"Klaviyo POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def upsert_profiles_bulk(
        self,
        profiles: List[Dict[str, Any]],
        list_id: Optional[str] = None,
        batch_size: int = _MAX_PROFILES_PER_JOB,
        dry_run: bool = False,
        logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-create or update Klaviyo profiles via async bulk-import jobs.

        Args:
            profiles: List of profile dicts. Each MUST carry at least one of
                ``email``, ``phone_number``, or ``external_id`` at the top
                level. Any other keys become custom properties (unless they
                match Klaviyo's reserved attribute names).
            list_id: Optional Klaviyo list id to add all profiles to.
            batch_size: Profiles per job (default = 10,000 = Klaviyo max).
            dry_run: If True, build payloads + log but skip the POST.
            logger: Optional Dagster-context logger.

        Returns:
            {sent, failed, batches, job_ids} — Klaviyo processes jobs
            asynchronously; job_ids can be polled via
            ``GET /api/profile-bulk-import-jobs/{id}`` if you need to
            confirm completion.
        """
        total = len(profiles)
        sent = failed = batches = 0
        job_ids: List[str] = []
        if total == 0:
            if logger is not None:
                logger.warning("Klaviyo profile-bulk-import: no rows.")
            return {"sent": 0, "failed": 0, "batches": 0, "job_ids": []}
        batch_size = min(batch_size, _MAX_PROFILES_PER_JOB)

        for start in range(0, total, batch_size):
            chunk = profiles[start : start + batch_size]
            data_list = [{"type": "profile", "attributes": p} for p in chunk]
            body: Dict[str, Any] = {
                "data": {
                    "type": "profile-bulk-import-job",
                    "attributes": {"profiles": {"data": data_list}},
                }
            }
            if list_id:
                body["data"]["relationships"] = {
                    "lists": {"data": [{"type": "list", "id": list_id}]}
                }
            if dry_run:
                if logger is not None:
                    logger.info(f"[dry_run] Would POST {len(chunk)} profiles to Klaviyo bulk-import")
                sent += len(chunk); batches += 1
                continue
            resp = self._post_raw("/api/profile-bulk-import-jobs", body)
            batches += 1
            if resp.status_code in (200, 201, 202):
                sent += len(chunk)
                try:
                    j = resp.json()
                    jid = (j.get("data") or {}).get("id")
                    if jid:
                        job_ids.append(jid)
                except Exception:
                    pass
            else:
                failed += len(chunk)
                if logger is not None:
                    logger.warning(f"Klaviyo POST failed: HTTP {resp.status_code} body={(resp.text or '')[:400]}")

        return {"sent": sent, "failed": failed, "batches": batches, "job_ids": job_ids}


class KlaviyoResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a KlaviyoResource under a resource key for downstream Klaviyo components."""

    resource_key: str = Field(default="klaviyo")
    api_key_env_var: str = Field(default="KLAVIYO_API_KEY")
    api_revision: str = Field(default=_DEFAULT_REVISION)
    base_url: str = Field(default="https://a.klaviyo.com")
    request_timeout_seconds: int = Field(default=30)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        r = KlaviyoResource(
            api_key_env_var=self.api_key_env_var,
            api_revision=self.api_revision,
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: r})
