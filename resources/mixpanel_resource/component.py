"""Mixpanel Resource component.

Registers a ``MixpanelResource`` with typed batch methods for
downstream Mixpanel components (``dataframe_to_mixpanel``, custom sinks).

Owns the Mixpanel wire protocol:
- Auth: HTTP Basic (service account username + secret) for ``/import``
  (bulk events) — Mixpanel requires SA credentials for high-volume imports.
- Auth: project token (in body) for ``/engage#profile-set`` (profile ops).
- Bulk event import via ``POST /import`` (up to 2000 events per call)
- Bulk profile ops via ``POST /engage#profile-set`` (up to 50 ops per call)

Docs:
- https://developer.mixpanel.com/reference/import-events
- https://developer.mixpanel.com/reference/profile-set
"""
from typing import Any, Dict, List

import dagster as dg
from pydantic import Field

_MAX_EVENTS_PER_IMPORT = 2000
_MAX_PROFILES_PER_ENGAGE = 50


class MixpanelResource(dg.ConfigurableResource):
    """Mixpanel REST API workhorse — auth + typed batch operations."""

    project_id: str = Field(description="Mixpanel project ID (required for /import).")
    project_token_env_var: str = Field(default="MIXPANEL_PROJECT_TOKEN",
        description="Env var holding the Mixpanel project token (used for /engage profile ops and event fallback).")
    service_account_username_env_var: str = Field(default="MIXPANEL_SERVICE_ACCOUNT_USERNAME",
        description="Env var holding the Mixpanel service account username (needed for /import).")
    service_account_secret_env_var: str = Field(default="MIXPANEL_SERVICE_ACCOUNT_SECRET",
        description="Env var holding the Mixpanel service account secret (needed for /import).")
    base_url: str = Field(default="https://api.mixpanel.com",
        description="Mixpanel API base URL. Use https://api-eu.mixpanel.com for EU residency projects.")
    request_timeout_seconds: int = Field(default=30)

    def _env(self, name: str, required: bool = True) -> str:
        import os
        v = os.environ.get(name, "")
        if required and not v:
            raise dg.Failure(f"env var {name!r} is unset — set your Mixpanel credential.")
        return v

    def _sa_auth(self) -> Any:
        from requests.auth import HTTPBasicAuth
        u = self._env(self.service_account_username_env_var)
        s = self._env(self.service_account_secret_env_var)
        return HTTPBasicAuth(u, s)

    def _token(self) -> str:
        return self._env(self.project_token_env_var)

    def _post_raw(self, path: str, json_body: Any, params: Any = None, use_sa: bool = False) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        kwargs: Dict[str, Any] = {
            "headers": {"Content-Type": "application/json", "Accept": "application/json"},
            "timeout": self.request_timeout_seconds,
        }
        if params:
            kwargs["params"] = params
        if use_sa:
            kwargs["auth"] = self._sa_auth()
        return requests.post(url, json=json_body, **kwargs)

    def post(self, path: str, json_body: Any, params: Any = None, use_sa: bool = False) -> Any:
        """Raw escape hatch — POST + parse JSON. Raises Failure on non-2xx."""
        resp = self._post_raw(path, json_body, params=params, use_sa=use_sa)
        if not (200 <= resp.status_code < 300):
            raise dg.Failure(f"Mixpanel POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def import_events_bulk(
        self, events: List[Dict[str, Any]],
        batch_size: int = _MAX_EVENTS_PER_IMPORT,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-import Mixpanel events via /import (service-account auth).

        Args:
            events: List of Mixpanel event dicts. Each MUST have ``event``
                (string) and ``properties`` with at least ``distinct_id``,
                ``$insert_id`` (dedup — Mixpanel drops repeats), and
                ``time`` (unix seconds).
            batch_size: Max 2000 per Mixpanel docs.

        Returns: {sent, failed, batches, num_records_imported, error_samples}
        """
        total = len(events)
        sent = failed = batches = imported = 0
        error_samples: List[Any] = []
        if total == 0:
            if logger is not None: logger.warning("Mixpanel /import: no events.")
            return {"sent": 0, "failed": 0, "batches": 0, "num_records_imported": 0, "error_samples": []}
        batch_size = min(batch_size, _MAX_EVENTS_PER_IMPORT)

        for start in range(0, total, batch_size):
            chunk = events[start : start + batch_size]
            if dry_run:
                if logger is not None: logger.info(f"[dry_run] Would POST {len(chunk)} events to Mixpanel /import")
                sent += len(chunk); batches += 1; continue
            resp = self._post_raw("/import", chunk,
                                  params={"strict": "1", "project_id": self.project_id},
                                  use_sa=True)
            batches += 1
            if 200 <= resp.status_code < 300:
                sent += len(chunk)
                try:
                    j = resp.json()
                    if isinstance(j, dict):
                        imported += int(j.get("num_records_imported") or 0)
                except Exception:
                    pass
            else:
                failed += len(chunk)
                sample = (resp.text or "")[:400]
                if len(error_samples) < 3:
                    error_samples.append(sample)
                if logger is not None:
                    logger.warning(f"Mixpanel /import failed: HTTP {resp.status_code} body={sample}")

        return {"sent": sent, "failed": failed, "batches": batches, "num_records_imported": imported, "error_samples": error_samples}

    def set_profiles_bulk(
        self, profile_ops: List[Dict[str, Any]],
        batch_size: int = _MAX_PROFILES_PER_ENGAGE,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-update Mixpanel user profiles via /engage#profile-set.

        Args:
            profile_ops: List of ``{$distinct_id, $set: {...}}`` dicts.
                Token is injected automatically per op.
            batch_size: Max 50 per Mixpanel docs.
        """
        total = len(profile_ops)
        sent = failed = batches = 0
        error_samples: List[Any] = []
        if total == 0:
            if logger is not None: logger.warning("Mixpanel /engage: no profiles.")
            return {"sent": 0, "failed": 0, "batches": 0, "error_samples": []}
        batch_size = min(batch_size, _MAX_PROFILES_PER_ENGAGE)
        token = self._token()

        for start in range(0, total, batch_size):
            chunk = [{**op, "$token": token} for op in profile_ops[start : start + batch_size]]
            if dry_run:
                if logger is not None: logger.info(f"[dry_run] Would POST {len(chunk)} profiles to Mixpanel /engage")
                sent += len(chunk); batches += 1; continue
            resp = self._post_raw("/engage#profile-set", chunk)
            batches += 1
            if 200 <= resp.status_code < 300:
                sent += len(chunk)
            else:
                failed += len(chunk)
                sample = (resp.text or "")[:400]
                if len(error_samples) < 3:
                    error_samples.append(sample)
                if logger is not None:
                    logger.warning(f"Mixpanel /engage failed: HTTP {resp.status_code} body={sample}")

        return {"sent": sent, "failed": failed, "batches": batches, "error_samples": error_samples}


class MixpanelResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a MixpanelResource under a resource key for downstream Mixpanel components."""

    resource_key: str = Field(default="mixpanel")
    project_id: str = Field(description="Mixpanel project ID (required for /import).")
    project_token_env_var: str = Field(default="MIXPANEL_PROJECT_TOKEN")
    service_account_username_env_var: str = Field(default="MIXPANEL_SERVICE_ACCOUNT_USERNAME")
    service_account_secret_env_var: str = Field(default="MIXPANEL_SERVICE_ACCOUNT_SECRET")
    base_url: str = Field(default="https://api.mixpanel.com")
    request_timeout_seconds: int = Field(default=30)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        r = MixpanelResource(
            project_id=self.project_id,
            project_token_env_var=self.project_token_env_var,
            service_account_username_env_var=self.service_account_username_env_var,
            service_account_secret_env_var=self.service_account_secret_env_var,
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: r})
