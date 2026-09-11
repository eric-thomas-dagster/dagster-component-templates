"""Iterable Resource component.

Registers an ``IterableResource`` with typed batch methods for
downstream Iterable components (``dataframe_to_iterable``, custom sinks).

Owns the Iterable wire protocol:
- Auth: ``Api-Key: <key>`` header
- Bulk user upsert via ``POST /api/users/bulkUpdate`` (up to 1,000 users)
- Bulk event track via ``POST /api/events/trackBulk`` (up to 1,000 events)
- Response parsing (200 OK; body's ``failCount`` / ``invalidEmails`` /
  ``disallowedEventNames`` inspected for per-row failures)

Docs: https://api.iterable.com/api/docs
"""
from typing import Any, Dict, List

import dagster as dg
from pydantic import Field

_MAX_USERS_PER_CALL = 1000
_MAX_EVENTS_PER_CALL = 1000


class IterableResource(dg.ConfigurableResource):
    """Iterable REST API workhorse — auth + typed batch operations."""

    api_key_env_var: str = Field(default="ITERABLE_API_KEY",
        description="Env var holding the Iterable API key.")
    base_url: str = Field(default="https://api.iterable.com",
        description="Iterable API base URL. Use https://api.eu.iterable.com for EU workspaces.")
    request_timeout_seconds: int = Field(default=30)

    def _token(self) -> str:
        import os
        t = os.environ.get(self.api_key_env_var)
        if not t: raise dg.Failure(f"env var {self.api_key_env_var!r} is unset — set your Iterable API key.")
        return t

    def _headers(self) -> Dict[str, str]:
        return {"Api-Key": self._token(), "Content-Type": "application/json"}

    def _post_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.post(url, json=json_body, headers=self._headers(),
                             timeout=self.request_timeout_seconds)

    def post(self, path: str, json_body: Any) -> Any:
        """Raw escape hatch — POST + parse JSON. Raises Failure on non-2xx."""
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            raise dg.Failure(f"Iterable POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def bulk_update_users(
        self, users: List[Dict[str, Any]],
        batch_size: int = _MAX_USERS_PER_CALL,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-upsert Iterable users via /api/users/bulkUpdate.

        Args:
            users: List of user dicts. Each must have ``userId`` OR ``email``
                (or both — set ``preferUserId: true`` on each to disambiguate).
                Extra fields go under ``dataFields``.
            batch_size: Max 1,000 per Iterable docs.
        """
        return self._run_batches("/api/users/bulkUpdate", "users", users,
                                 min(batch_size, _MAX_USERS_PER_CALL),
                                 dry_run, logger, invalid_key="invalidEmails")

    def bulk_track_events(
        self, events: List[Dict[str, Any]],
        batch_size: int = _MAX_EVENTS_PER_CALL,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-track events via /api/events/trackBulk. Each event needs
        ``eventName`` + user identifier (``userId`` or ``email``)."""
        return self._run_batches("/api/events/trackBulk", "events", events,
                                 min(batch_size, _MAX_EVENTS_PER_CALL),
                                 dry_run, logger, invalid_key="disallowedEventNames")

    def _run_batches(
        self, path: str, key_name: str, rows: List[Dict[str, Any]],
        batch_size: int, dry_run: bool, logger: Any, invalid_key: str,
    ) -> Dict[str, Any]:
        total = len(rows)
        sent = failed = soft_errors = batches = 0
        error_samples: List[Any] = []
        if total == 0:
            if logger is not None: logger.warning(f"Iterable {path}: no rows.")
            return {"sent": 0, "failed": 0, "soft_errors": 0, "batches": 0, "error_samples": []}

        for start in range(0, total, batch_size):
            chunk = rows[start : start + batch_size]
            body = {key_name: chunk}
            if dry_run:
                if logger is not None: logger.info(f"[dry_run] Would POST {len(chunk)} to {path}")
                sent += len(chunk); batches += 1; continue
            resp = self._post_raw(path, body)
            batches += 1
            if 200 <= resp.status_code < 300:
                sent += len(chunk)
                try:
                    j = resp.json()
                except Exception:
                    j = None
                if isinstance(j, dict):
                    fc = j.get("failCount") or 0
                    if fc:
                        soft_errors += fc
                        inv = j.get(invalid_key) or j.get("filteredOutFields") or []
                        error_samples.extend(inv[: max(0, 3 - len(error_samples))])
                        if logger is not None:
                            logger.warning(f"Iterable {path}: {fc} per-row failures; sample: {inv[:3]}")
            else:
                failed += len(chunk)
                if logger is not None:
                    logger.warning(f"Iterable POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:400]}")
        return {"sent": sent, "failed": failed, "soft_errors": soft_errors, "batches": batches, "error_samples": error_samples}


class IterableResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    resource_key: str = Field(default="iterable")
    api_key_env_var: str = Field(default="ITERABLE_API_KEY")
    base_url: str = Field(default="https://api.iterable.com")
    request_timeout_seconds: int = Field(default=30)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        r = IterableResource(
            api_key_env_var=self.api_key_env_var,
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: r})
