"""Amplitude Resource component.

Registers an ``AmplitudeResource`` with typed batch methods for
downstream Amplitude components (``dataframe_to_amplitude``, custom sinks).

Owns the Amplitude wire protocol:
- Auth: API key in JSON body (HTTP V2 API)
- Bulk event track via ``POST /2/httpapi`` (up to 1000 events per call)
- Response parsing (200 OK; body's ``code`` and ``events_ingested`` inspected)

Docs: https://amplitude.com/docs/apis/analytics/http-v2
"""
from typing import Any, Dict, List

import dagster as dg
from pydantic import Field

_MAX_EVENTS_PER_CALL = 1000


class AmplitudeResource(dg.ConfigurableResource):
    """Amplitude REST API workhorse — auth + typed batch operations."""

    api_key_env_var: str = Field(default="AMPLITUDE_API_KEY",
        description="Env var holding the Amplitude project API key.")
    base_url: str = Field(default="https://api2.amplitude.com",
        description="Amplitude API base URL. Use https://api.eu.amplitude.com for EU projects.")
    request_timeout_seconds: int = Field(default=30)

    def _token(self) -> str:
        import os
        t = os.environ.get(self.api_key_env_var)
        if not t: raise dg.Failure(f"env var {self.api_key_env_var!r} is unset — set your Amplitude API key.")
        return t

    def _post_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.post(url, json=json_body,
                             headers={"Content-Type": "application/json", "Accept": "*/*"},
                             timeout=self.request_timeout_seconds)

    def post(self, path: str, json_body: Any) -> Any:
        """Raw escape hatch — POST + parse JSON. Raises Failure on non-2xx."""
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            raise dg.Failure(f"Amplitude POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def track_events_bulk(
        self, events: List[Dict[str, Any]],
        batch_size: int = _MAX_EVENTS_PER_CALL,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-track Amplitude events via /2/httpapi.

        Args:
            events: List of event dicts. Each MUST have ``user_id`` OR
                ``device_id`` and ``event_type``. Optional ``event_properties``,
                ``user_properties``, ``time`` (unix ms), ``insert_id`` for
                de-dup, ``groups`` for account-level analytics.
            batch_size: Max 1000 events per HTTPV2 request.

        Returns: {sent, failed, batches, ingested, error_samples}
        """
        total = len(events)
        sent = failed = batches = ingested = 0
        error_samples: List[Any] = []
        if total == 0:
            if logger is not None: logger.warning("Amplitude /2/httpapi: no events.")
            return {"sent": 0, "failed": 0, "batches": 0, "ingested": 0, "error_samples": []}
        batch_size = min(batch_size, _MAX_EVENTS_PER_CALL)

        for start in range(0, total, batch_size):
            chunk = events[start : start + batch_size]
            body = {"api_key": self._token(), "events": chunk}
            if dry_run:
                if logger is not None: logger.info(f"[dry_run] Would POST {len(chunk)} events to Amplitude /2/httpapi")
                sent += len(chunk); batches += 1; continue
            resp = self._post_raw("/2/httpapi", body)
            batches += 1
            if 200 <= resp.status_code < 300:
                sent += len(chunk)
                try:
                    j = resp.json()
                    if isinstance(j, dict):
                        ingested += int(j.get("events_ingested") or 0)
                except Exception:
                    pass
            else:
                failed += len(chunk)
                sample = (resp.text or "")[:400]
                if len(error_samples) < 3:
                    error_samples.append(sample)
                if logger is not None:
                    logger.warning(f"Amplitude POST failed: HTTP {resp.status_code} body={sample}")

        return {"sent": sent, "failed": failed, "batches": batches, "ingested": ingested, "error_samples": error_samples}


class AmplitudeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an AmplitudeResource under a resource key for downstream Amplitude components."""

    resource_key: str = Field(default="amplitude")
    api_key_env_var: str = Field(default="AMPLITUDE_API_KEY")
    base_url: str = Field(default="https://api2.amplitude.com")
    request_timeout_seconds: int = Field(default=30)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        r = AmplitudeResource(
            api_key_env_var=self.api_key_env_var,
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: r})
