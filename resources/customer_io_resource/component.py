"""Customer.io Resource component.

Registers a ``CustomerIoResource`` with typed batch methods for
downstream Customer.io components (``dataframe_to_customer_io``,
custom sinks).

Owns the Customer.io wire protocol:
- Auth: HTTP Basic (Site ID + Track API key)
- Bulk operations via ``POST /api/v2/batch`` (up to 100 operations per
  batch, ~500KB payload limit). Operations include identify / track /
  delete / suppress / unsuppress.
- Response parsing (200 OK with per-op errors surfaced in the body's
  ``errors`` array)

Docs: https://customer.io/docs/api/track/#operation/batch
"""
from typing import Any, Dict, List

import dagster as dg
from pydantic import Field

_MAX_OPS_PER_BATCH = 100


class CustomerIoResource(dg.ConfigurableResource):
    """Customer.io Track API workhorse — auth + typed batch operations."""

    site_id_env_var: str = Field(default="CUSTOMER_IO_SITE_ID",
        description="Env var holding your Customer.io Site ID.")
    api_key_env_var: str = Field(default="CUSTOMER_IO_API_KEY",
        description="Env var holding your Customer.io Track API key.")
    base_url: str = Field(default="https://track.customer.io",
        description="Customer.io Track API base URL. Use https://track-eu.customer.io for EU workspaces.")
    request_timeout_seconds: int = Field(default=30)

    def _env(self, name: str) -> str:
        import os
        v = os.environ.get(name, "")
        if not v:
            raise dg.Failure(f"env var {name!r} is unset — set your Customer.io credential.")
        return v

    def _auth(self) -> Any:
        from requests.auth import HTTPBasicAuth
        return HTTPBasicAuth(self._env(self.site_id_env_var), self._env(self.api_key_env_var))

    def _post_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.post(url, json=json_body, auth=self._auth(),
                             headers={"Content-Type": "application/json", "Accept": "application/json"},
                             timeout=self.request_timeout_seconds)

    def post(self, path: str, json_body: Any) -> Any:
        """Raw escape hatch — POST + parse JSON. Raises Failure on non-2xx."""
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            raise dg.Failure(f"Customer.io POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def batch_ops(
        self, operations: List[Dict[str, Any]],
        batch_size: int = _MAX_OPS_PER_BATCH,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-execute Customer.io Track operations via /api/v2/batch.

        Args:
            operations: List of op dicts. Each dict must have ``type``
                (e.g. 'identify', 'event', 'delete') and ``identifiers``
                (with ``id``, ``email``, or ``cio_id``). Full op-specific
                fields per Customer.io v2 batch docs.
            batch_size: Max 100 per request (Customer.io limit).

        Returns: {sent, failed, batches, soft_errors, error_samples}
        """
        total = len(operations)
        sent = failed = batches = soft_errors = 0
        error_samples: List[Any] = []
        if total == 0:
            if logger is not None: logger.warning("Customer.io /api/v2/batch: no operations.")
            return {"sent": 0, "failed": 0, "batches": 0, "soft_errors": 0, "error_samples": []}
        batch_size = min(batch_size, _MAX_OPS_PER_BATCH)

        for start in range(0, total, batch_size):
            chunk = operations[start : start + batch_size]
            body = {"batch": chunk}
            if dry_run:
                if logger is not None: logger.info(f"[dry_run] Would POST {len(chunk)} ops to Customer.io /api/v2/batch")
                sent += len(chunk); batches += 1; continue
            resp = self._post_raw("/api/v2/batch", body)
            batches += 1
            if 200 <= resp.status_code < 300:
                sent += len(chunk)
                try:
                    j = resp.json()
                    errs = (j or {}).get("errors") if isinstance(j, dict) else None
                    if errs:
                        soft_errors += len(errs)
                        if len(error_samples) < 3:
                            error_samples.extend(errs[: max(0, 3 - len(error_samples))])
                        if logger is not None:
                            logger.warning(f"Customer.io batch: {len(errs)} per-op errors; sample: {errs[:3]}")
                except Exception:
                    pass
            else:
                failed += len(chunk)
                sample = (resp.text or "")[:400]
                if len(error_samples) < 3:
                    error_samples.append(sample)
                if logger is not None:
                    logger.warning(f"Customer.io batch failed: HTTP {resp.status_code} body={sample}")

        return {"sent": sent, "failed": failed, "batches": batches, "soft_errors": soft_errors, "error_samples": error_samples}


class CustomerIoResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a CustomerIoResource under a resource key for downstream Customer.io components."""

    resource_key: str = Field(default="customer_io")
    site_id_env_var: str = Field(default="CUSTOMER_IO_SITE_ID")
    api_key_env_var: str = Field(default="CUSTOMER_IO_API_KEY")
    base_url: str = Field(default="https://track.customer.io")
    request_timeout_seconds: int = Field(default=30)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        r = CustomerIoResource(
            site_id_env_var=self.site_id_env_var,
            api_key_env_var=self.api_key_env_var,
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: r})
