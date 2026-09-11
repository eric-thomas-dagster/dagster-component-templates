"""Segment Resource component.

Registers a ``SegmentResource`` with typed batch methods for
downstream Segment components (``dataframe_to_segment``, custom sinks).

Owns the Segment wire protocol:
- Auth: HTTP Basic with write_key + empty password
- Bulk operations via ``POST /v1/batch`` (batch array of
  identify/track/page/screen/group/alias ops, up to 500KB total)
- Response parsing (200 OK on success)

Docs: https://segment.com/docs/connections/sources/catalog/libraries/server/http-api/#batch
"""
from typing import Any, Dict, List

import dagster as dg
from pydantic import Field

_MAX_OPS_PER_BATCH = 100  # conservative — actual limit is 500KB payload
_MAX_PAYLOAD_BYTES = 480_000  # leave headroom under Segment's 500KB


class SegmentResource(dg.ConfigurableResource):
    """Segment HTTP API workhorse — auth + typed batch operations."""

    write_key_env_var: str = Field(default="SEGMENT_WRITE_KEY",
        description="Env var holding your Segment Source write key.")
    base_url: str = Field(default="https://api.segment.io",
        description="Segment API base URL. Use https://events.eu1.segmentapis.com for EU regional workspaces.")
    request_timeout_seconds: int = Field(default=30)

    def _write_key(self) -> str:
        import os
        k = os.environ.get(self.write_key_env_var, "")
        if not k: raise dg.Failure(f"env var {self.write_key_env_var!r} is unset — set your Segment write key.")
        return k

    def _auth(self) -> Any:
        from requests.auth import HTTPBasicAuth
        return HTTPBasicAuth(self._write_key(), "")

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
            raise dg.Failure(f"Segment POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def batch_ops(
        self, operations: List[Dict[str, Any]],
        batch_size: int = _MAX_OPS_PER_BATCH,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-execute Segment HTTP API ops via POST /v1/batch.

        Args:
            operations: List of op dicts. Each must have ``type``
                (``identify`` / ``track`` / ``page`` / ``screen`` /
                ``group`` / ``alias``) + one of ``userId`` or
                ``anonymousId`` + type-specific fields (``traits``,
                ``event``, ``properties``, ...).
            batch_size: Cap per HTTP request. Also enforced by
                approximate payload byte-budget (~480KB, under
                Segment's 500KB hard cap).

        Returns: {sent, failed, batches, error_samples}
        """
        import json as _json
        total = len(operations)
        sent = failed = batches = 0
        error_samples: List[Any] = []
        if total == 0:
            if logger is not None: logger.warning("Segment /v1/batch: no operations.")
            return {"sent": 0, "failed": 0, "batches": 0, "error_samples": []}
        batch_size = max(1, batch_size)

        # Pack ops into batches respecting BOTH op-count AND payload-size.
        packed_batches: List[List[Dict[str, Any]]] = []
        current: List[Dict[str, Any]] = []
        current_bytes = 0
        for op in operations:
            op_bytes = len(_json.dumps(op).encode("utf-8"))
            if current and (len(current) >= batch_size or current_bytes + op_bytes >= _MAX_PAYLOAD_BYTES):
                packed_batches.append(current)
                current = []
                current_bytes = 0
            current.append(op)
            current_bytes += op_bytes
        if current:
            packed_batches.append(current)

        for chunk in packed_batches:
            body = {"batch": chunk}
            if dry_run:
                if logger is not None: logger.info(f"[dry_run] Would POST {len(chunk)} ops to Segment /v1/batch")
                sent += len(chunk); batches += 1; continue
            resp = self._post_raw("/v1/batch", body)
            batches += 1
            if 200 <= resp.status_code < 300:
                sent += len(chunk)
            else:
                failed += len(chunk)
                sample = (resp.text or "")[:400]
                if len(error_samples) < 3:
                    error_samples.append(sample)
                if logger is not None:
                    logger.warning(f"Segment /v1/batch failed: HTTP {resp.status_code} body={sample}")

        return {"sent": sent, "failed": failed, "batches": batches, "error_samples": error_samples}


class SegmentResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SegmentResource under a resource key for downstream Segment components."""

    resource_key: str = Field(default="segment")
    write_key_env_var: str = Field(default="SEGMENT_WRITE_KEY")
    base_url: str = Field(default="https://api.segment.io")
    request_timeout_seconds: int = Field(default=30)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        r = SegmentResource(
            write_key_env_var=self.write_key_env_var,
            base_url=self.base_url,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: r})
