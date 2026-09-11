"""Braze Resource component.

Registers a ``BrazeResource`` for use by any downstream Braze component.
Owns the Braze wire protocol end-to-end:

- Auth (Bearer token from env var + region-specific REST endpoint)
- Batching to Braze's per-endpoint limits (75 for /users/track, 50 for
  /catalogs/{name}/items)
- POST + response parsing (200-with-`errors` non-fatal, non-2xx fatal)
- Retryable failures + soft-error surfacing so downstream sinks can
  report clean summary metadata

Downstream components (``dataframe_to_braze``, custom Braze sinks/readers)
call ``resource.track_users(...)`` / ``resource.upsert_catalog_items(...)``
without re-implementing HTTP + batching + error semantics.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_CATALOG_ID_ALLOWED = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"


def _validate_catalog_id(raw_id: str) -> Optional[str]:
    """Braze catalog item IDs: strings, max 250 chars, [A-Za-z0-9_-] only.
    Returns None (skip) for invalid IDs."""
    if not raw_id or len(raw_id) > 250:
        return None
    for ch in raw_id:
        if ch not in _CATALOG_ID_ALLOWED:
            return None
    return raw_id


class BrazeResource(dg.ConfigurableResource):
    """Braze REST API workhorse — holds auth + endpoint, exposes typed
    batch operations for downstream sinks."""

    api_key_env_var: str = Field(
        default="BRAZE_API_KEY",
        description="Env var holding the Braze REST API key.",
    )
    rest_endpoint: str = Field(
        description=(
            "Region-specific Braze REST endpoint URL "
            "(e.g. https://rest.iad-01.braze.com). Look up in the Braze "
            "dashboard under Settings → REST API Keys."
        ),
    )
    request_timeout_seconds: int = Field(
        default=30,
        description="Per-request HTTP timeout (seconds).",
    )

    # ─── Wire internals ────────────────────────────────────────────

    def _token(self) -> str:
        import os
        token = os.environ.get(self.api_key_env_var)
        if not token:
            raise dg.Failure(
                f"env var {self.api_key_env_var!r} is empty or unset — "
                f"set your Braze REST API key."
            )
        return token

    def _base_url(self) -> str:
        return self.rest_endpoint.rstrip("/")

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token()}",
            "Content-Type": "application/json",
        }

    def _post_raw(self, path: str, json_body: Any) -> Any:
        """Low-level POST helper — returns the requests.Response object.
        Callers handle status + parsing."""
        import requests
        path = "/" + path.lstrip("/")
        url = self._base_url() + path
        return requests.post(
            url, json=json_body, headers=self._headers(),
            timeout=self.request_timeout_seconds,
        )

    # ─── Public: raw escape hatch ─────────────────────────────────

    def post(self, path: str, json_body: Any) -> Any:
        """POST ``json_body`` to ``{rest_endpoint}{path}``. Returns the
        parsed JSON response. Raises ``dg.Failure`` on non-2xx.

        Escape hatch for endpoints not covered by the typed methods
        below (campaigns.trigger, canvas.trigger, subscriptions.status,
        etc.). Auto-batching / error accounting NOT applied — that's
        what ``track_users`` and ``upsert_catalog_items`` are for.
        """
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            body = (resp.text or "")[:500]
            raise dg.Failure(
                f"Braze POST {path} failed: HTTP {resp.status_code} body={body}"
            )
        try:
            return resp.json()
        except Exception:
            return None

    # ─── Public: typed batch operations ──────────────────────────

    def track_users(
        self,
        attributes: List[Dict[str, Any]],
        batch_size: int = 75,
        dry_run: bool = False,
        logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch POST to ``/users/track``.

        Auto-batches to Braze's max of 75 attribute objects per call
        (documented at https://www.braze.com/docs/api/endpoints/user_data/post_user_track).
        Handles Braze's 200-with-``errors`` non-fatal error shape.

        Args:
            attributes: List of user attribute dicts. Each must carry a
                primary identifier (``external_id`` / ``braze_id`` /
                ``user_alias``) OR a secondary identifier (``email`` /
                ``phone``). Callers are responsible for shaping.
            batch_size: Rows per POST (default 75, Braze's max).
                Lower reduces rate-limit pressure at the cost of more
                requests.
            dry_run: If True, build payloads + log but skip the POST.
            logger: Optional Dagster-context logger for warnings.

        Returns:
            {sent, failed, soft_errors, batches, error_samples} — send
            counts (both hard 4xx/5xx failures AND soft 200-with-errors
            counted separately), batch count, and up to 3 sampled soft-
            error dicts for debugging.
        """
        return self._run_batches(
            path="/users/track",
            key_name="attributes",
            all_rows=attributes,
            batch_size=min(batch_size, 75),
            dry_run=dry_run,
            logger=logger,
        )

    def upsert_catalog_items(
        self,
        catalog_name: str,
        items: List[Dict[str, Any]],
        batch_size: int = 50,
        dry_run: bool = False,
        logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch POST to ``/catalogs/{catalog_name}/items``.

        Auto-batches to Braze's max of 50 items per call. Validates each
        item's ``id`` field (must exist, be a string ≤250 chars matching
        ``[A-Za-z0-9_-]+``) and drops invalid items with a warning —
        Braze would otherwise reject the whole batch on one bad id.

        Args:
            catalog_name: Braze catalog name.
            items: List of item dicts. Each must carry an ``id`` field
                (any other fields become the item's data).
            batch_size: Items per POST (default 50, Braze's max).
            dry_run: If True, build payloads + log but skip the POST.
            logger: Optional Dagster-context logger for warnings.

        Returns:
            Same shape as track_users.
        """
        # Pre-validate item ids; drop invalid and warn.
        valid_items: List[Dict[str, Any]] = []
        dropped_ids: List[str] = []
        for item in items:
            raw_id = item.get("id")
            if raw_id is None:
                continue
            clean_id = _validate_catalog_id(str(raw_id))
            if clean_id is None:
                dropped_ids.append(str(raw_id)[:60])
                continue
            item["id"] = clean_id
            valid_items.append(item)
        if dropped_ids and logger is not None:
            logger.warning(
                f"Braze catalog: dropped {len(dropped_ids)} items with invalid ids "
                f"(sample: {dropped_ids[:3]}). IDs must be ≤250 chars, letters/digits/_-  only."
            )

        return self._run_batches(
            path=f"/catalogs/{catalog_name}/items",
            key_name="items",
            all_rows=valid_items,
            batch_size=min(batch_size, 50),
            dry_run=dry_run,
            logger=logger,
        )

    # ─── Shared batch runner ─────────────────────────────────────

    def _run_batches(
        self,
        path: str,
        key_name: str,
        all_rows: List[Dict[str, Any]],
        batch_size: int,
        dry_run: bool,
        logger: Any,
    ) -> Dict[str, Any]:
        """Shared POST loop for track_users + upsert_catalog_items.
        Returns {sent, failed, soft_errors, batches, error_samples}."""
        total = len(all_rows)
        sent = failed = batches = soft_errors = 0
        error_samples: List[Any] = []

        if total == 0:
            if logger is not None:
                logger.warning(f"Braze {path}: no rows to send.")
            return {"sent": 0, "failed": 0, "soft_errors": 0, "batches": 0, "error_samples": []}

        for start in range(0, total, batch_size):
            chunk = all_rows[start : start + batch_size]
            payload = {key_name: chunk}

            if dry_run:
                if logger is not None:
                    logger.info(f"[dry_run] Would POST {len(chunk)} rows to {path}")
                sent += len(chunk)
                batches += 1
                continue

            resp = self._post_raw(path, payload)
            batches += 1
            if 200 <= resp.status_code < 300:
                sent += len(chunk)
                # Braze can return 2xx with a non-fatal `errors` array.
                # Batch succeeded overall — un-affected rows applied —
                # but affected rows didn't. Surface as warnings.
                try:
                    payload_json = resp.json()
                except Exception:
                    payload_json = None
                if isinstance(payload_json, dict):
                    errs = payload_json.get("errors")
                    if isinstance(errs, list) and errs:
                        soft_errors += len(errs)
                        for e in errs[:3]:
                            if logger is not None:
                                logger.warning(f"Braze soft error: {e}")
                        # Save up to 3 samples for the returned metadata.
                        error_samples.extend(errs[: max(0, 3 - len(error_samples))])
            else:
                failed += len(chunk)
                body = (resp.text or "")[:400]
                if logger is not None:
                    logger.warning(
                        f"Braze POST {path} failed: HTTP {resp.status_code} body={body}"
                    )

        return {
            "sent": sent,
            "failed": failed,
            "soft_errors": soft_errors,
            "batches": batches,
            "error_samples": error_samples,
        }


class BrazeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a BrazeResource under a resource key for use by downstream
    Braze components (``dataframe_to_braze``, custom sinks, custom readers)."""

    resource_key: str = Field(
        default="braze",
        description="Dagster resource key. Downstream components reference this.",
    )
    api_key_env_var: str = Field(
        default="BRAZE_API_KEY",
        description="Env var holding the Braze REST API key.",
    )
    rest_endpoint: str = Field(
        description="Region-specific Braze REST endpoint URL.",
    )
    request_timeout_seconds: int = Field(
        default=30,
        description="Per-request HTTP timeout (seconds).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = BrazeResource(
            api_key_env_var=self.api_key_env_var,
            rest_endpoint=self.rest_endpoint,
            request_timeout_seconds=self.request_timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
