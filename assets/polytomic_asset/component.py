"""Polytomic Sync Asset Component.

Extends StateBackedComponent so the Polytomic API is called once at prepare time
(write_state_to_path) and cached on disk. build_defs_from_state builds one Dagster
asset per Polytomic sync with zero network calls, keeping code-server reloads fast.

At execution time each asset triggers the corresponding Polytomic sync run and
optionally polls until it completes.

On first load (state_path is None) returns empty Definitions — run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import dagster as dg
import requests

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None
    _HAS_STATE_BACKED = False

_POLYTOMIC_BASE_URL = "https://app.polytomic.com/api"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sanitize_name(name: str) -> str:
    """Convert a Polytomic sync name to a valid Dagster asset key segment."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()


def _polytomic_headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}"}


def _list_syncs(api_key: str) -> list[dict]:
    """Call GET /api/syncs and return the raw list of sync dicts."""
    resp = requests.get(
        f"{_POLYTOMIC_BASE_URL}/syncs",
        headers=_polytomic_headers(api_key),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def _merge_spec(base: dg.AssetSpec, ov: dict) -> dg.AssetSpec:
    """Merge an override dict into a base AssetSpec."""
    extra_deps = [dg.AssetKey.from_user_string(d) for d in ov.get("deps", [])]
    return dg.AssetSpec(
        key=dg.AssetKey.from_user_string(ov["key"]) if "key" in ov else base.key,
        description=ov.get("description", base.description),
        group_name=ov.get("group_name", base.group_name),
        metadata={**(base.metadata or {}), **(ov.get("metadata") or {})},
        tags={**(base.tags or {}), **(ov.get("tags") or {})},
        kinds=set(ov["kinds"]) if "kinds" in ov else base.kinds,
        deps=list(base.deps or []) + extra_deps,
    )


def _apply_item_overrides(
    default_spec: dg.AssetSpec,
    item_name: str,
    overrides: Optional[dict],
) -> list[dg.AssetSpec]:
    """Apply assets_by_X overrides. Returns list (usually 1, but >1 if one item → multiple assets)."""
    if not overrides or item_name not in overrides:
        return [default_spec]
    ov = overrides[item_name]
    if isinstance(ov, list):
        return [_merge_spec(default_spec, o) for o in ov]
    return [_merge_spec(default_spec, ov)]


# ── Core defs builder ─────────────────────────────────────────────────────────

def _build_polytomic_defs(
    syncs: list[dict],
    api_key_env_var: str,
    group_name: str,
    key_prefix: Optional[str],
    wait_for_completion: bool,
    poll_interval_seconds: int,
    sync_timeout_seconds: int,
    assets_by_sync_name: Optional[dict] = None,
) -> dg.Definitions:
    """Build Definitions from a cached list of Polytomic sync dicts (no network calls)."""
    from dagster import AssetExecutionContext

    assets = []

    for sync in syncs:
        sync_id: str = sync["id"]
        name: str = sync["name"]
        mode: str = sync.get("mode", "unknown")
        target_connection: str = sync.get("target_connection", "")
        target_object: str = sync.get("target_object", "")

        sanitized = _sanitize_name(name)
        if key_prefix:
            asset_key = dg.AssetKey([key_prefix, sanitized])
        else:
            asset_key = dg.AssetKey([sanitized])

        description = (
            f"Polytomic sync: {name} \u2192 {target_connection}/{target_object} ({mode})"
        )

        # Capture loop variables for the closure
        _sync_id = sync_id
        _name = name
        _mode = mode
        _target_connection = target_connection
        _target_object = target_object

        default_spec = dg.AssetSpec(
            key=asset_key,
            description=description,
            group_name=group_name,
            kinds={"polytomic"},
            metadata={
                "polytomic/sync_id": dg.MetadataValue.text(_sync_id),
                "polytomic/mode": dg.MetadataValue.text(_mode),
                "polytomic/target_connection": dg.MetadataValue.text(_target_connection),
                "polytomic/target_object": dg.MetadataValue.text(_target_object),
            },
        )
        expanded_specs = _apply_item_overrides(default_spec, name, assets_by_sync_name)

        if len(expanded_specs) == 1:
            spec = expanded_specs[0]

            @dg.asset(
                key=spec.key,
                group_name=spec.group_name,
                description=spec.description,
                metadata=dict(spec.metadata or {}),
                tags=dict(spec.tags or {}),
                kinds=spec.kinds or {"polytomic"},
                deps=list(spec.deps or []),
            )
            def _asset_fn(
                context: AssetExecutionContext,
                *,
                __sync_id=_sync_id,
                __name=_name,
                __api_key_env_var=api_key_env_var,
                __wait=wait_for_completion,
                __poll=poll_interval_seconds,
                __timeout=sync_timeout_seconds,
            ) -> dg.MaterializeResult:
                api_key = os.environ[__api_key_env_var]
                headers = _polytomic_headers(api_key)

                context.log.info(f"Starting Polytomic sync '{__name}' (id={__sync_id})")
                start_resp = requests.post(
                    f"{_POLYTOMIC_BASE_URL}/syncs/{__sync_id}/executions",
                    headers=headers,
                    timeout=30,
                )
                start_resp.raise_for_status()
                sync_run_id: str = start_resp.json()["data"]["sync_run_id"]
                context.log.info(f"Polytomic sync run started: {sync_run_id}")

                if not __wait:
                    return dg.MaterializeResult(
                        metadata={
                            "sync_id": dg.MetadataValue.text(__sync_id),
                            "sync_run_id": dg.MetadataValue.text(sync_run_id),
                            "status": dg.MetadataValue.text("triggered"),
                            "records_processed": dg.MetadataValue.int(0),
                        }
                    )

                elapsed = 0
                status = "pending"
                records_processed = 0
                while elapsed < __timeout:
                    time.sleep(__poll)
                    elapsed += __poll
                    status_resp = requests.get(
                        f"{_POLYTOMIC_BASE_URL}/syncs/{__sync_id}/executions/{sync_run_id}",
                        headers=headers,
                        timeout=30,
                    )
                    status_resp.raise_for_status()
                    run_data = status_resp.json().get("data", {})
                    status = run_data.get("status", "unknown")
                    records_processed = run_data.get("records_processed", 0) or 0
                    errors = run_data.get("errors") or []
                    context.log.info(
                        f"Polytomic sync run {sync_run_id} status: {status} "
                        f"(records_processed={records_processed})"
                    )
                    if status == "succeeded":
                        break
                    if status in ("failed", "cancelled", "error"):
                        error_detail = "; ".join(str(e) for e in errors) if errors else status
                        raise Exception(
                            f"Polytomic sync run {sync_run_id} ended with status '{status}': "
                            f"{error_detail}"
                        )

                if elapsed >= __timeout and status not in ("succeeded",):
                    raise Exception(
                        f"Polytomic sync run {sync_run_id} timed out after {__timeout}s "
                        f"(last status: {status})"
                    )

                return dg.MaterializeResult(
                    metadata={
                        "sync_id": dg.MetadataValue.text(__sync_id),
                        "sync_run_id": dg.MetadataValue.text(sync_run_id),
                        "records_processed": dg.MetadataValue.int(records_processed),
                        "status": dg.MetadataValue.text(status),
                    }
                )

            assets.append(_asset_fn)

        else:
            # One sync → multiple Dagster assets
            _expanded_specs = expanded_specs
            _safe_name = sanitized

            @dg.multi_asset(specs=_expanded_specs, name=f"{_safe_name}_multi")
            def _multi_asset_fn(
                context: AssetExecutionContext,
                *,
                __sync_id=_sync_id,
                __name=_name,
                __api_key_env_var=api_key_env_var,
                __wait=wait_for_completion,
                __poll=poll_interval_seconds,
                __timeout=sync_timeout_seconds,
                __specs=_expanded_specs,
            ):
                api_key = os.environ[__api_key_env_var]
                headers = _polytomic_headers(api_key)

                context.log.info(f"Starting Polytomic sync '{__name}' (id={__sync_id})")
                start_resp = requests.post(
                    f"{_POLYTOMIC_BASE_URL}/syncs/{__sync_id}/executions",
                    headers=headers,
                    timeout=30,
                )
                start_resp.raise_for_status()
                sync_run_id: str = start_resp.json()["data"]["sync_run_id"]
                context.log.info(f"Polytomic sync run started: {sync_run_id}")

                if not __wait:
                    for spec in __specs:
                        yield dg.MaterializeResult(
                            asset_key=spec.key,
                            metadata={
                                "sync_id": dg.MetadataValue.text(__sync_id),
                                "sync_run_id": dg.MetadataValue.text(sync_run_id),
                                "status": dg.MetadataValue.text("triggered"),
                                "records_processed": dg.MetadataValue.int(0),
                            },
                        )
                    return

                elapsed = 0
                status = "pending"
                records_processed = 0
                while elapsed < __timeout:
                    time.sleep(__poll)
                    elapsed += __poll
                    status_resp = requests.get(
                        f"{_POLYTOMIC_BASE_URL}/syncs/{__sync_id}/executions/{sync_run_id}",
                        headers=headers,
                        timeout=30,
                    )
                    status_resp.raise_for_status()
                    run_data = status_resp.json().get("data", {})
                    status = run_data.get("status", "unknown")
                    records_processed = run_data.get("records_processed", 0) or 0
                    errors = run_data.get("errors") or []
                    context.log.info(
                        f"Polytomic sync run {sync_run_id} status: {status} "
                        f"(records_processed={records_processed})"
                    )
                    if status == "succeeded":
                        break
                    if status in ("failed", "cancelled", "error"):
                        error_detail = "; ".join(str(e) for e in errors) if errors else status
                        raise Exception(
                            f"Polytomic sync run {sync_run_id} ended with status '{status}': "
                            f"{error_detail}"
                        )

                if elapsed >= __timeout and status not in ("succeeded",):
                    raise Exception(
                        f"Polytomic sync run {sync_run_id} timed out after {__timeout}s "
                        f"(last status: {status})"
                    )

                for spec in __specs:
                    yield dg.MaterializeResult(
                        asset_key=spec.key,
                        metadata={
                            "sync_id": dg.MetadataValue.text(__sync_id),
                            "sync_run_id": dg.MetadataValue.text(sync_run_id),
                            "records_processed": dg.MetadataValue.int(records_processed),
                            "status": dg.MetadataValue.text(status),
                        },
                    )

            assets.append(_multi_asset_fn)

    if not assets:
        return dg.Definitions()

    return dg.Definitions(assets=assets)


# ── Component ─────────────────────────────────────────────────────────────────

if _HAS_STATE_BACKED:
    @dataclass
    class PolytomicAssetComponent(StateBackedComponent, dg.Resolvable):
        """Polytomic reverse-ETL component — one Dagster asset per Polytomic sync.

        Uses StateBackedComponent to cache the sync list from the Polytomic API so
        code-server reloads require no network calls. Populate or refresh the cache
        with:
          dagster dev                      (automatic in dev)
          dg utils refresh-defs-state      (CI/CD or image build)

        At execution time each asset triggers the Polytomic sync via
        POST /api/syncs/{id}/executions and (by default) polls until the run
        completes.

        Example:
            ```yaml
            type: dagster_component_templates.PolytomicAssetComponent
            attributes:
              api_key_env_var: POLYTOMIC_API_KEY
              group_name: reverse_etl
              wait_for_completion: true
              poll_interval_seconds: 15
            ```
        """

        api_key_env_var: str = dg.Field(
            description="Name of the environment variable containing the Polytomic API key."
        )
        organization_id: Optional[str] = dg.Field(
            default=None,
            description="Optional: filter syncs to a specific Polytomic organization ID.",
        )
        name_filter: Optional[str] = dg.Field(
            default=None,
            description="Optional substring filter applied to sync names.",
        )
        exclude_sync_ids: Optional[list[str]] = dg.Field(
            default=None,
            description="Optional list of Polytomic sync IDs to exclude.",
        )
        group_name: str = dg.Field(
            default="polytomic",
            description="Dagster asset group name assigned to all generated assets.",
        )
        key_prefix: Optional[str] = dg.Field(
            default=None,
            description="Optional prefix prepended to every generated asset key.",
        )
        wait_for_completion: bool = dg.Field(
            default=True,
            description="When True, each asset polls until the sync run completes.",
        )
        poll_interval_seconds: int = dg.Field(
            default=15,
            description="Seconds between status-poll requests while waiting for completion.",
        )
        sync_timeout_seconds: int = dg.Field(
            default=7200,
            description="Maximum seconds to wait before raising a timeout error.",
        )
        assets_by_sync_name: Optional[dict] = dg.Field(
            default=None,
            description="Override AssetSpec per sync name. Value can be a single override dict or a list of dicts (one sync → multiple assets).",
        )
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=ResolvedDefsStateConfig
        )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"PolytomicAssetComponent[{self.api_key_env_var}]",
            )

        def write_state_to_path(self, state_path: Path) -> None:
            """Fetch all Polytomic syncs via the API and cache them to disk."""
            api_key = os.environ[self.api_key_env_var]
            all_syncs = _list_syncs(api_key)

            # Normalise each sync to the fields we actually need
            records = []
            for s in all_syncs:
                target = s.get("target") or {}
                records.append({
                    "id": s["id"],
                    "name": s.get("name", s["id"]),
                    "mode": s.get("mode", "unknown"),
                    "target_connection": target.get("connection_name", ""),
                    "target_object": target.get("object", ""),
                    "status": s.get("status", ""),
                })

            # Apply optional filters
            if self.organization_id:
                records = [r for r in records if s.get("organization_id") == self.organization_id]
            if self.name_filter:
                records = [r for r in records if self.name_filter in r["name"]]
            if self.exclude_sync_ids:
                excluded = set(self.exclude_sync_ids)
                records = [r for r in records if r["id"] not in excluded]

            state_path.write_text(json.dumps(records, indent=2))

        def build_defs_from_state(
            self,
            context: dg.ComponentLoadContext,
            state_path: Optional[Path],
        ) -> dg.Definitions:
            """Build asset specs from the cached sync list — no network calls."""
            if state_path is None or not state_path.exists():
                if hasattr(context, "log"):
                    context.log.warning(  # type: ignore[union-attr]
                        "PolytomicAssetComponent: no cached state found. "
                        "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                    )
                return dg.Definitions()

            syncs = json.loads(state_path.read_text())
            return _build_polytomic_defs(
                syncs=syncs,
                api_key_env_var=self.api_key_env_var,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                wait_for_completion=self.wait_for_completion,
                poll_interval_seconds=self.poll_interval_seconds,
                sync_timeout_seconds=self.sync_timeout_seconds,
                assets_by_sync_name=self.assets_by_sync_name,
            )

else:
    # Fallback: StateBackedComponent is not available in this Dagster version.
    # Falls back to calling the Polytomic API on every build_defs (no caching).
    class PolytomicAssetComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Polytomic reverse-ETL component (fallback — no state caching).

        Upgrade to dagster>=1.8 to get StateBackedComponent caching.
        """
        api_key_env_var: str = dg.Field(
            description="Name of the environment variable containing the Polytomic API key."
        )
        organization_id: Optional[str] = dg.Field(default=None)
        name_filter: Optional[str] = dg.Field(default=None)
        exclude_sync_ids: Optional[list[str]] = dg.Field(default=None)
        group_name: str = dg.Field(default="polytomic")
        key_prefix: Optional[str] = dg.Field(default=None)
        wait_for_completion: bool = dg.Field(default=True)
        poll_interval_seconds: int = dg.Field(default=15)
        sync_timeout_seconds: int = dg.Field(default=7200)
        assets_by_sync_name: Optional[dict] = dg.Field(default=None)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            api_key = os.environ[self.api_key_env_var]
            all_syncs = _list_syncs(api_key)

            records = []
            for s in all_syncs:
                target = s.get("target") or {}
                records.append({
                    "id": s["id"],
                    "name": s.get("name", s["id"]),
                    "mode": s.get("mode", "unknown"),
                    "target_connection": target.get("connection_name", ""),
                    "target_object": target.get("object", ""),
                    "status": s.get("status", ""),
                })

            if self.name_filter:
                records = [r for r in records if self.name_filter in r["name"]]
            if self.exclude_sync_ids:
                excluded = set(self.exclude_sync_ids)
                records = [r for r in records if r["id"] not in excluded]

            return _build_polytomic_defs(
                syncs=records,
                api_key_env_var=self.api_key_env_var,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                wait_for_completion=self.wait_for_completion,
                poll_interval_seconds=self.poll_interval_seconds,
                sync_timeout_seconds=self.sync_timeout_seconds,
                assets_by_sync_name=self.assets_by_sync_name,
            )
