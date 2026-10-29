"""Qlik Cloud Workspace Component.

Sibling to `qlik_cloud_app_reload` (which stays as-is for the single-app
case): wraps N Qlik Cloud apps as Dagster assets from ONE component
instance, with ONE shared observation sensor, instead of one component
instance (and one sensor) per app.

Gap this closes (real review feedback on `qlik_cloud_app_reload`): a shop
with 30 dashboards needed 30 separate `QlikCloudAppReloadComponent`
instances -- each its own YAML block, each its own polling sensor if
enabled. `QlikCloudWorkspaceComponent` takes one `apps:` list instead.

Same real Qlik Cloud REST API as `qlik_cloud_app_reload` (verified against
https://qlik.dev/apis/rest/reloads/ directly):

  POST {tenant_url}/api/v1/reloads            {"appId": ..., "partial": ...}
  GET  {tenant_url}/api/v1/reloads/{reloadId}  -> status, startTime, endTime, errorMessage
  GET  {tenant_url}/api/v1/reloads?appId=...   -> {data: [...]}  (list, for the observation sensor)

Status enum (confirmed from the same docs): QUEUED, RELOADING, CANCELING
(active) / SUCCEEDED, FAILED, CANCELED, EXCEEDED_LIMIT (terminal).

Transport seam: `QlikCloudTransport` is the one place HTTP calls happen --
`trigger_reload` / `get_reload` / `list_reloads`. Tests (and any future
demo/stand-in mode) swap it for a fake implementing the same three methods
instead of patching `requests` calls scattered through the component.

`assets_by_app` mirrors the official `dagster-databricks`
`DatabricksWorkspaceComponent.assets_by_task_key` shape (same convention
`snowflake_workspace.assets_by_name` already uses in this registry) for
per-app asset-spec overrides (key / group_name / description / deps /
metadata / tags / kinds / owners), keyed by Qlik Cloud app id.
"""
import time
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


_TERMINAL = {"SUCCEEDED", "FAILED", "CANCELED", "EXCEEDED_LIMIT"}
# resourceId tag written on every Dagster-triggered reload, so the shared
# observation sensor can tell its own triggers apart from everyone else's --
# there is no "triggered by Dagster" flag on the real API, this is the only
# knob the real /api/v1/reloads request body exposes for it (resourceType +
# resourceId).
_DAGSTER_RESOURCE_ID = "dagster"


class QlikCloudTransport:
    """The one place HTTP calls to Qlik Cloud happen. Swap for a fake
    implementing these three methods in tests (or a future demo mode) instead
    of patching `requests` calls scattered through the component."""

    def __init__(self, tenant_url: str, api_key: str):
        self._base = tenant_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    def trigger_reload(self, app_id: str, partial: bool) -> Dict[str, Any]:
        import requests

        resp = requests.post(
            f"{self._base}/api/v1/reloads",
            headers=self._headers,
            json={"appId": app_id, "partial": partial, "resourceType": "api", "resourceId": _DAGSTER_RESOURCE_ID},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def get_reload(self, reload_id: str) -> Dict[str, Any]:
        import requests

        resp = requests.get(f"{self._base}/api/v1/reloads/{reload_id}", headers=self._headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def list_reloads(self, app_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        import requests

        resp = requests.get(
            f"{self._base}/api/v1/reloads",
            headers=self._headers,
            params={"appId": app_id, "limit": limit, "sort": "-startTime"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])

    def poll_to_terminal(self, reload_id: str, poll_interval_seconds: int, timeout_seconds: int) -> Dict[str, Any]:
        deadline = time.time() + timeout_seconds
        reload_obj = self.get_reload(reload_id)
        while reload_obj.get("status") not in _TERMINAL:
            if time.time() >= deadline:
                raise TimeoutError(
                    f"Qlik Cloud reload {reload_id} did not reach a terminal status within "
                    f"{timeout_seconds}s (last status: {reload_obj.get('status')})"
                )
            time.sleep(poll_interval_seconds)
            reload_obj = self.get_reload(reload_id)
        return reload_obj


class QlikCloudAppSpec(dg.Model, dg.Resolvable):
    """One Qlik Cloud app wrapped as a Dagster asset."""

    app_id: str = Field(description="Qlik Cloud app id to reload.")
    asset_key: str = Field(description="Dagster asset key for this app.")
    description: Optional[str] = Field(default=None, description="Human-readable description.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group (defaults to the component's group_name).")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys this app's reload depends on.")
    partial: bool = Field(default=False, description="Partial reload instead of a full reload, for this app.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags, merged onto the component's defaults.")


class QlikCloudWorkspaceComponent(dg.Component, dg.Model, dg.Resolvable):
    """N Qlik Cloud apps as Dagster assets from one component instance, with
    one shared observation sensor. Use `qlik_cloud_app_reload` instead for a
    single app with no desire to manage a list.

    Example:
        ```yaml
        type: dagster_community_components.QlikCloudWorkspaceComponent
        attributes:
          tenant_url_env_var: QLIK_CLOUD_TENANT_URL
          api_key_env_var: QLIK_CLOUD_API_KEY
          apps:
            - app_id: 60f1b3e2a1b2c3d4e5f6a7b8
              asset_key: bi/finance_consolidated_app
              deps: [warehouse/finance_fact]
            - app_id: 71a2c4f3b2c3d4e5f6a7b8c9
              asset_key: bi/sales_dashboard_app
              deps: [warehouse/sales_fact]
          polling_sensor: true
        ```
    """

    tenant_url_env_var: str = Field(
        default="QLIK_CLOUD_TENANT_URL",
        description="Env var holding the tenant base URL, e.g. https://my-tenant.us.qlikcloud.com (Qlik Cloud is tenant-scoped -- there is no shared hostname).",
    )
    api_key_env_var: str = Field(
        default="QLIK_CLOUD_API_KEY",
        description="Env var holding a Qlik Cloud API key (Bearer token -- see qlik.dev's 'generate your first API key').",
    )
    apps: List[QlikCloudAppSpec] = Field(
        default_factory=list,
        description="Qlik Cloud apps to wrap as Dagster assets, one entry per dashboard/app.",
    )
    assets_by_app: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Override the asset spec for specific apps by app_id. Mirrors the official "
            "dagster-databricks `DatabricksWorkspaceComponent.assets_by_task_key` shape (same "
            "convention `snowflake_workspace.assets_by_name` uses in this registry).\n\n"
            "Per-app keys (all optional): key, group_name, description, deps (merged with the "
            "app's own deps), metadata (merged), tags (merged), kinds (replaces), owners (replaces)."
        ),
    )
    poll_interval_seconds: int = Field(default=15, description="Seconds between reload-status polls.")
    timeout_seconds: int = Field(default=1800, description="Max seconds to wait for a reload to reach a terminal status before failing the run.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group for apps without their own group_name.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags applied to every app asset (merged with each app's own tags).")
    owners: Optional[List[str]] = Field(default=None, description="Default asset owners for apps without their own.")

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds ONE sensor (not one per app) that polls GET /api/v1/reloads?appId=... "
            "for every declared app, for reloads this component did NOT trigger (e.g. a scheduled "
            "reload configured in Qlik Cloud itself, or someone reloading from the Qlik hub), and "
            "emits AssetObservation for each one -- distinguished from this component's own "
            "triggers via the resourceId tag ('dagster') set on every reload it starts."
        ),
    )
    sensor_name: str = Field(default="qlik_cloud_workspace_observer", description="Sensor name. Only used when polling_sensor=True.")
    check_interval_seconds: int = Field(default=300, description="Seconds between sensor polls. Only used when polling_sensor=True.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), csv=(), tags=('tags',))

    def _apply_asset_overrides(self, app_id: str, base_kwargs: dict) -> dict:
        """Merge per-app overrides from `assets_by_app` into @asset kwargs.
        Mirrors `snowflake_workspace._apply_asset_overrides` exactly."""
        if not self.assets_by_app:
            return base_kwargs
        override = self.assets_by_app.get(app_id)
        if not override:
            return base_kwargs
        result = dict(base_kwargs)
        if "key" in override:
            result.pop("key", None)
            result["key"] = dg.AssetKey.from_user_string(str(override["key"]))
        if "group_name" in override:
            result["group_name"] = override["group_name"]
        if "description" in override:
            result["description"] = override["description"]
        if "deps" in override:
            existing = list(result.get("deps") or [])
            existing.extend(dg.AssetKey.from_user_string(d) for d in override["deps"])
            result["deps"] = existing
        if "metadata" in override:
            existing = dict(result.get("metadata") or {})
            existing.update(override["metadata"])
            result["metadata"] = existing
        if "tags" in override:
            existing = dict(result.get("tags") or {})
            existing.update(override["tags"])
            result["tags"] = existing
        if "kinds" in override:
            result["kinds"] = set(override["kinds"])
        if "owners" in override:
            result["owners"] = override["owners"]
        return result

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        _self = self

        def _transport() -> QlikCloudTransport:
            tenant_url = os.environ.get(_self.tenant_url_env_var, "")
            api_key = os.environ.get(_self.api_key_env_var, "")
            if not tenant_url or not api_key:
                raise ValueError(f"{_self.tenant_url_env_var} and {_self.api_key_env_var} must both be set.")
            return QlikCloudTransport(tenant_url, api_key)

        assets_list: List[dg.AssetsDefinition] = []
        for app in self.apps:
            def _make_asset(_app=app):
                base_kwargs: Dict[str, Any] = {
                    "key": dg.AssetKey.from_user_string(_app.asset_key),
                    "group_name": _app.group_name or _self.group_name,
                    "description": _app.description or f"Qlik Cloud app {_app.app_id}",
                    "deps": [dg.AssetKey.from_user_string(d) for d in (_app.deps or [])],
                    "owners": _app.owners or _self.owners,
                    "tags": {**(_self.tags or {}), **(_app.tags or {})} or None,
                    "kinds": {"qlik", "bi"},
                }
                kwargs = _self._apply_asset_overrides(_app.app_id, base_kwargs)

                @dg.asset(**kwargs)
                def _reload_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
                    transport = _transport()
                    triggered = transport.trigger_reload(_app.app_id, _app.partial)
                    reload_id = triggered["id"]
                    context.log.info(f"Qlik Cloud: triggered reload {reload_id} for app {_app.app_id}")

                    final = transport.poll_to_terminal(reload_id, _self.poll_interval_seconds, _self.timeout_seconds)
                    status = final.get("status")
                    metadata: Dict[str, Any] = {
                        "qlik_reload_id": reload_id,
                        "qlik_app_id": _app.app_id,
                        "status": status,
                    }
                    start, end = final.get("startTime"), final.get("endTime")
                    if start and end:
                        try:
                            from datetime import datetime
                            fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
                            duration = (datetime.strptime(end, fmt) - datetime.strptime(start, fmt)).total_seconds()
                            metadata["duration_seconds"] = duration
                        except Exception:
                            pass

                    if status != "SUCCEEDED":
                        metadata["error_code"] = final.get("errorCode")
                        metadata["error_message"] = final.get("errorMessage")
                        raise RuntimeError(
                            f"Qlik Cloud reload {reload_id} ended with status={status}: "
                            f"{final.get('errorMessage') or 'no error message'}"
                        )

                    return dg.MaterializeResult(metadata=metadata)

                return _reload_asset

            assets_list.append(_make_asset())

        sensors_list: List[Any] = []
        if self.polling_sensor and self.apps:
            apps_by_id = {app.app_id: app for app in self.apps}

            @dg.sensor(
                name=self.sensor_name,
                minimum_interval_seconds=self.check_interval_seconds,
                asset_selection=dg.AssetSelection.assets(*(dg.AssetKey.from_user_string(a.asset_key) for a in self.apps)),
            )
            def _workspace_observer(context: dg.SensorEvaluationContext):
                transport = _transport()
                seen = set((context.cursor or "").split(",")) if context.cursor else set()
                events = []
                new_seen_ids: List[str] = []

                for app in apps_by_id.values():
                    try:
                        reloads = transport.list_reloads(app.app_id, limit=20)
                    except Exception as e:
                        context.log.error(f"Qlik Cloud reload-history query failed for app {app.app_id}: {e}")
                        continue
                    for r in reloads:
                        rid = r.get("id")
                        if not rid or rid in seen:
                            continue
                        new_seen_ids.append(rid)
                        if r.get("status") not in _TERMINAL:
                            continue
                        if r.get("resourceId") == _DAGSTER_RESOURCE_ID:
                            continue  # this component's own trigger, not an external one
                        events.append(dg.AssetObservation(
                            asset_key=dg.AssetKey.from_user_string(app.asset_key),
                            metadata={
                                "qlik_reload_id": rid,
                                "qlik_app_id": app.app_id,
                                "status": r.get("status"),
                                "trigger_type": r.get("type"),
                            },
                        ))

                if not new_seen_ids:
                    return dg.SensorResult(skip_reason="No new reloads across any app since last tick")

                new_cursor = ",".join(sorted((seen | set(new_seen_ids)))[-200:])
                if events:
                    return dg.SensorResult(asset_events=events, cursor=new_cursor)
                return dg.SensorResult(cursor=new_cursor, skip_reason="No externally-triggered reloads since last tick")

            sensors_list.append(_workspace_observer)

        return dg.Definitions(assets=assets_list, sensors=sensors_list)
