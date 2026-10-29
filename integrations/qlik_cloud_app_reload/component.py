"""Qlik Cloud App Reload Component.

Triggers a reload of a Qlik Cloud app via the real Qlik Cloud REST API
(verified against https://qlik.dev/apis/rest/reloads/ directly, not guessed):

  POST {tenant_url}/api/v1/reloads            {"appId": ..., "partial": ...}
  GET  {tenant_url}/api/v1/reloads/{reloadId}  -> status, startTime, endTime, errorMessage

Status enum (confirmed from the same docs): QUEUED, RELOADING, CANCELING
(active) / SUCCEEDED, FAILED, CANCELED, EXCEEDED_LIMIT (terminal).

Qlik Cloud is tenant-scoped (https://<tenant>.<region>.qlikcloud.com) and
auth is a Bearer API key (qlik.dev's own "generate your first API key"
tutorial: Authorization: Bearer <api_key>).

Qlik Cloud is a DIFFERENT product from Qlik Compose (qlik_compose_workspace /
qlik_compose_workflow_trigger_job elsewhere in this registry) -- Compose is a
data-warehouse-automation/ETL-generation tool; Qlik Cloud / Qlik Sense is
Qlik's BI/analytics/dashboarding product. This component is the first
registry coverage for Qlik Cloud specifically.
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
# resourceId tag written on every Dagster-triggered reload, so the polling
# sensor can tell its own triggers apart from everyone else's (there is no
# "triggered by Dagster" flag on the real API -- this is the only knob the
# real /api/v1/reloads request body exposes for it: resourceType + resourceId).
_DAGSTER_RESOURCE_ID = "dagster"


def _auth_headers(api_key: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def _trigger_reload(tenant_url: str, api_key: str, app_id: str, partial: bool) -> Dict[str, Any]:
    import requests
    resp = requests.post(
        f"{tenant_url.rstrip('/')}/api/v1/reloads",
        headers=_auth_headers(api_key),
        json={"appId": app_id, "partial": partial, "resourceType": "api", "resourceId": _DAGSTER_RESOURCE_ID},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _get_reload(tenant_url: str, api_key: str, reload_id: str) -> Dict[str, Any]:
    import requests
    resp = requests.get(
        f"{tenant_url.rstrip('/')}/api/v1/reloads/{reload_id}",
        headers=_auth_headers(api_key),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _poll_to_terminal(tenant_url: str, api_key: str, reload_id: str, poll_interval_seconds: int, timeout_seconds: int) -> Dict[str, Any]:
    deadline = time.time() + timeout_seconds
    reload_obj = _get_reload(tenant_url, api_key, reload_id)
    while reload_obj.get("status") not in _TERMINAL:
        if time.time() >= deadline:
            raise TimeoutError(
                f"Qlik Cloud reload {reload_id} did not reach a terminal status within "
                f"{timeout_seconds}s (last status: {reload_obj.get('status')})"
            )
        time.sleep(poll_interval_seconds)
        reload_obj = _get_reload(tenant_url, api_key, reload_id)
    return reload_obj


class QlikCloudAppReloadComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materializable asset that triggers a Qlik Cloud app reload and polls it to completion.

    Example:
        ```yaml
        type: dagster_community_components.QlikCloudAppReloadComponent
        attributes:
          asset_key: bi/finance_consolidated_app
          app_id: 60f1b3e2a1b2c3d4e5f6a7b8
          tenant_url_env_var: QLIK_CLOUD_TENANT_URL
          api_key_env_var: QLIK_CLOUD_API_KEY
          deps: [warehouse/finance_fact]
        ```
    """

    asset_key: str = Field(description="Dagster asset key for the Qlik Cloud app.")
    app_id: str = Field(description="Qlik Cloud app id to reload.")
    tenant_url_env_var: str = Field(
        default="QLIK_CLOUD_TENANT_URL",
        description="Env var holding the tenant base URL, e.g. https://my-tenant.us.qlikcloud.com (Qlik Cloud is tenant-scoped -- there is no shared hostname).",
    )
    api_key_env_var: str = Field(
        default="QLIK_CLOUD_API_KEY",
        description="Env var holding a Qlik Cloud API key (Bearer token -- see qlik.dev's 'generate your first API key').",
    )
    partial: bool = Field(default=False, description="Partial reload instead of a full reload.")
    poll_interval_seconds: int = Field(default=15, description="Seconds between reload-status polls.")
    timeout_seconds: int = Field(default=1800, description="Max seconds to wait for the reload to reach a terminal status before failing the run.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Human-readable description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys (e.g. the warehouse fact table this app reads).")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds a sensor that polls GET /api/v1/reloads?appId=... for reloads "
            "this component did NOT trigger (e.g. a scheduled reload configured in Qlik Cloud "
            "itself, or someone reloading from the Qlik hub), and emits AssetObservation for "
            "each one -- distinguished from this asset's own triggers via the resourceId tag "
            "('dagster') set on every reload this component starts. Off by default."
        ),
    )
    sensor_name: Optional[str] = Field(default=None, description="Unique sensor name. Defaults to '{asset_key}__reload_observer'. Only used when polling_sensor=True.")
    check_interval_seconds: int = Field(default=300, description="Seconds between sensor polls. Only used when polling_sensor=True.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners'), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        _self = self
        key = dg.AssetKey.from_user_string(self.asset_key)

        @dg.asset(
            key=key,
            group_name=self.group_name,
            description=self.description or f"Qlik Cloud app {self.app_id}",
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            owners=self.owners,
            tags=self.tags,
            kinds={"qlik", "bi"},
        )
        def _reload_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            tenant_url = os.environ.get(_self.tenant_url_env_var, "")
            api_key = os.environ.get(_self.api_key_env_var, "")
            if not tenant_url or not api_key:
                raise ValueError(
                    f"{_self.tenant_url_env_var} and {_self.api_key_env_var} must both be set."
                )

            triggered = _trigger_reload(tenant_url, api_key, _self.app_id, _self.partial)
            reload_id = triggered["id"]
            context.log.info(f"Qlik Cloud: triggered reload {reload_id} for app {_self.app_id}")

            final = _poll_to_terminal(
                tenant_url, api_key, reload_id, _self.poll_interval_seconds, _self.timeout_seconds
            )
            status = final.get("status")
            metadata: Dict[str, Any] = {
                "qlik_reload_id": reload_id,
                "qlik_app_id": _self.app_id,
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

        assets_list = [_reload_asset]
        sensors_list = []

        if self.polling_sensor:
            sensor_name = self.sensor_name or f"{key.to_user_string().replace('/', '_')}__reload_observer"

            @dg.sensor(name=sensor_name, minimum_interval_seconds=self.check_interval_seconds)
            def _reload_observer(context: dg.SensorEvaluationContext):
                import os as _os
                tenant_url = _os.environ.get(_self.tenant_url_env_var, "")
                api_key = _os.environ.get(_self.api_key_env_var, "")
                if not tenant_url or not api_key:
                    return dg.SensorResult(skip_reason=f"{_self.tenant_url_env_var}/{_self.api_key_env_var} not set")

                import requests
                try:
                    resp = requests.get(
                        f"{tenant_url.rstrip('/')}/api/v1/reloads",
                        headers=_auth_headers(api_key),
                        params={"appId": _self.app_id, "limit": 20, "sort": "-startTime"},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    reloads = resp.json().get("data", [])
                except Exception as e:
                    return dg.SensorResult(skip_reason=f"Qlik Cloud reload-history query failed: {e}")

                seen = set((context.cursor or "").split(",")) if context.cursor else set()
                events = []
                new_seen_ids = []
                for r in reloads:
                    rid = r.get("id")
                    if not rid or rid in seen:
                        continue
                    new_seen_ids.append(rid)
                    if r.get("status") not in _TERMINAL:
                        continue
                    if (r.get("resourceId") == _DAGSTER_RESOURCE_ID):
                        continue  # this component's own trigger, not an external one
                    events.append(dg.AssetObservation(
                        asset_key=key,
                        metadata={
                            "qlik_reload_id": rid,
                            "status": r.get("status"),
                            "trigger_type": r.get("type"),
                        },
                    ))

                if not new_seen_ids:
                    return dg.SensorResult(skip_reason="No new reloads since last tick")

                new_cursor = ",".join(sorted((seen | set(new_seen_ids)))[-50:])
                if events:
                    return dg.SensorResult(asset_events=events, cursor=new_cursor)
                return dg.SensorResult(cursor=new_cursor, skip_reason="No externally-triggered reloads since last tick")

            sensors_list.append(_reload_observer)

        return dg.Definitions(assets=assets_list, sensors=sensors_list)
