"""FivetranSyncTriggerJobComponent.

Trigger a Fivetran connector sync via REST API.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


class FivetranSyncTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Fivetran connector sync via REST API."""

    job_name: Optional[str] = Field(default=None, description="Dagster job name. Required unless `asset_name` is set.")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule). Ignored when `asset_name` is set.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Dagster job tags")

    api_key_env: str = Field(default="FIVETRAN_API_KEY")
    api_secret_env: str = Field(default="FIVETRAN_API_SECRET")
    connector_id: str = Field(description="Fivetran connection ID (Fivetran's REST API and UI now call this resource a 'connection'; the field keeps its original name for backward compatibility).")
    force: bool = Field(default=False, description="Sync even if no changes detected")
    wait_for_completion: bool = Field(
        default=False,
        description="Poll the connector's GET endpoint until the sync finishes. Fivetran's API "
        "has no discrete sync/job id -- completion is detected by comparing `succeeded_at`/"
        "`failed_at` timestamps (and `status.sync_state`) against the values recorded just "
        "before triggering, per Fivetran's own documented poll-for-completion pattern.",
    )
    poll_interval_seconds: int = Field(default=15)
    timeout_seconds: int = Field(default=3600)

    asset_name: Optional[str] = Field(
        default=None,
        description="If set, this component emits a real materializable `@dg.asset` (with "
        "`deps`) instead of a plain op-job. `job_name`/`schedule` are ignored when this is set.",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys. Only used when `asset_name` is set.")
    group_name: Optional[str] = Field(default=None, description="Asset group name. Only used when `asset_name` is set.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps',), csv=(), tags=('tags',))

    @model_validator(mode="after")
    def _require_job_name_or_asset_name(self):
        if not self.asset_name and not self.job_name:
            raise ValueError("Set either `job_name` (op-job shape) or `asset_name` (asset shape).")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Fivetran Sync Trigger Job', editable=True)

    def _trigger_and_wait(self, context) -> Dict[str, Any]:
        """Trigger the sync, optionally poll to completion; return a metadata dict.
        Shared by the op-job shape and the asset shape.

        Fivetran's current REST API (fivetran.com/docs/rest-api/api-reference/connections/
        sync-connection) exposes this under `/v1/connections/{connectionId}/sync`, not the
        older `/connectors/.../force` path this component used to call -- confirmed against
        the live docs, which list no `/connectors/.../force`/`/sync` path at all; requesting
        `/connectors/{id}/sync-connector`'s URL redirects to the `/connections/` reference
        page. `force` is a JSON body field on that `/sync` call, not a URL segment.

        Fivetran's API has no sync/job id to poll directly -- the documented pattern
        (fivetran.com/docs/rest-api/how-to/run-manual-sync-and-poll-for-completion) is to
        record `succeeded_at`/`failed_at` before triggering, then poll the connection GET
        endpoint (fivetran.com/docs/rest-api/api-reference/connections/connection-details,
        confirmed `succeeded_at`/`failed_at` live at the top level of `data` and
        `status.sync_state` nested under `status`) until one of them changes."""
        import requests, time
        auth = (_require_env(self.api_key_env), _require_env(self.api_secret_env))
        base_url = f"https://api.fivetran.com/v1/connections/{self.connector_id}"

        baseline_succeeded_at = baseline_failed_at = None
        if self.wait_for_completion:
            gr = requests.get(base_url, auth=auth, timeout=30)
            if gr.status_code >= 300:
                raise Exception(f"fivetran connector lookup failed: {gr.status_code} {gr.text[:200]}")
            data = gr.json().get("data", {})
            baseline_succeeded_at = data.get("succeeded_at")
            baseline_failed_at = data.get("failed_at")

        r = requests.post(f"{base_url}/sync", json={"force": self.force}, auth=auth, timeout=60)
        if r.status_code >= 300:
            raise Exception(f"fivetran trigger failed: {r.status_code} {r.text[:200]}")
        context.log.info(f"triggered {self.connector_id}")

        status = "triggered"
        if self.wait_for_completion:
            deadline = time.time() + self.timeout_seconds
            while time.time() < deadline:
                time.sleep(self.poll_interval_seconds)
                gr = requests.get(base_url, auth=auth, timeout=30)
                data = gr.json().get("data", {})
                succeeded_at, failed_at = data.get("succeeded_at"), data.get("failed_at")
                sync_state = (data.get("status") or {}).get("sync_state")
                context.log.info(f"poll: sync_state={sync_state} succeeded_at={succeeded_at} failed_at={failed_at}")
                if failed_at and failed_at != baseline_failed_at:
                    raise Exception(f"fivetran connector {self.connector_id} sync failed at {failed_at}")
                if succeeded_at and succeeded_at != baseline_succeeded_at:
                    status = "succeeded"
                    break
            else:
                raise Exception(f"timed out waiting for connector {self.connector_id} to finish syncing")

        return {"fivetran/connector_id": self.connector_id, "fivetran/status": status}

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        if self.asset_name:
            @dg.asset(
                key=dg.AssetKey.from_user_string(self.asset_name),
                group_name=self.group_name,
                deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
                description=f"Triggers a Fivetran sync for connector {self.connector_id}.",
            )
            def _fivetran_trigger_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
                metadata = _self._trigger_and_wait(context)
                return dg.MaterializeResult(metadata=metadata)

            return dg.Definitions(assets=[_fivetran_trigger_asset])

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            context.add_output_metadata(_self._trigger_and_wait(context))

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _the_op()

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule, job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
