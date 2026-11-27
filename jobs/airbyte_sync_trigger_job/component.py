"""AirbyteSyncTriggerJobComponent.

Trigger an Airbyte connection sync via REST API — fire-and-forget or wait for completion.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field



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


class AirbyteSyncTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger an Airbyte connection sync via REST API — fire-and-forget or wait for completion."""

    job_name: Optional[str] = Field(default=None, description="Dagster job name. Required unless `asset_name` is set.")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule). Ignored when `asset_name` is set.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Dagster job tags")

    airbyte_url: str = Field(description="Airbyte server URL (e.g. 'https://airbyte.acme.com')")
    connection_id: str = Field(description="Connection UUID to sync")
    username_env: Optional[str] = Field(default="AIRBYTE_USER")
    password_env: Optional[str] = Field(default="AIRBYTE_PASSWORD")
    wait_for_completion: bool = Field(default=False)
    poll_interval_seconds: int = Field(default=15)
    timeout_seconds: int = Field(default=3600)

    asset_name: Optional[str] = Field(
        default=None,
        description="If set, this component emits a real materializable `@dg.asset` (with "
        "`deps`) instead of a plain op-job, returning the real Airbyte job id in metadata. "
        "`job_name`/`schedule` are ignored when this is set. For connections managed by a "
        "declared Airbyte workspace, `integrations/airbyte_assets` (the official "
        "`dagster_airbyte` wrapper) is usually the better fit -- this component is for "
        "triggering a self-hosted/legacy Airbyte instance directly via its basic-auth "
        "`/api/v1/` REST endpoints, which is a different auth/API surface.",
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
        return ComponentFormConfig(label='Airbyte Sync Trigger Job', editable=True)

    def _sync_and_wait(self, context) -> Dict[str, Any]:
        """Trigger the sync, optionally poll to completion; return a metadata dict.
        Shared by the op-job shape and the asset shape."""
        import requests, time
        auth = (_require_env(self.username_env), _require_env(self.password_env)) if self.username_env else None
        r = requests.post(f"{self.airbyte_url}/api/v1/connections/sync",
                          json={"connectionId": self.connection_id}, auth=auth, timeout=60)
        if r.status_code >= 300:
            raise Exception(f"airbyte sync failed: {r.status_code} {r.text[:200]}")
        job_id = r.json().get("job", {}).get("id")
        context.log.info(f"started job {job_id}")
        status = "started"
        if self.wait_for_completion and job_id:
            deadline = time.time() + self.timeout_seconds
            while time.time() < deadline:
                jr = requests.post(f"{self.airbyte_url}/api/v1/jobs/get",
                                   json={"id": job_id}, auth=auth, timeout=30)
                status = jr.json().get("job", {}).get("status")
                context.log.info(f"poll: status={status}")
                if status in ("succeeded", "failed", "cancelled"):
                    if status != "succeeded":
                        raise Exception(f"airbyte job {job_id} {status}")
                    break
                time.sleep(self.poll_interval_seconds)
            else:
                raise Exception(f"timed out waiting for {job_id}")
        return {"airbyte/job_id": job_id, "airbyte/connection_id": self.connection_id, "airbyte/status": status}

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        if self.asset_name:
            @dg.asset(
                key=dg.AssetKey.from_user_string(self.asset_name),
                group_name=self.group_name,
                deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
                description=f"Triggers an Airbyte sync for connection {self.connection_id}.",
            )
            def _airbyte_trigger_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
                metadata = _self._sync_and_wait(context)
                return dg.MaterializeResult(metadata=metadata)

            return dg.Definitions(assets=[_airbyte_trigger_asset])

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            context.add_output_metadata(_self._sync_and_wait(context))

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
