"""VictoriaMetricsSnapshotJobComponent.

Create a VictoriaMetrics snapshot (point-in-time copy) via the
`/snapshot/create` HTTP endpoint, with optional retention-based cleanup
of older snapshots via `/snapshot/delete`. Snapshots are hardlink-cheap
on disk and are the canonical way to back up VM single-node before a
destructive op or scheduled offsite copy.

Endpoints used:
  - POST {base_url}/snapshot/create        → {"status":"ok","snapshot":"<id>"}
  - GET  {base_url}/snapshot/list          → {"status":"ok","snapshots":[...]}
  - POST {base_url}/snapshot/delete        → {"status":"ok"} (per-id)

Snapshot IDs are timestamp-prefixed strings like
``20260528T100000Z-AB1234567890ABCD`` so retention math is exact-string,
no clock-skew between Dagster and VM.

Docs:
  https://docs.victoriametrics.com/single-server-victoriametrics/#how-to-work-with-snapshots

Note: snapshot endpoints are gated behind `-snapshotAuthKey` on the VM
server. The component accepts an optional ``snapshot_auth_key_env_var``
that supplies the key as a query parameter.
"""
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import dagster as dg
from pydantic import Field


# Snapshot IDs look like 20210123T173400Z-AB1234567890ABCD (vmstorage format,
# UTC, T-separated) or 20260528100000-AB1234567890ABCD (older vmsingle format).
# Accept both shapes — pull the YYYYMMDD(T?)HHMMSS prefix.
_SNAPSHOT_TS_RE = re.compile(r"^(\d{8})T?(\d{6})Z?-")


def _parse_snapshot_age(snapshot_id: str) -> Optional[datetime]:
    m = _SNAPSHOT_TS_RE.match(snapshot_id)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


class VictoriaMetricsSnapshotJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run VictoriaMetrics `/snapshot/create` as a Dagster job, optionally with retention cleanup.

    Example (daily snapshot, keep 7 days):

        ```yaml
        type: dagster_community_components.VictoriaMetricsSnapshotJobComponent
        attributes:
          job_name: vm_daily_snapshot
          schedule: "0 3 * * *"
          base_url_env_var: VM_BASE_URL
          retention_hours: 168          # 7 days
          group_name: victoriametrics
        ```
    """

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    base_url_env_var: str = Field(
        default="VM_BASE_URL",
        description="Env var with VictoriaMetrics base URL (e.g. http://localhost:8428).",
    )
    snapshot_auth_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var with VM `-snapshotAuthKey` value (sent as ?authKey=…). Required when the VM server is started with -snapshotAuthKey.",
    )
    bearer_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var with bearer token (for vmauth-fronted clusters).",
    )

    retention_hours: Optional[int] = Field(
        default=None,
        description="If set, after creating the new snapshot, delete any existing snapshots older than this many hours. Leave None to never auto-delete.",
    )

    request_timeout_seconds: int = Field(default=60, ge=1)
    group_name: Optional[str] = Field(default="victoriametrics", description="Asset/op group name (op naming only — this is a job).")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _snapshot_op(context: dg.OpExecutionContext):
            try:
                import requests
            except ImportError:
                raise ImportError(
                    "victoriametrics_snapshot_job requires `requests`; "
                    "install it into the project (uv add requests)."
                )

            base_url = os.environ.get(_self.base_url_env_var, "")
            if not base_url:
                raise RuntimeError(
                    f"Set {_self.base_url_env_var} to the VictoriaMetrics base URL "
                    "(e.g. http://localhost:8428)."
                )
            base_url = base_url.rstrip("/")

            params: dict = {}
            if _self.snapshot_auth_key_env_var and os.environ.get(_self.snapshot_auth_key_env_var):
                params["authKey"] = os.environ[_self.snapshot_auth_key_env_var]
            headers: dict = {}
            if _self.bearer_token_env_var and os.environ.get(_self.bearer_token_env_var):
                headers["Authorization"] = f"Bearer {os.environ[_self.bearer_token_env_var]}"

            # 1) Create the new snapshot.
            create_resp = requests.post(
                f"{base_url}/snapshot/create",
                params=params,
                headers=headers,
                timeout=_self.request_timeout_seconds,
            )
            create_resp.raise_for_status()
            body = create_resp.json()
            if body.get("status") != "ok":
                raise RuntimeError(f"snapshot/create returned non-OK: {body}")
            new_snapshot = body.get("snapshot")
            context.log.info(f"VM snapshot created: {new_snapshot}")

            deleted: list = []
            kept: list = []
            # 2) Retention sweep (optional).
            if _self.retention_hours is not None:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=_self.retention_hours)
                list_resp = requests.get(
                    f"{base_url}/snapshot/list",
                    params=params,
                    headers=headers,
                    timeout=_self.request_timeout_seconds,
                )
                list_resp.raise_for_status()
                existing = list_resp.json().get("snapshots", [])
                for snap in existing:
                    if snap == new_snapshot:
                        kept.append(snap)
                        continue
                    snap_dt = _parse_snapshot_age(snap)
                    if snap_dt is None:
                        # Unparseable — be conservative, keep it.
                        kept.append(snap)
                        continue
                    if snap_dt < cutoff:
                        del_resp = requests.post(
                            f"{base_url}/snapshot/delete",
                            params={**params, "snapshot": snap},
                            headers=headers,
                            timeout=_self.request_timeout_seconds,
                        )
                        if del_resp.ok and del_resp.json().get("status") == "ok":
                            deleted.append(snap)
                            context.log.info(f"VM snapshot deleted (retention): {snap}")
                        else:
                            context.log.warning(
                                f"VM snapshot delete failed for {snap}: {del_resp.status_code} {del_resp.text[:200]}"
                            )
                    else:
                        kept.append(snap)

            context.add_output_metadata({
                "vm/snapshot_id": new_snapshot,
                "vm/snapshots_deleted": len(deleted),
                "vm/snapshots_kept": len(kept),
                "vm/deleted_ids": deleted,
            })

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _snapshot_op()

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.RUNNING
                    if self.default_status.upper() == "RUNNING"
                    else dg.DefaultScheduleStatus.STOPPED
                ),
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
