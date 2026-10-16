"""JDEOrchestrationTriggerJobComponent.

Trigger a JDE Orchestrator orchestration and optionally poll to completion.
"""
import time
from typing import Any, Dict, Optional

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


class JDEOrchestrationTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a JDE orchestration via REST.

    Example:

        ```yaml
        type: dagster_community_components.JDEOrchestrationTriggerJobComponent
        attributes:
          job_name: nightly_ar_reconciliation
          orchestration: JDE_AR_Recon
          inputs:
            AsOfDate: "2026-07-10"
            Currency: USD
          resource_key: jde_orchestrator_resource
          wait_for_completion: true
          timeout_seconds: 1800
        ```
    """

    job_name: str = Field(description="Dagster job name.")
    orchestration: str = Field(description="Orchestration name as defined in Orchestrator Studio.")
    inputs: Optional[Dict[str, Any]] = Field(default=None, description="Input parameter overrides for the orchestration.")
    async_mode: bool = Field(default=False, description="Submit async (get a jobId, poll for completion). Set true for long-running orchestrations.")
    wait_for_completion: bool = Field(default=True)
    poll_interval_seconds: int = Field(default=15)
    timeout_seconds: int = Field(default=1800)

    resource_key: str = Field(default="jde_orchestrator_resource")
    schedule: Optional[str] = Field(default=None)
    default_status: str = Field(default="STOPPED")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}

        @dg.op(name=f"{self.job_name}_op", required_resource_keys=required_resource_keys)
        def _the_op(context: dg.OpExecutionContext):
            self = _self
            try:
                import requests
            except ImportError as e:
                raise Exception("requests library not installed") from e

            resource = getattr(context.resources, self.resource_key)
            session = requests.Session()
            session.verify = resource.verify_ssl
            headers = resource.get_auth_headers()

            url = resource.orchestration_url(self.orchestration)
            body: dict = self.inputs.copy() if self.inputs else {}
            if self.async_mode:
                url = f"{url}?asynchronous=true"

            r = session.post(url, json=body, headers=headers, timeout=self.timeout_seconds)
            if r.status_code >= 400:
                raise Exception(
                    f"JDE orchestration failed: {r.status_code} {r.text[:200]} (orchestration={self.orchestration})"
                )
            context.log.info(f"JDE: {self.orchestration} submitted (status={r.status_code})")

            if not (self.async_mode and self.wait_for_completion):
                # Sync mode already returned the result.
                if self.async_mode is False:
                    return

            # Async polling.
            try:
                body_out = r.json() or {}
                job_id = body_out.get("jobId") or body_out.get("id")
            except ValueError:
                job_id = None
            if not job_id:
                context.log.warning("Async submission returned no jobId — cannot poll.")
                return

            deadline = time.time() + self.timeout_seconds
            terminal = {"SUCCESS", "COMPLETED", "FAILED", "ERROR", "CANCELED"}
            last_state = None
            while time.time() < deadline:
                time.sleep(self.poll_interval_seconds)
                sr = session.get(resource.status_url(job_id), headers=headers, timeout=30)
                if sr.status_code >= 300:
                    continue
                sbody = sr.json() or {}
                state = (sbody.get("status") or sbody.get("state") or "").upper()
                if state and state != last_state:
                    context.log.info(f"orchestration state: {state}")
                    last_state = state
                if state and state in terminal:
                    if state in ("FAILED", "ERROR", "CANCELED"):
                        raise Exception(f"JDE orchestration ended in {state} (orchestration={self.orchestration}, jobId={job_id})")
                    return
            raise Exception(f"JDE orchestration did not reach terminal state within {self.timeout_seconds}s (jobId={job_id})")

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _the_op()

        defs_kwargs: dict = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
