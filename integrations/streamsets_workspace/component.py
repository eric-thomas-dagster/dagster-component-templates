"""StreamSets Workspace Component.

Wraps **StreamSets Control Hub** -- the control plane for StreamSets
DataOps Platform (formerly StreamSets Data Collector/Transformer jobs) --
as a Dagster workspace component. One Dagster asset per configured Job.
With `action: start`, materializing a job's asset starts it and confirms
the result by polling job status -- StreamSets jobs are typically
**continuously-running streaming pipelines, not batch jobs that run to
completion**, so "materialize" here means "ensure this job is running"
(start it, then poll until it reaches ACTIVE), not "run it and wait for
it to complete".

What's independently verified vs. well-corroborated (documented honestly,
not glossed over):

  VERIFIED against IBM's own StreamSets Control Hub docs
  (ibm.com/docs/SSBH9Y, "Enabling Access to the REST APIs"):
    - Every call needs these headers:
        Content-Type: application/json
        X-Requested-By: <any client name>
        X-SS-REST-CALL: true
        X-SS-App-Component-Id: <API credential component ID>
        X-SS-App-Auth-Token: <API credential auth token>
    - API credentials are generated in Control Hub UI (Manage > API
      Credentials); the auth token is shown exactly once at creation and
      cannot be retrieved later -- regenerating keeps the same component ID
      but issues a new token. Max 10 credentials per user.
    - A real verified endpoint: GET /security/rest/v1/currentUser.

  VERIFIED against StreamSets/IBM's own Job Status documentation
  (docs.streamsets.com + ibm.com's Control Hub monitoring-jobs page):
    Job status is one of ACTIVE / INACTIVE / ACTIVATING / DEACTIVATING,
    each with an "... with error" variant surfaced via a `color` field
    (red = needs attention, green = running normally). ACTIVATING and
    DEACTIVATING are pure transition states -- no job actions are valid
    while in either.

  WELL-CORROBORATED BUT NOT FROM AN OFFICIAL REFERENCE this component's
  author could independently verify (multiple independent third-party
  integration write-ups -- an MCP-server implementation guide and a
  SoftwareAG webMethods connector's documented service names -- agree on
  the *shape*, but no literal endpoint-path string could be confirmed
  against an official StreamSets/IBM REST reference page):
    POST {base}/jobrunner/rest/v1/job/{jobId}/start
    POST {base}/jobrunner/rest/v1/job/{jobId}/stop
    GET  {base}/jobrunner/rest/v1/job/{jobId}/currentStatus

  Because of this, every Job Runner call in `StreamSetsTransport` below
  goes through three small methods (`start_job`/`stop_job`/`get_job_status`)
  with the literal path baked into exactly one place each -- if your
  Control Hub install uses a different path (version-specific APIs do
  shift), patch these three methods, not the asset/sensor logic around
  them. There is no independently-confirmed "list all jobs" endpoint, so
  jobs are declared explicitly via `jobs:`, not discovered.

A job's `currentStatus` response is not documented to expose a metrics/
throughput field at any officially-confirmed path, so the optional polling
sensor tracks STATUS CHANGES only (e.g. an externally-stopped job, an
error transition) rather than inventing an unverified metrics call --
documented as a deliberate scope limit, not an oversight.
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


_RUNNING_STATUS = "ACTIVE"
_STOPPED_STATUS = "INACTIVE"
_TRANSITION_STATUSES = {"ACTIVATING", "DEACTIVATING"}


class StreamSetsTransport:
    """The one place HTTP calls to StreamSets Control Hub happen. Swap for a
    fake implementing these methods in tests (or demo mode) instead of
    patching `requests` calls scattered through the component.

    Every Job Runner path (start/stop/currentStatus) lives in exactly one
    spot each -- see the module docstring for why these specific paths are
    well-corroborated rather than independently verified."""

    def __init__(self, base_url: str, component_id: str, auth_token: str):
        self._base = base_url.rstrip("/")
        # Headers verified against IBM's own Control Hub REST API docs.
        self._headers = {
            "Content-Type": "application/json",
            "X-Requested-By": "dagster-community-components",
            "X-SS-REST-CALL": "true",
            "X-SS-App-Component-Id": component_id,
            "X-SS-App-Auth-Token": auth_token,
        }

    def start_job(self, job_id: str) -> None:
        import requests

        resp = requests.post(f"{self._base}/jobrunner/rest/v1/job/{job_id}/start", headers=self._headers, timeout=60)
        resp.raise_for_status()

    def stop_job(self, job_id: str) -> None:
        import requests

        resp = requests.post(f"{self._base}/jobrunner/rest/v1/job/{job_id}/stop", headers=self._headers, timeout=60)
        resp.raise_for_status()

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        import requests

        resp = requests.get(f"{self._base}/jobrunner/rest/v1/job/{job_id}/currentStatus", headers=self._headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else {}

    def wait_for_status(self, job_id: str, target_statuses, poll_interval_seconds: int, timeout_seconds: int) -> str:
        """ACTIVATING/DEACTIVATING are pure transition states (per StreamSets'
        own Job Status docs) -- poll until the job lands on a terminal
        status (ACTIVE, or INACTIVE if start failed) or the timeout expires."""
        deadline = time.time() + timeout_seconds
        status = str(self.get_job_status(job_id).get("status") or "")
        while status not in target_statuses:
            if status and status not in _TRANSITION_STATUSES:
                # Landed on a terminal status that isn't the one we wanted
                # (e.g. INACTIVE after a failed start) -- no point polling further.
                raise RuntimeError(f"StreamSets job {job_id} reached status {status}, expected one of {target_statuses}")
            if time.time() >= deadline:
                raise TimeoutError(f"StreamSets job {job_id} did not reach {target_statuses} within {timeout_seconds}s (last status: {status})")
            time.sleep(poll_interval_seconds)
            status = str(self.get_job_status(job_id).get("status") or "")
        return status


class StreamSetsJobSpec(dg.Model, dg.Resolvable):
    """One StreamSets Control Hub Job wrapped as a Dagster asset."""

    job_id: str = Field(description="StreamSets Control Hub Job ID (Jobs view > job > Job ID column, or the Job Runner API).")
    asset_key: str = Field(description="Dagster asset key for this job.")
    description: Optional[str] = Field(default=None, description="Human-readable description.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group (defaults to the component's group_name).")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys this job depends on.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags, merged onto the component's defaults.")


class StreamSetsWorkspaceComponent(dg.Component, dg.Model, dg.Resolvable):
    """N StreamSets Control Hub Jobs as Dagster assets from one component
    instance, with one shared status-change observation sensor.

    StreamSets jobs are typically continuously-running streaming pipelines
    (not batch jobs that run to completion), so `action: start` means
    "ensure this job is running" (start it, then poll until it reaches
    ACTIVE) -- not "run it and wait for it to complete".

    Example:
        ```yaml
        type: dagster_community_components.StreamSetsWorkspaceComponent
        attributes:
          control_hub_url_env_var: STREAMSETS_CONTROL_HUB_URL   # e.g. https://cloud.streamsets.com
          component_id_env_var: STREAMSETS_COMPONENT_ID
          auth_token_env_var: STREAMSETS_AUTH_TOKEN
          jobs:
            - job_id: 8f6e1f2a-...:org
              asset_key: streamsets/orders_cdc_pipeline
            - job_id: 1a2b3c4d-...:org
              asset_key: streamsets/clickstream_pipeline
          action: start
          polling_sensor: true
        ```
    """

    control_hub_url_env_var: str = Field(
        default="STREAMSETS_CONTROL_HUB_URL",
        description="Env var holding the Control Hub base URL, e.g. https://cloud.streamsets.com (SaaS) or your on-prem Control Hub URL.",
    )
    component_id_env_var: str = Field(
        default="STREAMSETS_COMPONENT_ID",
        description="Env var with the API credential's Component ID (Control Hub > Manage > API Credentials).",
    )
    auth_token_env_var: str = Field(
        default="STREAMSETS_AUTH_TOKEN",
        description="Env var with the API credential's Auth Token. Shown once at credential creation -- regenerate the credential if lost (keeps the same Component ID, issues a new token).",
    )

    jobs: List[StreamSetsJobSpec] = Field(default_factory=list, description="Control Hub Jobs to wrap as Dagster assets, one entry per job.")
    assets_by_job: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Override the asset spec for specific jobs by job_id. Mirrors the official "
            "dagster-databricks `DatabricksWorkspaceComponent.assets_by_task_key` shape.\n\n"
            "Per-job keys (all optional): key, group_name, description, "
            "deps (merged with the job's own deps), metadata (merged), tags (merged), kinds "
            "(replaces), owners (replaces)."
        ),
    )

    action: str = Field(
        default="noop",
        description=(
            "noop (default) = external asset, declare-only, materialize does nothing. "
            "start = materialize starts the job and polls currentStatus until it reaches ACTIVE "
            "(or raises if it instead lands on INACTIVE, meaning the start failed). Since "
            "StreamSets jobs are typically continuous streaming pipelines, this means 'ensure "
            "running', not 'run to completion'."
        ),
    )
    poll_interval_seconds: int = Field(default=5, description="Seconds between status polls after start. Only used when action=start.")
    timeout_seconds: int = Field(default=120, description="Max seconds to wait for a job to reach ACTIVE before failing the run. Only used when action=start.")

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds ONE sensor (not one per job) that polls each job's currentStatus and "
            "emits AssetObservation only when the status changed since the last tick (e.g. a job "
            "stopped externally, or transitioned to an error variant). Status-only, not "
            "throughput/metrics -- no officially-confirmed metrics endpoint exists to poll "
            "(see the module docstring)."
        ),
    )
    sensor_name: str = Field(default="streamsets_workspace_observer", description="Sensor name. Only used when polling_sensor=True.")
    check_interval_seconds: int = Field(default=60, description="Seconds between sensor polls. Only used when polling_sensor=True.")

    require_active: bool = Field(
        default=False,
        description="If true, attach a `job_active` asset_check per job that fails when the job's current status isn't ACTIVE.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group for jobs without their own group_name.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags applied to every job asset (merged with each job's own tags).")
    owners: Optional[List[str]] = Field(default=None, description="Default asset owners for jobs without their own.")

    demo_mode: bool = Field(
        default=False,
        description=(
            "Skip the real Control Hub connection entirely. Start/stop and currentStatus all "
            "return synthetic, deterministic values instead of calling the REST API. Use to "
            "smoke-test the whole shape (assets_by_job, the sensor, the check) with no Control "
            "Hub instance available. `jobs:` is still declared explicitly as usual."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners",), csv=(), tags=("tags",))

    def _apply_asset_overrides(self, job_id: str, base_kwargs: dict) -> dict:
        """Merge per-job overrides from `assets_by_job` into @asset kwargs.
        Mirrors `mirth_connect_workspace._apply_asset_overrides` / `qlik_cloud_workspace`'s copy exactly."""
        if not self.assets_by_job:
            return base_kwargs
        override = self.assets_by_job.get(job_id)
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

    def _demo_status(self, job_id: str, tick: int) -> Dict[str, Any]:
        """Deterministic synthetic status -- seeded on job_id so it's stable
        across ticks except for the occasional simulated blip."""
        seed = abs(hash(job_id)) % 10
        if tick > 0 and tick % (seed + 7) == 0:
            return {"status": "ACTIVATING", "color": "GRAY"}
        return {"status": _RUNNING_STATUS, "color": "GREEN"}

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        _self = self

        def _transport() -> StreamSetsTransport:
            base_url = os.environ.get(_self.control_hub_url_env_var, "")
            component_id = os.environ.get(_self.component_id_env_var, "")
            auth_token = os.environ.get(_self.auth_token_env_var, "")
            if not base_url or not component_id or not auth_token:
                raise ValueError(f"{_self.control_hub_url_env_var}, {_self.component_id_env_var}, and {_self.auth_token_env_var} must all be set.")
            return StreamSetsTransport(base_url, component_id, auth_token)

        assets_list: List[dg.AssetsDefinition] = []
        for job in self.jobs:

            def _make_asset(_job=job):
                base_kwargs: Dict[str, Any] = {
                    "key": dg.AssetKey.from_user_string(_job.asset_key),
                    "group_name": _job.group_name or _self.group_name,
                    "description": _job.description or f"StreamSets Control Hub job {_job.job_id}",
                    "deps": [dg.AssetKey.from_user_string(d) for d in (_job.deps or [])],
                    "owners": _job.owners or _self.owners,
                    "tags": {**(_self.tags or {}), **(_job.tags or {})} or None,
                    "kinds": {"streamsets"},
                }
                kwargs = _self._apply_asset_overrides(_job.job_id, base_kwargs)

                @dg.asset(**kwargs)
                def _job_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
                    if _self.action == "noop":
                        return dg.MaterializeResult(metadata={"streamsets/action": "noop", "streamsets/job_id": _job.job_id})

                    if _self.demo_mode:
                        context.log.info(f"[STREAMSETS DEMO] start {_job.job_id} -> ACTIVE")
                        return dg.MaterializeResult(
                            metadata={"streamsets/job_id": _job.job_id, "streamsets/status": _RUNNING_STATUS, "streamsets/demo_mode": True}
                        )

                    transport = _transport()
                    transport.start_job(_job.job_id)
                    status = transport.wait_for_status(_job.job_id, {_RUNNING_STATUS}, _self.poll_interval_seconds, _self.timeout_seconds)
                    return dg.MaterializeResult(metadata={"streamsets/job_id": _job.job_id, "streamsets/status": status})

                return _job_asset

            assets_list.append(_make_asset())

        sensors_list: List[Any] = []
        if self.polling_sensor and self.jobs:
            jobs_by_id = {j.job_id: j for j in self.jobs}

            @dg.sensor(
                name=self.sensor_name,
                minimum_interval_seconds=self.check_interval_seconds,
                asset_selection=dg.AssetSelection.assets(*(dg.AssetKey.from_user_string(j.asset_key) for j in self.jobs)),
            )
            def _workspace_observer(context: dg.SensorEvaluationContext):
                import json as _json

                cursor_state: Dict[str, Any] = _json.loads(context.cursor) if context.cursor else {}
                last_status: Dict[str, str] = cursor_state.get("statuses", {})
                new_status: Dict[str, str] = dict(last_status)
                events = []

                demo_tick = int(cursor_state.get("tick", 0)) + 1 if _self.demo_mode else None

                for job in jobs_by_id.values():
                    try:
                        payload = _self._demo_status(job.job_id, demo_tick) if _self.demo_mode else _transport().get_job_status(job.job_id)
                    except Exception as e:
                        context.log.error(f"StreamSets currentStatus query failed for job {job.job_id}: {e}")
                        continue

                    status = str(payload.get("status") or "")
                    color = payload.get("color")
                    previous = last_status.get(job.job_id)
                    new_status[job.job_id] = status

                    if previous is None:
                        continue  # first sighting -- record baseline, nothing to diff against yet
                    if status == previous:
                        continue

                    events.append(
                        dg.AssetObservation(
                            asset_key=dg.AssetKey.from_user_string(job.asset_key),
                            metadata={
                                "streamsets/job_id": job.job_id,
                                "streamsets/status": status,
                                "streamsets/previous_status": previous,
                                **({"streamsets/color": color} if color else {}),
                            },
                        )
                    )

                new_cursor_state: Dict[str, Any] = {"statuses": new_status}
                if _self.demo_mode:
                    new_cursor_state["tick"] = demo_tick
                new_cursor = _json.dumps(new_cursor_state)

                if events:
                    return dg.SensorResult(asset_events=events, cursor=new_cursor)
                return dg.SensorResult(cursor=new_cursor, skip_reason="No job status changes since last tick")

            sensors_list.append(_workspace_observer)

        checks_list: List[Any] = []
        if self.require_active:
            for job in self.jobs:

                def _make_active_check(_job=job):
                    @dg.asset_check(asset=dg.AssetKey.from_user_string(_job.asset_key), name="job_active")
                    def _check(context):
                        if _self.demo_mode:
                            return dg.AssetCheckResult(passed=True, description="[STREAMSETS DEMO] status=ACTIVE")
                        status = str(_transport().get_job_status(_job.job_id).get("status") or "")
                        return dg.AssetCheckResult(
                            passed=status == _RUNNING_STATUS,
                            description=f"status={status or 'UNKNOWN'}",
                            metadata={"streamsets/status": status or "UNKNOWN"},
                        )

                    return _check

                checks_list.append(_make_active_check())

        return dg.Definitions(assets=assets_list, sensors=sensors_list, asset_checks=checks_list)
