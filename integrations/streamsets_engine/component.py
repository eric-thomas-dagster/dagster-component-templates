"""StreamSets Engine Component.

Wraps a STANDALONE StreamSets **Data Collector (SDC) engine** -- the single-
instance product, with NO Control Hub in front of it -- as a Dagster
workspace component. This is a genuinely different product/API from
`StreamSetsWorkspaceComponent` (which wraps Control Hub's Job Runner API):
some StreamSets customers run only bare SDC engine(s), authoring and running
pipelines directly against one engine's own REST API, with no fleet
orchestration layer at all. Use THIS component for that deployment shape;
use `StreamSetsWorkspaceComponent` when Control Hub (SaaS or self-managed
on-prem -- Control Hub itself is self-hostable, see its own docs) is in the
picture. One Dagster asset per configured pipeline.

LIVE-VERIFIED against a real `streamsets/datacollector:6.4.1` Docker
container (not secondhand blog posts) -- this engine's own REST API serves
its real OpenAPI/Swagger spec at `/rest/swagger.json`, which was fetched and
read directly from the running instance:

  CONFIRMED LIVE (both the endpoint paths AND actual request/response
  behavior, by hitting the real running container):
    - Auth: HTTP Basic (`security: [{"basic": []}]` in the live spec) with
      credentials from this engine's realm file (`/etc/sdc/basic-realm.properties`
      in the Docker image). `X-Requested-By: <any client name>` is the
      documented convention but was NOT actually enforced by this version
      for GET calls when tested live without it (still returned 200) --
      sent anyway below for safety/version-compatibility, since POST/PUT
      calls may enforce it even where GET didn't.
    - GET  /rest/v1/pipelines              -- list ALL pipelines. REAL,
      working, live-confirmed (returned `[]` on a fresh instance) -- a
      genuine discovery endpoint that does NOT exist for Control Hub's Job
      Runner API (see the sibling component's docstring). Lets this
      component optionally auto-discover pipelines instead of only
      declaring them by hand.
    - GET  /rest/v1/system/info             -- real version/build info.
    - GET  /rest/v1/pipeline/{pipelineId}/status  -- confirmed live (200,
      even for a nonexistent pipeline id -- this engine does not 404 that
      path, it just returns an empty body; treat an empty/missing `status`
      key as "not found" rather than relying on the HTTP status code here).
    - The exact pipeline status enum (20 values) and the exact
      `executionMode` enum were read directly from this engine's own
      OpenAPI schema (`PipelineStateJson` in `/rest/swagger.json`), not
      inferred or guessed:
        EDITED, STARTING, STARTING_ERROR, START_ERROR, RUNNING,
        RUNNING_ERROR, RUN_ERROR, FINISHING, FINISHED, RETRY, KILLED,
        STOPPING, STOPPED, STOPPING_ERROR, STOP_ERROR, DISCONNECTING,
        DISCONNECTED, CONNECTING, CONNECT_ERROR, DELETED
      RUNNING is the "started successfully" terminal state this component
      polls for; STOPPED is the clean-stop terminal state. Every other
      *_ERROR value plus KILLED/DELETED is treated as a terminal failure
      state (stop polling, raise) rather than something to keep waiting on.
    - A real per-pipeline metrics endpoint EXISTS and is reachable:
      GET /rest/v1/pipeline/{pipelineId}/metrics -- confirmed live (200,
      empty body for a pipeline with no metrics yet). Its schema
      (`MetricRegistryJson` in the live spec) is a standard Dropwizard
      Metrics registry: `{gauges, counters, histograms, meters, timers}`,
      each a map keyed by metric name (e.g. a stage's
      `stage.<name>.outputRecords.meter`). This is a genuine capability
      Control Hub's sibling component does NOT have (no confirmed metrics
      endpoint there) -- exposed here as best-effort passthrough metadata.
    - Endpoints for start/stop/forceStop/create/import ALSO exist in the
      live spec at the expected paths (POST .../start, .../stop,
      .../forceStop; PUT /rest/v1/pipeline/{title} to create) -- but see
      the honest limitation below.

  HONEST LIMITATION -- could NOT be live-exercised end-to-end, and this is
  a real environmental wall, not a guess:
    Every state-CHANGING call (create a pipeline, start, stop) returned
    HTTP 403 against this Docker image regardless of which realm user/role
    was used (including a dedicated `creator`-role user whose role
    literally appears in the endpoint's own declared "allowed roles" list
    -- ruling out a simple role-mismatch). The container ships
    `streamsets-activation-no-trial-*.jar`, and `GET /rest/v1/activation`
    confirmed `"valid": false` -- this specific Docker image has NO trial
    grace period and requires a real StreamSets-issued activation code
    (requested via StreamSets' Support portal, tied to this instance's
    `sdcId`) before ANY write action is allowed, independent of user role.
    Pure read/GET endpoints (status, metrics, pipelines list, system info)
    were NOT subject to this gate and were fully live-verified above.
    `start_pipeline`/`stop_pipeline`/`wait_for_status`'s request-building
    and response-parsing code is real and exercised by this component's
    test suite against a real local HTTP server shaped like this API (see
    tests/), but the actual "does a real engine transition RUNNING after a
    real start call" behavior was not observed first-hand -- document this
    gap plainly rather than claim more than was verified.
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


_RUNNING_STATUS = "RUNNING"
_STOPPED_STATUS = "STOPPED"
# Pure transition states -- not an error, not done yet, keep polling.
_TRANSITION_STATUSES = {"STARTING", "STOPPING", "FINISHING", "RETRY", "CONNECTING", "DISCONNECTING"}
# Every other status value is terminal (success or failure) -- stop polling.
_ALL_STATUSES = {
    "EDITED", "STARTING", "STARTING_ERROR", "START_ERROR", "RUNNING",
    "RUNNING_ERROR", "RUN_ERROR", "FINISHING", "FINISHED", "RETRY", "KILLED",
    "STOPPING", "STOPPED", "STOPPING_ERROR", "STOP_ERROR", "DISCONNECTING",
    "DISCONNECTED", "CONNECTING", "CONNECT_ERROR", "DELETED",
}


class StreamSetsEngineTransport:
    """The one place HTTP calls to a standalone SDC engine happen. Swap for a
    fake implementing these methods in tests (or demo mode) instead of
    patching `requests` calls scattered through the component.

    Every path here was confirmed live against a real
    `streamsets/datacollector:6.4.1` container's own `/rest/swagger.json` --
    see the module docstring for exactly what was and wasn't exercised
    end-to-end."""

    def __init__(self, base_url: str, username: str, password: str):
        self._base = base_url.rstrip("/")
        self._auth = (username, password)
        self._headers = {"X-Requested-By": "dagster-community-components"}

    def list_pipelines(self) -> List[Dict[str, Any]]:
        import requests

        resp = requests.get(f"{self._base}/rest/v1/pipelines", auth=self._auth, headers=self._headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def start_pipeline(self, pipeline_id: str) -> None:
        import requests

        resp = requests.post(f"{self._base}/rest/v1/pipeline/{pipeline_id}/start", auth=self._auth, headers=self._headers, timeout=60)
        resp.raise_for_status()

    def stop_pipeline(self, pipeline_id: str, force: bool = False) -> None:
        import requests

        action = "forceStop" if force else "stop"
        resp = requests.post(f"{self._base}/rest/v1/pipeline/{pipeline_id}/{action}", auth=self._auth, headers=self._headers, timeout=60)
        resp.raise_for_status()

    def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        import requests

        resp = requests.get(f"{self._base}/rest/v1/pipeline/{pipeline_id}/status", auth=self._auth, headers=self._headers, timeout=30)
        resp.raise_for_status()
        if not resp.content:
            return {}
        data = resp.json()
        return data if isinstance(data, dict) else {}

    def get_pipeline_metrics(self, pipeline_id: str) -> Dict[str, Any]:
        """Dropwizard-style metrics registry: {gauges, counters, histograms,
        meters, timers}, each keyed by metric name -- real schema confirmed
        from the live engine's own MetricRegistryJson definition. Returns {}
        for a pipeline with no metrics yet (e.g. never started) -- the live
        engine returns 200 with an empty body in that case, not 404."""
        import requests

        resp = requests.get(f"{self._base}/rest/v1/pipeline/{pipeline_id}/metrics", auth=self._auth, headers=self._headers, timeout=30)
        resp.raise_for_status()
        if not resp.content:
            return {}
        data = resp.json()
        return data if isinstance(data, dict) else {}

    def wait_for_status(self, pipeline_id: str, target_statuses, poll_interval_seconds: int, timeout_seconds: int) -> str:
        """Poll until the pipeline lands on a terminal status (RUNNING, or a
        *_ERROR/KILLED/STOPPED variant if start/stop failed) or the timeout
        expires. Transition states (STARTING, STOPPING, ...) are not terminal
        -- keep polling through those."""
        deadline = time.time() + timeout_seconds
        status = str(self.get_pipeline_status(pipeline_id).get("status") or "")
        while status not in target_statuses:
            if status and status not in _TRANSITION_STATUSES and status != "EDITED":
                raise RuntimeError(f"StreamSets pipeline {pipeline_id} reached status {status}, expected one of {target_statuses}")
            if time.time() >= deadline:
                raise TimeoutError(f"StreamSets pipeline {pipeline_id} did not reach {target_statuses} within {timeout_seconds}s (last status: {status})")
            time.sleep(poll_interval_seconds)
            status = str(self.get_pipeline_status(pipeline_id).get("status") or "")
        return status


class StreamSetsPipelineSpec(dg.Model, dg.Resolvable):
    """One standalone SDC pipeline wrapped as a Dagster asset."""

    pipeline_id: str = Field(description="StreamSets Data Collector pipeline ID (Pipelines list, or GET /rest/v1/pipelines).")
    asset_key: str = Field(description="Dagster asset key for this pipeline.")
    description: Optional[str] = Field(default=None, description="Human-readable description.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group (defaults to the component's group_name).")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys this pipeline depends on.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags, merged onto the component's defaults.")


class StreamSetsEngineComponent(dg.Component, dg.Model, dg.Resolvable):
    """N standalone StreamSets Data Collector (SDC) engine pipelines as
    Dagster assets from one component instance, with one shared
    status-change observation sensor.

    Different product/API from `StreamSetsWorkspaceComponent` (Control
    Hub's Job Runner API) -- use this one when there is no Control Hub in
    front of the engine(s) at all. See the module docstring for exactly
    what was live-verified against a real engine vs. not.

    Example:
        ```yaml
        type: dagster_community_components.StreamSetsEngineComponent
        attributes:
          base_url_env_var: STREAMSETS_ENGINE_URL   # e.g. http://localhost:18630
          username_env_var: STREAMSETS_USERNAME
          password_env_var: STREAMSETS_PASSWORD
          pipelines:
            - pipeline_id: orders_cdc_pipelineAbCd12
              asset_key: streamsets_engine/orders_cdc_pipeline
          action: start
          polling_sensor: true
          expose_metrics: true
        ```
    """

    base_url_env_var: str = Field(
        default="STREAMSETS_ENGINE_URL",
        description="Env var holding the SDC engine's base URL, e.g. http://localhost:18630 (no Control Hub involved).",
    )
    username_env_var: str = Field(default="STREAMSETS_USERNAME", description="Env var with the engine login username (HTTP Basic -- confirmed live against this engine's own OpenAPI spec).")
    password_env_var: str = Field(default="STREAMSETS_PASSWORD", description="Env var with the engine login password.")

    pipelines: List[StreamSetsPipelineSpec] = Field(default_factory=list, description="SDC pipelines to wrap as Dagster assets, one entry per pipeline. Find pipeline_id via the engine's Pipelines list UI or GET /rest/v1/pipelines (a real, working discovery endpoint on this engine -- unlike Control Hub's Job Runner API).")
    assets_by_pipeline: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Override the asset spec for specific pipelines by pipeline_id. Mirrors "
            "StreamSetsWorkspaceComponent's assets_by_job shape.\n\n"
            "Per-pipeline keys (all optional): key, group_name, description, "
            "deps (merged with the pipeline's own deps), metadata (merged), tags (merged), "
            "kinds (replaces), owners (replaces)."
        ),
    )

    action: str = Field(
        default="noop",
        description=(
            "noop (default) = external asset, declare-only, materialize does nothing. "
            "start = materialize starts the pipeline and polls status until RUNNING (or "
            "raises if it instead lands on a *_ERROR/KILLED status, meaning the start "
            "failed). stop = materialize stops the pipeline and polls until STOPPED."
        ),
    )
    force_stop: bool = Field(default=False, description="When action=stop, use forceStop instead of a graceful stop. Ignored for action=start/noop.")
    poll_interval_seconds: int = Field(default=5, description="Seconds between status polls. Only used when action=start/stop.")
    timeout_seconds: int = Field(default=120, description="Max seconds to wait for the target status before failing the run. Only used when action=start/stop.")

    polling_sensor: bool = Field(
        default=False,
        description="If true, adds ONE sensor (not one per pipeline) that polls each pipeline's status and emits AssetObservation only when the status changed since the last tick.",
    )
    sensor_name: str = Field(default="streamsets_engine_observer", description="Sensor name. Only used when polling_sensor=True.")
    check_interval_seconds: int = Field(default=60, description="Seconds between sensor polls. Only used when polling_sensor=True.")
    observe_metrics_in_sensor: bool = Field(
        default=False,
        description=(
            "If true (only used when polling_sensor=True), the sensor ALSO emits an AssetObservation "
            "carrying a live metrics snapshot (streamsets/metrics, same Dropwizard registry shape as "
            "expose_metrics) on every tick for each pipeline currently RUNNING -- independent of whether "
            "its status changed. Status-change observations (above) only fire when status actually "
            "changes, which would otherwise miss the common case of watching throughput climb while a "
            "pipeline stays RUNNING tick after tick. Fetches GET /rest/v1/pipeline/{id}/metrics (a "
            "read-only call, confirmed live without engine activation) once per RUNNING pipeline per tick."
        ),
    )

    require_running: bool = Field(
        default=False,
        description="If true, attach a `pipeline_running` asset_check per pipeline that fails when the pipeline's current status isn't RUNNING.",
    )

    expose_metrics: bool = Field(
        default=False,
        description=(
            "If true, action=start fetches /rest/v1/pipeline/{id}/metrics after reaching RUNNING "
            "and attaches the raw Dropwizard metrics registry (gauges/counters/histograms/meters/"
            "timers, keyed by metric name) as asset metadata. A real, reachable endpoint on this "
            "engine (confirmed live) -- Control Hub's sibling component has no equivalent. Exact "
            "metric names depend on the pipeline's own stages; this passes them through best-effort "
            "rather than parsing specific counters, since real populated values could not be "
            "observed live (see module docstring's honest limitation)."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group for pipelines without their own group_name.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags applied to every pipeline asset (merged with each pipeline's own tags).")
    owners: Optional[List[str]] = Field(default=None, description="Default asset owners for pipelines without their own.")

    demo_mode: bool = Field(
        default=False,
        description="Skip the real engine connection entirely. Start/stop and status all return synthetic, deterministic values instead of calling the REST API. Use to smoke-test the whole shape with no SDC engine available. `pipelines:` is still declared explicitly as usual.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners",), csv=(), tags=("tags",))

    def _apply_asset_overrides(self, pipeline_id: str, base_kwargs: dict) -> dict:
        """Merge per-pipeline overrides from `assets_by_pipeline` into @asset kwargs.
        Mirrors streamsets_workspace._apply_asset_overrides exactly."""
        if not self.assets_by_pipeline:
            return base_kwargs
        override = self.assets_by_pipeline.get(pipeline_id)
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

    def _demo_status(self, pipeline_id: str, tick: int) -> Dict[str, Any]:
        """Deterministic synthetic status -- seeded on pipeline_id so it's
        stable across ticks except for the occasional simulated blip."""
        seed = abs(hash(pipeline_id)) % 10
        if tick > 0 and tick % (seed + 7) == 0:
            return {"status": "STARTING"}
        return {"status": _RUNNING_STATUS}

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Stream Sets Engine', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        _self = self

        def _transport() -> StreamSetsEngineTransport:
            base_url = os.environ.get(_self.base_url_env_var, "")
            username = os.environ.get(_self.username_env_var, "")
            password = os.environ.get(_self.password_env_var, "")
            if not base_url or not username or not password:
                raise ValueError(f"{_self.base_url_env_var}, {_self.username_env_var}, and {_self.password_env_var} must all be set.")
            return StreamSetsEngineTransport(base_url, username, password)

        assets_list: List[dg.AssetsDefinition] = []
        for pipeline in self.pipelines:

            def _make_asset(_pipeline=pipeline):
                base_kwargs: Dict[str, Any] = {
                    "key": dg.AssetKey.from_user_string(_pipeline.asset_key),
                    "group_name": _pipeline.group_name or _self.group_name,
                    "description": _pipeline.description or f"StreamSets Data Collector pipeline {_pipeline.pipeline_id}",
                    "deps": [dg.AssetKey.from_user_string(d) for d in (_pipeline.deps or [])],
                    "owners": _pipeline.owners or _self.owners,
                    "tags": {**(_self.tags or {}), **(_pipeline.tags or {})} or None,
                    "kinds": {"streamsets"},
                    "metadata": None,
                }
                kwargs = _self._apply_asset_overrides(_pipeline.pipeline_id, base_kwargs)

                @dg.asset(**kwargs)
                def _pipeline_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
                    if _self.action == "noop":
                        return dg.MaterializeResult(metadata={"streamsets/action": "noop", "streamsets/pipeline_id": _pipeline.pipeline_id})

                    if _self.demo_mode:
                        context.log.info(f"[STREAMSETS ENGINE DEMO] {_self.action} {_pipeline.pipeline_id}")
                        target = _STOPPED_STATUS if _self.action == "stop" else _RUNNING_STATUS
                        return dg.MaterializeResult(
                            metadata={"streamsets/pipeline_id": _pipeline.pipeline_id, "streamsets/status": target, "streamsets/demo_mode": True}
                        )

                    transport = _transport()
                    metadata: Dict[str, Any] = {"streamsets/pipeline_id": _pipeline.pipeline_id}

                    if _self.action == "start":
                        transport.start_pipeline(_pipeline.pipeline_id)
                        status = transport.wait_for_status(_pipeline.pipeline_id, {_RUNNING_STATUS}, _self.poll_interval_seconds, _self.timeout_seconds)
                        metadata["streamsets/status"] = status
                        if _self.expose_metrics:
                            try:
                                metadata["streamsets/metrics"] = dg.MetadataValue.json(transport.get_pipeline_metrics(_pipeline.pipeline_id))
                            except Exception as e:  # noqa: BLE001
                                context.log.warning(f"Could not fetch metrics for {_pipeline.pipeline_id}: {e}")
                    elif _self.action == "stop":
                        transport.stop_pipeline(_pipeline.pipeline_id, force=_self.force_stop)
                        status = transport.wait_for_status(_pipeline.pipeline_id, {_STOPPED_STATUS}, _self.poll_interval_seconds, _self.timeout_seconds)
                        metadata["streamsets/status"] = status
                    else:
                        raise ValueError(f"action must be noop|start|stop, got {_self.action!r}")

                    return dg.MaterializeResult(metadata=metadata)

                return _pipeline_asset

            assets_list.append(_make_asset())

        sensors_list: List[Any] = []
        if self.polling_sensor and self.pipelines:
            pipelines_by_id = {p.pipeline_id: p for p in self.pipelines}

            @dg.sensor(
                name=self.sensor_name,
                minimum_interval_seconds=self.check_interval_seconds,
                asset_selection=dg.AssetSelection.assets(*(dg.AssetKey.from_user_string(p.asset_key) for p in self.pipelines)),
            )
            def _engine_observer(context: dg.SensorEvaluationContext):
                import json as _json

                cursor_state: Dict[str, Any] = _json.loads(context.cursor) if context.cursor else {}
                last_status: Dict[str, str] = cursor_state.get("statuses", {})
                new_status: Dict[str, str] = dict(last_status)
                events = []

                demo_tick = int(cursor_state.get("tick", 0)) + 1 if _self.demo_mode else None

                for pipeline in pipelines_by_id.values():
                    try:
                        payload = _self._demo_status(pipeline.pipeline_id, demo_tick) if _self.demo_mode else _transport().get_pipeline_status(pipeline.pipeline_id)
                    except Exception as e:
                        context.log.error(f"StreamSets status query failed for pipeline {pipeline.pipeline_id}: {e}")
                        continue

                    status = str(payload.get("status") or "")
                    previous = last_status.get(pipeline.pipeline_id)
                    new_status[pipeline.pipeline_id] = status
                    status_changed = previous is not None and status != previous

                    metadata: Dict[str, Any] = {"streamsets/pipeline_id": pipeline.pipeline_id, "streamsets/status": status}
                    if status_changed:
                        metadata["streamsets/previous_status"] = previous

                    metrics_fetched = False
                    if _self.observe_metrics_in_sensor and status == _RUNNING_STATUS and not _self.demo_mode:
                        try:
                            metadata["streamsets/metrics"] = dg.MetadataValue.json(_transport().get_pipeline_metrics(pipeline.pipeline_id))
                            metrics_fetched = True
                        except Exception as e:
                            context.log.warning(f"Could not fetch metrics for {pipeline.pipeline_id}: {e}")

                    if not status_changed and not metrics_fetched:
                        continue  # nothing changed and nothing new to report (includes first sighting)

                    events.append(
                        dg.AssetObservation(
                            asset_key=dg.AssetKey.from_user_string(pipeline.asset_key),
                            metadata=metadata,
                        )
                    )

                new_cursor_state: Dict[str, Any] = {"statuses": new_status}
                if _self.demo_mode:
                    new_cursor_state["tick"] = demo_tick
                new_cursor = _json.dumps(new_cursor_state)

                if events:
                    return dg.SensorResult(asset_events=events, cursor=new_cursor)
                return dg.SensorResult(cursor=new_cursor, skip_reason="No pipeline status changes since last tick")

            sensors_list.append(_engine_observer)

        checks_list: List[Any] = []
        if self.require_running:
            for pipeline in self.pipelines:

                def _make_running_check(_pipeline=pipeline):
                    @dg.asset_check(asset=dg.AssetKey.from_user_string(_pipeline.asset_key), name="pipeline_running")
                    def _check(context):
                        if _self.demo_mode:
                            return dg.AssetCheckResult(passed=True, description="[STREAMSETS ENGINE DEMO] status=RUNNING")
                        status = str(_transport().get_pipeline_status(_pipeline.pipeline_id).get("status") or "")
                        return dg.AssetCheckResult(
                            passed=status == _RUNNING_STATUS,
                            description=f"status={status or 'UNKNOWN'}",
                            metadata={"streamsets/status": status or "UNKNOWN"},
                        )

                    return _check

                checks_list.append(_make_running_check())

        return dg.Definitions(assets=assets_list, sensors=sensors_list, asset_checks=checks_list)
