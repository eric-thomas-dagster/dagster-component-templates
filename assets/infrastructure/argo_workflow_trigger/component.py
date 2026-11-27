"""Argo Workflow Trigger — materializable asset that submits a workflow.

POSTs to the Argo Server REST endpoint to submit a Workflow (either from
scratch or from a WorkflowTemplate). Captures the resulting workflow
name in asset metadata so a paired ``argo_workflow_sensor`` (in
observation mode) can watch it to terminal status -- OR, if
``wait_for_completion: true``, this asset polls the workflow to a
terminal phase itself (GET /api/v1/workflows/{namespace}/{name}),
fails on Failed/Error/Omitted, and surfaces ``status.outputs.parameters``
as materialization metadata.

API: POST /api/v1/workflows/{namespace}/submit
     body: { resourceKind: "WorkflowTemplate" | "ClusterWorkflowTemplate",
             resourceName: <template name>, submitOptions: {...} }
     OR
     POST /api/v1/workflows/{namespace}
     body: { workflow: <Argo Workflow YAML/JSON> }
     GET  /api/v1/workflows/{namespace}/{name}   (poll, when wait_for_completion)

Use Case B (Dagster owns the schedule) — for Case A (Argo owns it),
use ``external_argo_workflow`` + ``argo_workflow_sensor`` only, no
trigger.

`demo_mode: true` (default) simulates the whole submit/poll lifecycle on
stdout -- no Argo cluster, no `requests` call, no env vars required. Flip to
`false` and set `host_env_var` / `token_env_var` to hit a real Argo Server.

Docs: https://argo-workflows.readthedocs.io/en/latest/rest-api/
"""
import os
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

ARGO_TERMINAL_SUCCESS = {"Succeeded", "Skipped"}
ARGO_TERMINAL_FAIL = {"Failed", "Error", "Omitted"}


# ───────────────────────────────────────────────────────────────────────────
# Partition helper (canonical shape, copied from external_*_asset /
# http_external_asset -- self-contained per the registry's distribution model)
# ───────────────────────────────────────────────────────────────────────────

def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date)."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _render_partition_placeholders(value: str, context: "dg.AssetExecutionContext") -> str:
    """Substitute {partition_key} / {date} / {partition.<dim>} in a parameter value."""
    if not context.has_partition_key:
        return value
    pk = context.partition_key
    rendered = value.replace("{partition_key}", pk).replace("{date}", pk)
    keys_by_dimension = getattr(pk, "keys_by_dimension", None)
    if keys_by_dimension:
        for dim_name, dim_val in keys_by_dimension.items():
            rendered = rendered.replace("{partition." + dim_name + "}", dim_val)
    return rendered



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


def _simulate_argo(context, _self, rendered_params: Dict[str, str]) -> dg.MaterializeResult:
    """Simulate the submit (+ optional poll) lifecycle on stdout -- no cluster required."""
    wf_label = _self.workflow_template or "inline-manifest"
    suffix = (context.partition_key if context.has_partition_key else "latest").lower()
    suffix = "".join(c if c.isalnum() else "-" for c in suffix)[:16]
    wf_name = f"{wf_label}-{suffix}"

    submit_options: Dict[str, Any] = {}
    if rendered_params:
        submit_options["parameters"] = [f"{k}={v}" for k, v in rendered_params.items()]
    if _self.workflow_template:
        url = f"<demo-argo-server>/api/v1/workflows/{_self.namespace}/submit"
        context.log.info(f"[SUBMIT] POST {url}")
        context.log.info(f"  resourceKind=WorkflowTemplate resourceName={_self.workflow_template} submitOptions={submit_options}")
    else:
        url = f"<demo-argo-server>/api/v1/workflows/{_self.namespace}"
        context.log.info(f"[SUBMIT] POST {url} (inline manifest)")
    context.log.info(f"  Response: 200 Created — workflow name: {wf_name}")

    metadata: Dict[str, Any] = {
        "argo_namespace": _self.namespace,
        "demo_mode": True,
    }
    if _self.workflow_template:
        metadata["argo_workflow_template"] = _self.workflow_template
    metadata["argo_workflow_name"] = wf_name
    if rendered_params:
        metadata["argo_parameters"] = dg.MetadataValue.json(rendered_params)

    if _self.wait_for_completion:
        for phase in ("Pending", "Running", "Succeeded"):
            context.log.info(f"[POLL]   GET .../workflows/{_self.namespace}/{wf_name} -> phase={phase}")
        outputs = {f"{k}_result": f"processed:{v}" for k, v in rendered_params.items()} or {"status": "ok"}
        context.log.info(f"[DONE]   {wf_name} -> Succeeded. status.outputs.parameters: {outputs}")
        metadata["argo_phase"] = "Succeeded"
        metadata["argo_outputs"] = dg.MetadataValue.json(outputs)

    return dg.MaterializeResult(metadata=metadata)


def _execute_argo(context, _self, rendered_params: Dict[str, str]) -> dg.MaterializeResult:
    """Real Argo Server REST API submit (+ optional poll-to-terminal) lifecycle."""
    try:
        import requests
    except ImportError:
        raise ImportError("argo_workflow_trigger requires `requests`: pip install requests")

    host = os.environ.get(_self.host_env_var, "").rstrip("/")
    if not host:
        raise RuntimeError(f"{_self.host_env_var} not set.")
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if _self.token_env_var and os.environ.get(_self.token_env_var):
        headers["Authorization"] = f"Bearer {_require_env(_self.token_env_var)}"

    if _self.workflow_template:
        # Submit from WorkflowTemplate.
        url = f"{host}/api/v1/workflows/{_self.namespace}/submit"
        submit_options: Dict[str, Any] = {}
        if rendered_params:
            submit_options["parameters"] = [
                f"{k}={v}" for k, v in rendered_params.items()
            ]
        body = {
            "resourceKind": "WorkflowTemplate",
            "resourceName": _self.workflow_template,
            "submitOptions": submit_options,
            "namespace": _self.namespace,
        }
    else:
        # Submit an inline manifest.
        url = f"{host}/api/v1/workflows/{_self.namespace}"
        body = {"workflow": _self.workflow_manifest, "namespace": _self.namespace}

    context.log.info(f"Submitting Argo Workflow: POST {url}")
    resp = requests.post(
        url, headers=headers, json=body,
        timeout=_self.request_timeout_seconds, verify=_self.verify_ssl,
    )
    resp.raise_for_status()
    payload = resp.json()
    wf_meta = payload.get("metadata") or {}
    wf_name = wf_meta.get("name")

    metadata: Dict[str, Any] = {
        "argo_namespace": _self.namespace,
        "argo_server_url": dg.MetadataValue.url(host),
    }
    if _self.workflow_template:
        metadata["argo_workflow_template"] = _self.workflow_template
    if wf_name:
        metadata["argo_workflow_name"] = wf_name
    if rendered_params:
        metadata["argo_parameters"] = dg.MetadataValue.json(rendered_params)

    if _self.wait_for_completion:
        if not wf_name:
            raise RuntimeError(
                "wait_for_completion=true but the submit response had no metadata.name to poll."
            )
        status_url = f"{host}/api/v1/workflows/{_self.namespace}/{wf_name}"
        started = time.monotonic()
        phase = "Pending"
        status_blob: Dict[str, Any] = {}
        while True:
            if time.monotonic() - started > _self.poll_timeout_seconds:
                raise TimeoutError(
                    f"Argo workflow {wf_name} did not reach a terminal phase within "
                    f"{_self.poll_timeout_seconds}s (last phase: {phase})."
                )
            poll_resp = requests.get(status_url, headers=headers, timeout=_self.request_timeout_seconds, verify=_self.verify_ssl)
            poll_resp.raise_for_status()
            wf_body = poll_resp.json()
            status_blob = wf_body.get("status") or {}
            phase = status_blob.get("phase") or "Pending"
            context.log.info(f"[POLL]   {wf_name} -> {phase}")
            if phase in ARGO_TERMINAL_SUCCESS or phase in ARGO_TERMINAL_FAIL:
                break
            time.sleep(_self.poll_interval_seconds)

        metadata["argo_phase"] = phase
        metadata["argo_started_at"] = status_blob.get("startedAt")
        metadata["argo_finished_at"] = status_blob.get("finishedAt")
        outputs = ((status_blob.get("outputs") or {}).get("parameters")) or []
        if outputs:
            metadata["argo_outputs"] = dg.MetadataValue.json(outputs)

        if phase in ARGO_TERMINAL_FAIL:
            raise RuntimeError(
                f"Argo workflow {wf_name} terminal failure: {phase} "
                f"({status_blob.get('message') or 'no message'})"
            )

    return dg.MaterializeResult(metadata=metadata)


class ArgoWorkflowTriggerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger an Argo Workflow run via the Argo Server REST API.

    Supports `partitions_def` (daily/weekly/monthly/hourly/cron/static/dynamic/
    multi-axis, same shape as the rest of the registry) with `{partition_key}` /
    `{date}` / `{partition.<dim>}` substitution in `parameters` values. With
    `wait_for_completion: true` the asset polls GET
    /api/v1/workflows/{namespace}/{name} to a terminal phase, fails on
    Failed/Error/Omitted, and surfaces `status.outputs.parameters` as
    materialization metadata. `demo_mode: true` (default) simulates the whole
    submit/poll lifecycle on stdout -- no cluster, no env vars required.

    Example — submit from a WorkflowTemplate, partitioned, waiting for outputs:

        ```yaml
        type: dagster_community_components.ArgoWorkflowTriggerComponent
        attributes:
          asset_key: argo/etl/nightly_aggregation
          workflow_template: nightly-aggregation
          namespace: argo
          parameters:
            INPUT_DATE: "{partition_key}"
            REGION: us-east-1
          partition_type: daily
          partition_start: "2024-01-01"
          wait_for_completion: true
          host_env_var: ARGO_SERVER_URL
          token_env_var: ARGO_TOKEN
        ```

    The returned workflow name (Argo generates it server-side, often
    with a 5-char suffix like `nightly-aggregation-xyz12`) lands in
    asset metadata so a paired ``argo_workflow_sensor`` can pick it up --
    unless `wait_for_completion: true`, in which case this asset itself
    records the terminal phase and outputs, no paired sensor needed.

    **Do NOT pair on the same asset_key as ``external_argo_workflow``** —
    those represent the same workflow with different schedule ownership.
    Pick the Case (A or B) that matches your setup; the components are
    alternatives, not complements.
    """

    asset_key: str = Field(
        description=(
            "Dagster asset key for the workflow (e.g. "
            "'argo/etl/nightly_aggregation'). Use '/' separators."
        ),
    )
    workflow_template: Optional[str] = Field(
        default=None,
        description=(
            "Argo WorkflowTemplate name to instantiate. Set this OR "
            "workflow_manifest, not both."
        ),
    )
    workflow_manifest: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Full Argo Workflow manifest as a dict (apiVersion / kind / "
            "metadata / spec). Use this for one-off workflows that "
            "aren't pre-registered as templates. Set this OR "
            "workflow_template, not both."
        ),
    )
    namespace: str = Field(default="argo", description="Kubernetes namespace.")
    parameters: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Workflow parameters (key=value). Passed via submitOptions."
            "parameters as `name=value` strings."
        ),
    )

    demo_mode: bool = Field(
        default=True,
        description=(
            "Simulate the Argo Server submit/poll lifecycle on stdout -- no cluster, no "
            "`requests` call, no env vars required. Flip to False and set host_env_var / "
            "token_env_var to hit a real Argo Server."
        ),
    )
    host_env_var: str = Field(
        default="ARGO_SERVER_URL",
        description="Env var with the Argo Server URL (used when demo_mode=false).",
    )
    token_env_var: Optional[str] = Field(
        default="ARGO_TOKEN",
        description="Env var with the bearer token. Leave empty for anonymous-read clusters.",
    )
    verify_ssl: bool = Field(default=True, description="Verify TLS certs.")
    request_timeout_seconds: int = Field(default=60, ge=1)

    wait_for_completion: bool = Field(
        default=False,
        description=(
            "Poll GET /api/v1/workflows/{namespace}/{name} until the workflow reaches a "
            "terminal phase. Fails the asset on Failed/Error/Omitted; surfaces "
            "status.outputs.parameters as materialization metadata on success. When False "
            "(default) the asset returns as soon as Argo accepts the submission -- pair with "
            "argo_workflow_sensor (asset_event_type=observation) to record terminal status."
        ),
    )
    poll_interval_seconds: int = Field(default=15, ge=1, description="Seconds between status polls (wait_for_completion only).")
    poll_timeout_seconds: int = Field(default=3600, ge=1, description="Seconds to wait for a terminal phase before failing (wait_for_completion only).")

    # Partitions — canonical shape used across the registry.
    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'cron' | 'static' | 'dynamic'.")
    partition_start: Optional[str] = Field(default=None, description="Required for daily/weekly/monthly/hourly/cron -- ISO start date.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for partition_type='static'.")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required for partition_type='cron', e.g. '0 0 * * 1-5'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition_type='cron' keys.")
    partition_end_offset: int = Field(default=0, description="Passed through to the underlying time-window partitions definition.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the partitions definition (defaults to UTC).")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Required for partition_type='dynamic'.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partitions -- set instead of partition_type.")

    group_name: Optional[str] = Field(default="argo", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'argo', 'workflow').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Argo Workflow Trigger', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.workflow_template) == bool(self.workflow_manifest):
            raise ValueError(
                f"argo_workflow_trigger {self.asset_key!r}: set exactly one of "
                f"`workflow_template` or `workflow_manifest`."
            )

        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("argo")
        kinds.add("workflow")

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset,
            timezone=self.partition_timezone,
        )

        @dg.asset(
            key=dg.AssetKey(_self.asset_key.split("/")),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            partitions_def=partitions_def,
            description=_self.description or (
                f"Submit Argo Workflow "
                f"{_self.workflow_template or '(inline manifest)'}. "
                + (
                    "Polls to a terminal phase and surfaces outputs."
                    if _self.wait_for_completion else
                    "Pairs with argo_workflow_sensor (asset_event_type=observation) "
                    "to record terminal status on this same asset."
                )
            ),
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            rendered_params: Dict[str, str] = {
                k: _render_partition_placeholders(str(v), context)
                for k, v in (_self.parameters or {}).items()
            }

            if _self.demo_mode:
                return _simulate_argo(context, _self, rendered_params)
            return _execute_argo(context, _self, rendered_params)

        return dg.Definitions(assets=[_asset])
