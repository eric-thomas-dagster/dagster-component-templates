"""Argo Workflow Trigger — materializable asset that submits a workflow.

POSTs to the Argo Server REST endpoint to submit a Workflow (either from
scratch or from a WorkflowTemplate). Captures the resulting workflow
name in asset metadata so a paired ``argo_workflow_sensor`` (in
observation mode) can watch it to terminal status.

API: POST /api/v1/workflows/{namespace}/submit
     body: { resourceKind: "WorkflowTemplate" | "ClusterWorkflowTemplate",
             resourceName: <template name>, submitOptions: {...} }
     OR
     POST /api/v1/workflows/{namespace}
     body: { workflow: <Argo Workflow YAML/JSON> }

Use Case B (Dagster owns the schedule) — for Case A (Argo owns it),
use ``external_argo_workflow`` + ``argo_workflow_sensor`` only, no
trigger.

Docs: https://argo-workflows.readthedocs.io/en/latest/rest-api/
"""
import os
from typing import Any, Dict, List, Optional

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


class ArgoWorkflowTriggerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger an Argo Workflow run via the Argo Server REST API.

    Example — submit from a WorkflowTemplate with parameters:

        ```yaml
        type: dagster_community_components.ArgoWorkflowTriggerComponent
        attributes:
          asset_key: argo/etl/nightly_aggregation
          workflow_template: nightly-aggregation
          namespace: argo
          parameters:
            INPUT_DATE: "2026-05-27"
            REGION: us-east-1
          host_env_var: ARGO_SERVER_URL
          token_env_var: ARGO_TOKEN
        ```

    The returned workflow name (Argo generates it server-side, often
    with a 5-char suffix like `nightly-aggregation-xyz12`) lands in
    asset metadata so a paired ``argo_workflow_sensor`` can pick it up.

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

    host_env_var: str = Field(
        default="ARGO_SERVER_URL",
        description="Env var with the Argo Server URL.",
    )
    token_env_var: Optional[str] = Field(
        default="ARGO_TOKEN",
        description="Env var with the bearer token. Leave empty for anonymous-read clusters.",
    )
    verify_ssl: bool = Field(default=True, description="Verify TLS certs.")
    request_timeout_seconds: int = Field(default=60, ge=1)

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

        @dg.asset(
            key=dg.AssetKey(_self.asset_key.split("/")),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Submit Argo Workflow "
                f"{_self.workflow_template or '(inline manifest)'}. "
                f"Pairs with argo_workflow_sensor (asset_event_type=observation) "
                f"to record terminal status on this same asset."
            ),
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
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
                if _self.parameters:
                    submit_options["parameters"] = [
                        f"{k}={v}" for k, v in _self.parameters.items()
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
            if _self.parameters:
                metadata["argo_parameters"] = dg.MetadataValue.json(dict(_self.parameters))

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_asset])
