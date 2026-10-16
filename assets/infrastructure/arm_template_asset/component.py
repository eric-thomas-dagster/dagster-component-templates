"""ArmTemplateAssetComponent.

Deploy an Azure Resource Manager (ARM) template as a Dagster asset.
"""

import json
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


class ArmTemplateAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Deploy an Azure Resource Manager (ARM) template as a Dagster asset."""


    asset_name: str = Field(description="Dagster asset name for this ARM deployment")
    resource_group: str = Field(description="Azure resource group to deploy into")
    deployment_name: str = Field(description="ARM deployment name (idempotent — re-runs update in place)")
    template_file: Optional[str] = Field(default=None, description="Local path to ARM JSON template")
    template_uri: Optional[str] = Field(default=None, description="HTTPS URL of remote ARM template (mutually exclusive with template_file)")
    parameters_file: Optional[str] = Field(default=None, description="Local path to parameters JSON file")
    parameters: Optional[dict] = Field(default=None, description="Inline parameter overrides")
    subscription_id: Optional[str] = Field(default=None, description="Azure subscription ID (defaults to AZURE_SUBSCRIPTION_ID env)")
    location: Optional[str] = Field(default=None, description="Azure region (only required for subscription-scope deployments)")
    mode: str = Field(default="Incremental", description="Deployment mode: 'Incremental' or 'Complete'")
    what_if: bool = Field(default=False, description="Run a what-if preview only (no changes applied)")

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="infrastructure")
    deps: Optional[list[str]] = Field(default=None)
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=(), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        retry = None
        if self.retry_policy_max_retries:
            retry = dg.RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL if self.retry_policy_backoff == "exponential" else dg.Backoff.LINEAR,
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or "Deploy an Azure Resource Manager (ARM) template as a Dagster asset.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['azure', 'arm']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import subprocess

            # Use `az` CLI rather than the Python SDK — avoids the
            # azure-mgmt-resource version churn (.deployments accessor moved
            # to a separate package in recent versions). `az` uses the
            # caller's existing `az login` session.
            verb = "what-if" if _self.what_if else "create"
            cmd = [
                "az", "deployment", "group", verb,
                "--resource-group", _self.resource_group,
                "--name", _self.deployment_name,
                "--mode", _self.mode,
                "--output", "json",
            ]
            if _self.template_file:
                cmd += ["--template-file", _self.template_file]
            elif _self.template_uri:
                cmd += ["--template-uri", _self.template_uri]
            else:
                raise ValueError("Either template_file or template_uri is required")

            sub_id = _self.subscription_id or os.environ.get("AZURE_SUBSCRIPTION_ID")
            if sub_id:
                cmd += ["--subscription", sub_id]

            for k, v in (_self.parameters or {}).items():
                cmd += ["--parameters", f"{k}={v}"]
            if _self.parameters_file:
                cmd += ["--parameters", f"@{_self.parameters_file}"]

            context.log.info("running: " + " ".join(cmd))
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(
                    f"az deployment group {verb} failed (exit {result.returncode}):\n"
                    f"stderr: {result.stderr}\nstdout: {result.stdout}"
                )

            try:
                payload = json.loads(result.stdout) if result.stdout else {}
                outputs = payload.get("properties", {}).get("outputs", {}) if isinstance(payload, dict) else {}
            except json.JSONDecodeError:
                outputs = {}

            return dg.MaterializeResult(metadata={
                "outputs": dg.MetadataValue.json(outputs),
                "deployment_name": dg.MetadataValue.text(_self.deployment_name),
                "resource_group": dg.MetadataValue.text(_self.resource_group),
                "what_if": dg.MetadataValue.bool(_self.what_if),
            })

        return dg.Definitions(assets=[_asset])
