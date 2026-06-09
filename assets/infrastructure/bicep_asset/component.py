"""BicepAssetComponent.

Deploy a Bicep template as a Dagster asset (compiles via `az bicep build` then deploys via ARM).
"""

import json
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class BicepAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Deploy a Bicep template as a Dagster asset (compiles via `az bicep build` then deploys via ARM)."""


    asset_name: str = Field(description="Dagster asset name for this Bicep deployment")
    resource_group: str = Field(description="Azure resource group to deploy into")
    deployment_name: str = Field(description="ARM deployment name (idempotent)")
    bicep_file: str = Field(description="Local path to .bicep file")
    parameters_file: Optional[str] = Field(default=None, description="Local path to parameters JSON")
    parameters: Optional[dict] = Field(default=None, description="Inline parameter overrides")
    subscription_id: Optional[str] = Field(default=None, description="Azure subscription ID (env: AZURE_SUBSCRIPTION_ID)")
    az_bin: str = Field(default="az", description="Path to the az CLI binary")
    mode: str = Field(default="Incremental", description="'Incremental' or 'Complete'")
    what_if: bool = Field(default=False, description="What-if preview only")

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="infrastructure")
    deps: Optional[list[str]] = Field(default=None)
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

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
            description=self.description or "Deploy a Bicep template as a Dagster asset (compiles via `az bicep build` then deploys via ARM).",
            group_name=self.group_name,
            kinds=set(self.kinds or ['azure', 'bicep']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import subprocess

            # The whole flow uses `az` CLI: it's already required for `az bicep
            # build`, plus shipping the SDK pulls in azure-mgmt-resource +
            # azure-identity which version-churn frequently and complicate
            # auth. `az` CLI uses your existing `az login` session.
            verb = "what-if" if _self.what_if else "create"
            cmd = [
                _self.az_bin, "deployment", "group", verb,
                "--resource-group", _self.resource_group,
                "--name", _self.deployment_name,
                "--template-file", _self.bicep_file,
                "--mode", _self.mode,
                "--output", "json",
            ]
            if _self.subscription_id:
                cmd += ["--subscription", _self.subscription_id]
            elif os.environ.get("AZURE_SUBSCRIPTION_ID"):
                cmd += ["--subscription", os.environ["AZURE_SUBSCRIPTION_ID"]]

            # Inline parameters become `--parameters key=value`. Parameter
            # files become `--parameters @path.json`.
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

            # Surface the deployment outputs as Dagster metadata so downstream
            # assets (or callers via dg list events) can pick them up.
            try:
                payload = json.loads(result.stdout) if result.stdout else {}
                outputs = (
                    payload.get("properties", {}).get("outputs", {}) if isinstance(payload, dict) else {}
                )
            except json.JSONDecodeError:
                outputs = {}

            return dg.MaterializeResult(metadata={
                "outputs": dg.MetadataValue.json(outputs),
                "deployment_name": dg.MetadataValue.text(_self.deployment_name),
                "resource_group": dg.MetadataValue.text(_self.resource_group),
                "bicep_file": dg.MetadataValue.text(_self.bicep_file),
                "what_if": dg.MetadataValue.bool(_self.what_if),
            })

        return dg.Definitions(assets=[_asset])
