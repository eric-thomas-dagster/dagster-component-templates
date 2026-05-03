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
            name=self.asset_name,
            description=self.description or "Deploy a Bicep template as a Dagster asset (compiles via `az bicep build` then deploys via ARM).",
            group_name=self.group_name,
            kinds=set(self.kinds or ['azure', 'bicep']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import subprocess, tempfile

            # 1. Compile bicep -> ARM JSON via az CLI
            with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as out:
                arm_json_path = out.name
            cmd = [_self.az_bin, "bicep", "build", "--file", _self.bicep_file, "--outfile", arm_json_path]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"az bicep build failed: {result.stderr}")
            context.log.info(f"compiled {_self.bicep_file} -> {arm_json_path}")

            # 2. Reuse the ARM deployment client
            from azure.identity import DefaultAzureCredential
            from azure.mgmt.resource import ResourceManagementClient

            sub_id = _self.subscription_id or os.environ["AZURE_SUBSCRIPTION_ID"]
            client = ResourceManagementClient(DefaultAzureCredential(), sub_id)

            with open(arm_json_path) as f:
                template = json.load(f)

            params = {}
            if _self.parameters_file:
                with open(_self.parameters_file) as f:
                    raw = json.load(f)
                    params = raw.get("parameters", raw)
            if _self.parameters:
                for k, v in _self.parameters.items():
                    params[k] = {"value": v}

            deployment_props = {"mode": _self.mode, "template": template, "parameters": params}

            if _self.what_if:
                poller = client.deployments.begin_what_if(_self.resource_group, _self.deployment_name, {"properties": deployment_props})
                result = poller.result()
                outputs = {"changes": [str(c) for c in (result.changes or [])]}
            else:
                poller = client.deployments.begin_create_or_update(_self.resource_group, _self.deployment_name, {"properties": deployment_props})
                result = poller.result()
                outputs = result.properties.outputs or {}

            return dg.MaterializeResult(metadata={
                "outputs": dg.MetadataValue.json(outputs),
                "deployment_name": dg.MetadataValue.text(_self.deployment_name),
                "bicep_file": dg.MetadataValue.text(_self.bicep_file),
                "what_if": dg.MetadataValue.bool(_self.what_if),
            })

        return dg.Definitions(assets=[_asset])
