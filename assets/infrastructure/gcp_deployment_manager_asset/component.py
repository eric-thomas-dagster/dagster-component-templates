"""GcpDeploymentManagerAssetComponent.

Deploy a GCP Deployment Manager configuration (.yaml or jinja) as a Dagster asset.
"""

import json
import os
from typing import Optional

import dagster as dg
from pydantic import Field


class GcpDeploymentManagerAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Deploy a GCP Deployment Manager configuration (.yaml or jinja) as a Dagster asset."""


    asset_name: str = Field(description="Dagster asset name for this DM deployment")
    project_id: str = Field(description="GCP project ID")
    deployment_name: str = Field(description="Deployment Manager deployment name (idempotent)")
    config_file: str = Field(description="Local path to deployment config (YAML / jinja)")
    properties: Optional[dict] = Field(default=None, description="Inline property overrides")
    preview: bool = Field(default=False, description="Preview only (don't apply)")

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
            description=self.description or "Deploy a GCP Deployment Manager configuration (.yaml or jinja) as a Dagster asset.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['gcp', 'deployment-manager']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry,
        )
        def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import yaml, subprocess

            # Use gcloud CLI for simplicity (the python SDK works but adds heavy deps)
            cmd = [
                "gcloud", "deployment-manager", "deployments",
                "describe" if False else "create",
                _self.deployment_name,
                f"--project={_self.project_id}",
                f"--config={_self.config_file}",
            ]
            # describe to detect existence, then create OR update
            check = subprocess.run(["gcloud", "deployment-manager", "deployments", "describe",
                                    _self.deployment_name, f"--project={_self.project_id}"],
                                    capture_output=True, text=True)
            exists = check.returncode == 0
            verb = "update" if exists else "create"
            cmd = ["gcloud", "deployment-manager", "deployments", verb, _self.deployment_name,
                   f"--project={_self.project_id}", f"--config={_self.config_file}"]
            if _self.preview:
                cmd.append("--preview")

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"gcloud deployment-manager {verb} failed: {result.stderr}")
            context.log.info(result.stdout)

            return dg.MaterializeResult(metadata={
                "deployment_name": dg.MetadataValue.text(_self.deployment_name),
                "verb": dg.MetadataValue.text(verb),
                "preview": dg.MetadataValue.bool(_self.preview),
            })

        return dg.Definitions(assets=[_asset])
