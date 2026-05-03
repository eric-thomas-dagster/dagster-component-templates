"""K8sJobAssetComponent.

Wraps `dagster-k8s` to launch a Kubernetes Job on materialization, wait for completion, and surface stdout/stderr in the Dagster run log. Useful for compute that needs to run in-cluster (training jobs, batch jobs, anything containerized).
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class K8sJobAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Kubernetes Job as a Dagster asset via dagster-k8s."""

    asset_name: str = Field(description="Dagster asset name.")
    image: str = Field(description="Container image (e.g. 'my-registry/my-job:v1').")
    command: Optional[List[str]] = Field(default=None, description="Override container command.")
    namespace: str = Field(default="default", description="Kubernetes namespace.")
    env_vars: Optional[Dict[str, str]] = Field(default=None, description="Env vars to inject.")
    cpu_limit: Optional[str] = Field(default=None, description="CPU limit (e.g. '1000m').")
    memory_limit: Optional[str] = Field(default=None, description="Memory limit (e.g. '2Gi').")
    group_name: str = Field(default="k8s", description="Asset group.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_k8s import execute_k8s_job, k8s_job_op
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
        cmd = self.command
        env_vars = self.env_vars or {}
        cpu = self.cpu_limit
        mem = self.memory_limit
        ns = self.namespace
        image = self.image

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=f"Kubernetes Job: {image}",
            deps=deps_keys,
        )
        def _k8s_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            container_config = {"image": image}
            if cmd:
                container_config["command"] = cmd
            if cpu or mem:
                container_config["resources"] = {"limits": {k: v for k, v in {"cpu": cpu, "memory": mem}.items() if v}}
            if env_vars:
                container_config["env"] = [{"name": k, "value": v} for k, v in env_vars.items()]
            execute_k8s_job(
                context=context,
                image=image,
                command=cmd,
                k8s_job_name=context.run_id[:32],
                namespace=ns,
                resources=container_config.get("resources"),
                env_vars=env_vars,
            )
            return dg.MaterializeResult(metadata={"image": dg.MetadataValue.text(image), "namespace": dg.MetadataValue.text(ns)})
        return dg.Definitions(assets=[_k8s_asset])

