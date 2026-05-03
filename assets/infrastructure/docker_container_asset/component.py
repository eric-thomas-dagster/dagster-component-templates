"""DockerContainerAssetComponent.

Wraps `dagster-docker` to run a container per materialization, wait for completion, and stream logs into the Dagster run log. Lighter-weight than k8s for local-dev or single-host pipelines.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class DockerContainerAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Docker container as a Dagster asset via dagster-docker."""

    asset_name: str = Field(description="Dagster asset name.")
    image: str = Field(description="Docker image.")
    command: Optional[List[str]] = Field(default=None, description="Container command.")
    env_vars: Optional[Dict[str, str]] = Field(default=None, description="Env vars.")
    network: Optional[str] = Field(default=None, description="Docker network name.")
    group_name: str = Field(default="docker", description="Asset group.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_docker import execute_docker_container
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
        image = self.image; cmd = self.command; env_vars = self.env_vars; network = self.network

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=f"Docker container: {image}",
            deps=deps_keys,
        )
        def _docker_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            execute_docker_container(
                context=context, image=image, command=cmd or None,
                env_vars=env_vars or None, networks=[network] if network else None,
            )
            return dg.MaterializeResult(metadata={"image": dg.MetadataValue.text(image)})
        return dg.Definitions(assets=[_docker_asset])

