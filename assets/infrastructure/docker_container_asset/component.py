"""DockerContainerAssetComponent.

Wraps `dagster-docker` to run a container per materialization, wait for completion, and stream logs into the Dagster run log. Lighter-weight than k8s for local-dev or single-host pipelines.
"""
from typing import Dict, List, Optional

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


class DockerContainerAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Docker container as a Dagster asset via dagster-docker."""

    asset_name: str = Field(description="Dagster asset name.")
    image: str = Field(description="Docker image.")
    entrypoint: Optional[List[str]] = Field(default=None, description="Override the container ENTRYPOINT (separate from command/CMD).")
    command: Optional[List[str]] = Field(default=None, description="Container command (CMD).")
    env_vars: Optional[Dict[str, str]] = Field(default=None, description="Env vars.")
    network: Optional[str] = Field(default=None, description="Docker network name.")
    registry: Optional[Dict[str, str]] = Field(
        default=None,
        description="Credentials for a non-public Docker registry to pull `image` from. Keys: 'url', 'username', 'password' (dagster-docker's execute_docker_container 'registry' param).",
    )
    container_kwargs: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Raw key-value pairs passed straight through to the Docker Python SDK's containers.create() (https://docker-py.readthedocs.io/en/stable/containers.html) -- e.g. volumes, labels, working_dir, user, mem_limit. Not simplified into dedicated fields since the available options vary too much.",
    )
    group_name: str = Field(default="docker", description="Asset group.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'entrypoint', 'command'), csv=(), tags=('registry',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Docker Container Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_docker import execute_docker_container
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
        image = self.image; entrypoint = self.entrypoint; cmd = self.command
        env_vars = self.env_vars; network = self.network
        registry = self.registry; container_kwargs = self.container_kwargs

        # execute_docker_container's env_vars is List[str] of "KEY=value" (DockerContainerContext
        # treats a bare "KEY" entry with no "=" as "read this var name from the LAUNCHER's own
        # environment"), NOT a dict -- passing a dict directly iterates its KEYS only, each one
        # a bare var name, which silently pulls from the launcher's environment instead of the
        # user-configured value. Same real bug class fixed in k8s_job_asset earlier this session.
        env_var_list = [f"{k}={v}" for k, v in env_vars.items()] if env_vars else None

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=f"Docker container: {image}",
            deps=deps_keys,
        )
        def _docker_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            execute_docker_container(
                context=context, image=image, entrypoint=entrypoint or None, command=cmd or None,
                env_vars=env_var_list, networks=[network] if network else None,
                registry=registry or None, container_kwargs=container_kwargs or None,
            )
            return dg.MaterializeResult(metadata={"image": dg.MetadataValue.text(image)})
        return dg.Definitions(assets=[_docker_asset])

