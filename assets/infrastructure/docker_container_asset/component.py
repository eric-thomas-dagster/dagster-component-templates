"""DockerContainerAssetComponent.

Wraps `dagster-docker` to run a container per materialization, wait for completion, and stream logs into the Dagster run log. Lighter-weight than k8s for local-dev or single-host pipelines.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field



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


def _parse_docker_env_vars(env_var_list):
    """Mirror dagster._core.utils.parse_env_var's KEY=value / bare-KEY-from-launcher-env semantics,
    used only by the direct-client path below (the delegate path already gets this from
    dagster_docker itself)."""
    import os
    out = {}
    for item in env_var_list or []:
        if "=" in item:
            k, v = item.split("=", 1)
            out[k] = v
        else:
            out[item] = os.environ.get(item, "")
    return out


class DockerContainerAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Docker container as a Dagster asset via dagster-docker.

    By default this delegates entirely to `dagster_docker.execute_docker_container` (the
    battle-tested, officially-maintained path). Setting `container_name`, `docker_base_url`,
    any `tls_*` field, or `retrieve_output` switches to a "direct" code path that creates and
    drives the container itself -- `execute_docker_container`'s public API has no hook for a
    custom container name or a non-default/TLS daemon connection (confirmed by reading its real
    implementation: the container name is always derived internally via a hash of the run id and
    op name, and its Docker client is always `docker.client.from_env()`), and `retrieve_output`
    needs a container handle after the run to fetch logs, which the delegate path never exposes.
    `force_pull`/`auto_remove`/`mounts`/`ports` work in EITHER path without needing the switch --
    none of them collide with what the delegate path already passes internally.
    """

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
        description="Raw key-value pairs passed straight through to the Docker Python SDK's containers.create() (https://docker-py.readthedocs.io/en/stable/containers.html) -- e.g. volumes, labels, working_dir, user, mem_limit. Not simplified into dedicated fields since the available options vary too much. Do NOT put 'name' here -- dagster-docker's own execute_docker_container always passes its own 'name=' first, so a 'name' key here raises 'got multiple values for keyword argument'; use container_name instead.",
    )
    container_name: Optional[str] = Field(
        default=None,
        description="A fixed container name, instead of dagster-docker's auto-derived hash(run_id + op_name). Not possible via container_kwargs (see its description) -- setting this switches to this component's own direct container-creation path instead of delegating to execute_docker_container.",
    )
    force_pull: bool = Field(
        default=False,
        description="Pull `image` unconditionally before creating the container, even if a (possibly stale) copy is already cached locally. The delegate path only pulls on a cache MISS (docker.errors.ImageNotFound); this explicitly re-pulls every run.",
    )
    auto_remove: bool = Field(
        default=False,
        description="Remove the container automatically once it exits (the real Docker auto_remove flag), instead of leaving a stopped container behind for later inspection.",
    )
    mounts: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Docker volume/bind mounts, as a list of docker.types.Mount constructor kwargs, e.g. [{target: '/data', source: 'my-vol', type: 'volume'}] or [{target: '/host-dir', source: '/local/path', type: 'bind', read_only: true}].",
    )
    ports: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Container->host port publishing, as the real Docker SDK 'ports' dict, e.g. {'8080/tcp': 8080}. Incompatible with network_mode='host' and, per the Docker SDK, with the single `network` field already set here when publishing requires default bridge networking -- test your specific combination.",
    )
    docker_base_url: Optional[str] = Field(
        default=None,
        description="Connect to a non-default Docker daemon (e.g. 'tcp://remote-host:2376') instead of the local daemon resolved by DOCKER_HOST/the default socket. Setting this switches to this component's own direct container-creation path, since dagster_docker.execute_docker_container always uses docker.client.from_env() with no override hook.",
    )
    tls_ca_cert_path: Optional[str] = Field(default=None, description="Path to a CA cert file, for connecting to a TLS-secured Docker daemon (usually set together with docker_base_url). Setting this switches to the direct path.")
    tls_client_cert_path: Optional[str] = Field(default=None, description="Path to a client cert file for TLS client auth. Requires tls_client_key_path too.")
    tls_client_key_path: Optional[str] = Field(default=None, description="Path to a client key file for TLS client auth. Requires tls_client_cert_path too.")
    tls_verify: bool = Field(default=True, description="Verify the Docker daemon's TLS certificate. Only meaningful when docker_base_url/tls_ca_cert_path/tls_client_cert_path are set.")
    retrieve_output: bool = Field(
        default=False,
        description="Capture the container's combined stdout/stderr and attach it as asset metadata ('container_output', truncated to the last 4000 characters). The delegate path only prints container output to the run log, with no way to also hand it back to the caller as a value -- enabling this switches to this component's own direct container-creation path, which keeps a handle to the container after it exits.",
    )
    group_name: str = Field(default="docker", description="Asset group.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'entrypoint', 'command'), csv=(), tags=('registry',), strs=('env_vars',))

    @model_validator(mode="after")
    def _validate_tls_pair(self):
        if self.tls_client_cert_path and not self.tls_client_key_path:
            raise ValueError("tls_client_key_path is required when tls_client_cert_path is set.")
        if self.tls_client_key_path and not self.tls_client_cert_path:
            raise ValueError("tls_client_cert_path is required when tls_client_key_path is set.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Docker Container Asset', editable=True)

    def _needs_direct_client(self) -> bool:
        return bool(
            self.container_name or self.docker_base_url or self.tls_ca_cert_path
            or self.tls_client_cert_path or self.retrieve_output
        )

    def _make_docker_client(self):
        import docker

        if self.docker_base_url or self.tls_ca_cert_path or self.tls_client_cert_path:
            tls_config = None
            if self.tls_ca_cert_path or self.tls_client_cert_path:
                client_cert = (self.tls_client_cert_path, self.tls_client_key_path) if self.tls_client_cert_path else None
                tls_config = docker.tls.TLSConfig(client_cert=client_cert, ca_cert=self.tls_ca_cert_path, verify=self.tls_verify)
            kwargs: Dict[str, Any] = {}
            if self.docker_base_url:
                kwargs["base_url"] = self.docker_base_url
            if tls_config is not None:
                kwargs["tls"] = tls_config
            return docker.DockerClient(**kwargs)
        return docker.from_env()

    def _execute_direct(self, image, entrypoint, cmd, env_var_list, network_list, registry, container_kwargs):
        import docker
        import docker.errors
        from docker.types import Mount

        client = self._make_docker_client()
        if registry:
            client.login(registry=registry["url"], username=registry["username"], password=registry["password"])

        merged_kwargs = dict(container_kwargs or {})
        if self.mounts:
            merged_kwargs["mounts"] = [Mount(**m) for m in self.mounts]
        if self.ports:
            merged_kwargs["ports"] = self.ports
        if self.auto_remove:
            merged_kwargs["auto_remove"] = True

        create_kwargs: Dict[str, Any] = dict(
            name=self.container_name,
            detach=True,
            network=network_list[0] if network_list else None,
            entrypoint=entrypoint,
            command=cmd,
            environment=_parse_docker_env_vars(env_var_list),
            **merged_kwargs,
        )

        if self.force_pull:
            client.images.pull(image)
        try:
            container = client.containers.create(image, **create_kwargs)
        except docker.errors.ImageNotFound:
            client.images.pull(image)
            container = client.containers.create(image, **create_kwargs)

        if network_list and len(network_list) > 1:
            for network_name in network_list[1:]:
                client.networks.get(network_name).connect(container)

        container.start()

        collected = [] if self.retrieve_output else None
        for line in container.logs(stdout=True, stderr=True, stream=True, follow=True):
            text = line.decode("utf-8", errors="replace") if isinstance(line, (bytes, bytearray)) else str(line)
            print(text, end="")  # noqa: T201
            if collected is not None:
                collected.append(text)

        exit_status = container.wait()["StatusCode"]
        if exit_status != 0:
            raise Exception(f"Docker container returned exit code {exit_status}")
        return exit_status, ("".join(collected) if collected is not None else None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
        image = self.image; entrypoint = self.entrypoint; cmd = self.command
        env_vars = self.env_vars; network = self.network
        registry = self.registry; container_kwargs = self.container_kwargs
        needs_direct = self._needs_direct_client()

        # execute_docker_container's env_vars is List[str] of "KEY=value" (DockerContainerContext
        # treats a bare "KEY" entry with no "=" as "read this var name from the LAUNCHER's own
        # environment"), NOT a dict -- passing a dict directly iterates its KEYS only, each one
        # a bare var name, which silently pulls from the launcher's environment instead of the
        # user-configured value. Same real bug class fixed in k8s_job_asset earlier this session.
        env_var_list = [f"{k}={v}" for k, v in env_vars.items()] if env_vars else None
        network_list = [network] if network else None

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=f"Docker container: {image}",
            deps=deps_keys,
        )
        def _docker_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            metadata: Dict[str, Any] = {"image": dg.MetadataValue.text(image)}

            if needs_direct:
                _exit_status, output = self._execute_direct(
                    image, entrypoint, cmd, env_var_list, network_list, registry, container_kwargs,
                )
                if output is not None:
                    metadata["container_output"] = dg.MetadataValue.text(output[-4000:])
                return dg.MaterializeResult(metadata=metadata)

            from dagster_docker import execute_docker_container

            merged_kwargs = dict(container_kwargs or {})
            if self.mounts:
                from docker.types import Mount
                merged_kwargs["mounts"] = [Mount(**m) for m in self.mounts]
            if self.ports:
                merged_kwargs["ports"] = self.ports
            if self.auto_remove:
                merged_kwargs["auto_remove"] = True

            if self.force_pull:
                import docker
                docker.from_env().images.pull(image)

            execute_docker_container(
                context=context, image=image, entrypoint=entrypoint or None, command=cmd or None,
                env_vars=env_var_list, networks=network_list,
                registry=registry or None, container_kwargs=merged_kwargs or None,
            )
            return dg.MaterializeResult(metadata=metadata)
        return dg.Definitions(assets=[_docker_asset])
