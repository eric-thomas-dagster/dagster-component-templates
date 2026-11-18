"""PipesPythonAssetComponent.

Run a Python script in a fresh virtualenv or a given external interpreter, through Dagster
Pipes -- the Airflow `@task.virtualenv` / `@task.external_python` equivalent.

Verified real gap before building: this repo's two existing Pipes components (`ecs_pipes_asset`,
`modal_pipes_asset`) both use `dagster.PipesSubprocessClient`/other Pipes clients only against
hosted containers (an ECS task, a Modal function) -- neither creates a local virtualenv or
targets a given local interpreter. No component anywhere in this repo calls `venv.create` or
similar.

The script itself must call `dagster_pipes.open_dagster_pipes()` and report back via
`context.report_asset_materialization(...)` -- same convention as `ecs_pipes_asset`/
`modal_pipes_asset`'s `app_file`/task definitions: this component launches an EXISTING script
you wrote, it does not serialize a Python callable's body into another interpreter (Dagster
Pipes itself is script-based, not callable-serialization-based).
"""
import hashlib
import os
import subprocess
import sys
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN generic timeout wrapper (inlined per component; keep copies identical — see tests/test_generic_timeout_drift.py)
def _run_with_timeout(fn, timeout_seconds, *args, **kwargs):
    """Bounds how long this step waits for `fn`, raising dg.Failure past timeout_seconds.
    Python threads can't be forcibly killed -- fn keeps running in the background past the
    deadline (same documented trade-off as this repo's own @timeout decorator); this bounds
    the STEP's wait, not the underlying call itself. Deliberately NOT `with ThreadPoolExecutor()`
    -- that form's __exit__ calls shutdown(wait=True), which blocks until the background call
    finishes anyway, silently defeating the timeout for a call that hangs indefinitely."""
    if not timeout_seconds:
        return fn(*args, **kwargs)
    import concurrent.futures
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn, *args, **kwargs)
    try:
        result = future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError:
        executor.shutdown(wait=False)
        raise dg.Failure(description=f"Exceeded timeout_seconds={timeout_seconds}.") from None
    executor.shutdown(wait=False)
    return result
# END generic timeout wrapper


def _venv_python_path(venv_path: str) -> str:
    if os.name == "nt":
        return os.path.join(venv_path, "Scripts", "python.exe")
    return os.path.join(venv_path, "bin", "python")


def _ensure_venv(venv_path: str, requirements: List[str], reuse: bool, log) -> str:
    """Builds (or reuses) a virtualenv at `venv_path` with `dagster-pipes` + `requirements`
    installed. Tracks a hash of the requirements list in `.requirements_hash` inside the venv
    so a changed requirements list rebuilds, and an unchanged one (with reuse=True) doesn't."""
    python_bin = _venv_python_path(venv_path)
    marker_path = os.path.join(venv_path, ".requirements_hash")
    req_hash = hashlib.sha256("\n".join(sorted(requirements)).encode("utf-8")).hexdigest()

    if reuse and os.path.exists(python_bin) and os.path.exists(marker_path):
        with open(marker_path) as f:
            if f.read().strip() == req_hash:
                log.info(f"Reusing existing venv at {venv_path} (requirements unchanged).")
                return python_bin

    import venv as venv_module
    log.info(f"Building venv at {venv_path} (requirements: {requirements})...")
    venv_module.create(venv_path, with_pip=True, clear=True)
    pip_bin = os.path.join(venv_path, "Scripts", "pip.exe") if os.name == "nt" else os.path.join(venv_path, "bin", "pip")
    subprocess.run([pip_bin, "install", "--quiet", "dagster-pipes", *requirements], check=True)
    with open(marker_path, "w") as f:
        f.write(req_hash)
    return python_bin


class PipesPythonAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Python script in a fresh virtualenv or a given external interpreter, through
    Dagster Pipes -- the Airflow `@task.virtualenv` / `@task.external_python` equivalent.

    The script must call `dagster_pipes.open_dagster_pipes()` itself and report back via the
    Pipes context (same convention as this repo's other Pipes components).
    """

    asset_name: str = Field(description="Dagster asset key emitted by this component. The script may also report additional asset materializations via the Pipes context.")
    script_path: str = Field(description="Path (relative to the Dagster process working directory) to the Python script to run. Must call dagster_pipes.open_dagster_pipes() itself.")

    mode: Literal["virtualenv", "external_python"] = Field(
        default="virtualenv",
        description="'virtualenv': build/reuse a fresh venv from `requirements`. 'external_python': run against an existing interpreter at `python_executable`, no venv created.",
    )
    requirements: Optional[List[str]] = Field(default=None, description="mode='virtualenv' only: pip package specs to install into the venv, alongside dagster-pipes (always installed).")
    venv_path: Optional[str] = Field(default=None, description="mode='virtualenv' only: where to build/cache the venv. Defaults to './.pipes_venvs/{asset_name}'.")
    reuse_venv: bool = Field(default=True, description="mode='virtualenv' only: reuse the existing venv at venv_path if its installed requirements already match (tracked via a hash file). False always rebuilds.")
    python_executable: Optional[str] = Field(default=None, description="mode='external_python' only (required then): path to an existing Python interpreter to run the script with.")

    args: Optional[List[str]] = Field(default=None, description="Extra command-line arguments appended after the script path.")
    env_vars: Optional[Dict[str, str]] = Field(default=None, description="Additional env vars for the script's process (merged with the parent process's own environment).")
    cwd: Optional[str] = Field(default=None, description="Working directory for the script.")
    extras: Optional[Dict[str, Any]] = Field(default=None, description="Arbitrary extra data passed through to the script via the Pipes context (PipesSubprocessClient.run(extras=...)).")

    group_name: str = Field(default="infrastructure", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy. A retry re-runs the script (and, in mode='virtualenv', may rebuild the venv if reuse_venv=False).")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    pool: Optional[str] = Field(
        default=None,
        description=(
            "Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed "
            "number of slots shared across every asset/op using the same pool name). The "
            "pool's own slot count is configured at the deployment level (dagster.yaml's "
            "concurrency: pools:, or the Dagster+ UI), not here."
        ),
    )
    timeout_seconds: Optional[float] = Field(
        default=None,
        description="Fail this asset if the script hasn't finished after this many seconds. Enforced via a background-thread wrapper -- PipesSubprocessClient.run() has no native timeout, and on timeout the spawned child process is NOT terminated (Python threads can't be forcibly killed, and this component has no handle on the Popen object PipesSubprocessClient creates internally); this only bounds how long the asset step waits, leaving an orphaned process running until it exits on its own.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        if not isinstance(data, dict):
            return data
        out = dict(data)
        for k in ("requirements", "args", "deps", "owners", "kinds"):
            v = out.get(k)
            if isinstance(v, str):
                out[k] = [x.strip() for x in v.split(",") if x.strip()]
        v = out.get("asset_tags")
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            out["asset_tags"] = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        return out

    @model_validator(mode="after")
    def _validate_mode_fields(self):
        if self.mode == "virtualenv":
            if self.python_executable:
                raise ValueError("PipesPythonAssetComponent: `python_executable` is only used with mode='external_python'.")
        else:
            if not self.python_executable:
                raise ValueError("PipesPythonAssetComponent: mode='external_python' requires `python_executable`.")
            if self.requirements:
                raise ValueError("PipesPythonAssetComponent: `requirements` is only used with mode='virtualenv'.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Pipes Python Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]
        venv_path = component.venv_path or os.path.join(".pipes_venvs", component.asset_name)

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(component.asset_name),
            description=component.description,
            group_name=component.group_name,
            deps=deps_keys,
            owners=component.owners or [],
            tags=component.asset_tags or None,
            kinds=set(component.kinds) if component.kinds else None,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _pipes_asset(context: dg.AssetExecutionContext):
            if component.mode == "virtualenv":
                python_bin = _ensure_venv(venv_path, component.requirements or [], component.reuse_venv, context.log)
            else:
                python_bin = component.python_executable

            command = [python_bin, component.script_path, *(component.args or [])]
            client = dg.PipesSubprocessClient(env=component.env_vars or None, cwd=component.cwd)

            def _do_run():
                invocation = client.run(context=context, command=command, extras=component.extras)
                return invocation.get_materialize_result()

            return _run_with_timeout(_do_run, component.timeout_seconds)

        return dg.Definitions(assets=[_pipes_asset])
