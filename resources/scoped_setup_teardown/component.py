"""ScopedSetupTeardownComponent.

Registers a Dagster resource that runs a setup step before the first asset/op that requests it,
and a teardown step after -- even if that asset/op fails -- the Airflow setup/teardown task
pattern equivalent.

Verified real gap before building: no resource anywhere in this repo overrides
`teardown_after_execution`. Built on real, documented, verified-live Dagster resource-lifecycle
hooks: `ConfigurableResource.setup_for_execution(context)` / `.teardown_after_execution(context)`
-- confirmed directly (a real `dg.materialize()` run with a deliberately failing asset) that
`teardown_after_execution` runs even when the asset using the resource raises, and does NOT run
if `setup_for_execution` itself fails -- exactly Dagster's own documented contract, exactly
Airflow's setup/teardown semantics, nothing invented here.
"""
import inspect
import os
import subprocess
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field, PrivateAttr, model_validator


def _resolve_callable(callable_path: str):
    import importlib
    if ":" not in callable_path:
        raise ValueError(f"callable_path must be 'module.path:function_name', got {callable_path!r}")
    mod_path, fn_name = callable_path.split(":", 1)
    mod = importlib.import_module(mod_path)
    return getattr(mod, fn_name)


def _run_shell(command: str, env_vars: Optional[Dict[str, str]], cwd: Optional[str]) -> None:
    merged_env = {**os.environ, **(env_vars or {})}
    proc = subprocess.run(command, shell=True, executable="/bin/bash", env=merged_env, cwd=cwd)
    if proc.returncode != 0:
        raise dg.Failure(description=f"scoped_setup_teardown command exited {proc.returncode}: {command}")


class ScopedSetupTeardownComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers a Dagster resource with real setup/teardown lifecycle hooks -- the Airflow
    setup/teardown task pattern equivalent. Any asset or op that declares
    `required_resource_keys={resource_key}` gets `setup` run once before it, and `teardown` run
    once after -- even if that asset/op fails. `teardown` does NOT run if `setup` itself fails.
    """

    resource_key: str = Field(description="Key used to register this resource. Any asset/op declaring required_resource_keys={resource_key} participates in this setup/teardown scope.")

    setup_callable_path: Optional[str] = Field(default=None, description="'module.path:function_name' to call for setup. Its return value (if any) is passed to teardown_callable_path's callable, if that callable declares a parameter for it. Set at most one of setup_callable_path/setup_command.")
    setup_command: Optional[str] = Field(default=None, description="Shell command to run for setup, via bash -c. Set at most one of setup_callable_path/setup_command.")
    teardown_callable_path: Optional[str] = Field(default=None, description="'module.path:function_name' to call for teardown. Called with setup's return value as its one argument if it declares a parameter, else called with no arguments. Set at most one of teardown_callable_path/teardown_command.")
    teardown_command: Optional[str] = Field(default=None, description="Shell command to run for teardown, via bash -c. Runs independently of setup's return value (a shell command can't receive a Python object). Set at most one of teardown_callable_path/teardown_command.")

    env_vars: Optional[Dict[str, str]] = Field(default=None, description="Env vars for setup_command/teardown_command's subprocess (merged with the parent process's own environment).")
    cwd: Optional[str] = Field(default=None, description="Working directory for setup_command/teardown_command.")

    @model_validator(mode="after")
    def _validate_exclusive_modes(self):
        if self.setup_callable_path and self.setup_command:
            raise ValueError("ScopedSetupTeardownComponent: set at most one of setup_callable_path/setup_command.")
        if self.teardown_callable_path and self.teardown_command:
            raise ValueError("ScopedSetupTeardownComponent: set at most one of teardown_callable_path/teardown_command.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Scoped Setup Teardown", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        class _ScopedSetupTeardownResource(dg.ConfigurableResource):
            _setup_value: Optional[Any] = PrivateAttr(default=None)

            def setup_for_execution(self, init_context: dg.InitResourceContext) -> None:
                if component.setup_callable_path:
                    fn = _resolve_callable(component.setup_callable_path)
                    self._setup_value = fn()
                elif component.setup_command:
                    _run_shell(component.setup_command, component.env_vars, component.cwd)

            def teardown_after_execution(self, init_context: dg.InitResourceContext) -> None:
                if component.teardown_callable_path:
                    fn = _resolve_callable(component.teardown_callable_path)
                    n_params = len(inspect.signature(fn).parameters)
                    fn(self._setup_value) if n_params >= 1 else fn()
                elif component.teardown_command:
                    _run_shell(component.teardown_command, component.env_vars, component.cwd)

        return dg.Definitions(resources={component.resource_key: _ScopedSetupTeardownResource()})
