"""Test helpers for PrefectFlowRunAssetComponent: loads the real component.py by path (no installed
package needed), and stubs the `prefect` package (not a repo dependency) via sys.modules so the real
import inside build_defs()'s asset body succeeds without installing it."""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType


class FakeStateType:
    def __init__(self, value):
        self.value = value


class FakeState:
    def __init__(self, name="Completed", type_value="COMPLETED", message=""):
        self.name = name
        self.type = FakeStateType(type_value)
        self.message = message


class FakeFlowRun:
    def __init__(self, id="run-1", name="my-flow-run", state=None):
        self.id = id
        self.name = name
        self.state = state or FakeState()


class FakeRunDeployment:
    """Records the kwargs run_deployment was called with."""
    last_kwargs = None

    def __call__(self, **kwargs):
        FakeRunDeployment.last_kwargs = kwargs
        return FakeFlowRun()


def install_fake_prefect():
    FakeRunDeployment.last_kwargs = None
    prefect_pkg = types.ModuleType("prefect")
    deployments_mod = types.ModuleType("prefect.deployments")
    deployments_mod.run_deployment = FakeRunDeployment()
    prefect_pkg.deployments = deployments_mod
    sys.modules["prefect"] = prefect_pkg
    sys.modules["prefect.deployments"] = deployments_mod
    return deployments_mod.run_deployment


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("prefect_flow_run_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
