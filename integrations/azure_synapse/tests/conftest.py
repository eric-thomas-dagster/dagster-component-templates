"""Shared test helpers for AzureSynapseComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed -- and, since azure-identity/azure-mgmt-synapse/
azure-synapse-artifacts are NOT installed in this environment at all, this
also confirms the module's azure imports are genuinely lazy (deferred into
the functions that need them), not at module load time.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("azure_synapse_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
