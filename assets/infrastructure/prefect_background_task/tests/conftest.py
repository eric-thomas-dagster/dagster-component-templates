"""Test helpers for PrefectBackgroundTaskAssetComponent: loads the real component.py by path (no
installed package needed), stubs `prefect.settings` (not a repo dependency), and registers a fake
importable task module in sys.modules so `_import_task`'s real importlib.import_module resolves it."""
import contextlib
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


class FakeTaskRun:
    def __init__(self, task_run_id="task-run-1", state=None):
        self.task_run_id = task_run_id
        self.state = state or FakeState()


class FakeFuture:
    def __init__(self, task_run_id="task-run-1"):
        self.task_run_id = task_run_id


class FakeTask:
    """Stands in for an @task-decorated function: records .delay()'s kwargs."""
    last_kwargs = None

    def delay(self, **kwargs):
        FakeTask.last_kwargs = kwargs
        return FakeFuture()


def install_fakes():
    FakeTask.last_kwargs = None

    prefect_pkg = sys.modules.get("prefect") or types.ModuleType("prefect")
    settings_mod = types.ModuleType("prefect.settings")
    settings_mod.PREFECT_API_KEY = "PREFECT_API_KEY"
    settings_mod.PREFECT_API_URL = "PREFECT_API_URL"
    settings_mod.temporary_settings = lambda *a, **kw: contextlib.nullcontext()
    prefect_pkg.settings = settings_mod
    sys.modules["prefect"] = prefect_pkg
    sys.modules["prefect.settings"] = settings_mod

    task_mod = types.ModuleType("_fake_prefect_task_module")
    task_mod.my_task = FakeTask()
    sys.modules["_fake_prefect_task_module"] = task_mod
    return FakeTask


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("prefect_background_task_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
