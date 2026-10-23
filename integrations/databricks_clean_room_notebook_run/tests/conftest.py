import importlib.util
from pathlib import Path


def load_component_module():
    here = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("databricks_clean_room_notebook_run_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
