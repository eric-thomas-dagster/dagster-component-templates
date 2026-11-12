"""Test helpers for DorisQueryAssetComponent: loads component.py via importlib."""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("doris_query_asset_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeDorisResource:
    """Same method the real DorisResource exposes to this asset: get_engine()."""

    def __init__(self, engine):
        self._engine = engine
        self.get_engine_calls = 0

    def get_engine(self):
        self.get_engine_calls += 1
        return self._engine
