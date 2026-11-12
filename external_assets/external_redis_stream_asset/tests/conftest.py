"""Test helpers for ExternalRedisStreamAsset.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("external_redis_stream_asset_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeObserveResource:
    """Stands in for a resource exposing `.observe(stream_name) -> dict` -- the
    pre-existing resource_key contract documented on this component's field."""

    def __init__(self, observed):
        self._observed = observed
        self.calls = []

    def observe(self, stream_name):
        self.calls.append(stream_name)
        return dict(self._observed)
