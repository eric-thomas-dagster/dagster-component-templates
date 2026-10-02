"""Shared test helpers for FigmentStakingRewardsIngestionComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. `_fetch_rewards` (the one external,
paid-API boundary) is monkeypatched wholesale in tests, while request-body
construction (`_build_rewards_body`), response flattening
(`_flatten_rewards_payload`), and pagination following are all exercised
for real.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "figment_staking_rewards_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeFigmentResource:
    """Stands in for the real FigmentResource -- `.get_client()` returns a
    sentinel since `_fetch_rewards` is monkeypatched wholesale in tests and
    never makes a real request."""

    base_url = "https://api.figment.io"

    def get_client(self):
        return object()


def metadata_for(result, asset_name: str) -> dict:
    mats = result.asset_materializations_for_node(asset_name)[0]
    return {k: (v.value if hasattr(v, "value") else v) for k, v in mats.metadata.items()}


def output_value(result, node_name: str):
    return result.output_for_node(node_name, "result")
