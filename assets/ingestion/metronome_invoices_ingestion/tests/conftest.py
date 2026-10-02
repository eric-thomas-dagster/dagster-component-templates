"""Shared test helpers for MetronomeInvoicesIngestionComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. `_list_customers_page` and
`_list_invoices_page` (the two external, paid-API boundaries) are
monkeypatched wholesale in tests -- pagination following, customer
resolution, and DataFrame assembly are all exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "metronome_invoices_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeMetronomeResource:
    """Stands in for the real MetronomeResource -- `.get_client()` returns a
    sentinel since `_list_customers_page` / `_list_invoices_page` are
    monkeypatched wholesale in tests and never make a real request."""

    base_url = "https://api.metronome.com/v1"

    def get_client(self):
        return object()


def metadata_for(result, asset_name: str) -> dict:
    """Reads an asset's materialization metadata for a single-asset job."""
    mats = result.asset_materializations_for_node(asset_name)[0]
    return {k: (v.value if hasattr(v, "value") else v) for k, v in mats.metadata.items()}


def output_value(result, node_name: str):
    """Reads the Output's value (the DataFrame) for a single-asset job."""
    return result.output_for_node(node_name, "result")
