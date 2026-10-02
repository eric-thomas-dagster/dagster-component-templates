"""Shared test helpers for ZocdocAvailabilityUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. `FakeZocdocResource` stands in for the
real ZocdocResource; the one external HTTP call (`_zocdoc_put_timeslots`)
is monkeypatched wholesale in tests.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "zocdoc_availability_upsert_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


class FakeZocdocResource:
    """Stands in for the real ZocdocResource. `.get_client()` returns a
    bare object -- never used for a real network call since
    `_zocdoc_put_timeslots` is monkeypatched wholesale in every test."""

    def __init__(self, base_url="https://api-developer-sandbox.zocdoc.com/"):
        self._base_url = base_url

    def get_client(self):
        return object()

    def get_base_url(self):
        return self._base_url


def metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# A realistic upstream DataFrame row-shape with patient-adjacent-looking
# (but NOT actually patient-identifying -- this is a scheduling sink, not
# a patient data source) free-text fields, used by test_phi_safety.py to
# prove no row content of any kind leaks into metadata.
SENSITIVE_NOTE_VALUE = "Dr. Grant prefers no new-patient slots before 9am due to clinic staffing"
