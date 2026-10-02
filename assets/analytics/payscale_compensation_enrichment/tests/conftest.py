"""Shared test helpers for PayscaleCompensationEnrichmentComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The real external call -- a PayScale report
lookup -- is isolated behind this component's own module-level
`_fetch_compensation_report(resource, answers)` function, which is the ONE
thing these tests monkeypatch (mirroring the repo's "mock only the paid/
external call" convention). No real HTTP, and no real `payscale_resource`,
is ever needed to exercise this component's logic.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "payscale_compensation_enrichment_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResource:
    """Stand-in for an injected payscale_resource. Not used directly by
    these tests (which monkeypatch `_fetch_compensation_report` instead),
    but kept available for any test that wants to assert the resource
    object itself is passed through unchanged."""

    def __init__(self):
        self.calls = []

    def get_pay_report(self, answers):
        self.calls.append(answers)
        return {"BasePayReport": {"Percentile50": 90000}}
