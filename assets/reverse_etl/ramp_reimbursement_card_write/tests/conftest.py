"""Shared test helpers for RampReimbursementCardWriteComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. `FakeRampResource` stands in for the
real `RampResource` (the one external, paid-API boundary) -- it records
every call so tests can assert exact request shapes without ever hitting
the network. Its `create_virtual_card` returns a response shaped exactly
like Ramp's documented Vault API response (including pan/cvv), so tests
can verify the component redacts them before they reach metadata.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "ramp_reimbursement_card_write_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    """A fresh @asset closure per call -- needed because materializing the
    same function object twice under different DataFrames in one test file
    would otherwise share the same closed-over `df`."""
    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


class FakeRampResource:
    """In-memory stand-in for `RampResource`. Mirrors its real method
    signatures exactly (same positional/keyword args) so the component's
    calls are exercised for real."""

    def __init__(self, fail_on=None):
        self.mileage_calls = []
        self.receipt_calls = []
        self.virtual_card_calls = []
        self.card_update_calls = []
        self._fail_on = fail_on or set()
        self._next_card_id = 1000

    def create_mileage_reimbursement(self, **kwargs):
        if "mileage" in self._fail_on:
            raise RuntimeError("HTTP 400: simulated mileage failure")
        self.mileage_calls.append(kwargs)
        return {"id": f"reimb_{len(self.mileage_calls)}", "state": "DRAFT"}

    def upload_reimbursement_receipt(self, **kwargs):
        if "receipt" in self._fail_on:
            raise RuntimeError("HTTP 400: simulated receipt failure")
        self.receipt_calls.append(kwargs)
        return {"id": f"reimb_{len(self.receipt_calls)}", "state": "DRAFT"}

    def create_virtual_card(self, **kwargs):
        if "virtual_card" in self._fail_on:
            raise RuntimeError("HTTP 403: simulated Vault API failure (production review pending)")
        self.virtual_card_calls.append(kwargs)
        self._next_card_id += 1
        card_id = f"card_{self._next_card_id}"
        return {
            "spend_limit_id": f"sl_{self._next_card_id}",
            "user_id": kwargs.get("user_id"),
            "display_name": kwargs.get("display_name"),
            "card": {
                "id": card_id,
                "pan": "4111111111111111",
                "cvv": "123",
                "expiration": "2030-01",
            },
        }

    def update_physical_card(self, **kwargs):
        if "card_update" in self._fail_on:
            raise RuntimeError("HTTP 400: simulated card update failure")
        self.card_update_calls.append(kwargs)
        return {"id": kwargs.get("card_id"), "display_name": kwargs.get("display_name")}
