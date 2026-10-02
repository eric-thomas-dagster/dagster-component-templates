"""Shared test helpers for DynamicsCrmRecordUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real `dynamics_crm_resource` (its
own OAuth + HTTP boundary, tested separately under
resources/dynamics_crm_resource/tests) is never imported here -- a minimal
fake resource stands in for it, exposing only `upsert_by_key(...)` the way
the real resource does, so this component's own logic (dual source
resolution, validation, per-row body construction / alternate-key
exclusion, action counting, metadata) is exercised for real while only the
one external network boundary is faked.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "dynamics_crm_record_upsert_component", component_py
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


class FakeDynamicsCrmResource:
    """Stands in for DynamicsCrmResource -- records every upsert_by_key()
    call and returns a scripted action/id per call (or raises, to exercise
    the error-collection path)."""

    def __init__(self, actions=None, id_prefix="guid-", raise_on=None):
        # `actions` -- optional list consumed in order (one action per
        # call); falls back to a fixed default when exhausted/unset.
        self._actions = list(actions) if actions is not None else None
        self._id_prefix = id_prefix
        # `raise_on` -- optional set of key_values that should raise
        # instead of returning, to simulate a per-row API error.
        self._raise_on = set(raise_on or [])
        self.calls = []

    def upsert_by_key(self, entity_set, key_name, key_value, body, *, prefer_representation=True):
        self.calls.append(
            {
                "entity_set": entity_set,
                "key_name": key_name,
                "key_value": key_value,
                "body": dict(body),
                "prefer_representation": prefer_representation,
            }
        )
        if key_value in self._raise_on:
            raise RuntimeError(f"simulated Dataverse error for {key_value}")

        if not prefer_representation:
            return {"action": "unknown", "id": f"{entity_set}({key_name}='{key_value}')"}

        if self._actions:
            action = self._actions[(len(self.calls) - 1) % len(self._actions)]
        else:
            action = "created"
        return {"action": action, "id": f"{self._id_prefix}{len(self.calls)}"}
