"""Shared test helpers for WorkableCandidateUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real Workable API is never called
-- a minimal in-memory FakeWorkableResource stands in for the one
external-call boundary (the resource object), matching the convention of
mocking only the resource while exercising all of this component's own
logic (dual source resolution, search-then-write branching, field
mapping, stage moves, validation, metadata) for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "workable_candidate_upsert_component", component_py
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


class FakeWorkableResource:
    """Stands in for the real WorkableResource -- an in-memory dict keyed
    by email simulates existing candidates, and every method call is
    tracked so tests can assert on what the component actually did."""

    def __init__(self, existing: dict | None = None):
        # email -> {"id":..., "stage":..., **fields}
        self._candidates: dict = dict(existing or {})
        self._id_counter = 1000
        self.find_calls: list = []
        self.create_job_calls: list = []
        self.create_talent_pool_calls: list = []
        self.update_calls: list = []
        self.move_calls: list = []

    def find_candidate_by_email(self, email: str) -> list:
        self.find_calls.append(email)
        cand = self._candidates.get(email)
        return [cand] if cand else []

    def create_job_candidate(self, shortcode: str, body: dict) -> dict:
        self.create_job_calls.append({"shortcode": shortcode, "body": body})
        self._id_counter += 1
        cand = {"id": self._id_counter, **body}
        self._candidates[body["email"]] = cand
        return cand

    def create_talent_pool_candidate(self, body: dict) -> dict:
        self.create_talent_pool_calls.append({"body": body})
        self._id_counter += 1
        cand = {"id": self._id_counter, **body}
        self._candidates[body["email"]] = cand
        return cand

    def update_candidate(self, candidate_id, body: dict) -> dict:
        self.update_calls.append({"candidate_id": candidate_id, "body": body})
        for cand in self._candidates.values():
            if cand["id"] == candidate_id:
                cand.update(body)
                return cand
        raise AssertionError(f"update_candidate called with unknown id {candidate_id!r}")

    def move_candidate(self, candidate_id, member_id, target_stage) -> dict:
        self.move_calls.append(
            {"candidate_id": candidate_id, "member_id": member_id, "target_stage": target_stage}
        )
        for cand in self._candidates.values():
            if cand["id"] == candidate_id:
                cand["stage"] = target_stage
                return cand
        raise AssertionError(f"move_candidate called with unknown id {candidate_id!r}")
