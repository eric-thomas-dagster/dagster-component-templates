"""Shared test helpers for LeverCandidateUpsertComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. A FakeLeverResource stands in for the real
`lever_resource` -- the one external-call boundary -- so tests exercise all
of this component's own logic (dual source resolution, email-match search,
tag/stage/archive branching, create-body construction, metadata) for real,
without ever touching `requests`.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "lever_candidate_upsert_component", component_py
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


class FakeLeverResource:
    """In-memory stand-in for `LeverResource`.

    Opportunities are stored in a list of dicts shaped like Lever's real
    `data` records: `{"id":..., "emails": [...], "tags": [...], "stage":
    ..., "archived_reason": None}`. Call-tracking lists let tests assert on
    exactly what was sent to each method.
    """

    def __init__(self, perform_as: str = "fake-user-id"):
        self.perform_as = perform_as
        self._opportunities: list = []
        self._id_counter = 0

        self.create_calls: list = []
        self.add_tags_calls: list = []
        self.update_stage_calls: list = []
        self.archive_calls: list = []

    def seed_opportunity(self, email: str, **fields) -> dict:
        """Test helper -- pre-populate an existing opportunity."""
        self._id_counter += 1
        opp = {
            "id": f"opp-{self._id_counter}",
            "emails": [email],
            "tags": [],
            "stage": None,
            "archived_reason": None,
        }
        opp.update(fields)
        self._opportunities.append(opp)
        return opp

    def _find_by_id(self, opportunity_id: str) -> dict:
        for opp in self._opportunities:
            if opp["id"] == opportunity_id:
                return opp
        raise AssertionError(f"no fake opportunity with id={opportunity_id!r}")

    # ------------------------------------------------------------------ reads

    def search_opportunities_by_email(self, email: str) -> list:
        return [opp for opp in self._opportunities if email in opp.get("emails", [])]

    # ----------------------------------------------------------------- writes

    def create_opportunity(self, body: dict, posting_id=None) -> dict:
        self.create_calls.append({"body": body, "posting_id": posting_id})
        self._id_counter += 1
        opp = {
            "id": f"opp-{self._id_counter}",
            "emails": body.get("emails", []),
            "name": body.get("name"),
            "headline": body.get("headline"),
            "tags": list(body.get("tags") or []),
            "stage": body.get("stage"),
            "archived_reason": None,
        }
        self._opportunities.append(opp)
        return opp

    def add_tags(self, opportunity_id: str, tags: list) -> dict:
        self.add_tags_calls.append({"opportunity_id": opportunity_id, "tags": tags})
        opp = self._find_by_id(opportunity_id)
        opp["tags"] = list(opp.get("tags") or []) + list(tags)
        return opp

    def update_stage(self, opportunity_id: str, stage_id: str) -> dict:
        self.update_stage_calls.append({"opportunity_id": opportunity_id, "stage_id": stage_id})
        opp = self._find_by_id(opportunity_id)
        opp["stage"] = stage_id
        return opp

    def archive_opportunity(self, opportunity_id: str, reason: str) -> dict:
        self.archive_calls.append({"opportunity_id": opportunity_id, "reason": reason})
        opp = self._find_by_id(opportunity_id)
        opp["archived_reason"] = reason
        return opp
