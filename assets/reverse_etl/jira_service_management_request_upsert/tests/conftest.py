"""Shared test helpers for JiraServiceManagementRequestUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. `FakeJiraServiceManagementResource`
stands in for the real resource (the one external HTTP boundary),
matching this repo's convention of mocking at the resource-object level
(never raw `requests`) while exercising all of this component's own logic
(dual source resolution, JQL-clause building, validation, counting) for
real.

The fake's `search_issues_jql` does an exact-string lookup against JQL
strings seeded ahead of time via `seed_issue(jql, issue)` -- the test
constructs that expected JQL using the SAME `_jql_match_clause` helper the
component itself uses (exposed from the loaded module), so the test stays
honest about what JQL the component actually builds.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "jira_service_management_request_upsert_component", component_py
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


class FakeJiraServiceManagementResource:
    """In-memory stand-in for JiraServiceManagementResource.

    `search_issues_jql` does a literal dict lookup keyed by the exact JQL
    string it was told to expect (via `seed_issue`), rather than actually
    parsing JQL -- this keeps the fake simple while still exercising the
    component's real `_jql_match_clause` output end-to-end (the test seeds
    using that same helper).
    """

    def __init__(self):
        self._issues_by_jql: dict = {}
        self._issue_counter = 0

        self.search_calls: list = []
        self.create_calls: list = []
        self.update_calls: list = []
        self.comment_calls: list = []

        # Toggle to force an exception out of a resource call, to exercise
        # the component's per-row try/except error handling.
        self.raise_on_create = False
        self.raise_on_update = False

    def seed_issue(self, jql: str, issue_key: str, fields: dict = None):
        """Pre-seed a request that a given JQL string should match."""
        self._issues_by_jql[jql] = {"key": issue_key, "id": issue_key, "fields": fields or {}}

    def search_issues_jql(self, jql, fields=None, max_results=50):
        self.search_calls.append(jql)
        match = self._issues_by_jql.get(jql)
        return [match] if match else []

    def create_request(self, service_desk_id, request_type_id, request_field_values, raise_on_behalf_of=None):
        self.create_calls.append({
            "service_desk_id": service_desk_id,
            "request_type_id": request_type_id,
            "request_field_values": dict(request_field_values),
            "raise_on_behalf_of": raise_on_behalf_of,
        })
        if self.raise_on_create:
            raise RuntimeError("simulated create_request failure")
        self._issue_counter += 1
        issue_key = f"ITSM-{self._issue_counter}"
        return {"issueId": str(1000 + self._issue_counter), "issueKey": issue_key}

    def update_issue_fields(self, issue_id_or_key, fields):
        self.update_calls.append({"issue_id_or_key": issue_id_or_key, "fields": dict(fields)})
        if self.raise_on_update:
            raise RuntimeError("simulated update_issue_fields failure")

    def add_request_comment(self, issue_id_or_key, body_text, public=True):
        self.comment_calls.append({
            "issue_id_or_key": issue_id_or_key,
            "body_text": body_text,
            "public": public,
        })
        return {"id": "comment-1"}
