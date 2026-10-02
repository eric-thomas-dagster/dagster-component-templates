"""Shared test helpers for ConfluencePageUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real `ConfluenceResource` (which
would make HTTP calls to Confluence Cloud) is never used here -- a minimal
fake resource with an in-memory page store stands in for the one
external, paid-API boundary, matching the convention of mocking only the
external call while exercising all of this component's own logic (dual
source resolution, validation, title/body handling, batching, metadata)
for real.
"""
import importlib.util
import pathlib
from types import ModuleType
from typing import Optional

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "confluence_page_upsert_component", component_py
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


class FakeConfluenceResource:
    """Stands in for the real ConfluenceResource -- implements only the
    methods the sink calls (`upsert_page_by_title`, and the helpers it's
    built from), backed by an in-memory page store so search-then-write
    and version-number-plus-one logic can be verified for real rather than
    mocked away.
    """

    def __init__(self, fail_titles: Optional[set] = None):
        self.pages: dict = {}  # page_id -> page dict
        self._next_id = 1
        self.create_calls: list = []
        self.update_calls: list = []
        self.find_calls: list = []
        self.fail_titles = fail_titles or set()

    def find_page_by_title(self, space_id, title):
        self.find_calls.append((space_id, title))
        for p in self.pages.values():
            if p["spaceId"] == space_id and p["title"] == title:
                return p
        return None

    def get_page(self, page_id):
        return self.pages[page_id]

    def create_page(self, space_id, title, body_storage, status="current"):
        page_id = str(self._next_id)
        self._next_id += 1
        page = {
            "id": page_id,
            "spaceId": space_id,
            "title": title,
            "body": {"storage": {"value": body_storage}},
            "version": {"number": 1},
        }
        self.pages[page_id] = page
        self.create_calls.append(
            {"space_id": space_id, "title": title, "body_storage": body_storage}
        )
        return page

    def update_page(self, page_id, title, body_storage, current_version, status="current"):
        new_version = current_version + 1
        page = self.pages[page_id]
        page["title"] = title
        page["body"] = {"storage": {"value": body_storage}}
        page["version"] = {"number": new_version}
        self.update_calls.append(
            {
                "page_id": page_id,
                "title": title,
                "body_storage": body_storage,
                "current_version": current_version,
                "new_version": new_version,
            }
        )
        return page

    def upsert_page_by_title(self, space_id, title, body_storage):
        if title in self.fail_titles:
            raise RuntimeError(f"simulated failure for title={title!r}")
        existing = self.find_page_by_title(space_id, title)
        if existing:
            current = self.get_page(existing["id"])
            current_version = current["version"]["number"]
            page = self.update_page(existing["id"], title, body_storage, current_version)
            return {"action": "updated", "page": page}
        page = self.create_page(space_id, title, body_storage)
        return {"action": "created", "page": page}
