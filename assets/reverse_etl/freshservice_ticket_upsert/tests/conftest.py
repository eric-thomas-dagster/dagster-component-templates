"""Shared test helpers for FreshserviceTicketUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real `requests`-based HTTP calls
are never made -- FakeFreshserviceResource stands in for the one
external-call boundary (create_ticket / update_ticket / filter_tickets),
matching the convention of mocking only the external call while
exercising all of this component's own logic (dual source resolution,
key_field validation, fields_map splitting, in-run cache, error handling,
metadata) for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "freshservice_ticket_upsert_component", component_py
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


class FakeFreshserviceResource:
    """In-memory stand-in for FreshserviceResource. Simulates tickets as
    dicts keyed by autoincrement id, with a nested `custom_fields` dict
    just like the real Freshservice ticket payload shape. `filter_tickets`
    parses the `"field:'value'"` query shape and matches against either a
    top-level field or a key inside `custom_fields`."""

    def __init__(self, fail_on_create_for=None, fail_on_update_for=None):
        self.tickets: dict = {}
        self._next_id = 1
        self.create_calls: list = []
        self.update_calls: list = []
        self.filter_calls: list = []
        # Optional: key values that should raise when create/update is attempted,
        # to exercise per-row error handling.
        self._fail_on_create_for = set(fail_on_create_for or [])
        self._fail_on_update_for = set(fail_on_update_for or [])

    def create_ticket(self, fields: dict) -> dict:
        self.create_calls.append(fields)
        custom = fields.get("custom_fields") or {}
        marker = custom.get("unique_external_id") or fields.get("subject")
        if marker in self._fail_on_create_for:
            raise RuntimeError(f"simulated create failure for {marker!r}")
        ticket_id = self._next_id
        self._next_id += 1
        ticket = {"id": ticket_id, **fields}
        self.tickets[ticket_id] = ticket
        return ticket

    def update_ticket(self, ticket_id, fields: dict) -> dict:
        self.update_calls.append((ticket_id, fields))
        custom = fields.get("custom_fields") or {}
        marker = custom.get("unique_external_id") or fields.get("subject")
        if marker in self._fail_on_update_for:
            raise RuntimeError(f"simulated update failure for {marker!r}")
        ticket = self.tickets.setdefault(ticket_id, {"id": ticket_id})
        # Merge custom_fields rather than clobbering.
        if "custom_fields" in fields:
            merged_custom = dict(ticket.get("custom_fields") or {})
            merged_custom.update(fields["custom_fields"])
            ticket.update({k: v for k, v in fields.items() if k != "custom_fields"})
            ticket["custom_fields"] = merged_custom
        else:
            ticket.update(fields)
        return ticket

    def filter_tickets(self, query: str) -> list:
        self.filter_calls.append(query)
        field, _, rest = query.partition(":")
        value = rest.strip("'")
        matches = []
        for t in self.tickets.values():
            if field in t and str(t.get(field)) == value:
                matches.append(t)
                continue
            custom = t.get("custom_fields") or {}
            if field in custom and str(custom.get(field)) == value:
                matches.append(t)
        return matches

    def seed_ticket(self, ticket_id, **fields):
        """Pre-populate an existing ticket for update-path tests."""
        self.tickets[ticket_id] = {"id": ticket_id, **fields}
        self._next_id = max(self._next_id, ticket_id + 1)
        return self.tickets[ticket_id]
