"""Shared test helpers for Auth0UserUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. `FakeAuth0Resource` stands in for the
one external, paid-API boundary (the real Auth0 Management API) -- it
deliberately has NO delete method of any kind, matching the real
`auth0_resource`, so there is no way for this component's logic to
permanently remove a user even by accident. Everything this component
owns for real -- dual source resolution, validation, email matching,
sync vs. deactivate branching, ambiguous-match handling -- is exercised
against this fake.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "auth0_user_upsert_component", component_py
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


class FakeAuth0Resource:
    """In-memory stand-in for `Auth0Resource`. Has find_user_by_email /
    create_user / update_user / set_blocked -- and NOTHING else. There is
    no delete_user method, period; if any code path ever tried to call one
    it would raise AttributeError, exactly as it would against the real
    resource."""

    def __init__(self, existing_users=None):
        self.users = [dict(u) for u in (existing_users or [])]
        self._next_id = 1000
        self.create_calls = []
        self.update_calls = []
        self.set_blocked_calls = []

    def find_user_by_email(self, email):
        return [dict(u) for u in self.users if u.get("email") == email]

    def create_user(self, payload):
        self.create_calls.append(dict(payload))
        self._next_id += 1
        user = {"user_id": f"auth0|{self._next_id}", **payload}
        self.users.append(user)
        return dict(user)

    def update_user(self, user_id, payload):
        self.update_calls.append((user_id, dict(payload)))
        for u in self.users:
            if u["user_id"] == user_id:
                u.update(payload)
                return dict(u)
        raise ValueError(f"no such user: {user_id}")

    def set_blocked(self, user_id, blocked):
        self.set_blocked_calls.append((user_id, blocked))
        for u in self.users:
            if u["user_id"] == user_id:
                u["blocked"] = blocked
                return dict(u)
        raise ValueError(f"no such user: {user_id}")
