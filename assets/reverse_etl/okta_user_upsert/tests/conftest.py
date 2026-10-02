"""Shared test helpers for OktaUserUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. `FakeOktaResource` stands in for the
one external, paid-API boundary (the real Okta Users API) -- it
deliberately has NO delete method of any kind, matching the real
`okta_resource`, so there is no way for this component's logic to
permanently remove a user even by accident. Everything this component
owns for real -- dual source resolution, validation, login matching,
sync vs. deactivate branching, already-deactivated handling -- is
exercised against this fake.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "okta_user_upsert_component", component_py
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


class FakeOktaResource:
    """In-memory stand-in for `OktaResource`. Has find_user / create_user /
    update_user / deactivate_user -- and NOTHING else. There is no
    delete_user method, period; if any code path ever tried to call one it
    would raise AttributeError, exactly as it would against the real
    resource."""

    def __init__(self, existing_users=None):
        # existing_users: list of {"id", "status", "profile": {...}}
        self.users = [dict(u) for u in (existing_users or [])]
        self._next_id = 1000
        self.create_calls = []
        self.update_calls = []
        self.deactivate_calls = []

    def _find_by_login_or_email(self, identifier):
        for u in self.users:
            profile = u.get("profile", {})
            if profile.get("login") == identifier or profile.get("email") == identifier:
                return u
        return None

    def find_user(self, identifier):
        u = self._find_by_login_or_email(identifier)
        return dict(u) if u is not None else None

    def create_user(self, profile, credentials=None, activate=True):
        self.create_calls.append({"profile": dict(profile), "activate": activate})
        self._next_id += 1
        user = {"id": f"00u{self._next_id}", "status": "ACTIVE" if activate else "STAGED", "profile": dict(profile)}
        self.users.append(user)
        return dict(user)

    def update_user(self, user_id, profile):
        self.update_calls.append((user_id, dict(profile)))
        for u in self.users:
            if u["id"] == user_id:
                u["profile"].update(profile)
                return dict(u)
        raise ValueError(f"no such user: {user_id}")

    def deactivate_user(self, user_id, send_email=False):
        self.deactivate_calls.append((user_id, send_email))
        for u in self.users:
            if u["id"] == user_id:
                if u["status"] == "DEPROVISIONED":
                    raise RuntimeError("HTTP 400: user already DEPROVISIONED")
                u["status"] = "DEPROVISIONED"
                return
        raise ValueError(f"no such user: {user_id}")
