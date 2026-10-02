"""Shared test helpers for MSTeamsMessageSendComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real MS Teams webhook POST is
never made -- `_post_teams_message` (the one external boundary) is
monkeypatched wholesale in tests, while everything this component
actually owns (template rendering, card building, mode handling,
validation, per-row failure isolation, metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "msteams_message_send_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


class FakeTeamsClient:
    """Stands in for the real `dagster_msteams.TeamsClient` --
    `_post_teams_message` is monkeypatched wholesale in tests, so
    `.post_message` here is never actually invoked, but it's present so a
    misconfigured test that forgets to monkeypatch fails loudly instead of
    hitting the real network."""

    def post_message(self, payload):
        raise RuntimeError("real Teams webhook POST attempted in a test -- monkeypatch _post_teams_message")


class FakeMSTeamsResource:
    """Stands in for the real MSTeamsResourceComponent-registered resource."""

    def get_client(self) -> FakeTeamsClient:
        return FakeTeamsClient()


def metadata_for(result, asset_name: str) -> dict:
    """Output(metadata=...) carries its data on the materialization event,
    not the step output -- output_for_node() returns a sentinel for these,
    so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
