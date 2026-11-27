"""Shared test helpers for SlackResourceComponent (self-contained; loads component.py via
importlib so tests don't require the parent package to be pip-installed).

External boundaries mocked: `slack_sdk.WebClient.chat_postMessage` (token path) and
`requests.post` (webhook path) -- never a fake Slack server. EnvVar-wrapped fields only
resolve to real values once bound into an actual Dagster run, so tests drive the resource
through a tiny probe asset under dg.materialize().
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("slack_resource_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


def run_probe(mod, component, call_kwargs):
    """Builds the resource from `component`, runs one SlackPostResource.post_message() call
    inside a real materialized asset (so EnvVar fields resolve), and returns the result dict."""
    defs = component.build_defs(context=None)
    resource = defs.resources[component.resource_key]

    box = {}

    @dg.asset
    def _probe(slack_resource: mod.SlackPostResource):
        box["result"] = slack_resource.post_message(**call_kwargs)
        return dg.MaterializeResult()

    dg.materialize([_probe], resources={"slack_resource": resource})
    return box["result"]
