"""`_build_resources` and `_build_pipeline` are real, overridable methods (not
closures trapped inside build_defs), so a subclass can swap either one out --
e.g. a demo-mode subclass overriding `_build_pipeline` to force a local
in-memory destination regardless of the configured `destination`, or
`_build_resources` to add a resource kind this component doesn't natively
support. Real gap reported from production usage: only
`_resolve_destination_with_bucket` was reachable before this."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakePipeline, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_build_resources_and_build_pipeline_are_real_overridable_methods(mod):
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
    )
    assert callable(getattr(component, "_build_resources", None))
    assert callable(getattr(component, "_build_pipeline", None))


def test_subclass_can_override_build_pipeline(mod, monkeypatch):
    calls = []

    class DemoModeComponent(mod.DltYamlSourceComponent):
        def _build_pipeline(self, pipeline_name, dataset_name):
            calls.append((pipeline_name, dataset_name))
            return FakePipeline(table_rows={"orders": pd.DataFrame({"id": [1]})})

    component = DemoModeComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        destination="snowflake", destination_credentials_env_var="UNSET",
        rest_api_resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    assert calls == [("a_pipeline", "a")]
    df = result.output_for_node("a")
    assert list(df["id"]) == [1]


def test_subclass_can_override_build_resources(mod, monkeypatch):
    from .conftest import install_fake_dlt

    calls = []

    class TrackingComponent(mod.DltYamlSourceComponent):
        def _build_resources(self, selected_names=None, run_context=None):
            calls.append(selected_names)
            return super()._build_resources(selected_names=selected_names, run_context=run_context)

    install_fake_dlt(monkeypatch, mod, table_rows={
        "orders": pd.DataFrame({"id": [1]}),
        "customers": pd.DataFrame({"id": [2]}),
    })
    component = TrackingComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[
            {"name": "orders", "endpoint": {"path": "orders"}},
            {"name": "customers", "endpoint": {"path": "customers"}},
        ],
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result
    # proves the OVERRIDE actually ran (not the base class directly), and received the
    # same selected_names/run_context signature a hand-rolled replacement would need.
    assert calls == [None]
    df = result.output_for_node("a")
    assert set(df["_resource_type"]) == {"orders", "customers"}
