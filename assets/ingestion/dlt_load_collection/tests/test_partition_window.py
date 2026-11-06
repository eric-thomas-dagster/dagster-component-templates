"""source_type='rest_api' partition_window_params pushes the run's time window
into each selected resource's endpoint.params before rest_api_source() is
ever called -- and is ignored (with a warning) for the other two source
types, since pushing a window into a SQL table or a file glob isn't
meaningful the same way."""
import warnings

import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


CAPTURED = []


@pytest.fixture()
def capture_rest_api_source(monkeypatch):
    import dlt.sources.rest_api as rest_api_mod

    CAPTURED.clear()

    class _Permissive:
        def __getattr__(self, name):
            return self

        def __call__(self, *a, **k):
            return self

        def __iter__(self):
            return iter(())

    def fake_rest_api_source(config):
        CAPTURED.append(config)
        return _Permissive()

    monkeypatch.setattr(rest_api_mod, "rest_api_source", fake_rest_api_source)
    return CAPTURED


def test_partition_window_reaches_every_selected_resource(mod, monkeypatch, capture_rest_api_source):
    install_fake_dlt(monkeypatch, mod)
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="rest_api",
        client={"base_url": "https://x"},
        resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
        partition_type="daily", partition_start="2024-01-01", partition_date_column="created_at",
        partition_window_params={"start_param": "updated_since", "end_param": "updated_until"},
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False)
    assert result.success, result
    config = capture_rest_api_source[-1]
    params = config["resources"][0]["endpoint"]["params"]
    assert params["updated_since"] == "2024-01-02T00:00:00Z"
    assert params["updated_until"] == "2024-01-03T00:00:00Z"


def test_partition_window_ignored_with_a_warning_for_sql_database(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = mod.DltLoadCollectionComponent(
        asset_name="a", source_type="sql_database",
        connection_string="sqlite:///:memory:",
        resources=[{"name": "orders"}],
        partition_window_params={"start_param": "updated_since", "end_param": "updated_until"},
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        asset_def = component.build_defs(context=None).assets[0]
    assert any("partition_window_params" in str(w.message) for w in caught)
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
