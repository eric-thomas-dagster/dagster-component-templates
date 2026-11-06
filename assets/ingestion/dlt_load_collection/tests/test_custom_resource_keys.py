"""Real gap reported from production usage: a subclass adding its own checks
(a blocking rows-landed check, a column-contract check) needs custom
per-resource metadata (e.g. min_rows, contract_columns) in the resources
YAML. Any key not in this component's own known set broke rest_api's build
(dlt's EndpointResource validation rejects unknown keys) unless a subclass
stripped them manually first. The 'x_' prefix convention lets a resource
carry arbitrary subclass/user metadata that's guaranteed to never reach the
underlying dlt call, readable from the active resources field in a subclass."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_x_prefixed_keys_do_not_break_rest_api_resource_validation(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"orders": pd.DataFrame({"id": [1]})})
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{
            "name": "orders",
            "endpoint": {"path": "orders"},
            "x_min_rows": 10,
            "x_contract_columns": ["id", "amount"],
        }],
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success, result


def test_subclass_can_read_x_prefixed_keys_from_self_resources(mod, monkeypatch):
    """A subclass's own check logic reads the raw YAML-declared metadata directly off
    the active resources field -- this component makes no special effort to expose it
    beyond not stripping it from the field itself (only from what reaches dlt)."""
    component = mod.DltYamlSourceComponent(
        asset_name="a", source_type="rest_api", client={"base_url": "https://x"},
        rest_api_resources=[{"name": "orders", "endpoint": {"path": "orders"}, "x_min_rows": 10}],
    )
    assert component.rest_api_resources[0]["x_min_rows"] == 10
