"""hash_columns: salted SHA-256 pseudonymization wired onto every resource via
DltResource.add_map() before pipeline.run(). No real HTTP call is made (rest_api_source
is only built, never iterated), so this checks the resource's pipe gained the extra
step rather than fetching real data -- same convention as dlt_load_collection's own
test_hash_columns.py::test_hash_columns_wired_into_rest_api_resource."""
import dagster as dg
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, monkeypatch, **overrides):
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    kwargs = dict(asset_name="test_asset", account="acct", api_token="tok")
    kwargs.update(overrides)
    component = mod.ActiveCampaignIngestionComponent(**kwargs)
    defs = component.build_defs(context=None)
    dg.materialize(list(defs.assets), resources=defs.resources)
    return fake_pipeline.run_calls[-1]


def _step_counts(source):
    return {name: len(resource._pipe._steps) for name, resource in source.resources.items()}


def test_hash_columns_adds_a_pipe_step_to_every_resource(mod, monkeypatch):
    plain = _step_counts(_materialize(mod, monkeypatch))
    hashed = _step_counts(_materialize(mod, monkeypatch, hash_columns=["email"], hash_salt="s"))
    assert set(plain) == set(hashed) and plain  # same resources, non-empty
    for name in plain:
        assert hashed[name] == plain[name] + 1  # exactly the one hash-map step was added


def test_hash_salt_required_when_hash_columns_set(mod, monkeypatch):
    with pytest.raises(Exception, match="hash_salt"):
        _materialize(mod, monkeypatch, hash_columns=["email"])


def test_hash_salt_env_var_unset_raises_clearly(mod, monkeypatch):
    monkeypatch.delenv("ACTIVECAMPAIGN_HASH_SALT_TEST_UNSET", raising=False)
    with pytest.raises(Exception, match="ACTIVECAMPAIGN_HASH_SALT_TEST_UNSET"):
        _materialize(
            mod, monkeypatch, hash_columns=["email"],
            hash_salt_env_var="ACTIVECAMPAIGN_HASH_SALT_TEST_UNSET",
        )
