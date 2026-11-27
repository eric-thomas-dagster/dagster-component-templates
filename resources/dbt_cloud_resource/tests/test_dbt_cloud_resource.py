import os
from unittest.mock import MagicMock, patch

from .conftest import load_component_module

mod = load_component_module()


def test_disable_schedule_on_trigger_defaults_false(monkeypatch):
    monkeypatch.setenv("DBT_CLOUD_TOKEN", "tok")
    with patch("dagster_dbt.DbtCloudResource", MagicMock()) as mock_cls:
        component = mod.DbtCloudResourceComponent(
            auth_token_env_var="DBT_CLOUD_TOKEN", account_id=123
        )
        component.build_defs(context=None)
        _, kwargs = mock_cls.call_args
        assert kwargs["disable_schedule_on_trigger"] is False


def test_disable_schedule_on_trigger_explicit_true(monkeypatch):
    monkeypatch.setenv("DBT_CLOUD_TOKEN", "tok")
    with patch("dagster_dbt.DbtCloudResource", MagicMock()) as mock_cls:
        component = mod.DbtCloudResourceComponent(
            auth_token_env_var="DBT_CLOUD_TOKEN",
            account_id=123,
            disable_schedule_on_trigger=True,
        )
        component.build_defs(context=None)
        _, kwargs = mock_cls.call_args
        assert kwargs["disable_schedule_on_trigger"] is True


def test_resource_registered_under_resource_key(monkeypatch):
    monkeypatch.setenv("DBT_CLOUD_TOKEN", "tok")
    component = mod.DbtCloudResourceComponent(
        auth_token_env_var="DBT_CLOUD_TOKEN", account_id=123
    )
    defs = component.build_defs(context=None)
    assert "dbt_cloud_resource" in defs.resources
