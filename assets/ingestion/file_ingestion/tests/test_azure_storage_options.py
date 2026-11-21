"""Real, committed tests for a gap found against the ADF-converter feedback: file_ingestion already
read from any fsspec URI (s3://, gs://, abfss://, ...) via ambient credential discovery, but had no
way to supply an EXPLICIT Azure credential from an environment variable -- the same
azure_account_name_env_var/azure_account_key_env_var/azure_connection_string_env_var fields
cloud_storage_copy_asset already has. Confirms the shared `_azure_storage_options`/`_require_env`
block (drift-tested against cloud_storage_copy_asset's own copy) is correctly threaded through every
pandas reader `_read_file` dispatches to, and left as None (ambient discovery) for non-Azure URIs.
"""
import os
from unittest.mock import patch

import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(asset_name="orders", file_path="abfss://container@account.dfs.core.windows.net/orders.csv")
    kwargs.update(overrides)
    return mod.FileIngestionComponent(**kwargs)


# ── _azure_storage_options itself (pure function) ───────────────────────────────────────────────────

def test_non_azure_uri_returns_none(mod):
    assert mod._azure_storage_options("s3://bucket/key.csv", account_name_env_var="X") is None


def test_azure_uri_with_no_credentials_configured_returns_none(mod):
    assert mod._azure_storage_options("abfss://c@a.dfs.core.windows.net/x.csv") is None


def test_azure_uri_builds_account_name_and_key(mod, monkeypatch):
    monkeypatch.setenv("MY_ACCOUNT_NAME", "myaccount")
    monkeypatch.setenv("MY_ACCOUNT_KEY", "secret-key")
    opts = mod._azure_storage_options(
        "abfss://c@a.dfs.core.windows.net/x.csv",
        account_name_env_var="MY_ACCOUNT_NAME", account_key_env_var="MY_ACCOUNT_KEY",
    )
    assert opts == {"account_name": "myaccount", "account_key": "secret-key"}


def test_connection_string_takes_precedence_over_account_key(mod, monkeypatch):
    monkeypatch.setenv("MY_CONN", "DefaultEndpointsProtocol=https;...")
    monkeypatch.setenv("MY_KEY", "ignored-key")
    opts = mod._azure_storage_options(
        "abfss://c@a.dfs.core.windows.net/x.csv",
        account_key_env_var="MY_KEY", connection_string_env_var="MY_CONN",
    )
    assert opts == {"connection_string": "DefaultEndpointsProtocol=https;..."}


def test_missing_env_var_raises_a_clear_keyerror(mod, monkeypatch):
    monkeypatch.delenv("DOES_NOT_EXIST", raising=False)
    with pytest.raises(KeyError, match="DOES_NOT_EXIST"):
        mod._azure_storage_options("abfss://c@a.dfs.core.windows.net/x.csv", account_name_env_var="DOES_NOT_EXIST")


# ── threaded through FileIngestionComponent's fixed file_path (Branch B) ────────────────────────────

def test_fixed_file_path_passes_storage_options_to_pandas_for_an_azure_uri(mod, monkeypatch):
    monkeypatch.setenv("ACC_NAME", "myaccount")
    monkeypatch.setenv("ACC_KEY", "secret-key")
    component = _make_component(
        mod, file_path="abfss://container@myaccount.dfs.core.windows.net/orders.csv",
        azure_account_name_env_var="ACC_NAME", azure_account_key_env_var="ACC_KEY",
    )
    asset_def = component.build_defs(context=None).assets[0]

    captured = {}
    real_read_csv = pd.read_csv

    def _fake_read_csv(uri, **kwargs):
        captured.update(kwargs)
        return pd.DataFrame({"a": [1]})

    with patch("pandas.read_csv", _fake_read_csv):
        import dagster as dg
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)

    assert captured.get("storage_options") == {"account_name": "myaccount", "account_key": "secret-key"}


def test_fixed_file_path_passes_no_storage_options_for_a_non_azure_uri(mod):
    component = _make_component(mod, file_path="s3://bucket/orders.csv", azure_account_name_env_var="UNUSED")
    asset_def = component.build_defs(context=None).assets[0]

    captured = {}

    def _fake_read_csv(uri, **kwargs):
        captured.update(kwargs)
        return pd.DataFrame({"a": [1]})

    with patch("pandas.read_csv", _fake_read_csv):
        import dagster as dg
        dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=True)

    assert "storage_options" not in captured or captured["storage_options"] is None
