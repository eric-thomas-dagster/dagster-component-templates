"""Real, committed test for a gap found while comparing fabric-family
components against Microsoft's own Airflow provider / verifying delta-rs'
Azure auth options: with no service principal configured, this asset (and
its siblings fabric_lakehouse_resource / fabric_lakehouse_io_manager)
FORCED `use_azure_cli` storage_options -- which only works with an
interactive `az login` session and fails outright in any non-interactive
deployment (CI, a Dagster+ agent, an Azure VM). Confirmed via object_store's
own AzureConfigKey docs that `CredentialType`, when not forced, follows "the
default resolution order" (which can include managed identity). Fixed:
use_azure_cli is now an explicit opt-in field; the no-credential default
sends an empty storage_options dict instead of forcing CLI auth. This also
removed a dead `from azure.identity import DefaultAzureCredential,
ClientSecretCredential` import that was never actually used -- the
component always built storage_options by hand from plain env var reads.

Only the `deltalake.write_deltalake` call is faked (the real network/OneLake
hop); everything else -- component resolution, asset building, the upstream
dependency wiring -- runs for real.
"""
import importlib.util
import pathlib

import dagster as dg
import pandas as pd
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("dataframe_to_fabric_lakehouse_under_test", _HERE / "component.py")
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

DataframeToFabricLakehouseComponent = _mod.DataframeToFabricLakehouseComponent


@dg.asset(key=dg.AssetKey("upstream_df"))
def _upstream_df() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2, 3]})


def _component(**overrides):
    attrs = dict(
        asset_name="lakehouse_table", upstream_asset_key="upstream_df",
        workspace_id="ws-1", lakehouse_name="lh1", table="my_table",
    )
    attrs.update(overrides)
    return DataframeToFabricLakehouseComponent(**attrs)


def _materialize(comp, monkeypatch):
    import deltalake
    captured = {}

    def _fake_write(path, df, storage_options=None, **kw):
        captured["storage_options"] = storage_options

    monkeypatch.setattr(deltalake, "write_deltalake", _fake_write)

    defs = comp.build_defs(None)
    writer = next(iter(defs.assets))
    result = dg.materialize([_upstream_df, writer], raise_on_error=True)
    assert result.success
    return captured


def test_no_credential_sends_empty_storage_options_not_forced_cli(monkeypatch):
    comp = _component()
    captured = _materialize(comp, monkeypatch)
    assert captured["storage_options"] == {}


def test_use_azure_cli_opts_in_explicitly(monkeypatch):
    comp = _component(use_azure_cli=True)
    captured = _materialize(comp, monkeypatch)
    assert captured["storage_options"] == {"use_azure_cli": "true"}


def test_service_principal_creds_win_over_use_azure_cli(monkeypatch):
    monkeypatch.setenv("T", "tenant-1")
    monkeypatch.setenv("C", "client-1")
    monkeypatch.setenv("S", "secret-1")
    comp = _component(
        tenant_id_env_var="T", client_id_env_var="C", client_secret_env_var="S",
        use_azure_cli=True,
    )
    captured = _materialize(comp, monkeypatch)
    assert captured["storage_options"] == {
        "azure_tenant_id": "tenant-1", "azure_client_id": "client-1", "azure_client_secret": "secret-1",
    }
