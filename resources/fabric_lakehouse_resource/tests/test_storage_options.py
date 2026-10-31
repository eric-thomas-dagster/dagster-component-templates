"""Real, committed test for a gap found while comparing fabric-family
components against Microsoft's own Airflow provider / verifying delta-rs'
Azure auth options: with no service principal configured, this resource
(and its siblings fabric_lakehouse_io_manager / dataframe_to_fabric_lakehouse)
FORCED `use_azure_cli` storage_options -- which only works with an
interactive `az login` session and fails outright in any non-interactive
deployment (CI, a Dagster+ agent, an Azure VM). Confirmed via object_store's
own AzureConfigKey docs that `CredentialType`, when not forced, follows "the
default resolution order" (which can include managed identity) -- the
actual equivalent this storage layer has to azure-identity's
DefaultAzureCredential chain that fabric_workspace already uses on its own
(unrelated) Fabric-API auth surface. Fixed: use_azure_cli is now an explicit
opt-in field; the no-credential default sends an empty storage_options dict
instead of forcing CLI auth.
"""


def test_service_principal_creds_win_when_present(mod, monkeypatch):
    monkeypatch.setenv("T", "tenant-1")
    monkeypatch.setenv("C", "client-1")
    monkeypatch.setenv("S", "secret-1")
    res = mod.FabricLakehouseResource(
        workspace_id="ws-1", lakehouse_name="lh1",
        tenant_id_env_var="T", client_id_env_var="C", client_secret_env_var="S",
        use_azure_cli=True,  # should be ignored -- service principal takes precedence
    )
    assert res.storage_options() == {"azure_tenant_id": "tenant-1", "azure_client_id": "client-1", "azure_client_secret": "secret-1"}


def test_no_credential_sends_empty_dict_not_forced_cli(mod):
    res = mod.FabricLakehouseResource(workspace_id="ws-1", lakehouse_name="lh1")
    assert res.storage_options() == {}


def test_use_azure_cli_opts_in_explicitly(mod):
    res = mod.FabricLakehouseResource(workspace_id="ws-1", lakehouse_name="lh1", use_azure_cli=True)
    assert res.storage_options() == {"use_azure_cli": "true"}


def test_table_path_and_file_path_unaffected(mod):
    res = mod.FabricLakehouseResource(workspace_id="ws-1", lakehouse_name="lh1")
    assert res.table_path("my_table") == "abfss://ws-1@onelake.dfs.fabric.microsoft.com/lh1.Lakehouse/Tables/my_table"
    assert res.file_path("a/b.csv") == "abfss://ws-1@onelake.dfs.fabric.microsoft.com/lh1.Lakehouse/Files/a/b.csv"
