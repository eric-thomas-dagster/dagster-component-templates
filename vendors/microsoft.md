# Microsoft

The Microsoft surface is **~25 components** across the entire MS data stack: SQL Server, Azure Synapse, Microsoft Fabric (Lakehouses + Pipelines + Capacity), Azure Data Factory, Power BI, Microsoft Graph, Dynamics 365, SharePoint, Azure OpenAI, Azure Log Analytics + Sentinel, Azure Search, Azure Blob storage, Office 365 audit logs, and Azure Activity logs.

The repo's [`examples/`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/tree/main/examples) folder has dedicated end-to-end walkthroughs for `msgraph`, `dynamics365`, and `sap_s4hana` (which uses OData — Microsoft's protocol).

## Components — by sub-area

### SQL Server / Synapse

| Component | Category | Validation |
|---|---|---|
| [`mssql_resource`](https://dagster-component-ui.vercel.app/c/mssql_resource) | resource | `live` |
| [`mssql_io_manager`](https://dagster-component-ui.vercel.app/c/mssql_io_manager) | io_manager | `live` |
| [`azure_synapse`](https://dagster-component-ui.vercel.app/c/azure_synapse) | integration | `live` |
| [`synapse_sql_pool_admin_job`](https://dagster-component-ui.vercel.app/c/synapse_sql_pool_admin_job) | jobs | `live` |

### Microsoft Fabric (Lakehouse + Pipelines + Capacity)

| Component | Category | Validation |
|---|---|---|
| [`fabric_workspace`](https://dagster-component-ui.vercel.app/c/fabric_workspace) | integration | `live` |
| [`fabric_workspace_resource`](https://dagster-component-ui.vercel.app/c/fabric_workspace_resource) | resource | `live` |
| [`fabric_lakehouse_resource`](https://dagster-component-ui.vercel.app/c/fabric_lakehouse_resource) | resource | `live` |
| [`fabric_lakehouse_io_manager`](https://dagster-component-ui.vercel.app/c/fabric_lakehouse_io_manager) | io_manager | `live` |
| [`dataframe_to_fabric_lakehouse`](https://dagster-component-ui.vercel.app/c/dataframe_to_fabric_lakehouse) | sink | `live` |
| [`fabric_pipeline_trigger_job`](https://dagster-component-ui.vercel.app/c/fabric_pipeline_trigger_job) | jobs | `live` |
| [`fabric_capacity_admin_job`](https://dagster-component-ui.vercel.app/c/fabric_capacity_admin_job) | jobs | `code` |

### Azure data integration

| Component | Category | Validation |
|---|---|---|
| [`azure_data_factory`](https://dagster-component-ui.vercel.app/c/azure_data_factory) | integration | `live` |
| [`azure_stream_analytics`](https://dagster-component-ui.vercel.app/c/azure_stream_analytics) | integration | `code` |

### Azure storage + queues

| Component | Category | Validation |
|---|---|---|
| [`azure_blob_parquet_io_manager`](https://dagster-component-ui.vercel.app/c/azure_blob_parquet_io_manager) | io_manager | `code` |
| [`azure_table_reader`](https://dagster-component-ui.vercel.app/c/azure_table_reader) | source | `live` |
| [`dataframe_to_azure_table`](https://dagster-component-ui.vercel.app/c/dataframe_to_azure_table) | sink | `live` |

### Azure Search + AI

| Component | Category | Validation |
|---|---|---|
| [`azure_search_query`](https://dagster-component-ui.vercel.app/c/azure_search_query) | source | `live` |
| [`azure_search_indexer`](https://dagster-component-ui.vercel.app/c/azure_search_indexer) | sink | `live` |
| [`azure_openai_resource`](https://dagster-component-ui.vercel.app/c/azure_openai_resource) | resource | `code` |

### Microsoft Graph / SharePoint / Office 365 / Dynamics

| Component | Category | What it covers |
|---|---|---|
| Dedicated Microsoft Graph clients across multiple components | resource / source / ingestion | OneDrive, Outlook, Teams, Calendar, People, Sites, Drives |
| [`external_sharepoint_library`](https://dagster-component-ui.vercel.app/c/external_sharepoint_library) | external | Declare-only SharePoint document library |
| [`sharepoint_monitor`](https://dagster-component-ui.vercel.app/c/sharepoint_monitor) | sensor | Watch SharePoint library for new files |
| [`o365_audit_log_ingestion`](https://dagster-component-ui.vercel.app/c/o365_audit_log_ingestion) | ingestion | M365 Unified Audit Log → Dagster asset |
| Dynamics 365 surface (via OData) | source / sink | See `examples/dynamics365_pipeline.md` |

### Azure security / observability

| Component | Category | Validation |
|---|---|---|
| [`azure_log_analytics_query`](https://dagster-component-ui.vercel.app/c/azure_log_analytics_query) | source | `live` |
| [`audit_logs_to_sentinel`](https://dagster-component-ui.vercel.app/c/audit_logs_to_sentinel) | sink | `live` |
| [`azure_activity_log_ingestion`](https://dagster-component-ui.vercel.app/c/azure_activity_log_ingestion) | ingestion | `code` |

## Walkthroughs

| Topic | Walkthrough |
|---|---|
| Microsoft Graph (OneDrive / Outlook / Teams / SharePoint) | [`examples/msgraph_pipeline.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/msgraph_pipeline.md) |
| Dynamics 365 (Sales / Customer Service) via OData | [`examples/dynamics365_pipeline.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/dynamics365_pipeline.md) |
| SAP S/4HANA (OData — same protocol as Dynamics) | [`examples/sap_s4hana_pipeline.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/sap_s4hana_pipeline.md) |
| External-asset declarations across MS surfaces | [`examples/external_assets.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/external_assets.md) |

## Connection / auth — quick reference

| Surface | Auth |
|---|---|
| MSSQL / Synapse | SQL Server auth or Azure AD via ODBC. Connection-string env var. |
| Fabric | Azure AD service principal OR Power BI workspace token |
| Microsoft Graph / SharePoint / Dynamics / O365 | Azure AD app registration (tenant_id + client_id + client_secret) with the appropriate API permissions |
| Azure OpenAI | Azure AD or API-key auth |
| Azure Log Analytics / Sentinel | Workspace ID + shared key OR Managed Identity |

The `azure-identity` library handles the credential discovery chain (env vars → Managed Identity → CLI → ...).

## Gotchas

- **MSGraph permission scopes** — adding a new MSGraph component often requires granting the corresponding application permission in the AAD app registration **AND admin-consenting** it. The error you'll see is `Insufficient privileges to complete the operation` — solve at the AAD side, not the component.
- **Fabric workspace IDs** are GUIDs, not friendly names. Find via the Fabric portal URL or the Power BI REST `groups` endpoint.
- **OData pagination** — Dynamics / SAP / many Graph endpoints use `@odata.nextLink`. The OData components handle this; if writing a custom op, don't stop at the first page.
- **SharePoint document libraries vs drives** — same underlying SharePoint storage; the Graph API exposes them as `drives` while the SharePoint REST exposes them as `lists`. Use the Graph path unless you have a specific reason not to.

## Roadmap

- **Power BI workspace auto-discovery** — Microsoft already has an official `dagster-powerbi` integration that handles datasets + reports. Community could complement with capacity / workspace admin actions.
- **Azure DevOps Pipelines** — trigger ADO pipelines + watch for completion (mirrors the Argo / Airflow / Precisely pattern). Not shipped.

## See also

- Official `dagster-powerbi` and `dagster-azure` integrations — prefer these for the core resource + IO-manager paths in their domains
- [Microsoft Graph docs](https://learn.microsoft.com/graph/)
- [Fabric REST API](https://learn.microsoft.com/rest/api/fabric/)
- [Azure SDK for Python](https://learn.microsoft.com/azure/developer/python/sdk/azure-sdk-overview)
