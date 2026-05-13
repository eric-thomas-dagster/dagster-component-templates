# SAP CPI / Integration Suite Observation Sensor

Poll the SAP Integration Suite Message Processing Logs (MPL) OData API and emit `AssetObservation`s for completed iFlow runs. Lets Dagster track when SAP-side iFlows have run, with what status — **without Dagster owning the iFlow itself**.

## When to use this

- You have iFlows running on SAP Integration Suite (Cloud Integration, the successor to PI/PO)
- You want to **observe** them in Dagster's asset graph: status, message GUID, correlation ID, log timestamps
- Optionally: trigger downstream Dagster pipelines on iFlow completion

This is the **observation** half of the integration. To **trigger** an iFlow from Dagster, POST to its HTTPS endpoint with the bearer token (use [`rest_api_fetcher`](https://dagster-community-components-cli.vercel.app/c/rest_api_fetcher) or [`oauth_rest_ingestion`](https://dagster-community-components-cli.vercel.app/c/oauth_rest_ingestion)).

## Setup

1. **SAP BTP Cockpit** → Integration Suite → API Management → **OAuth Client Credentials** → create a client for your subaccount
2. Copy: `tmn_host` (Tenant Management Node URL), `client_id`, `client_secret`, `token_endpoint`
3. Configure [`oauth_token_resource`](../../resources/oauth_token_resource/) (client_credentials grant)
4. Configure this sensor

## What it emits

For each new MPL, an `AssetObservation` on the configured `asset_key` with metadata:

| Metadata key | Value |
|---|---|
| `sap_cpi.message_guid` | MPL GUID (cursor key) |
| `sap_cpi.status` | COMPLETED / FAILED / RETRY / etc. |
| `sap_cpi.iflow` | IntegrationFlow ID |
| `sap_cpi.log_start` | iFlow start timestamp |
| `sap_cpi.log_end` | iFlow end timestamp |
| `sap_cpi.correlation_id` | Correlation ID (for cross-system trace) |
| `sap_cpi.application_message_id` | Application Message ID (if set by sender) |

These show up in the asset's observation log in the Dagster UI — searchable by status, timeline-visualized.

## Cursor / dedup

The sensor tracks the max `MessageGuid` seen and uses it as a cursor — only newer MPLs trigger observations on the next poll. Re-runs of the sensor pick up where they left off.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `sensor_name` | `str` | yes | |
| `tmn_host` | `str` | yes | Integration Suite TMN URL |
| `iflow_id` | `str` | no | Filter to a single iFlow; empty = all |
| `asset_key` | `str` | yes | Slash-delimited |
| `oauth_token_resource_key` | `str` | yes | |
| `only_statuses` | `list` | no | Filter MPL statuses |
| `minimum_interval_seconds` | `int` | no | Default 60 |
| `batch_size` | `int` | no | Default 50 |
| `default_status` | `str` | no | `stopped` (default) / `running` |

## Where to find iFlow IDs

In Integration Suite UI: **Design → Integrations → <package>** → click the iFlow → top-of-page shows the **IntegrationFlow ID** (e.g. `orders_to_s4hana`). That's what goes in `iflow_id`.

## See also

- [`oauth_token_resource`](../../resources/oauth_token_resource/) — paired token manager
- [`sap_event_mesh_sensor`](../sap_event_mesh_sensor/) — event-driven sibling for Event Mesh
- [`airbyte_sync_sensor`](https://dagster-community-components-cli.vercel.app/c/airbyte_sync_sensor) — same pattern, different vendor
- Walkthrough: [`sap_cpi_pipeline.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/sap_cpi_pipeline.md)
