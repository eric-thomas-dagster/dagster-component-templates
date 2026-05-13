# SAP Event Mesh Sensor

Poll an SAP Event Mesh queue and trigger Dagster runs per event. Real-time-ish (latency bounded by polling interval).

SAP Event Mesh = AMQP 1.0 broker on BTP. Fronts S/4HANA business events, SuccessFactors events, custom app events. This sensor uses Event Mesh's REST endpoint (simpler than AMQP-WebSocket).

## Two operating modes

### Mode A — trigger a Dagster job per event

```yaml
job_name: process_order_event_job
```

Each message becomes one `RunRequest` against the named job. Useful when the event payload is small and you want one-event-one-run.

### Mode B — register a dynamic partition per event

```yaml
asset_selection: [process_invoice]
dynamic_partition_name: invoice_events
partition_key_template: "{message_id}"
```

Each message registers a new dynamic partition (named by the template) and fires a `RunRequest` materializing that partition. This is the **production-shape pattern** — partitions are re-runnable, message history is preserved in Dagster's run log.

## Auth — OAuth client_credentials via XSUAA

Pair with [`oauth_token_resource`](../../resources/oauth_token_resource/):

```yaml
# resources/em_token.yaml
type: dagster_component_templates.OAuthTokenResourceComponent
attributes:
  resource_key: em_token
  token_endpoint: https://my-tenant.authentication.eu10.hana.ondemand.com/oauth/token
  grant_type: client_credentials
  client_id_env_var: SAP_EM_CLIENT_ID
  client_secret_env_var: SAP_EM_CLIENT_SECRET
  auth_in: basic
```

XSUAA tokens last ~12 hours, auto-refresh by the resource.

## Setup

1. **BTP Cockpit** → Subscriptions → Event Mesh → Configure → **Create Service Instance**
2. Bind a service key. Copy: `messaging_host` URL, `clientid`, `clientsecret`, `token_endpoint`
3. Create queues + subscriptions to the S/4HANA event topics you care about (`sap.s4.beh.businesspartner.v1.BusinessPartner.Created.v1`, etc.)
4. Configure `oauth_token_resource` + this sensor

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `sensor_name` | `str` | yes | |
| `messaging_host` | `str` | yes | Event Mesh REST endpoint |
| `queue_name` | `str` | yes | Queue to consume |
| `oauth_token_resource_key` | `str` | yes | Pair with `oauth_token_resource` |
| `peek` | `bool` | no | true = read-only (don't pop); false = consume |
| `batch_size` | `int` | no | Default 10 |
| `minimum_interval_seconds` | `int` | no | Default 30 |
| `job_name` | `str` | mode A | Mutually exclusive with `asset_selection` |
| `asset_selection` | `list` | mode B | |
| `dynamic_partition_name` / `partition_key_template` | `str` | mode B | |
| `tags_template` | `dict` | no | Tags per RunRequest; templates resolve from message fields |
| `default_status` | `str` | no | `stopped` (default) or `running` |

## Templating

`partition_key_template` and `tags_template` values support:
- `{message_id}` — always available
- `{<field>}` — any top-level field of the message body (when payload is JSON)

Example:
```yaml
dynamic_partition_name: business_partner_events
partition_key_template: "{business_partner_id}"
tags_template:
  sap.event_type: "{event_type}"
  sap.tenant: "{tenant_id}"
```

## Common S/4HANA Cloud event topics

Configure subscriptions in the BTP Event Mesh UI:

| Topic | Fires when |
|---|---|
| `sap.s4.beh.businesspartner.v1.BusinessPartner.Created.v1` | New customer/vendor |
| `sap.s4.beh.businesspartner.v1.BusinessPartner.Changed.v1` | Customer/vendor updated |
| `sap.s4.beh.salesorder.v1.SalesOrder.Created.v1` | New sales order |
| `sap.s4.beh.purchaseorder.v1.PurchaseOrder.Released.v1` | PO released |
| `sap.s4.beh.workflow.v1.WorkflowTaskCreated.v1` | Workflow task created |

SAP's full catalog: SAP Business Accelerator Hub → Events.

## See also

- [`oauth_token_resource`](../../resources/oauth_token_resource/) — paired token manager
- [`sap_cpi_observation_sensor`](../sap_cpi_observation_sensor/) — observe Integration Suite iFlow runs
- [`s3_monitor` / `gcs_monitor` / etc.](https://dagster-community-components-cli.vercel.app/) — file-based sensors for the storage-layer pattern
- Walkthrough: [`sap_event_mesh_pipeline.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/sap_event_mesh_pipeline.md)
