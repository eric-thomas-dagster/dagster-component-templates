# ZendeskTicketSensor

Uses Zendesk's Incremental Tickets export API to find tickets created or updated since the cursor. Triggers a RunRequest per new ticket. Token-authed.

## Example

```yaml
type: dagster_component_templates.ZendeskTicketSensorComponent
attributes:
  sensor_name: zendesk_new_ticket_sensor
  asset_keys: [support_ticket_features]
  subdomain: mycompany
  email: ops@mycompany.com
  api_token_env_var: ZENDESK_API_TOKEN
  statuses: [new, open]
  minimum_interval_seconds: 300
```

## Requirements

```
requests
```
