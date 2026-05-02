# StripeEventSensor

Polls Stripe's Events API and triggers a RunRequest for each new event of the specified types. Cursor stored as last-seen event ID (Stripe events are ordered).

## Example

```yaml
type: dagster_component_templates.StripeEventSensorComponent
attributes:
  sensor_name: stripe_subscription_sensor
  asset_keys: [subscription_metrics]
  api_key_env_var: STRIPE_API_KEY
  event_types: [customer.subscription.created, customer.subscription.updated, customer.subscription.deleted]
  minimum_interval_seconds: 300
```

## Requirements

```
requests
```
