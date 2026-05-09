# Pub/Sub Publish

Publish DataFrame rows to a Google Cloud Pub/Sub topic — one message per row. Columns can be mapped into Pub/Sub message attributes for filter-based routing on subscribers.

```yaml
type: dagster_component_templates.PubSubPublishAssetComponent
attributes:
  asset_name: events_published
  upstream_asset_key: enriched_events
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  topic: my-events-topic
  attribute_columns: [event_type, region]
  group_name: stream
```

## Body shapes

- **Default**: each row JSON-serialized (minus `attribute_columns` + `ordering_key_column`) as the message body.
- **`message_column`**: single column's value used as the body. dict / list → JSON; bytes → raw; otherwise str.

## Required SA roles

`roles/pubsub.publisher` on the topic (or project). Pub/Sub API must be enabled.

If `auto_create_topic: true`, also `roles/pubsub.editor` (for topics.create).

## Sister components

- `pubsub_monitor` (sensor) — emit Dagster RunRequests when new messages land on a subscription.
- `pubsub_to_database_asset` — read from a subscription into a warehouse.
- `kafka_to_database_asset` / `sqs_to_database_asset` / etc. — same pattern, different streaming systems.
