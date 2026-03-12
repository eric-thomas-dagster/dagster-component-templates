# Google Pub/Sub Monitor Sensor

Pull messages from a Google Cloud Pub/Sub subscription and trigger jobs automatically.

## Overview

This sensor pulls messages from a Pub/Sub subscription on each evaluation. Messages are acknowledged after RunRequests are created. One RunRequest is created per message.

Pairs naturally with the `gcs_monitor` sensor — use Pub/Sub notifications on a GCS bucket to avoid polling entirely.

## Authentication

Uses Application Default Credentials (ADC): Workload Identity, `GOOGLE_APPLICATION_CREDENTIALS`, or `gcloud auth application-default login`.

### Required IAM Role

**Pub/Sub Subscriber** (`roles/pubsub.subscriber`) on the subscription.

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **project** - GCP project ID
- **subscription** - Subscription short name (not full path)
- **job_name** - Job to trigger per message

### Optional
- **max_messages_per_pull** (default `100`)
- **minimum_interval_seconds** (default `30`)
- **default_status** (`running` | `stopped`)

## Usage

```yaml
type: dagster_component_templates.PubSubMonitorSensorComponent
attributes:
  sensor_name: pubsub_orders_sensor
  project: my-gcp-project
  subscription: orders-subscription
  job_name: process_order_message
  max_messages_per_pull: 50
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "message_id": str,      # Pub/Sub message ID
      "data": str,            # Message data decoded as UTF-8
      "attributes": str,      # Message attributes dict as string
      "publish_time": str,    # ISO 8601 publish timestamp
      "subscription": str,    # Subscription name
      "project": str          # GCP project ID
    }
  }
}
```

## Usage with Assets

```python
from dagster import asset, Config, AssetExecutionContext
import json

class PubSubMessageConfig(Config):
    message_id: str
    data: str
    attributes: str
    publish_time: str
    subscription: str
    project: str

@asset
def process_pubsub_message(context: AssetExecutionContext, config: PubSubMessageConfig):
    payload = json.loads(config.data)
    context.log.info(f"Message {config.message_id}: {payload}")
    return payload
```

## GCS + Pub/Sub Integration

Use Pub/Sub GCS notifications as a more efficient alternative to the `gcs_monitor` sensor:

```bash
# Create a GCS notification that publishes to Pub/Sub when objects are created
gsutil notification create -t my-topic -f json -e OBJECT_FINALIZE gs://my-bucket
```

Then monitor the subscription instead of polling GCS directly.

## Requirements

- Python 3.8+, Dagster 1.5.0+, google-cloud-pubsub>=2.0.0

## License

MIT License
