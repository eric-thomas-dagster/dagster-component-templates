# SQS Monitor Sensor

Poll an AWS SQS queue for new messages and trigger jobs automatically.

## Overview

This sensor polls an SQS queue for messages on each evaluation. When messages are received, it creates one RunRequest per message, passes the message content via run_config, and deletes the messages from the queue. This follows the standard SQS consume-and-delete pattern.

## Authentication

Uses the standard boto3 credential chain: environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), `~/.aws/credentials`, or IAM instance profile.

### Required IAM Permissions

```json
{
  "Effect": "Allow",
  "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessageBatch", "sqs:GetQueueAttributes"],
  "Resource": "arn:aws:sqs:us-east-1:123456789:my-queue"
}
```

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **queue_url** - Full SQS queue URL
- **job_name** - Job to trigger per message

### Optional
- **max_messages_per_poll** (1–10, default `10`) — SQS limit per ReceiveMessage call
- **visibility_timeout_seconds** (default `30`) — Hide time while processing
- **region_name** — AWS region
- **minimum_interval_seconds** (default `30`)
- **default_status** (`running` | `stopped`)

## Usage

```yaml
type: dagster_component_templates.SQSMonitorSensorComponent
attributes:
  sensor_name: order_events_sensor
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/order-events
  job_name: process_order_event
  max_messages_per_poll: 10
  region_name: us-east-1
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "message_id": str,        # SQS message ID
      "receipt_handle": str,    # SQS receipt handle (already deleted by sensor)
      "body": str,              # Message body
      "sent_timestamp": str,    # Unix timestamp in milliseconds
      "queue_url": str,         # Queue URL
      "attributes": str         # SQS message attributes as string
    }
  }
}
```

## Usage with Assets

```python
from dagster import asset, Config, AssetExecutionContext
import json

class SQSMessageConfig(Config):
    message_id: str
    receipt_handle: str
    body: str
    sent_timestamp: str
    queue_url: str
    attributes: str

@asset
def process_sqs_message(context: AssetExecutionContext, config: SQSMessageConfig):
    payload = json.loads(config.body)
    context.log.info(f"Processing message {config.message_id}: {payload}")
    return payload
```

## Notes

- **SQS max batch size is 10** — for higher throughput, decrease `minimum_interval_seconds`
- **Messages are deleted immediately** after RunRequests are created. If the job fails, the message will not be retried from SQS — use a Dead Letter Queue for failure handling
- For FIFO queues, the `queue_url` must end in `.fifo`

## Requirements

- Python 3.8+, Dagster 1.5.0+, boto3>=1.26.0

## License

MIT License
