# Azure Service Bus Monitor Sensor

Poll an Azure Service Bus queue or topic subscription for new messages and trigger jobs automatically.

## Overview

This sensor receives messages from a Service Bus queue or topic subscription on each evaluation. Messages are completed (deleted) after RunRequests are created. One RunRequest is created per message.

## Authentication

Uses `DefaultAzureCredential` by default. Alternatively, provide a connection string via environment variable.

### Required RBAC Role

**Azure Service Bus Data Receiver** on the queue, topic, or namespace.

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **job_name** - Job to trigger per message
- **fully_qualified_namespace** OR **connection_string_env_var**
- **queue_name** OR (**topic_name** + **subscription_name**)

### Optional
- **max_messages_per_poll** (default `100`)
- **minimum_interval_seconds** (default `30`)
- **default_status** (`running` | `stopped`)

## Usage

### Queue
```yaml
type: dagster_component_templates.ServiceBusMonitorSensorComponent
attributes:
  sensor_name: servicebus_queue_sensor
  fully_qualified_namespace: mynamespace.servicebus.windows.net
  queue_name: data-ingestion-queue
  job_name: process_queue_message
```

### Topic Subscription
```yaml
type: dagster_component_templates.ServiceBusMonitorSensorComponent
attributes:
  sensor_name: servicebus_topic_sensor
  fully_qualified_namespace: mynamespace.servicebus.windows.net
  topic_name: data-events
  subscription_name: dagster-subscription
  job_name: process_topic_message
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "message_id": str,       # Service Bus message ID
      "body": str,             # Message body decoded as UTF-8
      "subject": str,          # Message subject/label
      "content_type": str,     # MIME content type
      "enqueued_time": str,    # ISO 8601 enqueue timestamp
      "source": str            # Queue name or topic/subscription path
    }
  }
}
```

## Requirements

- Python 3.8+, Dagster 1.5.0+, azure-servicebus>=7.11.0, azure-identity>=1.12.0

## License

MIT License
