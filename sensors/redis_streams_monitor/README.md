# Redis Streams Monitor Sensor

Monitor a Redis Stream for new entries and trigger jobs automatically.

## Overview

This sensor uses `XREAD` to poll a Redis Stream for entries newer than the last-seen entry ID. The entry ID is stored in the sensor cursor, enabling exactly-once processing across restarts. One RunRequest is created per stream entry.

Redis Streams are ideal for lightweight event-driven pipelines on teams already using Redis for caching or pub/sub.

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **stream_name** - Redis Stream key (e.g., `events:incoming`)
- **job_name** - Job to trigger per entry

### Optional
- **host** (default `localhost`)
- **port** (default `6379`)
- **db** (default `0`)
- **password_env_var** — Env var name for Redis password
- **use_tls** (default `false`)
- **max_entries_per_poll** (default `100`)
- **start_from_beginning** (default `false`) — On first run, read all existing entries vs. only new ones
- **minimum_interval_seconds** (default `30`)
- **default_status** (`running` | `stopped`)

## Usage

```yaml
type: dagster_component_templates.RedisStreamsMonitorSensorComponent
attributes:
  sensor_name: order_events_sensor
  stream_name: orders:events
  job_name: process_order_event
  host: redis.internal
  password_env_var: REDIS_PASSWORD
  max_entries_per_poll: 50
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "stream_name": str,   # Redis Stream key
      "entry_id": str,      # Stream entry ID (e.g., '1699000000000-0')
      "fields": str,        # Entry fields dict as string
      "host": str,          # Redis host
      "port": int           # Redis port
    }
  }
}
```

## Usage with Assets

```python
from dagster import asset, Config, AssetExecutionContext
import ast

class RedisStreamEntryConfig(Config):
    stream_name: str
    entry_id: str
    fields: str
    host: str
    port: int

@asset
def process_stream_entry(context: AssetExecutionContext, config: RedisStreamEntryConfig):
    fields = ast.literal_eval(config.fields)  # Parse string back to dict
    context.log.info(f"Entry {config.entry_id}: {fields}")
    return fields
```

## Producing Entries (for testing)

```python
import redis
r = redis.Redis()
r.xadd("orders:events", {"order_id": "123", "status": "placed", "amount": "99.99"})
```

## Cursor Design

The cursor stores the last-seen entry ID string (e.g., `1699000000000-0`). On each evaluation, `XREAD COUNT {max} STREAMS {stream} {last_id}` returns only entries with IDs greater than the cursor.

## Requirements

- Python 3.8+, Dagster 1.5.0+, redis>=4.0.0

## License

MIT License
