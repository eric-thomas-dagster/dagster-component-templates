# Kinesis Monitor Sensor

Monitor an AWS Kinesis Data Stream for new records and trigger jobs automatically.

## Overview

This sensor reads records from all shards of a Kinesis stream on each evaluation. Per-shard sequence numbers are stored in the sensor cursor to enable exactly-once processing across restarts. One RunRequest is created per record.

## Authentication

Uses the standard boto3 credential chain. Requires `kinesis:GetRecords`, `kinesis:GetShardIterator`, and `kinesis:ListShards` permissions on the stream.

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **stream_name** - Kinesis stream name
- **job_name** - Job to trigger per record

### Optional
- **max_records_per_shard** (default `100`) — Per-shard read limit per evaluation
- **initial_position** (`LATEST` | `TRIM_HORIZON`, default `LATEST`) — Start position on first run
- **region_name** — AWS region
- **minimum_interval_seconds** (default `30`)
- **default_status** (`running` | `stopped`)

## Usage

```yaml
type: dagster_component_templates.KinesisMonitorSensorComponent
attributes:
  sensor_name: clickstream_sensor
  stream_name: clickstream-events
  job_name: process_click_event
  max_records_per_shard: 100
  initial_position: LATEST
  region_name: us-east-1
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "stream_name": str,          # Kinesis stream name
      "shard_id": str,             # Shard ID
      "sequence_number": str,      # Record sequence number
      "partition_key": str,        # Record partition key
      "data_b64": str,             # Base64-encoded record data
      "arrival_timestamp": str,    # ISO 8601 approximate arrival time
      "region": str                # AWS region
    }
  }
}
```

## Usage with Assets

```python
from dagster import asset, Config, AssetExecutionContext
import base64, json

class KinesisRecordConfig(Config):
    stream_name: str
    shard_id: str
    sequence_number: str
    partition_key: str
    data_b64: str
    arrival_timestamp: str
    region: str

@asset
def process_kinesis_record(context: AssetExecutionContext, config: KinesisRecordConfig):
    data = json.loads(base64.b64decode(config.data_b64).decode("utf-8"))
    context.log.info(f"Processing record from shard {config.shard_id}: {data}")
    return data
```

## Cursor Design

Cursor stores `{shard_id: sequence_number}` JSON. On each evaluation a fresh shard iterator is created using `AFTER_SEQUENCE_NUMBER` + the stored sequence number, avoiding shard iterator expiry issues.

## Requirements

- Python 3.8+, Dagster 1.5.0+, boto3>=1.26.0

## License

MIT License
