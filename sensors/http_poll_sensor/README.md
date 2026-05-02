# HttpPollSensor

Generic HTTP polling sensor. Hits a URL on each evaluation, hashes the response body (or extracts a JSONPath value), and triggers a RunRequest when the hash/value changes vs the cursor. Useful for free public APIs or webhooks landing on a known URL — no platform-specific SDK required.

## Example

```yaml
type: dagster_component_templates.HttpPollSensorComponent
attributes:
  sensor_name: weather_changed_sensor
  asset_keys: [weather_report]
  url: "https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&current=temperature_2m"
  json_path: "current.temperature_2m"
  minimum_interval_seconds: 300
```

## Requirements

```
requests
```
