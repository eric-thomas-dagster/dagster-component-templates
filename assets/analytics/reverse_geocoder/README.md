# reverse_geocoder

Reverse geocode latitude/longitude coordinates in a DataFrame to human-readable addresses using Nominatim (free/OpenStreetMap), Google Maps, or HERE geocoding APIs. Optionally extracts city and country into separate columns. Success rate and provider are logged to asset metadata.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame with lat/lng columns |
| `lat_column` | `str` | `"latitude"` | Column name containing latitude values |
| `lng_column` | `str` | `"longitude"` | Column name containing longitude values |
| `provider` | `str` | `"nominatim"` | Geocoding provider: `nominatim`, `google`, `here` |
| `api_key_env_var` | `Optional[str]` | `null` | Environment variable name for the API key (paid providers) |
| `user_agent` | `str` | `"dagster_reverse_geocoder"` | User agent string for Nominatim |
| `output_address_column` | `str` | `"address"` | Column name for the resolved full address |
| `output_city_column` | `Optional[str]` | `null` | Optional column name for city |
| `output_country_column` | `Optional[str]` | `null` | Optional column name for country |
| `timeout` | `int` | `10` | Request timeout in seconds |
| `batch_delay` | `float` | `1.0` | Seconds between requests (rate limiting) |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Example YAML

```yaml
component_type: reverse_geocoder
description: Reverse geocode GPS event coordinates to addresses.

asset_name: gps_events_with_address
upstream_asset_key: gps_events
lat_column: latitude
lng_column: longitude
provider: nominatim
output_address_column: address
output_city_column: city
output_country_column: country
group_name: geospatial
```

## Metadata Logged

- `total_rows` — Total input rows processed
- `geocoded_count` — Number of rows successfully reverse geocoded
- `success_rate` — Percentage of rows successfully geocoded
- `provider` — Geocoding provider used

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `geopy`
