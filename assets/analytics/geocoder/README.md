# Geocoder Component

Geocode addresses to latitude/longitude coordinates by adding geo columns to an upstream Dagster asset DataFrame.

## Overview

The `GeocoderComponent` iterates over an address column in a DataFrame and uses a geocoding API to resolve each address to latitude and longitude coordinates. Supports Nominatim (free, OpenStreetMap), Google Maps, and HERE geocoding providers.

## Use Cases

- **Customer mapping**: Add geo coordinates to customer records
- **Store locator**: Geocode retail locations
- **Logistics**: Geocode delivery addresses for routing
- **Market analysis**: Map sales territories

## Providers

| Provider | Cost | Rate Limit | API Key Required |
|----------|------|------------|-----------------|
| `nominatim` | Free | 1 req/sec | No |
| `google` | Paid | High | Yes (`GOOGLE_MAPS_API_KEY`) |
| `here` | Freemium | Medium | Yes (`HERE_API_KEY`) |

## Input Requirements

The upstream DataFrame must contain an address column with full address strings (e.g., "123 Main St, New York, NY 10001").

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `[all input columns]` | various | Original data preserved |
| `latitude` | float | Geocoded latitude (null if failed) |
| `longitude` | float | Geocoded longitude (null if failed) |
| `country_column` (optional) | string | Normalized country name |

## Configuration

### Nominatim (free)

```yaml
type: dagster_component_templates.GeocoderComponent
attributes:
  asset_name: geocoded_stores
  upstream_asset_key: stores
  address_column: address
  provider: nominatim
  user_agent: my_dagster_pipeline
  batch_delay: 1.1
```

### Google Maps

```yaml
type: dagster_component_templates.GeocoderComponent
attributes:
  asset_name: geocoded_customers
  upstream_asset_key: customers
  address_column: full_address
  provider: google
  api_key_env_var: GOOGLE_MAPS_API_KEY
  batch_delay: 0.05
  country_column: country
```

## Notes

- Nominatim requires `batch_delay >= 1.0` second to comply with usage policy
- Failed geocodes produce `null` values in lat/lng columns
- Use `user_agent` to identify your application to Nominatim

## Dependencies

- `pandas>=1.5.0`
- `geopy>=2.3.0`
