# AddressStandardizeComponent

Parse messy address strings into structured components (street number,
road, city, state, postcode, ZIP+4, country) — a free / open-source
drop-in for Alteryx's **CASS** (Coding Accuracy Support System)
address-cleansing tool.

## Providers

| `provider` | What it is | Dep | Notes |
|---|---|---|---|
| `libpostal` | Best parser quality (open source MIT) | `brew install libpostal` + `pip install pypostal` | Offline, no network. The parser most projects converge on. |
| `geoapify` | Commercial geocoder w/ batch API | `pip install requests` + `GEOAPIFY_API_KEY` | ~3k requests/day free tier. Returns standardized parts + lat/lng. |
| `nominatim` | OSM free service | `pip install requests` + `NOMINATIM_USER_AGENT` | 1 req/sec rate limit; best for small batches. |
| `regex` | Pure-Python US-address regex fallback | none | No dependencies, no network. Handles ~70-80% of US street addresses + ZIP+4. |

## What this is NOT

USPS **CASS-certification** is a paid licensed product. None of the free
providers above are CASS-certified — they parse, but don't guarantee
USPS deliverability or DPV. For certified output use a commercial
vendor (Smarty / Loqate / USPS itself).

## Use Cases

- Clean raw customer address strings before joining to ZIP-level demographics
- Split "123 Main St, Cleveland, OH 44102" into structured columns
- Extract ZIP+4 from US addresses that include extended codes
- Build a normalized address column for fuzzy joins / dedup

## Example

```yaml
type: dagster_community_components.AddressStandardizeComponent
attributes:
  asset_name: customer_addresses_parsed
  upstream_asset_key: raw_customers
  address_column: full_address
  provider: regex
  group_name: data_quality
```

## Output Columns

The component appends these columns to the input DataFrame (configurable
prefix; defaults to `addr_`):

| Column | Description |
|---|---|
| `addr_house_number` | "123" |
| `addr_road` | "Main St" |
| `addr_unit` | "Apt 4B" / "Suite 100" / "#7" (provider-dependent) |
| `addr_city` | "Cleveland" |
| `addr_state` | "OH" (state abbreviation when available) |
| `addr_postcode` | "44102" (ZIP5 for US, alphanumeric for international) |
| `addr_postcode_plus4` | "1234" (ZIP+4 last segment) |
| `addr_country` | "US" / "USA" / "United States" (provider-dependent) |

Rows that fail to parse get `None` in each output column; the asset
still materializes (logs the parse failure and emits a `parsed_failed`
metadata count).

## Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `asset_name` | yes | — | Output Dagster asset name |
| `upstream_asset_key` | yes | — | Upstream DataFrame asset key |
| `address_column` | yes | — | Column containing the raw address string |
| `provider` | yes | `regex` | One of `libpostal` / `geoapify` / `nominatim` / `regex` |
| `api_key_env_var` | no | `GEOAPIFY_API_KEY` | Env var for the provider's API key (geoapify only) |
| `user_agent` | no | `dagster-address-standardize` | User-Agent header for Nominatim |
| `output_prefix` | no | `addr_` | Prefix for emitted component columns |
| `keep_original` | no | `true` | Keep the original address column in output |
| `rate_limit_seconds` | no | `1.0` | Per-row delay for rate-limited providers |
| `timeout_seconds` | no | `10` | HTTP timeout per request |
| `group_name` | no | `None` | Dagster asset group name |
