# REST API Fetcher Asset

Fetch data from REST APIs and materialize as Dagster assets with support for authentication, various response formats, and caching.

## Overview

This asset component makes HTTP requests to REST API endpoints and materializes the response data. Perfect for:
- Fetching data from external APIs
- Integrating third-party services
- Building data pipelines from API sources
- Scheduled API data collection

## Features

- **Multiple HTTP Methods**: GET, POST, PUT, DELETE, PATCH
- **Authentication**: Basic Auth, Bearer Token
- **Flexible Output**: JSON, DataFrame, CSV, Parquet
- **JSON Path Extraction**: Extract nested data from responses
- **Caching**: Optional local caching of results
- **Headers & Parameters**: Full control over requests
- **SSL Verification**: Configurable SSL/TLS verification
- **Timeout Control**: Configurable request timeouts

## Configuration

### Required Parameters

- **asset_name** (string) - Name of the asset
- **api_url** (string) - URL of the API endpoint

### Optional Parameters

- **method** (string) - HTTP method (default: `"GET"`)
- **headers** (string) - JSON string of HTTP headers
- **params** (string) - JSON string of query parameters
- **body** (string) - JSON string of request body
- **auth_type** (string) - Authentication type: `"basic"` or `"bearer"`
- **auth_username** (string) - Username for basic auth
- **auth_password** (string) - Password for basic auth
- **auth_token** (string) - Bearer token
- **timeout** (integer) - Request timeout in seconds (default: `30`)
- **output_format** (string) - Output format: `"json"`, `"dataframe"`, `"csv"`, `"parquet"` (default: `"json"`)
- **json_path** (string) - JSON path to extract data (e.g., `"data.results"`)
- **cache_results** (boolean) - Whether to cache results (default: `false`)
- **cache_path** (string) - Path to cache file
- **verify_ssl** (boolean) - Verify SSL certificates (default: `true`)
- **description** (string) - Asset description
- **group_name** (string) - Asset group

## Usage Examples

### Basic GET Request

```yaml
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: users_data
  api_url: https://api.example.com/users
  method: GET
  output_format: dataframe
```

### With Query Parameters

```yaml
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: filtered_data
  api_url: https://api.example.com/data
  params: '{"page": 1, "limit": 100, "status": "active"}'
  output_format: dataframe
```

### With Bearer Token Authentication

```yaml
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: protected_api_data
  api_url: https://api.example.com/protected/data
  auth_type: bearer
  auth_token: ${API_TOKEN}  # Use environment variable
  output_format: json
```

### With Custom Headers

```yaml
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: api_with_headers
  api_url: https://api.example.com/data
  headers: '{"Accept": "application/json", "X-Custom-Header": "value"}'
  output_format: dataframe
```

### POST Request with Body

```yaml
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: create_record
  api_url: https://api.example.com/records
  method: POST
  headers: '{"Content-Type": "application/json"}'
  body: '{"name": "Test", "value": 123}'
  output_format: json
```

### Extract Nested Data with JSON Path

```yaml
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: nested_data
  api_url: https://api.example.com/response
  json_path: data.results  # Extract data.results from response
  output_format: dataframe
```

### With Caching

```yaml
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: cached_api_data
  api_url: https://api.example.com/data
  output_format: parquet
  cache_results: true
  cache_path: /tmp/api_cache.parquet
```

## Authentication

### Bearer Token

Use environment variables for tokens:

```yaml
attributes:
  auth_type: bearer
  auth_token: ${API_TOKEN}
```

Set the environment variable:
```bash
export API_TOKEN="your-token-here"
```

### Basic Authentication

```yaml
attributes:
  auth_type: basic
  auth_username: ${API_USERNAME}
  auth_password: ${API_PASSWORD}
```

Set environment variables:
```bash
export API_USERNAME="user"
export API_PASSWORD="password"
```

## Output Formats

### JSON (Default)

Returns raw JSON response:
```yaml
output_format: json
```

### DataFrame

Converts JSON response to pandas DataFrame:
```yaml
output_format: dataframe
```

Best for:
- List of objects from API
- Tabular data processing

### CSV

Converts JSON to CSV string:
```yaml
output_format: csv
cache_results: true
cache_path: /data/output.csv
```

### Parquet

Converts JSON to Parquet bytes:
```yaml
output_format: parquet
cache_results: true
cache_path: /data/output.parquet
```

## JSON Path Extraction

Extract nested data from API responses:

**API Response:**
```json
{
  "status": "success",
  "data": {
    "results": [
      {"id": 1, "name": "Item 1"},
      {"id": 2, "name": "Item 2"}
    ],
    "total": 2
  }
}
```

**Configuration:**
```yaml
json_path: data.results
```

**Result:** Only the `results` array is extracted and processed.

## Common Use Cases

### 1. Daily API Data Fetch

```yaml
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: daily_metrics
  api_url: https://api.example.com/metrics/daily
  auth_type: bearer
  auth_token: ${METRICS_API_TOKEN}
  output_format: dataframe
  description: Fetch daily metrics from API
  group_name: metrics
```

Schedule with a Dagster schedule to run daily.

### 2. Paginated API Fetch

Create multiple assets for different pages:

```yaml
# Page 1
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: data_page_1
  api_url: https://api.example.com/data
  params: '{"page": 1, "per_page": 100}'
  output_format: dataframe
```

### 3. External Data Integration

```yaml
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: weather_data
  api_url: https://api.weather.com/v3/current
  params: '{"lat": 40.7128, "lon": -74.0060}'
  headers: '{"X-API-Key": "${WEATHER_API_KEY}"}'
  output_format: json
```

### 4. Webhook Response Data

```yaml
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: webhook_response
  api_url: https://api.example.com/webhook/data
  method: POST
  body: '{"event": "data_request", "timestamp": "2024-01-15T10:00:00Z"}'
  output_format: json
```

## Downstream Processing

Chain with other assets for data transformation:

```python
from dagster import asset

@asset(deps=["api_data"])
def processed_data(context):
    # Process the API data
    # ...
    pass
```

## Error Handling

The component handles common errors:

- **Network Errors**: Logged and raised
- **HTTP Errors**: Status code errors raised
- **JSON Decode Errors**: Falls back to raw text
- **Invalid JSON Path**: Logged warning, returns full response
- **Timeout**: Configurable timeout with error on expiration

## Metadata

Each materialization includes metadata:
- API URL
- HTTP method
- Status code
- Number of rows (for DataFrame output)
- Number of columns (for DataFrame output)
- Column names (for DataFrame output)

## Troubleshooting

### Issue: "Connection timeout"

**Solution**: Increase timeout:
```yaml
timeout: 60  # seconds
```

### Issue: "SSL certificate verification failed"

**Solution**: Disable SSL verification (not recommended for production):
```yaml
verify_ssl: false
```

### Issue: "Invalid JSON response"

**Solution**: Check API response format. Component falls back to text if not JSON.

### Issue: "Cannot convert to DataFrame"

**Solution**: Ensure API returns a list of objects or use `json_path` to extract the list:
```yaml
json_path: data.results
```

## Performance Tips

1. **Use Caching**: Enable caching for expensive API calls
2. **Set Appropriate Timeout**: Balance between reliability and performance
3. **Use Parquet**: For large datasets, use parquet format for efficient storage
4. **Limit Data**: Use query parameters to limit API response size

## Security Best Practices

1. **Use Environment Variables**: Never hard-code credentials
```yaml
auth_token: ${API_TOKEN}
```

2. **Verify SSL**: Keep SSL verification enabled in production
```yaml
verify_ssl: true
```

3. **Rotate Credentials**: Regularly rotate API keys and tokens

4. **Least Privilege**: Use API keys with minimum required permissions

## Requirements

- requests >= 2.28.0
- pandas >= 2.0.0
- pyarrow >= 10.0.0

## Contributing

Found a bug or have a feature request?
- GitHub Issues: https://github.com/eric-thomas-dagster/dagster-component-templates/issues

## License

MIT License
