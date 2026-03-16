# BigQuery IO Manager

Register a BigQueryPandasIOManager so assets are automatically stored in and loaded from BigQuery.

## Installation

```
pip install dagster-gcp-pandas
```

## Configuration

```yaml
type: dagster_component_templates.BigQueryIOManagerComponent
attributes:
  resource_key: io_manager
  project: my-gcp-project
  dataset: dagster_assets
  location: US
```

## Authentication

Uses Application Default Credentials (ADC) by default. Set `gcp_credentials_env_var` to a JSON service account key if needed:

```bash
export GCP_CREDENTIALS='{"type": "service_account", ...}'
```
