# Google Analytics Resource

Register a `GoogleAnalyticsResource` wrapping the GA4 Data API (`BetaAnalyticsDataClient`) for use by other Dagster components.

## Installation

```bash
pip install google-analytics-data
```

## Configuration

```yaml
type: dagster_component_templates.GoogleAnalyticsResourceComponent
attributes:
  resource_key: google_analytics_resource  # key other components use
  property_id: "123456789"  # your GA4 property ID
  gcp_credentials_env_var: GCP_SERVICE_ACCOUNT_JSON  # env var holding service account JSON
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: google_analytics_resource
```

## Authentication

`gcp_credentials_env_var` accepts an **environment variable name** (not the secret value itself).
The variable must hold the full JSON content of a GCP service account key file:

```bash
export GCP_SERVICE_ACCOUNT_JSON='{"type": "service_account", "project_id": "...", ...}'
```

Grant the service account the "Viewer" role on your GA4 property in Google Analytics Admin.
