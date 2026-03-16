# Google Sheets Resource

Register a `GoogleSheetsResource` wrapping the `gspread` client for use by other Dagster components.

## Installation

```bash
pip install gspread
```

## Configuration

```yaml
type: dagster_component_templates.GoogleSheetsResourceComponent
attributes:
  resource_key: google_sheets_resource  # key other components use
  gcp_credentials_env_var: GCP_SERVICE_ACCOUNT_JSON  # env var holding service account JSON
  scopes: https://www.googleapis.com/auth/spreadsheets  # optional
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: google_sheets_resource
```

## Authentication

`gcp_credentials_env_var` accepts an **environment variable name** (not the secret value itself).
The variable must hold the full JSON content of a GCP service account key file:

```bash
export GCP_SERVICE_ACCOUNT_JSON='{"type": "service_account", "project_id": "...", ...}'
```

Create a service account in the GCP Console, download the JSON key, and share your target spreadsheet with the service account email.
