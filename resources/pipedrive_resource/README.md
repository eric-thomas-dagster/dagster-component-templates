# Pipedrive Resource

Register a `PipedriveResource` for the Pipedrive CRM API for use by other Dagster components.

## Installation

```bash
pip install pipedrive-python-lib
```

## Configuration

```yaml
type: dagster_component_templates.PipedriveResourceComponent
attributes:
  resource_key: pipedrive_resource  # key other components use
  api_token_env_var: PIPEDRIVE_API_TOKEN  # env var holding your API token
  company_domain: mycompany  # optional: your Pipedrive subdomain
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: pipedrive_resource
```

## Authentication

`api_token_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export PIPEDRIVE_API_TOKEN="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

The resource provides a configured `requests.Session` via `get_client()` with the API token automatically appended to all requests. Find your API token in Pipedrive under Personal Preferences > API.
