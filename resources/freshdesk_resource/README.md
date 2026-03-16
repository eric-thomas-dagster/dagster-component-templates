# Freshdesk Resource

Register a `FreshdeskResource` for the Freshdesk REST API for use by other Dagster components.

## Installation

```bash
pip install requests
```

## Configuration

```yaml
type: dagster_component_templates.FreshdeskResourceComponent
attributes:
  resource_key: freshdesk_resource  # key other components use
  domain: mycompany.freshdesk.com  # your Freshdesk domain
  api_key_env_var: FRESHDESK_API_KEY  # env var holding your API key
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: freshdesk_resource
```

## Authentication

`api_key_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export FRESHDESK_API_KEY="XXXXXXXXXXXXXXXXXXXX"
```

The resource provides a configured `requests.Session` via `get_session()` using HTTP Basic Auth `(api_key, "X")` against `https://{domain}/api/v2`. Find your API key in Freshdesk under Profile Settings.
