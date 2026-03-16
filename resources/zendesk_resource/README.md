# Zendesk Resource

Register a `ZendeskResource` wrapping the `zenpy` client for use by other Dagster components.

## Installation

```bash
pip install zenpy
```

## Configuration

```yaml
type: dagster_component_templates.ZendeskResourceComponent
attributes:
  resource_key: zendesk_resource  # key other components use
  subdomain: mycompany  # your Zendesk subdomain
  email: admin@mycompany.com  # agent email
  api_token_env_var: ZENDESK_API_TOKEN  # env var name holding the API token
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: zendesk_resource
```

## Authentication

`api_token_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export ZENDESK_API_TOKEN="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

Generate an API token in Zendesk Admin Center under Apps and Integrations > APIs > Zendesk API.
