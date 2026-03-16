# HubSpot Resource

Register a `HubSpotResource` wrapping the `hubspot-api-client` for use by other Dagster components.

## Installation

```bash
pip install hubspot-api-client
```

## Configuration

```yaml
type: dagster_component_templates.HubSpotResourceComponent
attributes:
  resource_key: hubspot_resource  # key other components use
  access_token_env_var: HUBSPOT_ACCESS_TOKEN  # env var name holding the token
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: hubspot_resource
```

## Authentication

`access_token_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export HUBSPOT_ACCESS_TOKEN="pat-na1-XXXXXXXXXXXX"
```

Create a private app in HubSpot under Settings > Integrations > Private Apps to obtain the access token.
