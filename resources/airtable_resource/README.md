# Airtable Resource

Register an `AirtableResource` wrapping the `pyairtable` client for use by other Dagster components.

## Installation

```bash
pip install pyairtable
```

## Configuration

```yaml
type: dagster_component_templates.AirtableResourceComponent
attributes:
  resource_key: airtable_resource  # key other components use
  api_key_env_var: AIRTABLE_API_KEY  # env var name holding the token
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: airtable_resource
```

## Authentication

`api_key_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export AIRTABLE_API_KEY="patXXXXXXXXXXXXXX"
```

Obtain a personal access token from [airtable.com/create/tokens](https://airtable.com/create/tokens).
