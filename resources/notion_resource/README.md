# Notion Resource

Register a `NotionResource` wrapping the `notion-client` SDK for use by other Dagster components.

## Installation

```bash
pip install notion-client
```

## Configuration

```yaml
type: dagster_component_templates.NotionResourceComponent
attributes:
  resource_key: notion_resource  # key other components use
  token_env_var: NOTION_TOKEN  # env var name holding the integration token
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: notion_resource
```

## Authentication

`token_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export NOTION_TOKEN="secret_XXXXXXXXXXXXXXXXXXXX"
```

Create an internal integration at [notion.so/my-integrations](https://www.notion.so/my-integrations) to obtain the token.
