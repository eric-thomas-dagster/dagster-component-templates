# Asana Resource

Register an `AsanaResource` wrapping the Asana Python SDK for use by other Dagster components.

## Installation

```bash
pip install asana
```

## Configuration

```yaml
type: dagster_component_templates.AsanaResourceComponent
attributes:
  resource_key: asana_resource  # key other components use
  access_token_env_var: ASANA_ACCESS_TOKEN  # env var holding your personal access token
  workspace_gid: "1234567890123456"  # optional Asana workspace GID
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: asana_resource
```

## Authentication

`access_token_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export ASANA_ACCESS_TOKEN="1/XXXXXXXXXXXXXXXXXX:XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

Generate a personal access token in Asana under My Profile Settings > Apps > Manage Developer Apps.
