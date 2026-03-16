# Github Resource

Register a dagster-github GithubResource for use by other components

## Installation

`pip install dagster-github`

## Configuration

```yaml
type: dagster_component_templates.GithubResourceComponent
attributes:
  resource_key: github_resource  # key other components use
  # Required
  github_app_id: <github_app_id>
  github_app_private_rsa_key_env_var: MY_GITHUB_APP_PRIVATE_RSA_KEY_ENV_VAR  # env var name
  github_installation_id: <github_installation_id>
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: github_resource
```

## Authentication

All secret fields accept an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export GITHUB_APP_PRIVATE_RSA_KEY_ENV_VAR_VALUE="your-secret-here"
```

