# Jira Resource

Register a `JiraResource` wrapping the `jira` Python client for use by other Dagster components.

## Installation

```bash
pip install jira
```

## Configuration

```yaml
type: dagster_component_templates.JiraResourceComponent
attributes:
  resource_key: jira_resource  # key other components use
  server_url: https://mycompany.atlassian.net  # your Jira instance URL
  username: admin@mycompany.com  # Atlassian account email
  api_token_env_var: JIRA_API_TOKEN  # env var name holding the API token
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: jira_resource
```

## Authentication

`api_token_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export JIRA_API_TOKEN="ATATT3xFfGF0XXXXXXXXXXXX"
```

Generate an API token at [id.atlassian.com/manage-profile/security/api-tokens](https://id.atlassian.com/manage-profile/security/api-tokens).
