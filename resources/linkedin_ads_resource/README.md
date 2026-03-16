# LinkedIn Ads Resource

Register a `LinkedInAdsResource` for the LinkedIn Marketing API for use by other Dagster components.

## Installation

```bash
pip install linkedin-api
```

## Configuration

```yaml
type: dagster_component_templates.LinkedInAdsResourceComponent
attributes:
  resource_key: linkedin_ads_resource  # key other components use
  access_token_env_var: LINKEDIN_ACCESS_TOKEN  # env var holding OAuth2 access token
  ad_account_id: "123456789"  # optional LinkedIn Ad Account ID
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: linkedin_ads_resource
```

## Authentication

`access_token_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export LINKEDIN_ACCESS_TOKEN="AQXxxxxxxxxxxxxxxxx"
```

The resource provides pre-built authorization headers via `get_headers()` for use with the LinkedIn Marketing API (`https://api.linkedin.com/v2/`). Obtain an OAuth2 access token by creating a LinkedIn App at [linkedin.com/developers](https://www.linkedin.com/developers/apps).
