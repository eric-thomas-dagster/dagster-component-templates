# Facebook Ads Resource

Register a `FacebookAdsResource` wrapping the Facebook Business SDK for use by other Dagster components.

## Installation

```bash
pip install facebook-business
```

## Configuration

```yaml
type: dagster_component_templates.FacebookAdsResourceComponent
attributes:
  resource_key: facebook_ads_resource  # key other components use
  app_id_env_var: FACEBOOK_APP_ID  # env var holding your App ID
  app_secret_env_var: FACEBOOK_APP_SECRET  # env var holding your App Secret
  access_token_env_var: FACEBOOK_ACCESS_TOKEN  # env var holding long-lived token
  ad_account_id: act_123456789  # your Ad Account ID (include 'act_' prefix)
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: facebook_ads_resource
```

## Authentication

All secret env var fields accept **environment variable names** (not the secret values themselves).
Set the corresponding environment variables before running Dagster:

```bash
export FACEBOOK_APP_ID="1234567890"
export FACEBOOK_APP_SECRET="abcdef1234567890abcdef1234567890"
export FACEBOOK_ACCESS_TOKEN="EAAxxxxxxxxxxxxxxx"
```

Create a Facebook App at [developers.facebook.com](https://developers.facebook.com) and generate a long-lived user access token with `ads_read` permission.
