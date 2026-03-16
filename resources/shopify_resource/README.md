# Shopify Resource

Register a `ShopifyResource` wrapping the `ShopifyAPI` client for use by other Dagster components.

## Installation

```bash
pip install ShopifyAPI
```

## Configuration

```yaml
type: dagster_component_templates.ShopifyResourceComponent
attributes:
  resource_key: shopify_resource  # key other components use
  shop_url: mystore.myshopify.com  # your store's myshopify.com URL
  access_token_env_var: SHOPIFY_ACCESS_TOKEN  # env var name holding the token
  api_version: "2024-01"  # optional, defaults to 2024-01
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: shopify_resource
```

## Authentication

`access_token_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export SHOPIFY_ACCESS_TOKEN="shpat_XXXXXXXXXXXXXXXXXXXX"
```

Create a custom app in your Shopify Admin under Apps > Develop Apps to obtain an Admin API access token.
