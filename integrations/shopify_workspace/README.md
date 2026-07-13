# ShopifyWorkspaceComponent

Auto-emit one Dagster asset per top-level **Shopify resource** (products / orders / customers / etc.) for a single store. `StateBackedComponent` — resource list is a static enumeration filtered by selector. Materializing runs a paginated REST list call.

The workspace-shape peer of `shopify_ingestion`.

## Example

```yaml
type: dagster_community_components.ShopifyWorkspaceComponent
attributes:
  shop_env_var: SHOPIFY_SHOP
  access_token_env_var: SHOPIFY_ACCESS_TOKEN
  api_version: "2024-04"
  resource_selector:
    by_name: [products, orders, customers, inventory_items]
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Resources available

The workspace knows about 14 canonical Shopify resources: `products`, `orders`, `customers`, `inventory_items`, `discount_codes`, `draft_orders`, `collections`, `checkouts`, `abandoned_checkouts`, `fulfillments`, `gift_cards`, `locations`, `price_rules`, `shipping_zones`.

## Multi-store deployments

Shopify's REST API is single-store per host. For a multi-store setup, instantiate one workspace per store — each with its own `shop_env_var` and `access_token_env_var`.

## Related

- `shopify_resource`
- `shopify_ingestion` — single-resource counterpart with filters
