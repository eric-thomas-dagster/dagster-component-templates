# StripeWorkspaceComponent

Auto-emit one Dagster asset per top-level **Stripe resource** (customers / charges / subscriptions / invoices / payment_intents / …). `StateBackedComponent` — resource list is a static enumeration filtered by selector. Materializing runs a paginated List call against `api.stripe.com/v1/{resource}`.

The workspace-shape peer of `stripe_ingestion`.

## Example

```yaml
type: dagster_community_components.StripeWorkspaceComponent
attributes:
  api_key_env_var: STRIPE_API_KEY
  resource_selector:
    by_name: [customers, charges, subscriptions, invoices, payment_intents]
  page_limit: 100
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Resources available

19 canonical Stripe resources including nested endpoints (`checkout/sessions`, `billing_portal/sessions`). See the component's `_RESOURCES` list for the full set.

## Multi-account deployments

Stripe accounts are keyed by API key. For multi-account setups, instantiate one workspace per account.

## Related

- `stripe_resource`
- `stripe_ingestion` — single-resource counterpart with filters
- `stripe_event_sensor` — event-driven trigger
