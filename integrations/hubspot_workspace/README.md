# HubSpotWorkspaceComponent

Auto-emit one Dagster asset per **HubSpot CRM object** (contacts, companies, deals, tickets, products, line_items + custom `p_*` objects). `StateBackedComponent` — discovery cached to disk. Materializing an asset fetches recent records via the CRM Objects API.

The workspace-shape peer of `hubspot_ingestion`.

## Example

```yaml
type: dagster_community_components.HubSpotWorkspaceComponent
attributes:
  access_token_env_var: HUBSPOT_ACCESS_TOKEN
  object_selector:
    by_name: [contacts, companies, deals, tickets]
  group_name: hubspot_crm
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Standard vs custom objects

The workspace always includes the six standard objects (`contacts`, `companies`, `deals`, `tickets`, `products`, `line_items`) plus any custom objects returned by `/crm/v3/schemas`. Custom objects come back with their `fullyQualifiedName` (e.g. `p_1234567_myobject`).

## Related

- `hubspot_resource` — connection + refresh-token handling
- `hubspot_ingestion` — single-object counterpart with query filtering
