# AirtableWorkspaceComponent

Auto-emit one Dagster asset per **Airtable table** by enumerating bases × tables via the Meta API. `StateBackedComponent` — discovery cached to disk. Materializing an asset reads records and emits a DataFrame with flattened fields.

The workspace-shape peer of `airtable_ingestion`.

## Example

```yaml
type: dagster_community_components.AirtableWorkspaceComponent
attributes:
  pat_env_var: AIRTABLE_PAT
  table_selector:
    by_pattern: ["*.Orders", "*.Customers"]
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Selector

Same Fivetran-shape as other workspaces. Match against **`BaseName.TableName`** OR just `TableName`:

```yaml
table_selector:
  by_name: [MyBase.Orders]           # exact fully qualified
  by_pattern: [*.Customers]          # any base, table named Customers
  by_pattern: [SalesDB.*]            # any table in SalesDB base
```

## PAT scopes required

- `data.records:read` — read records from tables
- `schema.bases:read` — enumerate bases and tables

Create the token at [airtable.com/create/tokens](https://airtable.com/create/tokens).

## Related

- `airtable_resource`
- `airtable_ingestion` — single-table counterpart
