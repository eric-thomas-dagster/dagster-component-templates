# SalesforceWorkspaceComponent

Auto-emit one Dagster asset per **Salesforce SObject** by enumerating the org's `/sobjects` catalog. `StateBackedComponent` — discovery cached to disk. Materializing an asset runs a SOQL query and returns a DataFrame.

The workspace-shape peer of `salesforce_ingestion` (single SObject → DataFrame). Use this when you want to bring in a whole set of SObjects with one YAML.

## Example

```yaml
type: dagster_community_components.SalesforceWorkspaceComponent
attributes:
  instance_url_env_var: SALESFORCE_INSTANCE_URL
  access_token_env_var: SALESFORCE_ACCESS_TOKEN
  api_version: "58.0"
  sobject_selector:
    by_name: [Account, Contact, Opportunity, Lead]
  group_name: salesforce_crm
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Selector

Same Fivetran-shape as other workspaces, plus `include_custom_only` to restrict to `*__c` SObjects.

## SOQL

Each asset runs:
```sql
SELECT FIELDS(STANDARD) FROM {SObject} LIMIT {query_limit}
```

Override with `query_fields: [Id, Name, Amount, StageName, CloseDate]` for a specific projection (recommended for large SObjects where FIELDS(STANDARD) hits the 200-field limit).

## Related

- `salesforce_resource` — connection + refresh-token handling
- `salesforce_ingestion` — single-SObject counterpart
- `salesforce_event_log_ingestion` — EventLogFile downloader (audit trail)
