# Azure Log Analytics Query

Materialize a DataFrame from a KQL query against an Azure Log Analytics workspace. Read-side counterpart to audit_logs_to_sentinel — write logs, then query them back.

## Use cases

- Pull operational telemetry (sign-ins, perf counters, errors) from
  Application Insights / Azure Monitor / Sentinel into a DataFrame
  pipeline for analytics
- Backfill from historical KQL queries
- Build downstream analytics on log data (anomaly detection, cohort
  analysis)
- Quality-check the inverse of `audit_logs_to_sentinel` — confirm
  records you wrote are indexed and queryable

## Config

```yaml
type: dagster_component_templates.AzureLogAnalyticsQueryComponent
attributes:
  asset_name: signin_failures_last_hour
  workspace_id: "12345678-1234-1234-1234-123456789012"
  timespan_iso8601: "PT1H"   # last hour
  query: |
    SigninLogs
    | where ResultType != 0
    | summarize failure_count = count() by bin(TimeGenerated, 5m), UserPrincipalName
    | order by failure_count desc
    | take 100
  tenant_id_env_var: AZURE_TENANT_ID
  client_id_env_var: AZURE_CLIENT_ID
  client_secret_env_var: AZURE_CLIENT_SECRET
```

## Auth + RBAC

Standard `DefaultAzureCredential`. Principal needs **Log Analytics Reader**
role on the workspace (or a custom role with
`Microsoft.OperationalInsights/workspaces/query/read`).

In Azure compute, omit the env vars and the resource picks up the
attached managed identity automatically.

## KQL examples

Common patterns to plug into the `query` field:

```kql
// Last 24h of activity, top 100
AzureActivity
| where TimeGenerated > ago(24h)
| summarize n = count() by Caller, ResourceProvider
| order by n desc | take 100
```

```kql
// Sentinel incidents
SecurityIncident
| where TimeGenerated > ago(7d)
| project Title, Severity, Status, CreatedTime
```

```kql
// Custom Dagster+ audit logs (if you wrote with audit_logs_to_sentinel)
DagsterPlusAudit_CL
| where TimeGenerated > ago(1d)
| summarize n = count() by event_type_s
```
