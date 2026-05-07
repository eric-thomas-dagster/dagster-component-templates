# Azure Data Explorer Query

Run a KQL query against an Azure Data Explorer (ADX) cluster and materialize the result as a DataFrame asset. Distinct from azure_log_analytics_query (different SDK + service).

## ADX vs Log Analytics

Both speak KQL but they're different services:

| | Log Analytics | Azure Data Explorer |
|---|---|---|
| Managed by Microsoft | ✓ | ✗ (you provision) |
| Used for | Azure operational logs (Sentinel, AppInsights) | Custom telemetry, IoT, security analytics |
| Pricing | Per-GB ingestion | Cluster + storage |
| SDK | azure-monitor-query | azure-kusto-data |
| Component | `azure_log_analytics_query` | **this one** |

Use this for raw ADX clusters; `azure_log_analytics_query` for Log Analytics workspaces.
