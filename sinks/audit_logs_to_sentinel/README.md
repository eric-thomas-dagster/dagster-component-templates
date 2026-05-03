# AuditLogsToSentinelComponent

Ship audit-log DataFrame to Microsoft Sentinel via the Log Analytics Data Collector API.

## Dependencies
- `pandas`
- `requests`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
