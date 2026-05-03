# AuditLogsToDatadogLogsComponent

Ship audit-log DataFrame to Datadog Logs via /api/v2/logs.

## Dependencies
- `pandas`
- `requests`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
