# AuditLogsToSplunkComponent

Ship audit-log DataFrame to Splunk via the HTTP Event Collector (HEC).

## Dependencies
- `pandas`
- `requests`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
