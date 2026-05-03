# AuditLogsToSumoLogicComponent

Ship audit-log DataFrame to Sumo Logic via an HTTP Hosted Collector.

## Dependencies
- `pandas`
- `requests`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
