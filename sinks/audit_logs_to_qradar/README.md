# AuditLogsToQradarComponent

Ship audit-log DataFrame to IBM QRadar via Syslog (TCP) — events go to a configured Log Source.

## Dependencies
- `pandas`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
