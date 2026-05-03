# SlackAuditLogIngestionComponent

Pull Slack Enterprise Grid audit log via /audit/v1/logs (requires audit_logs:read scope).

## Dependencies
- `pandas`
- `requests`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
