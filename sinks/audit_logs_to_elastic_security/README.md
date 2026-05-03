# AuditLogsToElasticSecurityComponent

Ship audit-log DataFrame to Elastic Security via the bulk indexing API.

## Dependencies
- `pandas`
- `elasticsearch>=8`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
