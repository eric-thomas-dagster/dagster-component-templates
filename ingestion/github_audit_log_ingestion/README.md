# GithubAuditLogIngestionComponent

Pull GitHub Enterprise / Org audit log via /orgs/:org/audit-log.

## Dependencies
- `pandas`
- `requests`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
