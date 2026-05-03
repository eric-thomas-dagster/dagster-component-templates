# OktaSystemLogIngestionComponent

Pull Okta System Log events (every authentication, MFA challenge, admin action) via /api/v1/logs.

## Dependencies
- `pandas`
- `requests`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
