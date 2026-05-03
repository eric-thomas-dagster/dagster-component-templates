# AuditLogsToChronicleComponent

Ship audit-log DataFrame to Google Chronicle (SecOps) via the unstructuredlogentries ingestion API.

## Dependencies
- `pandas`
- `google-auth`
- `requests`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
