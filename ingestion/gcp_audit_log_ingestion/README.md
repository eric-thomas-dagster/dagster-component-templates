# GcpAuditLogIngestionComponent

Pull GCP audit logs via the Cloud Logging API. Returns a DataFrame.

## Dependencies
- `pandas`
- `google-cloud-logging`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
