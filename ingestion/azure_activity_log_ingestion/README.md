# AzureActivityLogIngestionComponent

Pull Azure Activity Log entries (control-plane audit) via the Monitor REST API.

## Dependencies
- `pandas`
- `azure-mgmt-monitor`
- `azure-identity`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
