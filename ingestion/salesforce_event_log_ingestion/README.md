# SalesforceEventLogIngestionComponent

Pull Salesforce EventLogFile records (login, API access, report exports) via SOQL.

## Dependencies
- `pandas`
- `simple-salesforce`
- `requests`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
