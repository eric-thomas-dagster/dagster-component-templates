# SnowflakeAccessHistoryIngestionComponent

Pull Snowflake access history (every query, every column accessed) from the ACCOUNT_USAGE views.

## Dependencies
- `pandas`
- `snowflake-connector-python`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
