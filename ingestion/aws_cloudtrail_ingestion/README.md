# AwsCloudTrailIngestionComponent

Pull AWS CloudTrail events via the LookupEvents API. Returns a DataFrame per asset materialization.

## Dependencies
- `pandas`
- `boto3`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
