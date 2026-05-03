# BicepAssetComponent

Deploy a Bicep template as a Dagster asset (compiles via `az bicep build` then deploys via ARM).

## Dependencies
- `azure-mgmt-resource`
- `azure-identity`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
