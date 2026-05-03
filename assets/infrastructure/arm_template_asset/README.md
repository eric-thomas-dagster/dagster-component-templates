# ArmTemplateAssetComponent

Deploy an Azure Resource Manager (ARM) template as a Dagster asset.

## Dependencies
- `azure-mgmt-resource`
- `azure-identity`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
