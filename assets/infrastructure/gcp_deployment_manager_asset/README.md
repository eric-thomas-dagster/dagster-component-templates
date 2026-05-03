# GcpDeploymentManagerAssetComponent

Deploy a GCP Deployment Manager configuration (.yaml or jinja) as a Dagster asset.

## Dependencies
- `google-cloud-deployment-manager`
- `google-auth`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
