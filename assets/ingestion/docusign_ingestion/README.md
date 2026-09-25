# DocuSign Ingestion

Ingest DocuSign envelope and template data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** DocuSign requires OAuth2 JWT-Bearer grant (a service-account style flow): generate an RSA keypair, register the public key on an Integration Key in DocuSign admin, and grant one-time consent for the impersonated user_id (visit the /oauth/auth consent URL once as that user). This connector mints a fresh access token on every run using the private key. The exact data_selector for templates (envelopeTemplates) was not independently confirmed and may need adjustment. The envelopes resource IS genuinely bound to the partition window via from_date/to_date; templates/users are not.


> **Partition honesty note:** the `envelopes` resource is genuinely bound to the partition window via `from_date`/`to_date` params. `templates` and `users` are snapshot-style and are **not** bound -- every run re-fetches them in full regardless of partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `integration_key` | required | DocuSign Integration Key (client ID) with an RSA keypair registered for JWT grant. |
| `user_id` | required | GUID of the DocuSign user being impersonated (must have granted one-time JWT consent). |
| `private_key` | required | RSA private key (PEM format) matching the public key registered on the Integration Key. |
| `account_id` | optional | Specific DocuSign account ID to use. If unset, the default account from the JWT userinfo response is used. |
| `use_demo_env` | optional | Use DocuSign's demo/sandbox environment (account-d.docusign.com) instead of production. |
| `resources` | optional | Comma-separated list of resources to extract: envelopes, templates, users. Default: `envelopes` |

## Example
```yaml
type: dagster_component_templates.DocuSignIngestionComponent
attributes:
  asset_name: docusign_ingestion
  integration_key: "${DOCUSIGN_INTEGRATION_KEY}"
  user_id: "${DOCUSIGN_USER_ID}"
  private_key: "${DOCUSIGN_PRIVATE_KEY}"
  resources: "envelopes"
```
