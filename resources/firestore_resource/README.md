# FirestoreResourceComponent

A Dagster component that provides a Google Cloud Firestore `ConfigurableResource` backed by `google-cloud-firestore`.

## Installation

```
pip install google-cloud-firestore
```

## Configuration

```yaml
type: firestore_resource.component.FirestoreResourceComponent
attributes:
  resource_key: firestore_resource
  project: my-gcp-project
  gcp_credentials_env_var: GCP_SERVICE_ACCOUNT_JSON
  database: "(default)"
```

## Auth

Set the environment variable named in `gcp_credentials_env_var` to the full JSON content of a GCP service account key file. If omitted, Application Default Credentials (ADC) are used — suitable for Workload Identity or local `gcloud auth application-default login` setups.
