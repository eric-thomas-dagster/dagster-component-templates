# Firestore Reader Component

Query documents from a Google Cloud Firestore collection and return them as a Dagster asset DataFrame.

## Overview

The `FirestoreReaderComponent` connects to Google Cloud Firestore and reads documents from a specified collection. Supports Firestore query filters, ordering, and limiting. Results are returned as a pandas DataFrame with the document ID in the `_id` column.

## Use Cases

- **Mobile app backends**: Extract Firestore data for analytics
- **Real-time data**: Pull Firestore collections into batch pipelines
- **User data**: Read user profiles or activity stored in Firestore
- **Content management**: Extract CMS documents from Firestore

## Configuration

### Basic read

```yaml
type: dagster_component_templates.FirestoreReaderComponent
attributes:
  asset_name: firestore_products
  collection: products
  project_id_env_var: GCP_PROJECT_ID
```

### Filtered query

```yaml
type: dagster_component_templates.FirestoreReaderComponent
attributes:
  asset_name: active_users
  collection: users
  filters:
    - field: active
      op: "=="
      value: true
    - field: age
      op: ">="
      value: 18
  order_by: created_at
  limit: 1000
  group_name: sources
```

### With service account credentials

```yaml
type: dagster_component_templates.FirestoreReaderComponent
attributes:
  asset_name: secure_collection
  collection: sensitive_data
  project_id_env_var: GCP_PROJECT_ID
  credentials_env_var: GOOGLE_APPLICATION_CREDENTIALS
```

## Output Schema

| Column | Description |
|--------|-------------|
| `_id` | Firestore document ID |
| `[document fields]` | All document fields |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | Google Cloud project ID |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON file |

## Dependencies

- `pandas>=1.5.0`
- `google-cloud-firestore>=2.0.0`
