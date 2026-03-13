# Firestore Writer Component

Write a DataFrame to a Google Cloud Firestore collection as a Dagster sink asset.

## Overview

The `FirestoreWriterComponent` accepts an upstream DataFrame asset and writes each row as a Firestore document. Uses Firestore batch writes (up to 500 documents per batch) for efficiency. Supports custom document IDs from a DataFrame column and merge vs. overwrite modes.

## Use Cases

- **Mobile app data sync**: Write processed data to Firestore for mobile apps
- **Real-time updates**: Keep Firestore collections current with batch pipeline results
- **User profiles**: Update user documents in Firestore from analytics results
- **Content delivery**: Write CMS content to Firestore for serving

## Configuration

### Write with auto-generated document IDs

```yaml
type: dagster_component_templates.FirestoreWriterComponent
attributes:
  asset_name: write_products_to_firestore
  upstream_asset_key: enriched_products
  collection: products
  project_id_env_var: GCP_PROJECT_ID
```

### Write with custom document ID column

```yaml
type: dagster_component_templates.FirestoreWriterComponent
attributes:
  asset_name: write_users_to_firestore
  upstream_asset_key: processed_users
  collection: users
  id_column: user_id
  merge: false
  group_name: sinks
```

### Merge mode (update existing fields without overwriting)

```yaml
type: dagster_component_templates.FirestoreWriterComponent
attributes:
  asset_name: update_user_scores
  upstream_asset_key: computed_scores
  collection: users
  id_column: user_id
  merge: true
```

## Notes

- Batch size is limited to 500 documents per Firestore batch (automatically handled)
- When `id_column` is not specified, Firestore auto-generates document IDs

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | Google Cloud project ID |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON file |

## Dependencies

- `pandas>=1.5.0`
- `google-cloud-firestore>=2.0.0`
