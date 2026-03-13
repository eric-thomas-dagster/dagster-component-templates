# MongoDB Writer Component

Write a DataFrame to a MongoDB collection as a Dagster sink asset.

## Overview

The `MongodbWriterComponent` accepts an upstream DataFrame asset and writes its records to a MongoDB collection. Supports three write modes: replace (drop and re-insert), append (insert new records), and upsert (update existing or insert new based on a key field).

## Use Cases

- **ETL sink**: Write transformed data back to MongoDB
- **Data sync**: Keep MongoDB collections in sync with processed data
- **Upserts**: Incrementally update MongoDB documents based on a key field
- **Data migration**: Load data into MongoDB from other sources

## Configuration

### Replace mode (drop + insert)

```yaml
type: dagster_component_templates.MongodbWriterComponent
attributes:
  asset_name: write_products_to_mongo
  upstream_asset_key: transformed_products
  database: ecommerce
  collection: products
  if_exists: replace
  group_name: sinks
```

### Append mode

```yaml
type: dagster_component_templates.MongodbWriterComponent
attributes:
  asset_name: append_events_to_mongo
  upstream_asset_key: new_events
  database: analytics
  collection: events
  if_exists: append
```

### Upsert mode

```yaml
type: dagster_component_templates.MongodbWriterComponent
attributes:
  asset_name: upsert_users_to_mongo
  upstream_asset_key: user_updates
  database: myapp
  collection: users
  if_exists: upsert
  upsert_key: user_id
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `MONGODB_URI` | MongoDB connection string |

## Dependencies

- `pandas>=1.5.0`
- `pymongo>=4.0.0`
