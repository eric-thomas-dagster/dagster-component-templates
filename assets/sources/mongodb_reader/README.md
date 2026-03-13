# MongoDB Reader Component

Read documents from a MongoDB collection and return them as a Dagster asset DataFrame.

## Overview

The `MongodbReaderComponent` connects to a MongoDB instance and reads documents from a specified collection, supporting filtering, projection, sorting, and limiting. Results are returned as a pandas DataFrame with `_id` converted to string.

## Use Cases

- **Operational data pipelines**: Extract application data stored in MongoDB
- **Analytics**: Pull user activity or event documents for analysis
- **ETL**: Migrate MongoDB documents to a data warehouse
- **Data quality checks**: Sample and validate MongoDB collection contents

## Configuration

### Basic read (all documents)

```yaml
type: dagster_component_templates.MongodbReaderComponent
attributes:
  asset_name: mongo_products
  database: ecommerce
  collection: products
```

### Filtered and sorted read

```yaml
type: dagster_component_templates.MongodbReaderComponent
attributes:
  asset_name: active_users
  database: myapp
  collection: users
  query:
    active: true
    role: admin
  sort_field: created_at
  sort_direction: -1
  limit: 1000
  group_name: sources
```

### With field projection

```yaml
type: dagster_component_templates.MongodbReaderComponent
attributes:
  asset_name: user_emails
  database: myapp
  collection: users
  projection:
    email: 1
    name: 1
    _id: 0
```

## Output Schema

| Column | Description |
|--------|-------------|
| `_id` | MongoDB document ID (converted to string) |
| `[document fields]` | All document fields (or projected subset) |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `MONGODB_URI` | MongoDB connection string (default env var name) |

## Dependencies

- `pandas>=1.5.0`
- `pymongo>=4.0.0`
