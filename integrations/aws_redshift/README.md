# AWS Redshift

Imports AWS Redshift stored procedures and materialized views as Dagster assets using the Redshift Data API. Materialization executes the stored procedure (`CALL`) or refreshes the materialized view (`REFRESH MATERIALIZED VIEW`).

## Required packages

```
boto3>=1.26.0
botocore>=1.29.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `aws_region` | Yes | — | AWS region |
| `cluster_identifier` | Yes | — | Redshift cluster identifier |
| `database` | Yes | — | Redshift database name |
| `aws_access_key_id` | No | `null` | AWS access key ID (omit to use IAM role) |
| `aws_secret_access_key` | No | `null` | AWS secret access key |
| `aws_session_token` | No | `null` | AWS session token |
| `db_user` | No | `null` | Database user (omit to use IAM auth) |
| `secret_arn` | No | `null` | AWS Secrets Manager ARN for DB credentials |
| `import_stored_procedures` | No | `true` | Import stored procedures as assets |
| `import_materialized_views` | No | `true` | Import materialized views as assets |
| `import_scheduled_queries` | No | `true` | Import scheduled queries (placeholder) |
| `schema_name` | No | `public` | Schema to query for procedures and views |
| `filter_by_name_pattern` | No | `null` | Regex to filter entities by name |
| `exclude_name_pattern` | No | `null` | Regex to exclude entities by name |
| `group_name` | No | `aws_redshift` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.AWSRedshiftComponent
attributes:
  aws_region: us-east-1
  cluster_identifier: my-redshift-cluster
  database: analytics
  db_user: dagster
  import_stored_procedures: true
  import_materialized_views: true
  schema_name: public
  group_name: aws_redshift
```

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
