# Redshift IO Manager

Register a Redshift IO manager so assets are automatically stored in and loaded from Amazon Redshift.

## Installation

```
pip install psycopg2-binary sqlalchemy pandas
```

## Configuration

```yaml
type: dagster_component_templates.RedshiftIOManagerComponent
attributes:
  resource_key: io_manager
  host: my-cluster.us-east-1.redshift.amazonaws.com
  database: dev
  user: dagster_user
  password_env_var: REDSHIFT_PASSWORD
  schema_name: public
```

## Authentication

```bash
export REDSHIFT_PASSWORD=your-password
```
