# PostgreSQL IO Manager

Register a PostgreSQL IO manager so assets are automatically stored in and loaded from Postgres tables via SQLAlchemy.

## Installation

```
pip install psycopg2-binary sqlalchemy pandas
```

## Configuration

```yaml
type: dagster_component_templates.PostgresIOManagerComponent
attributes:
  resource_key: io_manager
  host: localhost
  database: analytics
  user: dagster_user
  password_env_var: POSTGRES_PASSWORD
  schema_name: public
  if_exists: replace
```

## Authentication

```bash
export POSTGRES_PASSWORD=your-password
```
