# Database Query Asset

Execute SQL queries against databases and materialize results as Dagster assets.

## Overview

Run SQL queries against any SQLAlchemy-compatible database and materialize the results. Perfect for:
- Data extraction and transformation
- Creating derived datasets
- Aggregating data
- Building data marts
- Scheduled reporting queries

## Features

- **Any Database**: Works with PostgreSQL, MySQL, SQLite, Snowflake, etc.
- **Flexible Queries**: Execute any SQL SELECT statement
- **Caching**: Optional parquet caching for performance
- **Metadata**: Automatic tracking of rows, columns, and query details
- **Dependency Tracking**: Works with Dagster's asset lineage

## Quick Examples

### Basic Query

```yaml
type: dagster_component_templates.DatabaseQueryComponent
attributes:
  asset_name: daily_sales
  database_url: ${DATABASE_URL}
  query: "SELECT * FROM sales WHERE date = CURRENT_DATE"
```

### Aggregation Query

```yaml
type: dagster_component_templates.DatabaseQueryComponent
attributes:
  asset_name: monthly_revenue
  database_url: ${DATABASE_URL}
  query: |
    SELECT
      DATE_TRUNC('month', order_date) as month,
      SUM(amount) as total_revenue
    FROM orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY 1
    ORDER BY 1 DESC
```

### With Caching

```yaml
type: dagster_component_templates.DatabaseQueryComponent
attributes:
  asset_name: customer_metrics
  database_url: ${DATABASE_URL}
  query: "SELECT * FROM customer_metrics"
  cache_to_parquet: true
  cache_path: /data/cache/customer_metrics.parquet
```

## Configuration

### Required
- **asset_name** - Asset name
- **database_url** - Database connection URL (use `${DATABASE_URL}` for env var)
- **query** - SQL query to execute

### Optional
- **cache_to_parquet** - Cache results to parquet (default: `false`)
- **cache_path** - Path to cache file
- **description** - Asset description
- **group_name** - Asset group

## Database URLs

### PostgreSQL
```
postgresql://user:password@localhost:5432/dbname
```

### MySQL
```
mysql://user:password@localhost:3306/dbname
```

### SQLite
```
sqlite:///path/to/database.db
```

### Snowflake
```
snowflake://user:password@account/database/schema?warehouse=wh
```

## Use Cases

### 1. Daily Aggregations

```yaml
type: dagster_component_templates.DatabaseQueryComponent
attributes:
  asset_name: daily_metrics
  database_url: ${DATABASE_URL}
  query: |
    SELECT
      CURRENT_DATE as report_date,
      COUNT(*) as total_orders,
      SUM(amount) as total_revenue,
      AVG(amount) as avg_order_value
    FROM orders
    WHERE date = CURRENT_DATE
  group_name: daily_reports
```

### 2. Data Extraction

```yaml
type: dagster_component_templates.DatabaseQueryComponent
attributes:
  asset_name: active_customers
  database_url: ${DATABASE_URL}
  query: |
    SELECT *
    FROM customers
    WHERE status = 'active'
      AND last_purchase_date >= CURRENT_DATE - INTERVAL '90 days'
```

### 3. Downstream Processing

Query results can be used by downstream assets:

```python
from dagster import asset

@asset(deps=["daily_sales"])
def sales_analysis(context):
    # Process the daily_sales query results
    pass
```

## Requirements

- pandas >= 2.0.0
- sqlalchemy >= 2.0.0
- Database-specific drivers (psycopg2, pymysql, snowflake-sqlalchemy, etc.)

## License

MIT License
