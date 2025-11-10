# DuckDB Query Reader

Read and transform data from DuckDB tables using SQL queries, creating new assets from query results.

## Overview

This component executes SQL queries against a DuckDB database and returns the results as pandas DataFrames. Perfect for creating filtered, aggregated, or joined views of data stored in DuckDB.

## Features

- **Full SQL Support**: Use any SQL syntax DuckDB supports
- **Create Derived Assets**: Turn SQL queries into reusable assets
- **Efficient Filtering**: Load only the data you need
- **Complex Transformations**: Joins, aggregations, window functions
- **Read-Only Mode**: Safely query without modifying data
- **DataFrame Output**: Results as pandas DataFrame for downstream processing

## Use Cases

### 1. Data Filtering
Select subset of data based on conditions:
- Active customers only
- Recent transactions
- High-value products

### 2. Aggregations
Calculate metrics and statistics:
- Daily revenue totals
- Customer segments
- Product performance

### 3. Joins
Combine multiple tables:
- Orders with customer details
- Transactions with account info
- Events with user metadata

### 4. Derived Assets
Create new assets from SQL transformations:
- Customer lifetime value calculations
- Moving averages
- Ranking and percentiles

## Configuration

### Required Fields

- **asset_name** (string): Name of the asset to create
- **database_path** (string): Path to the DuckDB database file
- **query** (string): SQL SELECT query to execute

### Optional Fields

- **description** (string): Asset description
- **group_name** (string): Asset group for organization

## Example Usage

### Basic Filtering
```yaml
type: duckdb_query_reader.DuckDBQueryReaderComponent

attributes:
  asset_name: active_customers
  database_path: "{{ project_root }}/data/demo.duckdb"
  query: |
    SELECT *
    FROM customers
    WHERE is_active = true
```

### Aggregation Query
```yaml
type: duckdb_query_reader.DuckDBQueryReaderComponent

attributes:
  asset_name: daily_revenue
  database_path: "{{ project_root }}/data/ecommerce.duckdb"
  query: |
    SELECT
      DATE(order_date) as date,
      COUNT(*) as num_orders,
      SUM(total) as revenue,
      AVG(total) as avg_order_value
    FROM orders
    WHERE status = 'delivered'
    GROUP BY DATE(order_date)
    ORDER BY date DESC
  description: "Daily revenue metrics"
  group_name: analytics
```

### Join Multiple Tables
```yaml
type: duckdb_query_reader.DuckDBQueryReaderComponent

attributes:
  asset_name: customer_orders
  database_path: "{{ project_root }}/data/demo.duckdb"
  query: |
    SELECT
      c.customer_id,
      c.first_name,
      c.last_name,
      c.city,
      c.state,
      COUNT(o.order_id) as num_orders,
      SUM(o.total) as total_spent
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.state
    HAVING COUNT(o.order_id) > 0
    ORDER BY total_spent DESC
  description: "Customer purchase summary"
```

### Window Functions
```yaml
type: duckdb_query_reader.DuckDBQueryReaderComponent

attributes:
  asset_name: moving_average_metrics
  database_path: "{{ project_root }}/data/metrics.duckdb"
  query: |
    SELECT
      timestamp,
      value,
      AVG(value) OVER (
        ORDER BY timestamp
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
      ) as moving_avg_24h
    FROM sensor_readings
    ORDER BY timestamp
```

### Top N Query
```yaml
type: duckdb_query_reader.DuckDBQueryReaderComponent

attributes:
  asset_name: top_products
  database_path: "{{ project_root }}/data/demo.duckdb"
  query: |
    SELECT
      product_id,
      name,
      category,
      price,
      stock_quantity,
      rating,
      num_reviews
    FROM products
    WHERE is_available = true
    AND rating >= 4.0
    ORDER BY num_reviews DESC
    LIMIT 100
  description: "Top 100 products by review count"
```

## Pipeline Integration

### Complete Demo Pipeline
```
1. Synthetic Data Generator (customers)
   ↓
2. DuckDB Table Writer (write to demo.duckdb/customers)
   ↓
3. DuckDB Query Reader (filter active, high-value customers)
   ↓
4. DataFrame Transformer (add customer tier)
   ↓
5. Data Quality Checks (validate tiers)
```

### Multi-Step Analysis
```
DuckDB Query Reader (raw data)
   ↓
DataFrame Transformer (clean & prepare)
   ↓
DuckDB Table Writer (write prepared data)
   ↓
DuckDB Query Reader (aggregated metrics)
```

## SQL Tips

### Performance
- Use `WHERE` clauses to filter early
- Select only needed columns, not `SELECT *`
- Create indexes for large tables (via raw DuckDB)
- Use `LIMIT` for exploratory queries

### Date/Time Functions
```sql
-- Today's data
WHERE DATE(timestamp) = CURRENT_DATE

-- Last 7 days
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'

-- This month
WHERE EXTRACT(MONTH FROM timestamp) = EXTRACT(MONTH FROM CURRENT_DATE)
```

### String Operations
```sql
-- Case-insensitive search
WHERE LOWER(name) LIKE '%keyword%'

-- String concatenation
SELECT first_name || ' ' || last_name as full_name

-- String replacement
SELECT REPLACE(email, '@example.com', '') as username
```

### Null Handling
```sql
-- Replace nulls
SELECT COALESCE(phone, 'N/A') as phone

-- Filter nulls
WHERE email IS NOT NULL

-- Count non-nulls
SELECT COUNT(phone) as phones_provided
```

## Output

Returns a pandas DataFrame with columns defined by your SELECT query.

Example:
```python
# Query: SELECT customer_id, first_name, city FROM customers LIMIT 3
#
# Output DataFrame:
#   customer_id  first_name    city
# 0  CUST000001        John  New York
# 1  CUST000002        Jane  Los Angeles
# 2  CUST000003     Michael  Chicago
```

## Troubleshooting

### Database Not Found
- Ensure a DuckDB Table Writer has run first
- Check the `database_path` is correct
- Verify the path uses `{{ project_root }}` for portability

### Table Not Found
- Check table was created by DuckDB Table Writer
- Verify table name spelling (case-sensitive)
- List tables: `SELECT * FROM information_schema.tables`

### Query Syntax Error
- Test query in DuckDB CLI or Python first
- Check for typos in table/column names
- Use proper quote types (single quotes for strings)

### Empty Results
- Add logging to verify query logic
- Test with simpler query first
- Check WHERE conditions aren't too restrictive

## Best Practices

1. **Descriptive Names**: Use clear asset names that describe the query result
2. **Document Queries**: Add comments in SQL for complex queries
3. **Test Queries**: Validate SQL before deploying
4. **Use Placeholders**: Employ `{{ project_root }}` for portable paths
5. **Group Related Assets**: Use `group_name` to organize analytics assets
6. **Start Simple**: Begin with basic queries, add complexity incrementally

## Technical Notes

- Connects to DuckDB in read-only mode for safety
- Query must be a SELECT statement
- Full DuckDB SQL dialect supported
- Results are loaded into memory as DataFrame
- For very large results, consider adding LIMIT or WHERE clauses
- Database connection is automatically closed after query
