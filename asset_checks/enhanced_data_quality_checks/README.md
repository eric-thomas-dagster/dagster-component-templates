# Enhanced Data Quality Checks Component

A comprehensive data quality validation framework with 15+ check types, automatic processing mode selection, and support for multi-asset configuration.

## Overview

The Enhanced Data Quality Checks component provides a powerful, flexible system for validating data quality across your Dagster assets. It automatically selects the best processing method (SQL vs dataframe) based on your configuration and supports both pandas and polars dataframes.

## Key Features

- **15+ Check Types**: From basic row counts and null checks to advanced anomaly detection and Benford's Law analysis
- **Automatic Processing**: Component automatically chooses between SQL and dataframe processing for optimal performance
- **Multi-Asset Support**: Configure checks for multiple assets in a single component definition
- **Grouping Support**: Run checks on grouped data (e.g., per date, per company, per region)
- **Historical Trend Analysis**: Compare current values against historical data for anomaly and delta detection
- **Environment-Aware**: Support for environment-specific table names and database connections
- **Performance Optimized**: Built-in data sampling for large datasets
- **Flexible Filtering**: Time-based filtering and custom WHERE clauses

## Check Types

### Basic Validation Checks

#### 1. Row Count Check
Validates that the number of rows falls within expected bounds.

```yaml
row_count_check:
  - name: "users_row_count"
    min_rows: 10
    max_rows: 1000000
    group_by: "company"  # Optional: check each group separately
    allowed_failures: 1  # Optional: allow 1 group to fail
    blocking: true
```

#### 2. Null Check
Checks for null values in specified columns.

```yaml
null_check:
  - name: "critical_columns_null_check"
    columns: ["user_id", "email", "created_at"]
    blocking: true
```

#### 3. Data Type Check
Validates that columns have the expected data types.

```yaml
data_type_check:
  - name: "users_data_type"
    columns:
      - column: "user_id"
        expected_type: "int"
      - column: "is_active"
        expected_type: "bool"
      - column: "created_at"
        expected_type: "datetime"
    blocking: true
```

#### 4. Range Check
Validates that numeric values fall within specified ranges.

```yaml
range_check:
  - name: "price_range"
    columns:
      - column: "price"
        min_value: 0.01
        max_value: 10000
      - column: "quantity"
        min_value: 1
        max_value: 100
    blocking: false
```

### Pattern and Value Validation

#### 5. Pattern Matching
Validates text columns against regex patterns.

```yaml
pattern_matching:
  - name: "email_format"
    column: "email"
    regex_pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    match_percentage: 99.0
    blocking: false
```

#### 6. Value Set Validation
Ensures column values are within an allowed set.

```yaml
value_set_validation:
  - name: "status_validation"
    column: "status"
    allowed_values: ["active", "inactive", "pending"]
    min_pct: 99.0
    blocking: true
```

#### 7. Uniqueness Check
Validates uniqueness of column combinations.

```yaml
uniqueness_check:
  - name: "user_email_uniqueness"
    columns:
      - columns: ["email"]  # Single column
  - name: "order_uniqueness"
    columns:
      - columns: ["user_id", "order_id"]  # Composite key
    blocking: true
```

### Statistical and Threshold Checks

#### 8. Static Threshold
Validates metrics against static thresholds.

```yaml
static_threshold:
  - name: "avg_order_value"
    metric: "mean:order_total"
    min_value: 10.0
    max_value: 500.0
    group_by: "date"
    allowed_failures: 2
    blocking: false
```

Supported metrics:
- `num_rows`: Row count
- `mean:column_name`: Average value
- `sum:column_name`: Sum of values
- `min:column_name`: Minimum value
- `max:column_name`: Maximum value

#### 9. Anomaly Detection
Detects anomalies in metrics using statistical methods.

```yaml
anomaly_detection:
  - name: "order_count_anomaly"
    metric: "num_rows"
    method: "z_score"  # z_score, iqr, or isolation_forest
    threshold: 2.0
    history: 10  # Use last 10 data points
    group_by: "date"
    allowed_failures: 1
    blocking: false
```

#### 10. Percent Delta
Tracks percent changes from historical values.

```yaml
percent_delta:
  - name: "order_volume_delta"
    metric: "num_rows"
    max_delta: 50.0  # Max 50% change allowed
    history: 5
    group_by: "date"
    allowed_failures: 1
    blocking: false
```

### Advanced Statistical Checks

#### 11. Entropy Analysis
Analyzes data diversity using Shannon entropy.

```yaml
entropy_analysis:
  - name: "product_diversity"
    column: "product_category"
    min_entropy: 1.0
    max_entropy: 3.0
    blocking: false
```

#### 12. Benford's Law
Validates numerical distributions against Benford's Law.

```yaml
benford_law:
  - name: "amount_distribution"
    column: "transaction_amount"
    threshold: 0.05
    digit_position: 1  # 1=first digit, 2=second, 12=first two
    min_samples: 100
    blocking: false
```

#### 13. Correlation Check
Analyzes correlations between column pairs.

```yaml
correlation_check:
  - name: "price_quantity_correlation"
    column_x: "price"
    column_y: "quantity"
    method: "pearson"  # pearson, spearman, or kendall
    min_correlation: -0.5
    max_correlation: 0.5
    blocking: false
```

#### 14. Predicted Range
Validates values against predicted ranges from historical data.

```yaml
predicted_range:
  - name: "predicted_revenue"
    metric: "sum:revenue"
    method: "moving_average"  # moving_average, linear_regression, exponential_smoothing, arima
    confidence: 0.95
    history: 10
    blocking: false
```

#### 15. Distribution Change
Detects distribution changes using statistical tests.

```yaml
distribution_change:
  - name: "value_distribution"
    metric: "mean:order_total"
    method: "ks_test"  # ks_test or chi_square
    significance_level: 0.05
    blocking: false
```

### Cross-Table and Custom Checks

#### 16. Cross-Table Validation
Validates data consistency across tables.

```yaml
cross_table_validation:
  - name: "order_user_consistency"
    source_table: "users"
    source_database: "postgres"
    join_columns: ["user_id"]
    validation_type: "row_count"  # row_count, column_values, or aggregate
    blocking: true
```

#### 17. Custom SQL Check
Execute custom SQL queries.

```yaml
custom_sql_check:
  - name: "order_total_validation"
    sql_query: "SELECT COUNT(*) FROM orders WHERE total != quantity * price"
    expected_result: 0
    comparison: "equals"
    description: "Ensure order totals are calculated correctly"
    blocking: true
```

#### 18. Custom Dataframe Check
Execute custom Python code on dataframes.

```yaml
custom_dataframe_check:
  - name: "dataframe_validation"
    python_code: "len(df[df['price'] > df['list_price']]) == 0"
    expected_result: True
    comparison: "equals"
    description: "Ensure sale price never exceeds list price"
    blocking: true
```

## Multi-Asset Configuration

Configure checks for multiple assets in a single component:

```yaml
type: dagster_component_templates.EnhancedDataQualityChecksComponent
attributes:
  assets:
    RAW_DATA.users:
      row_count_check:
        - name: "users_row_count"
          min_rows: 10
          max_rows: 1000000
      null_check:
        - name: "users_null_check"
          columns: ["user_id", "email"]

    RAW_DATA.orders:
      database_resource_key: "postgres"
      table_name: "public.orders"
      row_count_check:
        - name: "orders_row_count"
          min_rows: 100
      range_check:
        - name: "orders_range"
          columns:
            - column: "price"
              min_value: 0
              max_value: 10000
```

## Common Configuration Options

All check types support these common options:

- `name` (required): Unique name for the check
- `group_by` (optional): Column to group by for analysis
- `allowed_failures` (optional): Number of groups allowed to fail (default: 0)
- `blocking` (optional): Whether check failure blocks the pipeline (default: false)
- `severity` (optional): "WARN" or "ERROR" (default: "WARN")

## Global Configuration

### Data Source Configuration

```yaml
data_source_type: "database"  # or "dataframe"
database_resource_key: "duckdb"
table_name: "analytics.orders"
```

### Environment-Specific Configuration

```yaml
table_name_targets:
  dev: "dev_schema.orders"
  staging: "staging_schema.orders"
  prod: "prod_schema.orders"

database_resource_key_targets:
  dev: "dev_db"
  staging: "staging_db"
  prod: "prod_db"
```

### Data Filtering

```yaml
# Time-based filtering
time_filter_column: "created_at"
days_back: 7  # Last 7 days

# Or custom WHERE clause
where_clause: "status = 'active' AND region = 'US'"
```

### Performance Optimization

```yaml
# Sample large datasets for improved performance
sample_size: 10000
sample_method: "random"  # or "top"
```

## Complete Example

```yaml
type: dagster_component_templates.EnhancedDataQualityChecksComponent
attributes:
  assets:
    CLEANED.orders_cleaned:
      database_resource_key: "postgres"
      table_name: "analytics.orders_cleaned"

      # Row count with grouping
      row_count_check:
        - name: "orders_row_count_by_date"
          min_rows: 10
          max_rows: 1000000
          group_by: "date"
          allowed_failures: 2
          blocking: true

      # Null checks for critical columns
      null_check:
        - name: "critical_columns"
          columns: ["order_id", "user_id", "total", "date"]
          blocking: true

      # Data type validation
      data_type_check:
        - name: "column_types"
          columns:
            - column: "order_id"
              expected_type: "int"
            - column: "total"
              expected_type: "float"
            - column: "date"
              expected_type: "datetime"
          blocking: true

      # Range validation with grouping
      range_check:
        - name: "order_values"
          columns:
            - column: "total"
              min_value: 0.01
              max_value: 100000
            - column: "quantity"
              min_value: 1
              max_value: 100
          group_by: "date"
          allowed_failures: 2
          blocking: false

      # Anomaly detection
      anomaly_detection:
        - name: "order_count_anomaly"
          metric: "num_rows"
          method: "z_score"
          threshold: 2.0
          history: 10
          group_by: "date"
          allowed_failures: 1
          blocking: false

      # Custom SQL validation
      custom_sql_check:
        - name: "total_calculation"
          sql_query: "SELECT COUNT(*) FROM analytics.orders_cleaned WHERE total != quantity * price"
          expected_result: 0
          comparison: "equals"
          description: "Ensure order totals are calculated correctly"
          blocking: true
```

## Installation

1. Install the component in your Dagster project:
   ```bash
   # Using the Dagster Designer UI or CLI
   dg component install enhanced_data_quality_checks
   ```

2. Add the required dependencies to your project:
   ```bash
   pip install pandas numpy scipy
   ```

3. Create a `defs.yaml` file with your check configurations (see examples above)

## Tips and Best Practices

1. **Start Simple**: Begin with basic checks (row count, null checks) before adding advanced statistical checks

2. **Use Grouping**: Group checks by date, region, or other dimensions to catch localized issues

3. **Set Appropriate Thresholds**: Use `allowed_failures` to tolerate expected variations in grouped data

4. **Combine Check Types**: Use multiple complementary check types for comprehensive validation

5. **Blocking vs Non-Blocking**: Mark critical checks as `blocking: true` to prevent bad data from propagating

6. **Historical Checks**: Use anomaly detection and percent delta for time-series data

7. **Performance**: Use `sample_size` for large datasets to improve check performance

8. **Environment-Specific**: Use target configurations for environment-specific database connections

## Troubleshooting

### Check is failing unexpectedly
- Review the check metadata in the Dagster UI for detailed failure information
- Adjust thresholds or `allowed_failures` if failures are expected
- Use grouping to isolate issues to specific data segments

### Performance issues
- Enable data sampling with `sample_size` for large datasets
- Use SQL-based checks when possible (automatically selected for database sources)
- Consider filtering data with `where_clause` or time filters

### Environment configuration not working
- Ensure `DAGSTER_CLOUD_DEPLOYMENT_NAME` environment variable is set correctly
- Check that target keys match your environment names
- Verify database resource keys exist in your Dagster instance

## Support

For issues, questions, or contributions, please visit the [Dagster Community Slack](https://dagster.io/slack) or [GitHub repository](https://github.com/dagster-io/dagster).
