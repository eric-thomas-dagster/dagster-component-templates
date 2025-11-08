# S3 to Database Asset

Load files from S3 into database tables, designed to work with S3 sensors that pass file information via `run_config`.

## Overview

This component enables event-driven data ingestion from S3 to databases. It's designed to work seamlessly with sensors (like the S3 Sensor) that detect new files and pass their locations via run configuration.

### Sensor + Asset Pattern

```
┌──────────────┐         ┌─────────────────┐         ┌──────────────┐
│  S3 Sensor   │─────────│   Run Config    │─────────│ S3 to DB     │
│              │  pass   │                 │  reads  │   Asset      │
│ Detects new  │────────▶│  s3_bucket      │────────▶│              │
│ files in S3  │         │  s3_key         │         │ Writes to DB │
└──────────────┘         │  s3_size        │         └──────────────┘
                         │  s3_last_mod    │
                         └─────────────────┘
```

## Features

- **Event-Driven**: Triggered by sensors, processes files as they arrive
- **Multiple Formats**: Supports CSV, JSON, and Parquet files
- **Flexible DB Support**: Works with any SQLAlchemy-compatible database
- **Column Mapping**: Rename columns during ingestion
- **Type Casting**: Specify column data types
- **Schema Support**: Write to specific database schemas
- **Auto-Detection**: Automatically detect file format from extension
- **Chunked Writes**: Efficiently writes large datasets in batches

## Configuration

### Required Parameters

- **asset_name** (string) - Unique name for this asset
- **database_url** (string) - Database connection URL (use `${DB_URL}` for env var)
- **table_name** (string) - Name of the database table to write to

### Optional Parameters

- **schema_name** (string) - Database schema name
- **if_exists** (string) - Behavior if table exists: `fail`, `replace`, `append` (default: `append`)
- **aws_region** (string) - AWS region for S3 client
- **file_format** (string) - File format: `csv`, `json`, `parquet`, `auto` (default: `csv`)
- **csv_delimiter** (string) - CSV delimiter character (default: `,`)
- **json_orient** (string) - JSON orientation: `records`, `split`, `index`, `columns`, `values`
- **column_mapping** (string) - JSON mapping to rename columns
- **dtype_mapping** (string) - JSON mapping of column types
- **description** (string) - Asset description
- **group_name** (string) - Asset group for organization

## AWS Authentication

This component uses boto3 to access S3, which automatically discovers credentials from multiple sources **in this order**:

### Quick Start (Local Development)

**Option 1: AWS CLI Configuration** (Recommended)
```bash
# Install AWS CLI
brew install awscli  # macOS
# or: pip install awscli

# Configure credentials
aws configure
# Enter your AWS Access Key ID, Secret Access Key, and region

# Test access
aws s3 ls s3://your-bucket/

# Run Dagster (automatically uses configured credentials)
dagster dev
```

**Option 2: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"

dagster dev
```

**Option 3: AWS Profile**
```bash
# Use specific profile
export AWS_PROFILE=staging
dagster dev
```

### Production (IAM Roles)

When running on AWS infrastructure (EC2, ECS, EKS, Lambda):
- **No configuration needed!**
- AWS automatically provides credentials via IAM role
- This is the most secure option

### Credential Discovery Order

boto3 automatically checks these locations:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. AWS config file (`~/.aws/config`)
4. IAM role (when running on AWS infrastructure)
5. Container credentials (ECS, EKS)

### Troubleshooting

If you see "Unable to locate credentials":
```bash
# Test AWS access
aws s3 ls

# If this fails, configure credentials:
aws configure
```

For detailed setup instructions, see the **AWS Authentication Setup Guide** in the repository docs.

## Usage with S3 Sensor

### Step 1: Create the S3 Sensor

```yaml
# File: defs/components/s3_orders_sensor.yaml
type: dagster_designer_components.S3SensorComponent
attributes:
  sensor_name: s3_orders_sensor
  bucket_name: my-data-bucket
  prefix: incoming/orders/
  pattern: ".*\\.csv$"
  job_name: process_s3_files_job
  minimum_interval_seconds: 60
```

### Step 2: Create the S3 to Database Asset

```yaml
# File: defs/components/orders_to_db.yaml
type: dagster_component_templates.S3ToDatabaseAssetComponent
attributes:
  asset_name: orders_ingestion
  database_url: ${DATABASE_URL}
  table_name: orders
  schema_name: public
  if_exists: append
  file_format: csv
  description: Load order files from S3 to database
  group_name: ingestion
```

### Step 3: Create a Job

```yaml
# File: defs/components/process_orders_job.yaml
type: dagster_designer_components.JobComponent
attributes:
  job_name: process_s3_files_job
  asset_selection: ["orders_ingestion"]
  description: Process order files from S3
```

### How It Works

1. **Sensor Detects File**: S3 sensor detects new file `incoming/orders/2024-01-15.csv`
2. **Sensor Triggers Job**: Passes run_config with file information:
   ```python
   {
     "ops": {
       "config": {
         "s3_bucket": "my-data-bucket",
         "s3_key": "incoming/orders/2024-01-15.csv",
         "s3_size": 1024000,
         "s3_last_modified": "2024-01-15T10:30:00"
       }
     }
   }
   ```
3. **Asset Processes File**:
   - Downloads file from S3
   - Parses data (CSV/JSON/Parquet)
   - Applies transformations
   - Writes to database table

## Run Config Schema

This component expects run_config from sensors with:

```python
{
  "ops": {
    "config": {
      "s3_bucket": str,        # Required: S3 bucket name
      "s3_key": str,           # Required: Object key/path
      "s3_size": int,          # Optional: File size in bytes
      "s3_last_modified": str  # Optional: ISO format timestamp
    }
  }
}
```

## Database Connection Examples

### PostgreSQL
```
postgresql://user:password@localhost:5432/dbname
postgresql+psycopg2://user:password@localhost/dbname
```

### MySQL
```
mysql://user:password@localhost:3306/dbname
mysql+pymysql://user:password@localhost/dbname
```

### SQLite
```
sqlite:///path/to/database.db
```

### Snowflake
```
snowflake://user:password@account/database/schema?warehouse=wh
```

## Advanced Examples

### CSV with Column Mapping and Types

```yaml
type: dagster_component_templates.S3ToDatabaseAssetComponent
attributes:
  asset_name: customers_ingestion
  database_url: ${DATABASE_URL}
  table_name: customers
  file_format: csv
  csv_delimiter: "|"
  column_mapping: '{"cust_id": "customer_id", "cust_name": "name", "email_addr": "email"}'
  dtype_mapping: '{"customer_id": "int64", "signup_date": "datetime64[ns]"}'
```

### JSON Files

```yaml
type: dagster_component_templates.S3ToDatabaseAssetComponent
attributes:
  asset_name: events_ingestion
  database_url: ${DATABASE_URL}
  table_name: events
  file_format: json
  json_orient: records
  if_exists: append
```

### Parquet with Schema

```yaml
type: dagster_component_templates.S3ToDatabaseAssetComponent
attributes:
  asset_name: analytics_data
  database_url: ${DATABASE_URL}
  table_name: fact_orders
  schema_name: analytics
  file_format: parquet
  if_exists: append
```

### Auto-Detect Format

```yaml
type: dagster_component_templates.S3ToDatabaseAssetComponent
attributes:
  asset_name: multi_format_ingestion
  database_url: ${DATABASE_URL}
  table_name: data
  file_format: auto  # Detects .csv, .json, or .parquet
```

## Environment Variables

### Required
- **DATABASE_URL** - Database connection string

### Optional (if not using IAM roles)
- **AWS_ACCESS_KEY_ID** - AWS access key
- **AWS_SECRET_ACCESS_KEY** - AWS secret key
- **AWS_DEFAULT_REGION** - Default AWS region

## Requirements

### Python Packages
- boto3 >= 1.26.0
- pandas >= 2.0.0
- sqlalchemy >= 2.0.0
- pyarrow >= 10.0.0

### Database Drivers
Choose based on your database:
- PostgreSQL: `psycopg2-binary`
- MySQL: `pymysql`
- Snowflake: `snowflake-sqlalchemy`
- SQL Server: `pymssql`

### AWS Permissions
S3 bucket read permissions:
- `s3:GetObject`
- `s3:ListBucket`

## Complete Example: Orders Pipeline

### 1. S3 Sensor Configuration
```yaml
type: dagster_designer_components.S3SensorComponent
attributes:
  sensor_name: orders_sensor
  bucket_name: company-data
  prefix: orders/incoming/
  pattern: "orders_\\d{8}\\.csv$"  # Matches orders_20240115.csv
  job_name: orders_ingestion_job
  minimum_interval_seconds: 300
```

### 2. S3 to DB Asset Configuration
```yaml
type: dagster_component_templates.S3ToDatabaseAssetComponent
attributes:
  asset_name: orders_raw
  database_url: ${DATABASE_URL}
  table_name: orders_raw
  schema_name: staging
  file_format: csv
  csv_delimiter: ","
  if_exists: append
  column_mapping: '{"order_date": "created_at", "cust_id": "customer_id"}'
  dtype_mapping: '{"order_id": "int64", "customer_id": "int64", "amount": "float64"}'
  description: Raw orders from S3
  group_name: staging
```

### 3. Job Configuration
```yaml
type: dagster_designer_components.JobComponent
attributes:
  job_name: orders_ingestion_job
  asset_selection: ["orders_raw"]
  description: Ingest orders from S3 to database
```

### 4. Downstream Processing Asset
```python
# defs/assets.py
from dagster import asset

@asset(deps=["orders_raw"])
def orders_cleaned():
    """Clean and validate orders data."""
    # Your transformation logic here
    pass
```

## Troubleshooting

### Issue: "No run config provided"

**Solution:**
Ensure the asset is triggered by a sensor that passes run_config, not manually materialized.

### Issue: "Access Denied" from S3

**Solution:**
1. Verify AWS credentials are configured
2. Check S3 bucket permissions
3. Ensure IAM role/user has `s3:GetObject` permission

### Issue: "Table does not exist"

**Solution:**
1. Set `if_exists: 'fail'` to create the table on first run
2. Or manually create the table schema
3. Use `if_exists: 'replace'` to recreate the table

### Issue: "Connection refused" to database

**Solution:**
1. Verify DATABASE_URL is correct
2. Check database is running and accessible
3. Verify firewall/security group allows connections
4. Test connection manually: `psql $DATABASE_URL`

### Issue: Column type mismatch

**Solution:**
Use `dtype_mapping` to explicitly set column types:
```yaml
dtype_mapping: '{"id": "int64", "price": "float64", "date": "datetime64[ns]"}'
```

## Performance Tips

1. **Use Parquet**: Much faster than CSV for large files
2. **Set Appropriate Types**: Use `dtype_mapping` to avoid type inference
3. **Batch Processing**: Component automatically chunks writes in batches of 1000
4. **Compression**: Compress files in S3 to reduce download time
5. **Database Indexes**: Create indexes on frequently queried columns

## Monitoring

The asset provides metadata for each run:
- Number of rows inserted
- Number of columns
- Column names
- S3 source file details
- Target table information
- File format used

Access in Dagster UI under asset materialization metadata.

## Security Best Practices

1. **Use Environment Variables**: Never hard-code credentials
   ```yaml
   database_url: ${DATABASE_URL}
   ```

2. **IAM Roles**: Prefer IAM roles over access keys for S3 access

3. **Database Permissions**: Grant only necessary permissions:
   - CREATE TABLE (if table doesn't exist)
   - INSERT (for appending data)
   - TRUNCATE (if using replace mode)

4. **Network Security**: Use VPC endpoints for S3 and database access

## Contributing

Found a bug or have a feature request?
- Open an issue: https://github.com/eric-thomas-dagster/dagster-component-templates/issues
- Submit a PR: https://github.com/eric-thomas-dagster/dagster-component-templates/pulls

## License

MIT License
