# CSV File Ingestion Asset

Ingest CSV files into Dagster assets as pandas DataFrames with extensive configuration options.

## Features

- Read CSV files with configurable delimiters and encodings
- Select specific columns to read
- Parse date columns automatically
- Specify data types for columns
- Optional parquet caching for improved performance
- Automatic metadata tracking (row count, columns, memory usage)
- Skip header rows
- Configurable header row position

## Configuration

### Required Parameters

- **asset_name** (string) - Unique name for this asset
- **file_path** (string) - Path to the CSV file to ingest

### Optional Parameters

- **description** (string) - Description of this asset
- **group_name** (string) - Asset group name for organization
- **delimiter** (string) - CSV delimiter character (default: `,`)
- **encoding** (string) - File encoding (default: `utf-8`)
  - Options: `utf-8`, `latin-1`, `iso-8859-1`, `cp1252`
- **skip_rows** (number) - Number of rows to skip at the start (default: 0)
- **header_row** (number) - Row number to use as column names, 0-indexed (default: 0)
- **columns_to_read** (string) - Comma-separated list of column names to read (empty = all columns)
- **dtype_mapping** (string) - JSON mapping of column names to data types
- **parse_dates** (string) - Comma-separated list of columns to parse as dates
- **cache_to_parquet** (boolean) - Cache data to parquet for better performance (default: false)
- **parquet_path** (string) - Path for parquet cache (auto-generated if empty)

## Usage Example

### Basic CSV Ingestion

```yaml
type: dagster_component_templates.CSVFileIngestionComponent
attributes:
  asset_name: customer_data
  file_path: /data/customers.csv
  description: Customer master data
  group_name: raw_data
```

### Advanced Configuration with Type Mapping

```yaml
type: dagster_component_templates.CSVFileIngestionComponent
attributes:
  asset_name: sales_transactions
  file_path: /data/sales.csv
  description: Daily sales transactions
  group_name: raw_data
  delimiter: ","
  encoding: utf-8
  skip_rows: 2
  columns_to_read: transaction_id, customer_id, amount, created_at
  dtype_mapping: '{"transaction_id": "int64", "customer_id": "int64", "amount": "float64"}'
  parse_dates: created_at
  cache_to_parquet: true
  parquet_path: /data/cache/sales.parquet
```

### Tab-Separated File

```yaml
type: dagster_component_templates.CSVFileIngestionComponent
attributes:
  asset_name: log_data
  file_path: /logs/application.tsv
  delimiter: "\t"
  encoding: utf-8
```

## How It Works

1. **File Reading**: The component reads the CSV file using pandas `read_csv()`
2. **Type Conversion**: Automatically converts columns to specified data types
3. **Date Parsing**: Parses specified columns as datetime objects
4. **Caching**: Optionally caches data to parquet format for faster subsequent reads
5. **Metadata**: Tracks row count, column names, and memory usage
6. **DataFrame Output**: Returns a pandas DataFrame for downstream assets

## Parquet Caching

When `cache_to_parquet` is enabled:

1. First run: Reads CSV and saves to parquet cache
2. Subsequent runs: Uses parquet cache if it's newer than the CSV file
3. Cache invalidation: Automatically re-reads CSV if source file is updated

This can dramatically improve performance for large CSV files:
- 10x faster read times
- Smaller file sizes (compressed)
- Preserves data types

## Data Type Mapping

Specify column types using JSON format in `dtype_mapping`:

```json
{
  "customer_id": "int64",
  "revenue": "float64",
  "active": "bool",
  "category": "category"
}
```

Common pandas dtypes:
- `int64` - Integer
- `float64` - Float
- `bool` - Boolean
- `str` or `object` - String
- `category` - Categorical (memory efficient for repeated values)

## Date Parsing

List columns to parse as dates in `parse_dates`:

```
created_at, updated_at, deleted_at
```

Pandas will attempt to automatically detect and parse date formats.

## Column Selection

Read only specific columns with `columns_to_read`:

```
customer_id, name, email, revenue
```

This reduces memory usage and improves performance for wide CSV files.

## Encoding Options

Common encodings for CSV files:
- `utf-8` - Standard UTF-8 (default, recommended)
- `latin-1` / `iso-8859-1` - Western European
- `cp1252` - Windows Western European

If you encounter encoding errors, try `latin-1` or `cp1252`.

## Requirements

### Python Packages

- pandas >= 2.0.0
- pyarrow >= 10.0.0 (for parquet support)

### File Access

- Read access to the CSV file path
- Write access to parquet cache location (if caching enabled)

## Downstream Usage

The asset returns a pandas DataFrame that can be used by downstream assets:

```python
from dagster import asset

@asset(deps=["customer_data"])
def customer_analysis(context):
    # Access the DataFrame
    df = context.resources.io_manager.load_input(context, "customer_data")

    # Perform analysis
    summary = df.groupby("segment")["revenue"].sum()

    return summary
```

## Performance Tips

1. **Enable Parquet Caching**: For large CSV files (>100MB), enable caching
2. **Select Columns**: Only read columns you need with `columns_to_read`
3. **Specify Data Types**: Use `dtype_mapping` to avoid automatic type inference
4. **Use Appropriate Delimiter**: Ensure delimiter matches your file format
5. **Skip Unnecessary Rows**: Use `skip_rows` to skip header comments

## Troubleshooting

### Issue: "FileNotFoundError"

**Solution:**
1. Verify the file path is correct and absolute
2. Ensure the file exists and is readable
3. Check file permissions

### Issue: "UnicodeDecodeError"

**Solution:**
1. Try different encodings: `latin-1`, `cp1252`, `iso-8859-1`
2. Inspect the file in a text editor to identify encoding
3. Use `encoding='latin-1'` which is very permissive

### Issue: "ValueError: could not convert string to float"

**Solution:**
1. Check for non-numeric values in numeric columns
2. Use `dtype_mapping` to explicitly set column types
3. Clean data before ingestion or handle in downstream assets

### Issue: Slow Performance

**Solution:**
1. Enable parquet caching with `cache_to_parquet: true`
2. Use `columns_to_read` to read only needed columns
3. Specify data types with `dtype_mapping` to avoid type inference
4. Consider chunked reading for very large files (modify component)

### Issue: "ParserError: Error tokenizing data"

**Solution:**
1. Verify delimiter is correct
2. Check for malformed rows in the CSV
3. Use `skip_rows` to skip problematic header rows
4. Inspect the CSV file for structural issues

## Advanced Examples

### Reading CSV with Complex Header

```yaml
type: dagster_component_templates.CSVFileIngestionComponent
attributes:
  asset_name: survey_results
  file_path: /data/survey.csv
  skip_rows: 3  # Skip first 3 rows of metadata
  header_row: 0  # First non-skipped row is header
  encoding: utf-8
```

### Optimized Large File Ingestion

```yaml
type: dagster_component_templates.CSVFileIngestionComponent
attributes:
  asset_name: transaction_log
  file_path: /data/transactions.csv
  columns_to_read: id, user_id, amount, timestamp
  dtype_mapping: '{"id": "int64", "user_id": "int64", "amount": "float64"}'
  parse_dates: timestamp
  cache_to_parquet: true
  parquet_path: /data/cache/transactions.parquet
```

### European CSV Format (Semicolon Delimiter)

```yaml
type: dagster_component_templates.CSVFileIngestionComponent
attributes:
  asset_name: european_data
  file_path: /data/eu_data.csv
  delimiter: ";"
  encoding: latin-1
```

## Contributing

Found a bug or have a feature request?

- Open an issue: https://github.com/eric-thomas-dagster/dagster-component-templates/issues
- Submit a PR: https://github.com/eric-thomas-dagster/dagster-component-templates/pulls

## License

MIT License
