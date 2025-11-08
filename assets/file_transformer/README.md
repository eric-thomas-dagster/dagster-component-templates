# File Transformer Asset

Transform files between formats (CSV, JSON, Parquet, Excel) with optional data processing.

## Overview

Transform files automatically when triggered by file sensors, or process files on a schedule. Supports:
- **Formats**: CSV ↔ JSON ↔ Parquet ↔ Excel
- **Auto-detection**: Automatically detect input format
- **Data Processing**: Drop duplicates, fill NA values, select columns
- **Run Config Support**: Works seamlessly with Filesystem Sensor

## Quick Examples

### With Filesystem Sensor (Automatic)

```yaml
# Sensor detects CSV files, asset converts to Parquet
type: dagster_component_templates.FileTransformerComponent
attributes:
  asset_name: csv_to_parquet
  output_format: parquet
  output_directory: /data/processed
```

### Standalone (Fixed File)

```yaml
type: dagster_component_templates.FileTransformerComponent
attributes:
  asset_name: transform_data
  input_file_path: /data/input/data.csv
  output_format: parquet
  output_directory: /data/output
```

### With Data Processing

```yaml
type: dagster_component_templates.FileTransformerComponent
attributes:
  asset_name: clean_and_convert
  output_format: parquet
  output_directory: /data/clean
  drop_duplicates: true
  columns_to_keep: "id,name,date,amount"
  fill_na_value: "0"
```

## Configuration

### Required
- **asset_name** - Asset name
- **output_format** - Output format: `csv`, `json`, `parquet`, `excel`
- **output_directory** - Output directory path

### Optional
- **input_file_path** - Fixed input file (not needed with sensor)
- **input_format** - Input format or `auto` (default: `auto`)
- **output_filename** - Custom output filename
- **drop_duplicates** - Remove duplicate rows (default: `false`)
- **fill_na_value** - Value to replace NaN
- **columns_to_keep** - Comma-separated column list
- CSV/JSON/Parquet/Excel specific options

## Run Config Schema

When triggered by Filesystem Sensor:

```python
{
  "ops": {
    "config": {
      "file_path": str,      # From sensor
      "file_name": str,      # From sensor
      # ... other sensor data
    }
  }
}
```

## Complete Pipeline Example

### 1. Filesystem Sensor

```yaml
type: dagster_component_templates.FilesystemMonitorSensorComponent
attributes:
  sensor_name: watch_csv_files
  directory_path: /data/incoming
  file_pattern: ".*\\.csv$"
  job_name: convert_files_job
```

### 2. File Transformer Asset

```yaml
type: dagster_component_templates.FileTransformerComponent
attributes:
  asset_name: convert_to_parquet
  output_format: parquet
  output_directory: /data/processed
  parquet_compression: snappy
```

### 3. Job

```yaml
type: dagster_designer_components.JobComponent
attributes:
  job_name: convert_files_job
  asset_selection: ["convert_to_parquet"]
```

## Supported Transformations

- CSV → Parquet (most common)
- CSV → JSON
- JSON → Parquet
- Excel → CSV
- Parquet → CSV
- Any format to any format

## Requirements

- pandas >= 2.0.0
- pyarrow >= 10.0.0
- openpyxl >= 3.0.0 (for Excel support)

## License

MIT License
