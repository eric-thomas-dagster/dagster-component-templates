# DataFrame Transformer Asset

Transform DataFrames from upstream assets using Dagster's IO manager pattern. Works seamlessly with visual dependency drawing - just connect DataFrame-producing assets!

## Overview

This component transforms DataFrames automatically passed from upstream assets via IO managers. Simply draw connections in the visual editor - no configuration needed for dependencies!

**Compatible with:**
- REST API Fetcher (with `output_format: dataframe`)
- Database Query
- CSV File Ingestion
- Other DataFrame Transformers

## Quick Start

### 1. Create Upstream Asset

```yaml
# REST API that produces DataFrame
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: api_data
  api_url: https://api.example.com/sales
  output_format: dataframe
```

### 2. Create Transformer

```yaml
# No upstream_asset_key needed - set by drawing connections!
type: dagster_component_templates.DataFrameTransformerComponent
attributes:
  asset_name: cleaned_data
  drop_duplicates: true
  filter_columns: "id,name,amount"
```

### 3. Draw Connection

In the visual editor: `api_data` → `cleaned_data`

That's it! The IO manager automatically passes the DataFrame.

## Features

- **Visual Dependencies**: Draw connections, no manual configuration
- **Runtime Validation**: Clear errors if non-DataFrame inputs detected
- **Multiple Inputs**: Accepts multiple DataFrames (concat, merge, or use first)
- **Column Operations**: Filter, drop, rename columns
- **Row Operations**: Drop duplicates, drop/fill NA values
- **Data Filtering**: Pandas query expressions
- **Sorting**: Sort by columns
- **Aggregation**: Group by and aggregate

## Configuration

### Required
- **asset_name** - Name of this asset

### Optional Transformations
- **filter_columns** - Comma-separated columns to keep
- **drop_columns** - Comma-separated columns to drop
- **rename_columns** - JSON mapping: `{"old": "new"}`
- **drop_duplicates** - Remove duplicate rows
- **drop_na** - Remove rows with NA
- **fill_na_value** - Fill NA with value
- **filter_expression** - Pandas query: `"amount > 100"`
- **sort_by** - Comma-separated columns to sort by
- **sort_ascending** - Sort direction (default: true)
- **group_by** - Comma-separated columns to group by
- **agg_functions** - JSON: `{"amount": "sum", "id": "count"}`
- **combine_method** - How to combine multiple inputs: `concat`, `merge`, or `first`

## Example Pipeline

```yaml
# 1. Fetch from API
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: sales_api
  api_url: https://api.example.com/sales
  output_format: dataframe

# 2. Clean data
type: dagster_component_templates.DataFrameTransformerComponent
attributes:
  asset_name: sales_cleaned
  drop_duplicates: true
  filter_columns: "order_id,customer_id,amount,date"
  fill_na_value: "0"

# 3. Aggregate
type: dagster_component_templates.DataFrameTransformerComponent
attributes:
  asset_name: daily_summary
  group_by: "date"
  agg_functions: '{"amount": "sum", "order_id": "count"}'
```

**Visual Connections:**
```
sales_api → sales_cleaned → daily_summary
```

## Error Handling

If you connect a non-DataFrame asset, you'll get a clear error:

```
TypeError: DataFrame Transformer 'cleaned_data' received non-DataFrame inputs:
  - 'some_asset': dict

This component only accepts DataFrame inputs. Compatible assets:
  - REST API Fetcher (set output_format: dataframe)
  - Database Query (returns DataFrames by default)
  - CSV File Ingestion (returns DataFrames by default)
```

## Multiple DataFrame Inputs

Connect multiple DataFrames and choose how to combine them:

```yaml
combine_method: concat  # Stack vertically (default)
combine_method: merge   # SQL-style merge
combine_method: first   # Use only the first DataFrame
```

## Requirements

- pandas >= 2.0.0
- pyarrow >= 10.0.0

## License

MIT License
