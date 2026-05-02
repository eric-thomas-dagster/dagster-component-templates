# SelectRecords

Pick a contiguous range of rows from a DataFrame: by absolute index, by head/tail count, or by specific indices. A simple cousin of `filter` that doesn't require a predicate — useful for paginated extracts and quick previews.

## Example

```yaml
type: dagster_component_templates.SelectRecordsComponent
attributes:
  asset_name: top_100
  upstream_asset_key: ranked_customers
  mode: head
  n: 100
  group_name: prep
```


## Requirements

```
pandas
```
