# Dataframe Flatten Nested Columns

JSON-stringify columns whose values are dicts or lists. The standard fix for sinks (BigQuery `load_table_from_dataframe`, Snowflake's pandas writer, CSV writers) that can't infer a schema for object-dtype nested columns.

```yaml
type: dagster_component_templates.DataframeFlattenNestedColumnsComponent
attributes:
  asset_name: events_flat
  upstream_asset_key: recent_errors
```

## Typical chain

```
cloud_logging_query_asset    (produces dict-bearing DataFrame)
       └── events_flat       ← dataframe_flatten_nested_columns
                  └── events_bq  ← dataframe_to_bigquery
```

The flatten step is what makes the BQ load succeed — without it, you get `400 Empty schema specified for the load job. Please specify a schema that describes the data being loaded.`

## Behavior

- Default: walk every object-dtype column; if any row's value is a dict or list, JSON-encode the whole column. Scalar values pass through unchanged.
- `columns: [...]` — limit to a fixed allowlist (and only encode rows in those columns).
- `exclude_columns: [...]` — skip even auto-detected columns.

## When to use this vs. structured BQ types

If your target schema has `RECORD` / `STRUCT` / `JSON` columns and you want them to land that way (queryable as nested), pass an explicit `table_schema` to `dataframe_to_bigquery` instead of flattening. This component is the simpler "just stringify" path — JSON columns become BQ STRING. Query them with `JSON_QUERY` / `JSON_EXTRACT` server-side.

## Sister components

- `dataframe_to_bigquery` — common downstream
- `dataframe_to_gcs` — same fix when writing parquet with nested cells
- `dataframe_to_csv` — CSV can't represent nested at all; this is the standard pre-CSV step
