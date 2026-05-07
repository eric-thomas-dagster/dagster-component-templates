# SAP HANA Resource

SAP HANA database resource — provides a SQLAlchemy URL helper. Works with HANA Cloud, on-prem, and Azure HANA (preview). Use with existing dataframe_to_table / dataframe_from_sql.

## Companion components

Use with existing components — they accept any SQLAlchemy URL:

```yaml
# Read from HANA
type: dagster_component_templates.DataframeFromSqlComponent
attributes:
  database_url_env_var: HANA_URL          # set HANA_URL=<this resource's url>
  query: 'SELECT * FROM SAP_S4HANA.MARA WHERE MTART = ''FERT'''

# Write to HANA
type: dagster_component_templates.DataframeToTableComponent
attributes:
  database_url_env_var: HANA_URL
  table_name: dagster_aggregations
  if_exists: replace
```

## Validation status

Code-validated against the sqlalchemy-hana dialect spec. End-to-end
validation requires a HANA instance (free trial via SAP BTP).
