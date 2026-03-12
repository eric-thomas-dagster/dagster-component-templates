# dbt Cloud Job Sensor

Polls the dbt Cloud Admin API and triggers a Dagster job when a dbt Cloud job run completes.

**Common pattern**: dbt Cloud transforms → this sensor detects completion → Dagster runs downstream ML/reporting jobs.
