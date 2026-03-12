# Fivetran Sync Sensor

Polls the Fivetran REST API and triggers a Dagster job when a connector sync completes successfully.

This closes the most common enterprise handoff pattern: **Fivetran loads → Dagster transforms.**

## Three-component pattern

1. Fivetran runs on its own schedule and loads raw data
2. This sensor detects completion via the Fivetran API
3. Dagster job processes the freshly-loaded data
