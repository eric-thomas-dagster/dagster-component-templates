# New Relic Resource

New Relic API client wrapper (NerdGraph + Logs/Metrics/Events ingestion endpoints). Supports US + EU regions.

## Companion components

- `dataframe_to_newrelic_logs` — push DataFrame as log events
- `newrelic_nrql_query` — NRQL query → DataFrame

## Auth

User API Key from New Relic UI: User menu → API Keys → Create key → User. Set the env var pointed to by `api_key_env_var`.

## Validation

Code-validated against New Relic's NerdGraph + Logs/Metrics/Events API spec. End-to-end validation requires a New Relic account (free tier available).
