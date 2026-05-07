# DataFrame → New Relic Logs

Push DataFrame rows as log events to New Relic. Each row becomes one log entry; configurable timestamp + message fields, with logtype for downstream parsing.

## Companion

- `newrelic_resource` — for ad-hoc NerdGraph calls
- `newrelic_nrql_query` — read events back via NRQL

## Validation

Code-validated against the New Relic Logs API spec.
