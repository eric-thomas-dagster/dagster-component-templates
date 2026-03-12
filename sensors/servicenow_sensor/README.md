# ServiceNow Sensor

Polls the ServiceNow Table API and triggers Dagster jobs when records match a target state.

Common use cases:
- Trigger a data pipeline when a change request is approved
- React to incident creation in a monitored category
- Start a compliance report when an audit request is opened
