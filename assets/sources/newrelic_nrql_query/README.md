# New Relic NRQL Query

Run an NRQL query via NerdGraph → DataFrame. Use for ops dashboards, anomaly detection, capacity planning, alerting trends.

## NRQL examples

```
SELECT count(*) FROM Transaction SINCE 1 hour ago FACET appName
SELECT percentile(duration, 95) FROM Transaction WHERE appName='checkout' SINCE 1 day ago TIMESERIES
SELECT count(*) FROM Log WHERE level='ERROR' SINCE 1 hour ago FACET service
```
