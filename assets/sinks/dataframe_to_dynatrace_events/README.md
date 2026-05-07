# DataFrame → Dynatrace Events

Push DataFrame rows as Dynatrace events via the v2 Events API. Use for batch-job markers (deployments, ETL completions, anomalies) that should annotate Dynatrace's timeline.

## Event types

- CUSTOM_INFO — generic info events
- CUSTOM_DEPLOYMENT — deployment markers (great for ETL job completion)
- CUSTOM_ANNOTATION — annotations on existing events
- AVAILABILITY_EVENT, ERROR_EVENT, PERFORMANCE_EVENT, RESOURCE_CONTENTION_EVENT — synthetic events that drive Dynatrace problem detection
