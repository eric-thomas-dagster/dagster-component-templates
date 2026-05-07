# Dynatrace Metrics Query

Query Dynatrace metrics via the v2 Metrics API → DataFrame. Returns per-series rows with timestamps + dimensions.

## Metric selector examples

```
builtin:host.cpu.usage:splitBy(dt.entity.host):avg
builtin:tech.generic.cpu.usage:filter(eq(dt.entity.host,HOST-XXXX))
builtin:service.errors.total:splitBy(dt.entity.service):sum
```

See Dynatrace Metric Selector docs for the full grammar.
