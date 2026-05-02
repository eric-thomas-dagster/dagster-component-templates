# RssFeedSensor

Polls a public RSS or Atom feed and triggers a RunRequest for each new entry seen since the last evaluation (by entry GUID). Useful for newswire / blog / regulatory-filing-driven pipelines.

## Example

```yaml
type: dagster_component_templates.RssFeedSensorComponent
attributes:
  sensor_name: hn_top_sensor
  asset_keys: [latest_news_summary]
  feed_url: "https://hnrss.org/frontpage"
  max_entries_per_tick: 5
  minimum_interval_seconds: 600
```

## Requirements

```
feedparser
```
