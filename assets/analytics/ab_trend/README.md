# ABTrend

Takes A/B exposure rows and bucks them by date. Outputs one row per (date_bucket, variant) with sample size, conversion count, and conversion rate. Useful for spotting novelty effects or peeking-bias before final analysis.

## Example

```yaml
type: dagster_component_templates.ABTrendComponent
attributes:
  asset_name: checkout_v2_daily_trend
  upstream_asset_key: experiment_data
  variant_column: variant
  converted_column: converted
  date_column: exposed_at
  bucket: day
  group_name: experiments
```


## Requirements

```
pandas
```
