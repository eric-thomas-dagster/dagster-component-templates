# PayScale Compensation Enrichment Component

Enriches an upstream DataFrame of job records (title + location, optionally years of experience / education / skills) with compensation-benchmark columns from PayScale's **Jobalyzer** API — one lookup per row.

## ⚠️ PayScale credentials are NOT self-serve

**Before configuring this component, know that you cannot get a PayScale API key on your own.** This component depends on `payscale_resource`, and PayScale does not offer any public signup for Jobalyzer API access — `client_id`, `client_secret`, and `customer_id` are only issued as part of a **direct commercial agreement with PayScale**. developers.payscale.com documents the API thoroughly, but there is no "create an app" button anywhere on it. If your organization doesn't already have a PayScale account contact who can provision these credentials, this component cannot be used — don't spend time hunting for a developer portal signup; it doesn't exist. See `resources/payscale_resource/README.md` for the full detail.

## Purpose

Jobalyzer is a **per-lookup** compensation-benchmarking service, not a bulk export: for each job (title, city/state/country, years of experience, education, skills), PayScale returns a pay report built from its salary-survey database. This component calls it once per row of the upstream DataFrame — structurally the same pattern as this repo's `geocoder`/`reverse_geocoder` components — and appends the resulting benchmark figures as new columns.

[//]: # (FIELDS:START)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the asset to create |
| `upstream_asset_key` | `str` | Upstream asset key providing a DataFrame with job title/location data |
| `job_title_column` | `Union[str, int]` | Column with the job title to send as PayScale's `JobTitle` compensable factor |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"payscale_resource"` | Resource key registered by `PayscaleResourceComponent` |

### Input columns

| Field | Type | Default | Description |
|---|---|---|---|
| `city_column` | `Union[str, int]` | — | Optional column with city name (`City`) |
| `state_column` | `Union[str, int]` | — | Optional column with state/province (`State`) |
| `country_column` | `Union[str, int]` | — | Optional column with country name (`Country`) |
| `default_country` | `str` | `"United States"` | Country sent when `country_column` is unset or blank for a row |
| `years_experience_column` | `Union[str, int]` | — | Optional column with years of experience (`YearsExperience`) |
| `education_column` | `Union[str, int]` | — | Optional column with highest degree earned (`HighestDegreeEarned`) |
| `skills_column` | `Union[str, int]` | — | Optional column with comma-separated or list-valued skills (`Skills`) |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `auto_resolve_job_title` | `bool` | `true` | Let PayScale auto-match free-text job titles to a standardized title (`AutoResolveJobTitle`) |
| `include_total_pay` | `bool` | `true` | Also append `TotalPayReport` columns alongside `BasePayReport` |
| `output_column_prefix` | `str` | `"payscale_"` | Prefix applied to every appended column |
| `continue_on_error` | `bool` | `true` | Log + null-fill a row's columns on lookup failure instead of failing the whole asset |
| `batch_delay` | `float` | `0.0` | Seconds to sleep between rows |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Asset group for organization |
| `owners` | `List[str]` | — | Asset owners |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags |
| `kinds` | `List[str]` | — | Asset kinds (defaults to `['python']`) |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage mapping |
| `description` | `str` | — | Asset description |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes |
| `freshness_cron` | `str` | — | Cron schedule for the freshness policy |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | `'daily'`, `'weekly'`, `'monthly'`, `'hourly'`, `'static'`, `'multi'`, or unset |
| `partition_start` | `str` | — | ISO partition start date |
| `partition_date_column` | `Union[str, int]` | — | Column to filter to the current date partition |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec |
| `partition_values` | `str` | — | Comma-separated static/multi partition values |
| `partition_static_dim` | `str` | — | Static dimension name |
| `partition_static_column` | `Union[str, int]` | — | Column to filter to the current static partition |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries |
| `retry_policy_backoff` | `str` | `"exponential"` | `'linear'` or `'exponential'` |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `include_preview_metadata` | `bool` | `false` | Include a markdown preview of output rows in metadata |
| `preview_rows` | `int` | `25` | Rows to include in the preview |

[//]: # (FIELDS:END)

## Output columns

Flattened from PayScale's documented Pay report shape (`BasePayReport` / `TotalPayReport`, each with `Percentile10/25/50/75/90`, `Average`, `CurrencyName`; plus report-level `ReportRating` and `Context.MatchedJobTitle`/`Context.JobTitleRating`):

| Column (with `output_column_prefix`, default `payscale_`) | Source | Description |
|---|---|---|
| `median_base_pay` | `BasePayReport.Percentile50` | Median base pay |
| `base_pay_p10` / `base_pay_p25` / `base_pay_p75` / `base_pay_p90` | `BasePayReport.PercentileNN` | Base pay percentile range |
| `base_pay_average` | `BasePayReport.Average` | Mean base pay |
| `median_total_pay`, `total_pay_p10`, `total_pay_p90` | `TotalPayReport.*` | Total pay (base + bonus + commission + profit share), only when `include_total_pay: true` |
| `currency` | `BasePayReport.CurrencyName` | Currency of the figures above |
| `report_rating` | `ReportRating` | PayScale's 0–1 data-quality score for the report |
| `total_profiles_analyzed` | `TotalProfilesAnalyzed` | Number of salary profiles the report is based on |
| `matched_job_title` | `Context.MatchedJobTitle` | The standardized PayScale title your input `JobTitle` was matched to |
| `job_title_rating` | `Context.JobTitleRating` | How confident PayScale is in that title match |
| `error` | — | Non-null failure message for a row when `continue_on_error: true` and the lookup failed; null on success |

## Input requirements

The upstream DataFrame must contain a job title column. Location columns (city/state/country) are optional but recommended — PayScale documents `Country` as a required compensable factor, so this component falls back to `default_country` when `country_column` is unset or a given row's value is blank.

## Configuration

```yaml
type: dagster_component_templates.PayscaleCompensationEnrichmentComponent
attributes:
  asset_name: compensation_benchmarked_roles
  upstream_asset_key: open_roles
  resource_key: payscale_resource
  job_title_column: job_title
  city_column: city
  state_column: state
  country_column: country
  years_experience_column: years_experience
  education_column: highest_degree
  include_total_pay: true
  group_name: analytics
```

Requires a `PayscaleResourceComponent` registered under the matching `resource_key` — see `resources/payscale_resource/README.md`, including the OAuth2 flow, the async submit-then-poll report retrieval, and the per-report billing model.

## Notes

- Each row is one billable PayScale "report" (or two, if `include_total_pay` causes `requestedReports` to include more than `pay`'s single sub-report set — see the resource README's billing section). Cost scales directly with row count; filter the upstream DataFrame to only the roles you actually need benchmarked.
- `continue_on_error: true` (default) means a bad or unmatched row never fails the whole asset — check the `error` column and `rows_failed` output metadata to find them.
- PayScale's `AutoResolveJobTitle` (this component's `auto_resolve_job_title`, default `true`) lets free-text job titles match PayScale's standardized title taxonomy; `matched_job_title`/`job_title_rating` tell you what it matched to and how confidently.

## Dependencies

- `pandas>=1.5.0`
- `payscale_resource` (this repo's `PayscaleResourceComponent`)
