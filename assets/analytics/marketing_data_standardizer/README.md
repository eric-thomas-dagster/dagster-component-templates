# Marketing Data Standardizer Component

Transform platform-specific marketing data into a unified, standardized schema for cross-platform analysis. This component mimics the data model standardization capabilities of Supermetrics, Funnel.io, and Adverity.

## Purpose

Different marketing platforms use different field names and data structures. This component normalizes data from multiple platforms into a common schema, enabling:
- Cross-platform campaign performance comparison
- Unified marketing dashboards
- Multi-touch attribution analysis
- Consolidated reporting

## Standardized Schema

The output schema includes these standard fields:

| Field | Type | Description |
|-------|------|-------------|
| `date` | Date | Date of the metrics |
| `platform` | String | Source platform (facebook_ads, google_ads, etc.) |
| `campaign_id` | String | Campaign identifier |
| `campaign_name` | String | Campaign name |
| `ad_id` | String | Ad identifier (if available) |
| `ad_name` | String | Ad name (if available) |
| `impressions` | Integer | Number of times ads were shown |
| `clicks` | Integer | Number of clicks |
| `spend` | Float | Amount spent (normalized to currency units) |
| `conversions` | Float | Number of conversions |
| `conversion_value` | Float | Total conversion value |
| `reach` | Integer | Unique users reached (if available) |
| `ctr` | Float | Click-through rate (%) - calculated |
| `cpc` | Float | Cost per click - calculated |
| `cpm` | Float | Cost per 1000 impressions - calculated |
| `cpa` | Float | Cost per acquisition - calculated |
| `roas` | Float | Return on ad spend - calculated |

## Supported Platforms

- **Facebook Ads**: Campaigns, ads, and insights data
- **Google Ads**: Campaigns, ad groups, ads, and metrics
- **LinkedIn Ads**: Campaign and creative performance
- **TikTok Ads**: Campaign and ad metrics
- **Twitter/X Ads**: Campaign and promoted tweet metrics

## Usage

### Basic Example

```yaml
type: dagster_component_templates.MarketingDataStandardizerComponent
attributes:
  asset_name: standardized_facebook_data
  platform: "facebook_ads"
  upstream_asset_key: "facebook_ads_insights"
```

### Advanced Example

```yaml
type: dagster_component_templates.MarketingDataStandardizerComponent
attributes:
  asset_name: standardized_google_data
  platform: "google_ads"
  upstream_asset_key: "google_ads_campaigns"

  # Custom field mappings
  campaign_id_field: "campaignId"
  date_field: "day"
  spend_field: "cost_micros"

  # Google Ads stores cost in micros (divide by 1M)
  spend_multiplier: 0.000001

  # Date filtering
  filter_date_from: "2024-01-01"
  filter_date_to: "2024-12-31"

  group_name: "marketing_standardized"
```

### Multi-Platform Pipeline

Combine multiple standardizers for cross-platform analysis:

```yaml
# Standardize Facebook Ads
- type: dagster_component_templates.MarketingDataStandardizerComponent
  attributes:
    asset_name: std_facebook_data
    platform: "facebook_ads"
    upstream_asset_key: "facebook_ads_insights"

# Standardize Google Ads
- type: dagster_component_templates.MarketingDataStandardizerComponent
  attributes:
    asset_name: std_google_data
    platform: "google_ads"
    upstream_asset_key: "google_ads_campaigns"
    spend_multiplier: 0.000001

# Combine both (using DataFrame Transformer or custom SQL)
- type: dagster_component_templates.DataFrameTransformerComponent
  attributes:
    asset_name: unified_marketing_data
    combine_method: "concat"
```

## Platform-Specific Notes

### Facebook Ads
- Automatically detects standard Facebook fields
- Handles Facebook Insights data structure
- Spend is already in currency units (no multiplier needed)

### Google Ads
- **Important**: Set `spend_multiplier: 0.000001` for Google Ads
- Google stores costs in "micros" (1 million = $1)
- Example: `cost_micros: 50000000` → `spend: 50.00`

### LinkedIn Ads
- Detects `cost_in_local_currency` field
- Handles creative-level data

### TikTok Ads
- Uses `stat_time_day` for dates
- Standard field names (no multiplier needed)

### Twitter/X Ads
- Uses `billed_charge_local_micro` for spend
- Set `spend_multiplier: 0.000001` for Twitter data

## Calculated Metrics

These metrics are automatically calculated:

- **CTR (Click-Through Rate)**: `(clicks / impressions) * 100`
- **CPC (Cost Per Click)**: `spend / clicks`
- **CPM (Cost Per Mille)**: `(spend / impressions) * 1000`
- **CPA (Cost Per Acquisition)**: `spend / conversions`
- **ROAS (Return on Ad Spend)**: `conversion_value / spend`

## Field Auto-Detection

The component automatically detects common field names for each platform. Custom field names can be specified if your data uses non-standard naming.

### Auto-Detected Fields

**Facebook Ads**:
- Date: `date_start`, `date`, `created_time`
- Campaign ID: `campaign_id`
- Spend: `spend`

**Google Ads**:
- Date: `date`, `day`
- Campaign ID: `campaign_id`, `campaignId`
- Spend: `cost_micros`, `cost`, `metrics_cost_micros`

## Filtering

Apply filters to the standardized data:

```yaml
attributes:
  # Date range filtering
  filter_date_from: "2024-01-01"
  filter_date_to: "2024-12-31"

  # Campaign status filtering
  filter_campaign_status: "ACTIVE,PAUSED"
```

## Downstream Usage

Use standardized data with:

- **Attribution Modeling Component**: Multi-touch attribution analysis
- **Campaign Performance Dashboard**: Cross-platform KPI tracking
- **Customer 360**: Connect with CRM and analytics data
- **DataFrame Transformer**: Further aggregation and analysis
- **BI Tools**: Tableau, Looker, Power BI visualization

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the standardized output asset |
| `platform` | `Literal['facebook_ads', 'google_ads', 'linkedin_ads', 'tiktok_ads', 'twitter_ads']` | Source platform to standardize |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | Asset description |
| `group_name` | `str` | `"marketing_standardized"` | Asset group for organization |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']} |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `str` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `str` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `filter_date_from` | `str` | — | Filter data from this date (YYYY-MM-DD) |
| `filter_date_to` | `str` | — | Filter data to this date (YYYY-MM-DD) |
| `filter_campaign_status` | `str` | — | Filter by campaign status (e.g., 'ACTIVE,PAUSED') |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | Upstream asset containing raw platform data (automatically set via lineage) |
| `campaign_id_field` | `str` | — | Field name for campaign ID (auto-detected if not provided) |
| `campaign_name_field` | `str` | — | Field name for campaign name (auto-detected if not provided) |
| `date_field` | `str` | — | Field name for date (auto-detected if not provided) |
| `spend_field` | `str` | — | Field name for spend/cost (auto-detected if not provided) |
| `spend_multiplier` | `float` | `1.0` | Multiplier to convert spend to currency units (e.g., 0.000001 for micros) |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `include_preview_metadata` | `bool` | `true` | Include sample data preview in metadata |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when `include_preview_metadata` is True. For long DataFrames (>10x preview_rows), a random sample is used so the preview reflects the data distribution; otherwise head() is used. |

<!-- FIELDS:END -->

## Example Pipeline

```
Facebook Ads Ingestion
  ↓
Facebook Data Standardizer → Unified Marketing Data → Attribution Model
  ↓                              ↑                         ↓
Google Ads Ingestion             |                    Marketing ROI Report
  ↓                              |
Google Data Standardizer --------+
```

## Tips

1. **Spend Multipliers**: Always check if the platform stores costs in micros
2. **Field Mapping**: Use custom field names if your data has non-standard columns
3. **Data Quality**: Check for NULL values in key fields (campaign_id, date)
4. **Time Zones**: Be aware of timezone differences across platforms
5. **Currency**: Ensure all platforms use the same currency or add conversion logic

## Common Issues

**Issue**: Spend values are too high/low
**Solution**: Check `spend_multiplier`. Google Ads and Twitter use micros (0.000001)

**Issue**: Missing campaign names
**Solution**: Verify `campaign_name_field` or ensure upstream data includes names

**Issue**: Date parsing errors
**Solution**: Specify custom `date_field` or check date format in source data

## Related Components

- **Facebook Ads Ingestion**: Extract Facebook Ads data
- **Google Ads Ingestion**: Extract Google Ads data
- **Attribution Modeling**: Analyze marketing attribution
- **DataFrame Transformer**: Advanced data transformations
- **DuckDB Writer**: Persist standardized data

## Support

For issues or feature requests, create an issue in the dagster-component-templates repository.

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
