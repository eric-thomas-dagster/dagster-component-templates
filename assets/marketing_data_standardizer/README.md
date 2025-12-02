# Marketing Data Standardizer Component

Transform platform-specific marketing data into a unified, standardized schema for cross-platform analysis. This component mimics the data model standardization capabilities of Supermetrics, Funnel.io, and Adverity.

## Overview

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
  source_asset: "facebook_ads_insights"
```

### Advanced Example

```yaml
type: dagster_component_templates.MarketingDataStandardizerComponent
attributes:
  asset_name: standardized_google_data
  platform: "google_ads"
  source_asset: "google_ads_campaigns"

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
    source_asset: "facebook_ads_insights"

# Standardize Google Ads
- type: dagster_component_templates.MarketingDataStandardizerComponent
  attributes:
    asset_name: std_google_data
    platform: "google_ads"
    source_asset: "google_ads_campaigns"
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
