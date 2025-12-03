# TikTok Ads Ingestion Component

Ingest TikTok Ads data into your data warehouse using dlt's REST API source with TikTok Marketing API. This component provides capabilities similar to Supermetrics, Funnel.io, and Adverity for TikTok Ads data extraction.

## Features

- **Multiple Resources**: Extract advertisers, campaigns, ad groups, ads, videos, images, and reports
- **OAuth Authentication**: Secure access via TikTok Marketing API access tokens
- **Date Range Control**: Specify custom date ranges for performance reports
- **Flexible Reporting**: Multiple report types and data aggregation levels
- **Custom Metrics**: Select specific metrics or use comprehensive defaults
- **Multiple Destinations**: Load to DuckDB, Snowflake, BigQuery, Postgres, or Redshift

## Data Extracted

### Standard Resources
- **Advertisers**: Advertiser account information and settings
- **Campaigns**: Campaign configurations, budgets, objectives, and status
- **Ad Groups**: Ad group settings, targeting, and bidding strategies
- **Ads**: Individual ad creative, placements, and settings
- **Videos**: Video creative assets and metadata
- **Images**: Image creative assets and specifications
- **Reports**: Performance metrics and analytics

### Performance Metrics
Reports include:
- Impressions
- Clicks
- Cost (spend)
- Conversions
- Conversion Value
- CTR (Click-through rate)
- Average CPC
- Average CPM
- Conversion Rate
- Video metrics (plays, 2s views, 6s views, completion rate)
- Engagement metrics (likes, comments, shares)

## Prerequisites

### 1. TikTok Ads Account
Active TikTok Ads Manager account with advertising campaigns

### 2. TikTok Developer Application
Create a TikTok for Business app:
1. Go to https://ads.tiktok.com/marketing_api/apps
2. Create new app or use existing
3. Request access to Marketing API
4. Configure app settings and permissions
5. Generate access token

### 3. Access Token
Generate long-term access token with required permissions:
- **Campaign Management**: Read campaigns, ad groups, and ads
- **Reporting**: Read advertising reports and analytics
- **Creative**: Read video and image assets

### Token Generation:
1. Log in to TikTok Marketing API Portal
2. Navigate to your app
3. Generate long-term access token (valid for 365 days)
4. Store token securely

## Configuration

### Basic Example

```yaml
type: dagster_component_templates.TikTokAdsIngestionComponent
attributes:
  asset_name: tiktok_ads_data
  access_token: "${TIKTOK_ACCESS_TOKEN}"
  advertiser_ids: "1234567890,9876543210"
  resources: "campaigns,reports"
  destination: "duckdb"
```

### Advanced Example (with Custom Reports)

```yaml
type: dagster_component_templates.TikTokAdsIngestionComponent
attributes:
  asset_name: tiktok_ads_performance

  # Authentication
  access_token: "${TIKTOK_ACCESS_TOKEN}"
  advertiser_ids: "1234567890,9876543210,5555555555"

  # App credentials (optional, for token refresh)
  app_id: "${TIKTOK_APP_ID}"
  secret: "${TIKTOK_SECRET}"

  # Resources
  resources: "advertisers,campaigns,ad_groups,ads,reports"

  # Report configuration
  start_date: "2024-01-01"
  end_date: "2024-12-31"
  report_type: "BASIC"
  data_level: "AUCTION_AD"
  metrics: "spend,impressions,clicks,conversions,video_play_actions,video_watched_6s"

  # Destination
  destination: "snowflake"
  destination_config: '{"credentials": "snowflake://user:pass@account/db/schema"}'

  group_name: "marketing_analytics"
```

### Video Analytics Example

```yaml
type: dagster_component_templates.TikTokAdsIngestionComponent
attributes:
  asset_name: tiktok_video_analytics
  access_token: "${TIKTOK_ACCESS_TOKEN}"
  advertiser_ids: "1234567890"

  # Focus on video performance
  resources: "campaigns,ads,videos,reports"

  # Video-specific metrics
  start_date: "2024-10-01"
  end_date: "2024-12-31"
  report_type: "BASIC"
  data_level: "AUCTION_AD"
  metrics: "video_play_actions,video_watched_2s,video_watched_6s,average_video_play,average_video_play_per_user,video_views_p25,video_views_p50,video_views_p75,video_views_p100"

  destination: "bigquery"
```

## Environment Variables

Store sensitive credentials in environment variables:

```bash
# TikTok credentials
export TIKTOK_ACCESS_TOKEN="YOUR_ACCESS_TOKEN_HERE"
export TIKTOK_APP_ID="YOUR_APP_ID"
export TIKTOK_SECRET="YOUR_APP_SECRET"
```

Reference in YAML with `${VAR_NAME}` syntax.

## Finding Your Advertiser ID

1. Go to TikTok Ads Manager: https://ads.tiktok.com/
2. Look at the URL when viewing campaigns: `https://ads.tiktok.com/i18n/dashboard?aadvid={ADVERTISER_ID}`
3. The number after `aadvid=` is your Advertiser ID
4. Or use the Advertiser Info API endpoint to list all accessible advertisers

## Available Resources

### advertisers
Advertiser account details:
- Advertiser ID and name
- Company information
- Account balance
- Currency
- Timezone
- Status (ACTIVE, INACTIVE)

### campaigns
Campaign configuration:
- Campaign ID and name
- Objective (REACH, VIDEO_VIEWS, TRAFFIC, etc.)
- Status (ENABLE, DISABLE, DELETE)
- Budget type and amount
- Schedule (start/end dates)
- Campaign optimization goal

### ad_groups
Ad group settings:
- Ad group ID and name
- Campaign association
- Placement (TikTok, Pangle, Global App Bundle)
- Targeting (location, demographics, interests, devices)
- Bidding strategy and amount
- Budget and schedule
- Optimization events

### ads
Individual ad details:
- Ad ID and name
- Ad group association
- Display name
- Call-to-action
- Landing page URL
- Creative type
- Status

### videos
Video creative assets:
- Video ID
- File name and format
- Duration
- Resolution
- Size
- Preview URL
- Upload time

### images
Image creative assets:
- Image ID
- File name and format
- Dimensions
- Size
- Preview URL
- Upload time

### reports
Performance metrics with flexible configuration:
- Time series data (daily by default)
- Aggregated by data_level (advertiser, campaign, ad group, or ad)
- Customizable metric selection
- Optional demographic breakdowns (AUDIENCE report type)

## Report Configuration

### Report Types
- `BASIC`: Standard performance metrics (default)
- `AUDIENCE`: Demographics and audience insights
- `PLAYABLE_MATERIAL`: Playable ad creative performance

### Data Levels
Aggregate metrics at different levels:
- `AUCTION_ADVERTISER`: Account-level totals
- `AUCTION_CAMPAIGN`: Per-campaign metrics
- `AUCTION_ADGROUP`: Per-ad group metrics
- `AUCTION_AD`: Per-ad metrics (most granular)

### Available Metrics
**Standard metrics:**
- spend, cash_spend, voucher_spend
- impressions, reach
- clicks, ctr
- cpc, cpm
- conversions, cost_per_conversion, conversion_rate
- result, cost_per_result, result_rate

**Video metrics:**
- video_play_actions
- video_watched_2s, video_watched_6s
- video_views_p25, video_views_p50, video_views_p75, video_views_p100
- average_video_play, average_video_play_per_user

**Engagement metrics:**
- likes, comments, shares, follows
- clicks_on_music_disc
- profile_visits, profile_visits_rate

## Downstream Usage

The ingested data can be used by:
- **Marketing Data Standardizer**: Normalize TikTok Ads data into common schema
- **Attribution Modeling**: Multi-touch attribution analysis across platforms
- **Campaign Performance**: Cross-platform campaign dashboards
- **Creative Analytics**: Video performance and creative testing analysis
- **Customer 360**: Combine with CRM and analytics data

## Tips

1. **Token Expiration**: TikTok long-term tokens are valid for 365 days. Set up renewal reminders
2. **Multiple Advertisers**: Separate multiple advertiser IDs with commas
3. **Rate Limits**: TikTok API has rate limits; dlt handles retries automatically
4. **Report Granularity**: Use AUCTION_AD for most detailed insights, AUCTION_CAMPAIGN for overview
5. **Video Metrics**: Video ads have extensive completion and engagement metrics
6. **Data Latency**: Performance data may have 24-48 hour delay
7. **Custom Metrics**: Specify only needed metrics to reduce API calls and data volume

## Data Schema Examples

### Campaigns Table
- `campaign_id` - Campaign identifier
- `campaign_name` - Campaign name
- `advertiser_id` - Parent advertiser ID
- `objective_type` - Campaign objective (REACH, TRAFFIC, etc.)
- `status` - ENABLE, DISABLE, DELETE
- `budget_mode` - BUDGET_MODE_DAY or BUDGET_MODE_TOTAL
- `budget` - Budget amount
- `operation_status` - Current operational status

### Ad Groups Table
- `adgroup_id` - Ad group identifier
- `adgroup_name` - Ad group name
- `campaign_id` - Parent campaign ID
- `placement_type` - Placement (PLACEMENT_TYPE_AUTOMATIC, etc.)
- `placements` - Specific placements (TikTok, Pangle)
- `location_ids` - Targeted locations
- `age_groups` - Targeted age ranges
- `gender` - Gender targeting
- `bidding_strategy` - Bid strategy
- `bid` - Bid amount
- `optimization_goal` - What to optimize for

### Ads Table
- `ad_id` - Ad identifier
- `ad_name` - Ad name
- `adgroup_id` - Parent ad group ID
- `display_name` - Display name shown in ad
- `landing_page_url` - Destination URL
- `call_to_action` - CTA button type
- `creative_type` - VIDEO or IMAGE
- `video_id` - Associated video (if video ad)
- `image_ids` - Associated images (if image ad)

### Reports Table
- `dimensions` - Dimension values (e.g., stat_time_day, ad_id)
- `metrics.spend` - Total spend
- `metrics.impressions` - Total impressions
- `metrics.clicks` - Total clicks
- `metrics.ctr` - Click-through rate
- `metrics.cpc` - Cost per click
- `metrics.cpm` - Cost per thousand impressions
- `metrics.conversions` - Total conversions
- `metrics.conversion_rate` - Conversion rate
- `metrics.video_play_actions` - Video plays initiated
- `metrics.video_watched_6s` - 6-second video views

## Related Components

- **Google Ads Ingestion**: Ingest Google Ads data
- **Facebook Ads Ingestion**: Ingest Facebook Ads data
- **LinkedIn Ads Ingestion**: Ingest LinkedIn Ads data
- **Marketing Data Standardizer**: Normalize data across advertising platforms
- **Attribution Modeling**: Analyze marketing attribution
- **DataFrame Transformer**: Transform and clean the data

## Troubleshooting

### "Invalid access token" error
- Verify token was generated correctly in TikTok Marketing API portal
- Check if token has expired (365-day expiration)
- Ensure token has required permissions
- Regenerate token if needed

### "Permission denied" for advertiser
- Verify you have access to the advertiser account in Ads Manager
- Check that advertiser ID is correct (numeric ID, not name)
- Ensure your app has proper authorization from advertiser

### No report data returned
- Verify date range is within campaign active dates
- Check that campaigns have served impressions in date range
- Ensure data_level and report_type are compatible
- Note 24-48 hour data latency for recent dates

### Rate limit errors
- dlt automatically handles retries with exponential backoff
- Consider reducing number of advertisers queried simultaneously
- Use specific metrics instead of all metrics to reduce API calls
- Spread large extractions across multiple runs

### Missing video metrics
- Video metrics only available for video ad creatives
- Verify ads have video_id populated
- Check that report includes video-specific metrics

## API Reference

This component uses the TikTok Marketing API v1.3:
- API documentation: https://business-api.tiktok.com/portal/docs
- Authentication: https://business-api.tiktok.com/portal/docs?id=1738373164380162
- Reporting API: https://business-api.tiktok.com/portal/docs?id=1738864915188737

## Support

For issues with:
- **Component**: Create issue in dagster-component-templates repo
- **dlt REST API source**: Check [dlt REST API docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- **TikTok Marketing API**: Refer to [TikTok Business API docs](https://business-api.tiktok.com/portal/docs)

## Note on dlt Source

TikTok Ads is not currently available as a verified dlt source. This component uses dlt's generic REST API source to connect to the TikTok Marketing API. The implementation follows TikTok's official API patterns and provides production-ready data extraction for advertising workflows.

For updates on TikTok Ads becoming a verified source, check the [dlt verified sources page](https://dlthub.com/docs/dlt-ecosystem/verified-sources).
