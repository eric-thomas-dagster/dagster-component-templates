# Pinterest Ads Ingestion Component

Ingest Pinterest Ads data into your data warehouse using dlt's REST API source with Pinterest Marketing API. This component provides capabilities similar to Supermetrics, Funnel.io, and Adverity for Pinterest Ads data extraction.

## Features

- **Multiple Resources**: Extract ad accounts, campaigns, ad groups, ads, pins, keywords, and analytics
- **OAuth 2.0 Authentication**: Secure access via Pinterest access tokens
- **Date Range Control**: Specify custom date ranges for performance analytics
- **Flexible Granularity**: Hourly, daily, weekly, or monthly aggregation
- **Multiple Reporting Levels**: Account, campaign, ad group, ad, keyword, or pin-level metrics
- **Multiple Destinations**: Load to DuckDB, Snowflake, BigQuery, Postgres, or Redshift

- **DataFrame Output**: Returns pandas DataFrame by default for flexible transformation

## Output

**By default (no destination set)**: Uses in-memory DuckDB and **returns a pandas DataFrame**. Perfect for:
- Downstream transformation with DataFrame Transformer
- Feeding into standardizer components
- Direct Python/pandas manipulation
- Chaining with other Dagster assets

**With destination set**: Persists to your warehouse (Snowflake, BigQuery, etc.) and optionally returns DataFrame if `persist_and_return: true`

## Data Extracted

### Standard Resources
- **Ad Accounts**: Ad account information, settings, and permissions
- **Campaigns**: Campaign configurations, objectives, budgets, and status
- **Ad Groups**: Ad group settings, targeting, and bidding strategies
- **Ads**: Individual ad creative, placements, and settings
- **Pins**: Pin creative content and metadata
- **Keywords**: Keyword targeting and bid amounts
- **Analytics**: Performance metrics and conversion data

### Performance Metrics
Analytics include:
- Impressions, Clicks, Cost (spend)
- Saves, Pin clicks, Outbound clicks
- Total engagements
- CTR (Click-through rate)
- Average CPC, Average CPM
- Conversions, Conversion value
- ROAS (Return on Ad Spend)
- Video metrics (starts, complete views, MRC views)
- Earned impressions and clicks

## Prerequisites

### 1. Pinterest Business Account
Active Pinterest Business account with ad campaigns

### 2. Pinterest Developer App
Create a Pinterest app:
1. Go to https://developers.pinterest.com/
2. Create new app or use existing
3. Configure app settings
4. Request access to Pinterest Ads API
5. Wait for approval (typically 1-2 weeks)

### 3. Access Token
Generate OAuth 2.0 access token with required scopes:
- `ads:read`: Read advertising data
- `user_accounts:read`: Read user account information

### Token Generation:
1. Log in to Pinterest Developers Console
2. Navigate to your app
3. Generate access token with ads:read scope
4. Access tokens expire after 1 year by default
5. Implement refresh token flow for production use

## Configuration

### Basic Example

```yaml
type: dagster_component_templates.PinterestAdsIngestionComponent
attributes:
  asset_name: pinterest_ads_data
  access_token: "${PINTEREST_ACCESS_TOKEN}"
  ad_account_ids: "123456789,987654321"
  resources: "campaigns,analytics"
  destination: "duckdb"
```

### Advanced Example (with Custom Analytics)

```yaml
type: dagster_component_templates.PinterestAdsIngestionComponent
attributes:
  asset_name: pinterest_ads_performance

  # Authentication
  access_token: "${PINTEREST_ACCESS_TOKEN}"
  ad_account_ids: "123456789,987654321,555555555"

  # App credentials (optional, for token refresh)
  app_id: "${PINTEREST_APP_ID}"
  app_secret: "${PINTEREST_APP_SECRET}"

  # Resources
  resources: "ad_accounts,campaigns,ad_groups,ads,analytics"

  # Analytics configuration
  start_date: "2024-01-01"
  end_date: "2024-12-31"
  granularity: "DAY"
  level: "AD_GROUP"
  columns: "SPEND_IN_DOLLAR,IMPRESSION_1,CLICKTHROUGH_1,SAVE_1,PIN_CLICK_1,OUTBOUND_CLICK_1,TOTAL_CONVERSIONS,TOTAL_CONVERSION_VALUE"

  # Destination
  destination: "snowflake"
  destination_config: '{"credentials": "snowflake://user:pass@account/db/schema"}'

  group_name: "marketing_analytics"
```

### Pin-Level Analytics Example

```yaml
type: dagster_component_templates.PinterestAdsIngestionComponent
attributes:
  asset_name: pinterest_pin_analytics
  access_token: "${PINTEREST_ACCESS_TOKEN}"
  ad_account_ids: "123456789"

  # Focus on pin performance
  resources: "campaigns,ad_groups,ads,pins,analytics"

  # Pin-level metrics
  start_date: "2024-10-01"
  end_date: "2024-12-31"
  granularity: "DAY"
  level: "PIN_PROMOTION"
  columns: "SPEND_IN_DOLLAR,IMPRESSION_1,SAVE_1,PIN_CLICK_1,OUTBOUND_CLICK_1,TOTAL_ENGAGEMENT,EARNED_IMPRESSION,EARNED_SAVE"

  destination: "bigquery"
```

## Environment Variables

Store credentials securely:

```bash
# Pinterest credentials
export PINTEREST_ACCESS_TOKEN="YOUR_ACCESS_TOKEN_HERE"
export PINTEREST_APP_ID="YOUR_APP_ID"
export PINTEREST_APP_SECRET="YOUR_APP_SECRET"
```

Reference in YAML with `${VAR_NAME}` syntax.

## Finding Your Ad Account ID

1. Go to Pinterest Ads Manager: https://ads.pinterest.com/
2. Select your ad account
3. Look at URL: `https://ads.pinterest.com/advertiser/{AD_ACCOUNT_ID}/`
4. The numeric ID after `/advertiser/` is your Ad Account ID
5. Or use the Ad Accounts API endpoint to list accessible accounts

## Available Resources

### ad_accounts
Ad account details:
- Ad account ID and name
- Owner information
- Business information
- Country, currency, permissions
- Created timestamp

### campaigns
Campaign configuration:
- Campaign ID and name
- Ad account association
- Status (ACTIVE, PAUSED, ARCHIVED)
- Objective (AWARENESS, CONSIDERATION, CONVERSIONS, etc.)
- Daily spend cap
- Lifetime spend cap
- Start/end time
- Tracking URLs

### ad_groups
Ad group settings:
- Ad group ID and name
- Campaign association
- Status
- Budget type and amount
- Bid strategy
- Optimization goal
- Targeting (demographics, interests, keywords, placements)
- Start/end time

### ads
Individual ad details:
- Ad ID and name
- Ad group association
- Creative type (REGULAR, VIDEO, SHOPPING, etc.)
- Status
- Pin ID (creative)
- Destination URL
- Tracking URLs

### pins
Pin creative content:
- Pin ID
- Board ID
- Title, description
- Image/video URL
- Link
- Created timestamp
- Media type

### keywords
Keyword targeting:
- Keyword ID
- Ad group association
- Keyword value
- Match type (BROAD, PHRASE, EXACT)
- Bid amount
- Status

### analytics
Performance metrics:
- Time series data (by granularity)
- Aggregated by level (account, campaign, ad group, ad, keyword, or pin)
- Customizable metric columns
- Conversion attribution

## Analytics Configuration

### Granularity
Time aggregation options:
- `HOUR`: Hourly breakdown
- `DAY`: Daily breakdown (default)
- `WEEK`: Weekly breakdown
- `MONTH`: Monthly breakdown

### Level
Aggregate metrics at different levels:
- `AD_ACCOUNT`: Account-level totals
- `CAMPAIGN`: Per-campaign metrics
- `AD_GROUP`: Per-ad group metrics
- `AD`: Per-ad metrics
- `KEYWORD`: Per-keyword metrics
- `PIN_PROMOTION`: Per-pin metrics (most granular)

### Available Columns (Metrics)
**Standard metrics:**
- `SPEND_IN_DOLLAR` - Total spend in dollars
- `SPEND_IN_MICRO_DOLLAR` - Spend in micro dollars
- `IMPRESSION_1` - Total impressions (1-day attribution)
- `IMPRESSION_2` - Total impressions (view attribution)
- `CLICKTHROUGH_1` - Total clicks (1-day click)
- `CLICKTHROUGH_2` - Total clicks (view-through)
- `CTR` - Click-through rate
- `CPC_IN_DOLLAR` - Cost per click
- `CPM_IN_DOLLAR` - Cost per thousand impressions

**Engagement metrics:**
- `TOTAL_ENGAGEMENT` - Total engagements (saves + clicks)
- `SAVE_1` - Total saves (1-day click)
- `PIN_CLICK_1` - Pin clicks (closeup views)
- `OUTBOUND_CLICK_1` - Outbound clicks (to destination)

**Conversion metrics:**
- `TOTAL_CONVERSIONS` - Total conversions (all types)
- `TOTAL_CONVERSION_VALUE` - Total conversion value
- `ROAS` - Return on ad spend
- `ECTR` - Engagement CTR
- `ECPC_IN_DOLLAR` - Engagement cost per click
- `ECPM_IN_DOLLAR` - Engagement CPM

**Video metrics:**
- `VIDEO_MRC_VIEW_1` - MRC (Media Rating Council) views
- `VIDEO_V50_WATCH_TIME` - 50% watch time
- `VIDEO_START` - Video starts
- `VIDEO_3SEC_VIEW_2` - 3-second views

**Earned metrics:**
- `EARNED_IMPRESSION` - Earned impressions
- `EARNED_SAVE` - Earned saves
- `EARNED_PIN_CLICK` - Earned pin clicks

## Downstream Usage

The ingested data can be used by:
- **Marketing Data Standardizer**: Normalize Pinterest Ads data into common schema
- **Attribution Modeling**: Multi-touch attribution across platforms
- **Campaign Performance**: Cross-platform campaign dashboards
- **Creative Analytics**: Pin performance and creative testing
- **Customer 360**: Combine with CRM and analytics data

## Tips

1. **Token Expiration**: Pinterest tokens expire after 1 year. Set up refresh flow for production
2. **Multiple Accounts**: Separate ad account IDs with commas
3. **Rate Limits**: Pinterest API has rate limits; dlt handles retries automatically
4. **Attribution Windows**: Metrics use different attribution windows (1-day click, view-through)
5. **Earned Metrics**: Pinterest tracks earned (organic) engagement from ads
6. **Pin Performance**: Use PIN_PROMOTION level for most detailed creative insights
7. **Data Latency**: Performance data may have 24-48 hour delay

## Data Schema Examples

### Campaigns Table
- `id` - Campaign ID
- `name` - Campaign name
- `ad_account_id` - Parent ad account
- `status` - ACTIVE, PAUSED, ARCHIVED, etc.
- `objective_type` - Campaign objective
- `daily_spend_cap` - Daily budget limit (micros)
- `lifetime_spend_cap` - Total budget limit (micros)
- `start_time` - Campaign start timestamp
- `end_time` - Campaign end timestamp
- `tracking_urls` - Tracking URL template

### Ad Groups Table
- `id` - Ad group ID
- `name` - Ad group name
- `campaign_id` - Parent campaign
- `status` - ACTIVE, PAUSED, etc.
- `budget_type` - DAILY or LIFETIME
- `budget_in_micro_currency` - Budget amount
- `bid_in_micro_currency` - Bid amount
- `billable_event` - CLICKTHROUGH, IMPRESSION, VIDEO_V_50_MRC
- `optimization_goal_metadata` - Optimization settings
- `targeting_spec` - Targeting criteria
- `placement_group` - BROWSE, SEARCH, ALL

### Ads Table
- `id` - Ad ID
- `name` - Ad name
- `ad_group_id` - Parent ad group
- `creative_type` - REGULAR, VIDEO, SHOPPING, CAROUSEL
- `status` - ACTIVE, PAUSED, etc.
- `pin_id` - Associated pin (creative)
- `destination_url` - Landing page URL
- `tracking_urls` - Click tracking URLs

### Analytics Table
- `DATE` - Date of metrics (based on granularity)
- `AD_ACCOUNT_ID`, `CAMPAIGN_ID`, `AD_GROUP_ID`, `AD_ID`, `KEYWORD_ID`, or `PIN_PROMOTION_ID` - Entity identifiers (based on level)
- `SPEND_IN_DOLLAR` - Total spend
- `IMPRESSION_1` - Total impressions
- `CLICKTHROUGH_1` - Total clicks
- `CTR` - Click-through rate
- `CPC_IN_DOLLAR` - Cost per click
- `CPM_IN_DOLLAR` - Cost per mille
- `SAVE_1` - Total saves
- `PIN_CLICK_1` - Pin closeup clicks
- `OUTBOUND_CLICK_1` - Outbound clicks
- `TOTAL_CONVERSIONS` - Total conversions
- `TOTAL_CONVERSION_VALUE` - Conversion value

## Related Components

- **Google Ads Ingestion**: Ingest Google Ads data
- **Facebook Ads Ingestion**: Ingest Facebook Ads data
- **LinkedIn Ads Ingestion**: Ingest LinkedIn Ads data
- **TikTok Ads Ingestion**: Ingest TikTok Ads data
- **Twitter Ads Ingestion**: Ingest Twitter/X Ads data
- **Marketing Data Standardizer**: Normalize data across platforms
- **Attribution Modeling**: Analyze marketing attribution

## Troubleshooting

### "Invalid access token" error
- Verify token was generated with ads:read scope
- Check if token has expired (1-year default expiration)
- Ensure app has Pinterest Ads API access approved
- Regenerate token if needed

### "Permission denied" for ad account
- Verify you have access to the ad account in Ads Manager
- Check that ad account ID is correct (numeric ID)
- Ensure your Pinterest account has proper permissions
- Verify app has been granted access by account owner

### No analytics data returned
- Verify date range is within campaign active dates
- Check that campaigns have served impressions in date range
- Ensure columns (metrics) are valid for your account type
- Note 24-48 hour data latency for recent dates

### Rate limit errors
- dlt automatically handles retries with exponential backoff
- Consider reducing number of accounts queried simultaneously
- Use specific columns instead of all metrics
- Spread large extractions across multiple runs

### Missing video metrics
- Video metrics only available for video ad formats
- Verify ads have video creative type
- Check that columns include video-specific metrics

## API Reference

This component uses the Pinterest Marketing API v5:
- API documentation: https://developers.pinterest.com/docs/api/v5/
- Authentication: https://developers.pinterest.com/docs/getting-started/authentication/
- Analytics: https://developers.pinterest.com/docs/api/v5/#tag/ad_accounts

## Support

For issues with:
- **Component**: Create issue in dagster-component-templates repo
- **dlt REST API source**: Check [dlt REST API docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- **Pinterest Marketing API**: Refer to [Pinterest API docs](https://developers.pinterest.com/docs/api/v5/)

## Note on dlt Source

Pinterest Ads is not currently available as a verified dlt source. This component uses dlt's generic REST API source to connect to the Pinterest Marketing API. The implementation follows Pinterest's official API patterns and provides production-ready data extraction for advertising workflows.

For updates on Pinterest Ads becoming a verified source, check the [dlt verified sources page](https://dlthub.com/docs/dlt-ecosystem/verified-sources).
