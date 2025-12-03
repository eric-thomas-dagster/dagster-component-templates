# Twitter/X Ads Ingestion Component

Ingest Twitter/X Ads data into your data warehouse using dlt's REST API source with Twitter Ads API. This component provides capabilities similar to Supermetrics, Funnel.io, and Adverity for Twitter Ads data extraction.

## Features

- **Multiple Resources**: Extract accounts, campaigns, line items, promoted tweets, media, and analytics
- **OAuth 1.0a Authentication**: Secure access via Twitter API credentials
- **Date Range Control**: Specify custom date ranges for performance analytics
- **Flexible Granularity**: Hourly, daily, or total aggregation
- **Metric Groups**: Select from engagement, billing, video, conversion metrics
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
- **Accounts**: Ad account information, funding, and settings
- **Campaigns**: Campaign configurations, objectives, budgets, and status
- **Line Items**: Ad group-equivalent settings and targeting (formerly ad groups)
- **Promoted Tweets**: Promoted tweet configurations and associations
- **Tweets**: Tweet creative content and metadata
- **Media**: Media creative assets (images, videos, GIFs)
- **Analytics**: Performance metrics and engagement data

### Performance Metrics
Analytics include:
- Impressions, Clicks, Cost
- Engagements (total engagement actions)
- Retweets, Likes (favorites), Replies
- Follows, Unfollows
- URL clicks, Hashtag clicks, Detail expands
- App clicks, App installs
- CTR, Engagement Rate
- Average CPE (cost per engagement)
- Average CPM
- Video views, Video completion rates

## Prerequisites

### 1. Twitter Ads Account
Active Twitter Ads account with campaigns

### 2. Twitter Developer Account
Sign up for Twitter Developer access:
1. Go to https://developer.twitter.com/
2. Apply for developer account
3. Create a project and app
4. Enable Ads API access (requires separate application)

### 3. Twitter Ads API Access
Request Ads API access:
1. Apply through Twitter Ads Manager
2. Complete Ads API application form
3. Wait for approval (can take 1-2 weeks)
4. Once approved, your app can access Ads API

### 4. API Credentials
Generate OAuth 1.0a credentials:
1. In Twitter Developer Portal, navigate to your app
2. Go to "Keys and tokens" tab
3. Generate:
   - API Key (Consumer Key)
   - API Secret Key (Consumer Secret)
   - Access Token
   - Access Token Secret
4. Ensure app has Read permissions for Ads API

## Configuration

### Basic Example

```yaml
type: dagster_component_templates.TwitterAdsIngestionComponent
attributes:
  asset_name: twitter_ads_data
  consumer_key: "${TWITTER_CONSUMER_KEY}"
  consumer_secret: "${TWITTER_CONSUMER_SECRET}"
  access_token: "${TWITTER_ACCESS_TOKEN}"
  access_token_secret: "${TWITTER_ACCESS_TOKEN_SECRET}"
  account_ids: "abc123,xyz789"
  resources: "campaigns,analytics"
  destination: "duckdb"
```

### Advanced Example (with Custom Analytics)

```yaml
type: dagster_component_templates.TwitterAdsIngestionComponent
attributes:
  asset_name: twitter_ads_performance

  # Authentication
  consumer_key: "${TWITTER_CONSUMER_KEY}"
  consumer_secret: "${TWITTER_CONSUMER_SECRET}"
  access_token: "${TWITTER_ACCESS_TOKEN}"
  access_token_secret: "${TWITTER_ACCESS_TOKEN_SECRET}"

  # Account IDs
  account_ids: "abc123,xyz789,def456"

  # Resources
  resources: "accounts,campaigns,line_items,promoted_tweets,analytics"

  # Analytics configuration
  start_date: "2024-01-01"
  end_date: "2024-12-31"
  entity_type: "LINE_ITEM"
  granularity: "DAY"
  metric_groups: "ENGAGEMENT,BILLING,VIDEO,WEB_CONVERSION"
  placement: "ALL_ON_TWITTER"

  # Destination
  destination: "snowflake"
  destination_config: '{"credentials": "snowflake://user:pass@account/db/schema"}'

  group_name: "marketing_analytics"
```

### Video Campaign Analytics

```yaml
type: dagster_component_templates.TwitterAdsIngestionComponent
attributes:
  asset_name: twitter_video_analytics
  consumer_key: "${TWITTER_CONSUMER_KEY}"
  consumer_secret: "${TWITTER_CONSUMER_SECRET}"
  access_token: "${TWITTER_ACCESS_TOKEN}"
  access_token_secret: "${TWITTER_ACCESS_TOKEN_SECRET}"
  account_ids: "abc123"

  # Focus on video performance
  resources: "campaigns,line_items,promoted_tweets,media,analytics"

  # Video metrics
  start_date: "2024-10-01"
  end_date: "2024-12-31"
  entity_type: "PROMOTED_TWEET"
  granularity: "DAY"
  metric_groups: "ENGAGEMENT,VIDEO"

  destination: "bigquery"
```

## Environment Variables

Store credentials securely:

```bash
# Twitter API credentials (OAuth 1.0a)
export TWITTER_CONSUMER_KEY="YOUR_CONSUMER_KEY"
export TWITTER_CONSUMER_SECRET="YOUR_CONSUMER_SECRET"
export TWITTER_ACCESS_TOKEN="YOUR_ACCESS_TOKEN"
export TWITTER_ACCESS_TOKEN_SECRET="YOUR_ACCESS_TOKEN_SECRET"
```

Reference in YAML with `${VAR_NAME}` syntax.

## Finding Your Account ID

1. Go to Twitter Ads Manager: https://ads.twitter.com/
2. Select your ads account
3. Look at URL: `https://ads.twitter.com/accounts/{ACCOUNT_ID}/campaigns`
4. The alphanumeric string after `/accounts/` is your Account ID
5. Or use the Accounts API endpoint to list accessible accounts

## Available Resources

### accounts
Ad account details:
- Account ID and name
- Business name and ID
- Timezone
- Currency
- Approval status
- Created/deleted timestamps

### campaigns
Campaign configuration:
- Campaign ID and name
- Account association
- Funding instrument ID
- Status (ACTIVE, PAUSED, etc.)
- Daily budget
- Total budget
- Standard/draft delivery
- Start/end time
- Objective (TWEET_ENGAGEMENTS, VIDEO_VIEWS, etc.)

### line_items
Line item settings (ad group equivalent):
- Line item ID and name
- Campaign association
- Objective
- Bid type and amount
- Budget and pacing
- Start/end time
- Targeting criteria
- Placements
- Product type
- Optimization preference

### promoted_tweets
Promoted tweet configurations:
- Promoted tweet ID
- Line item association
- Tweet ID
- Approval status
- Paused/deleted status

### tweets
Tweet creative content:
- Tweet ID
- Text content
- Media entities
- User mentions, hashtags, URLs
- Created timestamp
- Engagement counts (organic)

### media
Media library assets:
- Media key
- Media ID
- Media type (IMAGE, VIDEO, GIF)
- Name
- File size
- Dimensions
- Duration (for video)
- Preview URL

### analytics
Performance metrics with flexible configuration:
- Time series data (by granularity)
- Entity-level aggregation
- Metric groups (engagement, billing, video, etc.)
- Placement breakdown

## Analytics Configuration

### Entity Types
Aggregate metrics at different levels:
- `ACCOUNT`: Account-level totals
- `FUNDING_INSTRUMENT`: Per funding source
- `CAMPAIGN`: Per-campaign metrics
- `LINE_ITEM`: Per-line item (ad group) metrics
- `PROMOTED_TWEET`: Per-promoted tweet (most granular)

### Granularity
Time aggregation:
- `HOUR`: Hourly breakdown
- `DAY`: Daily breakdown (default)
- `TOTAL`: Cumulative totals for date range

### Metric Groups
Available metric categories:
- `ENGAGEMENT`: Engagements, retweets, likes, replies, follows, clicks
- `BILLING`: Spend, impressions, billed metrics
- `VIDEO`: Video views, quartile completions, view time
- `MOBILE_CONVERSION`: App installs, app clicks, cost per install
- `WEB_CONVERSION`: Website clicks, conversions, cost per conversion
- `MEDIA`: Media views and engagements
- `LIFE_TIME_VALUE_MOBILE_CONVERSION`: LTV mobile conversion tracking

### Placement
- `ALL_ON_TWITTER`: Twitter-only placements (default)
- `PUBLISHER_NETWORK`: Twitter Audience Platform placements

## Downstream Usage

The ingested data can be used by:
- **Marketing Data Standardizer**: Normalize Twitter Ads data into common schema
- **Attribution Modeling**: Multi-touch attribution across platforms
- **Campaign Performance**: Cross-platform campaign dashboards
- **Social Engagement Analytics**: Tweet engagement and virality analysis
- **Customer 360**: Combine with CRM and analytics data

## Tips

1. **OAuth 1.0a**: Twitter Ads API uses OAuth 1.0a (not 2.0). Credentials don't expire but can be revoked
2. **Multiple Accounts**: Separate account IDs with commas
3. **Rate Limits**: Twitter has rate limits; dlt handles retries automatically
4. **Line Items vs Ad Groups**: Twitter uses "line items" terminology (equivalent to ad groups)
5. **Entity Hierarchy**: Account > Campaign > Line Item > Promoted Tweet
6. **Video Metrics**: Include VIDEO metric group for video campaign analysis
7. **Data Latency**: Analytics data may have 24-48 hour delay

## Data Schema Examples

### Campaigns Table
- `id` - Campaign ID
- `name` - Campaign name
- `account_id` - Parent account
- `funding_instrument_id` - Funding source
- `status` - ACTIVE, PAUSED, etc.
- `daily_budget_amount_local_micro` - Daily budget (micros)
- `total_budget_amount_local_micro` - Total budget (micros)
- `start_time` - Campaign start
- `end_time` - Campaign end
- `objective` - Campaign objective
- `created_at`, `updated_at`, `deleted`

### Line Items Table
- `id` - Line item ID
- `name` - Line item name
- `campaign_id` - Parent campaign
- `objective` - Line item objective
- `bid_type` - Bid strategy
- `bid_amount_local_micro` - Bid amount
- `total_budget_amount_local_micro` - Budget
- `target_cpa_local_micro` - Target CPA (if applicable)
- `placements` - Placement array
- `product_type` - PROMOTED_TWEETS, etc.

### Analytics Table
- `id` - Entity ID (campaign, line item, or tweet)
- `id_data` - Entity metadata
- `segment` - Segment information (if applicable)
- `metrics` - Nested metrics object containing:
  - `impressions` - Total impressions
  - `clicks` - Total clicks
  - `billed_charge_local_micro` - Spend
  - `engagements` - Total engagements
  - `retweets`, `likes`, `replies` - Specific engagements
  - `follows` - Follows from ad
  - `url_clicks` - Landing page clicks
  - `video_views_*` - Video view metrics

## Related Components

- **Google Ads Ingestion**: Ingest Google Ads data
- **Facebook Ads Ingestion**: Ingest Facebook Ads data
- **LinkedIn Ads Ingestion**: Ingest LinkedIn Ads data
- **TikTok Ads Ingestion**: Ingest TikTok Ads data
- **Marketing Data Standardizer**: Normalize data across platforms
- **Attribution Modeling**: Analyze marketing attribution

## Troubleshooting

### "Unauthorized" or "Invalid OAuth credentials"
- Verify all four OAuth 1.0a credentials are correct
- Check that access token matches consumer key
- Ensure credentials haven't been revoked
- Verify app has Ads API access enabled

### "Forbidden" or permission denied
- Confirm you have access to the ads account
- Verify Ads API access has been approved for your app
- Check account ID format (should be alphanumeric, not numeric)
- Ensure account is active and not suspended

### No analytics data returned
- Verify date range is within campaign active dates
- Check that campaigns have served impressions
- Ensure entity_type matches available entities
- Note 24-48 hour data latency for recent dates
- Verify metric_groups are valid for your campaign type

### Rate limit errors
- dlt handles retries automatically with exponential backoff
- Consider querying fewer accounts simultaneously
- Reduce analytics granularity from HOUR to DAY
- Spread large extractions across multiple runs

### "Campaign not found" errors
- Verify campaign IDs are from the correct account
- Check if campaigns have been deleted
- Use `with_deleted: true` param to include deleted entities

## API Reference

This component uses the Twitter Ads API v12:
- API documentation: https://developer.twitter.com/en/docs/twitter-ads-api
- Authentication: https://developer.twitter.com/en/docs/authentication/oauth-1-0a
- Analytics: https://developer.twitter.com/en/docs/twitter-ads-api/analytics

## Support

For issues with:
- **Component**: Create issue in dagster-component-templates repo
- **dlt REST API source**: Check [dlt REST API docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- **Twitter Ads API**: Refer to [Twitter Ads API docs](https://developer.twitter.com/en/docs/twitter-ads-api)

## Note on dlt Source

Twitter/X Ads is not currently available as a verified dlt source. This component uses dlt's generic REST API source to connect to the Twitter Ads API. The implementation follows Twitter's official OAuth 1.0a authentication and API patterns, providing production-ready data extraction for advertising workflows.

For updates on Twitter Ads becoming a verified source, check the [dlt verified sources page](https://dlthub.com/docs/dlt-ecosystem/verified-sources).
