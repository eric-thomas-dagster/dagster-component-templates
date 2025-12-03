# LinkedIn Ads Ingestion Component

Ingest LinkedIn Ads data into your data warehouse using dlt's REST API source with LinkedIn Marketing API. This component provides capabilities similar to Supermetrics, Funnel.io, and Adverity for LinkedIn Ads data extraction.

## Features

- **Multiple Resources**: Extract accounts, campaigns, creatives, campaign groups, analytics, and conversions
- **OAuth Authentication**: Secure access via LinkedIn OAuth 2.0 tokens
- **Date Range Control**: Specify custom date ranges for performance analytics
- **Flexible Granularity**: Daily, monthly, or cumulative analytics
- **Pivot Dimensions**: Analyze by campaign, creative, account, or company
- **Multiple Destinations**: Load to DuckDB, Snowflake, BigQuery, Postgres, or Redshift

## Data Extracted

### Standard Resources
- **Accounts**: Ad account information, budgets, and settings
- **Campaigns**: Campaign configurations, objectives, budgets, and status
- **Creatives**: Ad creatives including images, videos, text, and landing pages
- **Campaign Groups**: Campaign group hierarchies and organization
- **Analytics**: Performance metrics aggregated by various dimensions
- **Conversions**: Conversion tracking rules and performance

### Performance Metrics
Analytics data includes:
- Impressions
- Clicks
- Cost (spend)
- Conversions
- Conversion Value
- CTR (Click-through rate)
- Average CPC
- Average CPM
- Engagement metrics (likes, comments, shares, follows)
- Video metrics (views, completion rate, view time)

## Prerequisites

### 1. LinkedIn Ad Account
Active LinkedIn Campaign Manager account with advertising campaigns

### 2. LinkedIn Developer Application
Create a LinkedIn app to get OAuth credentials:
1. Go to https://www.linkedin.com/developers/apps
2. Create new app or use existing
3. Request access to Marketing Developer Platform
4. Add "Advertising API" product to your app
5. Configure OAuth redirect URLs

### 3. Access Token
Generate OAuth 2.0 access token with required permissions:
- `r_ads`: Read advertising accounts and campaigns
- `r_ads_reporting`: Read advertising performance analytics

### Token Generation Methods:

**Option A: LinkedIn OAuth Playground**
1. Use LinkedIn's OAuth flow
2. Authorize with `r_ads` and `r_ads_reporting` scopes
3. Exchange code for access token

**Option B: Marketing Developer Platform**
1. Go to Campaign Manager
2. Account Assets > Marketing Developer Platform
3. Create access token for your app

Note: LinkedIn access tokens expire after 60 days by default. Implement token refresh logic for production use.

## Configuration

### Basic Example

```yaml
type: dagster_component_templates.LinkedInAdsIngestionComponent
attributes:
  asset_name: linkedin_ads_data
  access_token: "${LINKEDIN_ACCESS_TOKEN}"
  account_ids: "123456789,987654321"
  resources: "campaigns,analytics"
  destination: "duckdb"
```

### Advanced Example (with Analytics Options)

```yaml
type: dagster_component_templates.LinkedInAdsIngestionComponent
attributes:
  asset_name: linkedin_ads_campaigns

  # Authentication
  access_token: "${LINKEDIN_ACCESS_TOKEN}"
  account_ids: "123456789,987654321,555555555"

  # Resources
  resources: "accounts,campaigns,creatives,analytics"

  # Analytics configuration
  start_date: "2024-01-01"
  end_date: "2024-12-31"
  time_granularity: "DAILY"
  pivot_by: "CAMPAIGN"

  # Destination
  destination: "snowflake"
  destination_config: '{"credentials": "snowflake://user:pass@account/db/schema"}'

  group_name: "marketing_analytics"
```

### Campaign Groups and Conversions Example

```yaml
type: dagster_component_templates.LinkedInAdsIngestionComponent
attributes:
  asset_name: linkedin_ads_full
  access_token: "${LINKEDIN_ACCESS_TOKEN}"
  account_ids: "123456789"

  # Extract all available resources
  resources: "accounts,campaigns,creatives,campaign_groups,analytics,conversions"

  # Analytics for last 90 days
  start_date: "2024-10-01"
  end_date: "2024-12-31"
  time_granularity: "DAILY"
  pivot_by: "CREATIVE"

  destination: "bigquery"
```

## Environment Variables

Store sensitive credentials in environment variables:

```bash
# LinkedIn credentials
export LINKEDIN_ACCESS_TOKEN="YOUR_ACCESS_TOKEN_HERE"
```

Reference in YAML with `${VAR_NAME}` syntax.

## Finding Your Ad Account ID

1. Go to LinkedIn Campaign Manager
2. Look at the URL: `https://www.linkedin.com/campaignmanager/accounts/{ACCOUNT_ID}/campaigns`
3. The number after `/accounts/` is your Ad Account ID
4. Or use the Accounts API endpoint to list all accessible accounts

## Available Resources

### accounts
Ad account details including:
- Account ID and name
- Account status (ACTIVE, DRAFT, CANCELED)
- Currency
- Account type
- Test account flag

### campaigns
Campaign configuration:
- Campaign ID and name
- Campaign group association
- Status (ACTIVE, PAUSED, ARCHIVED, etc.)
- Objective type (BRAND_AWARENESS, WEBSITE_VISITS, etc.)
- Daily/total budget
- Start and end dates
- Targeting criteria summary
- Bid strategy

### creatives
Ad creative details:
- Creative ID
- Associated campaign
- Creative type (SPONSORED_UPDATE, TEXT_AD, etc.)
- Status
- Creative content (text, images, videos)
- Call-to-action
- Landing page URL

### campaign_groups
Campaign group hierarchy:
- Campaign group ID and name
- Account association
- Status
- Budget allocation
- Member campaigns

### analytics
Performance metrics with flexible pivoting:
- Time series data (based on time_granularity)
- Pivot dimensions: campaign, creative, account, company
- All performance metrics listed above
- Attribution windows

### conversions
Conversion tracking:
- Conversion ID and name
- Type (DOWNLOAD, PURCHASE, SIGN_UP, etc.)
- Attribution settings
- Value rules
- Associated campaigns

## Analytics Configuration

### Time Granularity
- `DAILY`: Daily breakdown of metrics (default)
- `MONTHLY`: Monthly aggregation
- `ALL`: Cumulative totals for entire date range

### Pivot By
Group analytics by dimension:
- `CAMPAIGN`: Metrics per campaign
- `CREATIVE`: Metrics per creative
- `ACCOUNT`: Metrics per ad account
- `COMPANY`: Metrics per company page
- Leave empty for account-level totals

## Downstream Usage

The ingested data can be used by:
- **Marketing Data Standardizer**: Normalize LinkedIn Ads data into common schema
- **Attribution Modeling**: Multi-touch attribution analysis across platforms
- **Campaign Performance**: Cross-platform campaign dashboards
- **Customer 360**: Combine with CRM and analytics data for complete view

## Tips

1. **Token Expiration**: LinkedIn tokens expire after 60 days. Set up refresh logic or regenerate tokens regularly
2. **Multiple Accounts**: Separate multiple account IDs with commas (no spaces recommended)
3. **Rate Limits**: LinkedIn API has rate limits; dlt handles retries automatically
4. **Analytics Pivots**: Use pivot_by to get granular breakdowns; omit for account-level totals
5. **Video Metrics**: Video ads include additional metrics like view completion rate
6. **Incremental Loading**: For production, implement date-based incremental loading

## Data Schema Examples

### Campaigns Table
- `id` - Campaign URN identifier
- `name` - Campaign name
- `status` - ACTIVE, PAUSED, ARCHIVED, etc.
- `type` - Campaign objective (WEBSITE_VISITS, LEAD_GENERATION, etc.)
- `account` - Parent ad account URN
- `campaignGroup` - Parent campaign group URN
- `dailyBudget` - Daily budget amount (with currency)
- `totalBudget` - Total campaign budget
- `runSchedule` - Start and end dates
- `targetingCriteria` - Targeting settings

### Analytics Table
- `dateRange` - Date or date range of metrics
- `pivotValue` - Campaign/creative/account ID (based on pivot_by)
- `impressions` - Number of impressions
- `clicks` - Number of clicks
- `costInUsd` - Spend in USD
- `costInLocalCurrency` - Spend in account currency
- `conversions` - Total conversions
- `externalWebsiteConversions` - Website conversions
- `leadGenerationMailContactInfoShares` - Lead form submissions
- `likes` - Sponsored update likes
- `comments` - Sponsored update comments
- `shares` - Sponsored update shares
- `follows` - Company page follows
- `videoViews` - Video ad views
- `videoCompletions` - Video completion count

### Creatives Table
- `id` - Creative URN identifier
- `campaign` - Parent campaign URN
- `status` - ACTIVE, PAUSED, etc.
- `type` - SPONSORED_UPDATE, TEXT_AD, etc.
- `content` - Creative content object (text, images, videos)
- `callToAction` - CTA configuration
- `destinationUrl` - Landing page URL

## Related Components

- **Google Ads Ingestion**: Ingest Google Ads data
- **Facebook Ads Ingestion**: Ingest Facebook Ads data
- **Marketing Data Standardizer**: Normalize data across advertising platforms
- **Attribution Modeling**: Analyze marketing attribution
- **DataFrame Transformer**: Transform and clean the data

## Troubleshooting

### "Invalid access token" error
- Verify token has `r_ads` and `r_ads_reporting` permissions
- Check if token has expired (60-day default expiration)
- Regenerate token via LinkedIn Developer Portal

### "Permission denied" for account
- Verify you have access to the ad account in Campaign Manager
- Check that your app has "Advertising API" product added
- Ensure account IDs are numeric (not URNs)

### No analytics data returned
- Verify date range is within campaign active dates
- Check that campaigns have served impressions in date range
- Ensure account has analytics data available (may take 24-48 hours)

### Rate limit errors
- dlt automatically handles retries with exponential backoff
- Consider reducing number of accounts queried simultaneously
- Spread large extractions across multiple runs

## API Reference

This component uses the LinkedIn Marketing API v2:
- REST API documentation: https://docs.microsoft.com/en-us/linkedin/marketing/
- Campaign Management: https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/
- Analytics: https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/

## Support

For issues with:
- **Component**: Create issue in dagster-component-templates repo
- **dlt REST API source**: Check [dlt REST API docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- **LinkedIn Marketing API**: Refer to [LinkedIn API documentation](https://docs.microsoft.com/en-us/linkedin/marketing/)

## Note on dlt Source

LinkedIn Ads is not currently available as a verified dlt source. This component uses dlt's generic REST API source to connect to the LinkedIn Marketing API. The implementation follows LinkedIn's official API patterns and provides production-ready data extraction for advertising workflows.

For updates on LinkedIn Ads becoming a verified source, check the [dlt verified sources page](https://dlthub.com/docs/dlt-ecosystem/verified-sources).
