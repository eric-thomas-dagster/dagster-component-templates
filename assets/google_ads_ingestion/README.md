# Google Ads Ingestion Component

Ingest Google Ads data into your data warehouse using dlt's verified Google Ads source. This component provides capabilities similar to Supermetrics, Funnel.io, and Adverity for Google Ads data extraction.

## Features

- **Multiple Resources**: Extract customers, campaigns, ad groups, ads, keywords, and change events
- **Flexible Authentication**: OAuth or Service Account credentials
- **Date Range Control**: Specify custom date ranges for performance metrics
- **Multiple Destinations**: Load to DuckDB, Snowflake, BigQuery, Postgres, or Redshift
- **Comprehensive Metrics**: Get impressions, clicks, cost, conversions, and more
- **Incremental Loading**: dlt automatically handles incremental updates

## Data Extracted

### Standard Resources
- **Customers**: Advertiser account information
- **Campaigns**: Campaign configurations, budgets, and targeting
- **Ad Groups**: Ad group settings within campaigns
- **Ads**: Individual ad creative and settings
- **Keywords**: Keyword targeting and bids
- **Change Events**: Historical changes to account settings

### Performance Metrics
For each resource, performance data includes:
- Impressions
- Clicks
- Cost
- Conversions
- Conversion Value
- CTR (Click-through rate)
- Average CPC
- Average CPM
- Quality Score (for keywords)

## Prerequisites

### 1. Google Ads Account
Active Google Ads account with API access

### 2. Developer Token
Apply for a developer token at https://ads.google.com/home/tools/manager-accounts/

### 3. OAuth Credentials (Option A)
Create OAuth 2.0 credentials in Google Cloud Console:
1. Go to https://console.cloud.google.com/
2. Create a project (or use existing)
3. Enable Google Ads API
4. Create OAuth 2.0 credentials
5. Generate refresh token using OAuth playground

### 4. Service Account (Option B)
Create service account in Google Cloud Console:
1. Create service account
2. Grant Google Ads API access
3. Download JSON key file
4. Configure domain-wide delegation if needed

## Configuration

### Basic Example (OAuth)

```yaml
type: dagster_component_templates.GoogleAdsIngestionComponent
attributes:
  asset_name: google_ads_data
  customer_id: "123-456-7890"
  developer_token: "${GOOGLE_ADS_DEV_TOKEN}"
  client_id: "${GOOGLE_ADS_CLIENT_ID}"
  client_secret: "${GOOGLE_ADS_CLIENT_SECRET}"
  refresh_token: "${GOOGLE_ADS_REFRESH_TOKEN}"
  resources: "customers,campaigns"
  destination: "duckdb"
```

### Advanced Example (with Date Range)

```yaml
type: dagster_component_templates.GoogleAdsIngestionComponent
attributes:
  asset_name: google_ads_campaigns
  customer_id: "123-456-7890"

  # Authentication
  developer_token: "${GOOGLE_ADS_DEV_TOKEN}"
  client_id: "${GOOGLE_ADS_CLIENT_ID}"
  client_secret: "${GOOGLE_ADS_CLIENT_SECRET}"
  refresh_token: "${GOOGLE_ADS_REFRESH_TOKEN}"
  project_id: "${GOOGLE_CLOUD_PROJECT_ID}"

  # Resources
  resources: "customers,campaigns,ad_groups,ads"

  # Date range for performance metrics
  start_date: "2024-01-01"
  end_date: "2024-12-31"

  # Destination
  destination: "snowflake"
  destination_config: '{"credentials": "snowflake://user:pass@account/db/schema"}'

  group_name: "marketing_analytics"
```

### Service Account Example

```yaml
type: dagster_component_templates.GoogleAdsIngestionComponent
attributes:
  asset_name: google_ads_data
  customer_id: "123-456-7890"

  # Service Account Authentication
  developer_token: "${GOOGLE_ADS_DEV_TOKEN}"
  use_service_account: true
  project_id: "${GOOGLE_CLOUD_PROJECT_ID}"
  service_account_email: "service@project.iam.gserviceaccount.com"
  private_key: "${GOOGLE_ADS_PRIVATE_KEY}"
  impersonated_email: "admin@example.com"

  resources: "customers,campaigns"
  destination: "bigquery"
```

## Environment Variables

Store sensitive credentials in environment variables:

```bash
# OAuth credentials
export GOOGLE_ADS_DEV_TOKEN="your_dev_token"
export GOOGLE_ADS_CLIENT_ID="your_client_id.apps.googleusercontent.com"
export GOOGLE_ADS_CLIENT_SECRET="your_client_secret"
export GOOGLE_ADS_REFRESH_TOKEN="your_refresh_token"
export GOOGLE_CLOUD_PROJECT_ID="your-project-id"

# Service account (alternative)
export GOOGLE_ADS_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n..."
```

Reference in YAML with `${VAR_NAME}` syntax.

## Getting OAuth Refresh Token

Use Google OAuth Playground:
1. Go to https://developers.google.com/oauthplayground/
2. Click settings (gear icon)
3. Check "Use your own OAuth credentials"
4. Enter your Client ID and Client Secret
5. Select "Google Ads API v16" scope
6. Authorize and get refresh token

## Downstream Usage

The ingested data can be used by:
- **Marketing Data Standardizer**: Normalize Google Ads data into common schema
- **Attribution Modeling**: Multi-touch attribution analysis
- **Campaign Performance**: Cross-platform campaign dashboards
- **Customer 360**: Combine with CRM and analytics data

## Tips

1. **Customer ID Format**: Remove dashes if present (both `123-456-7890` and `1234567890` work)
2. **Manager Accounts**: Use customer ID of sub-accounts, not manager account ID
3. **Date Ranges**: Performance data availability depends on campaign age
4. **Rate Limits**: Google Ads API has rate limits; dlt handles retries
5. **Incremental Loading**: dlt tracks state automatically for subsequent runs

## Data Schema

### Customers Table
- `customer_id` - Customer account ID
- `descriptive_name` - Account name
- `currency_code` - Account currency
- `time_zone` - Account time zone
- `status` - Account status

### Campaigns Table
- `campaign_id` - Campaign ID
- `name` - Campaign name
- `status` - Campaign status (ENABLED, PAUSED, REMOVED)
- `advertising_channel_type` - SEARCH, DISPLAY, VIDEO, etc.
- `bidding_strategy_type` - Bidding strategy
- `budget_amount_micros` - Budget in micros (divide by 1M)
- `impressions` - Total impressions
- `clicks` - Total clicks
- `cost_micros` - Total cost in micros
- `conversions` - Total conversions

### Ad Groups Table
- `ad_group_id` - Ad group ID
- `campaign_id` - Parent campaign ID
- `name` - Ad group name
- `status` - Ad group status
- `cpc_bid_micros` - CPC bid in micros
- Performance metrics (impressions, clicks, cost, etc.)

### Ads Table
- `ad_id` - Ad ID
- `ad_group_id` - Parent ad group ID
- `type` - Ad type (TEXT_AD, RESPONSIVE_SEARCH_AD, etc.)
- `status` - Ad status
- `final_urls` - Landing page URLs
- Performance metrics

### Keywords Table
- `keyword_id` - Keyword ID
- `ad_group_id` - Parent ad group ID
- `text` - Keyword text
- `match_type` - EXACT, PHRASE, BROAD
- `status` - Keyword status
- `quality_score` - Quality score (1-10)
- Performance metrics

## Related Components

- **Facebook Ads Ingestion**: Ingest Facebook Ads data
- **Marketing Data Standardizer**: Normalize data across platforms
- **Attribution Modeling**: Analyze marketing attribution
- **DataFrame Transformer**: Transform and clean the data

## Support

For issues with:
- **Component**: Create issue in dagster-component-templates repo
- **dlt source**: Check [dlt Google Ads docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_ads)
- **Google Ads API**: Refer to [Google Ads API docs](https://developers.google.com/google-ads/api/docs/start)
