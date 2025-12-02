# Facebook Ads Ingestion Component

Ingest Facebook Ads data into your data warehouse using dlt's verified Facebook Ads source. This component provides capabilities similar to Supermetrics, Funnel.io, and Adverity for Facebook Ads data extraction.

## Features

- **Multiple Resources**: Extract campaigns, ad sets, ads, creatives, leads, and insights
- **Flexible Configuration**: Customize date ranges, fields, breakdowns, and filters
- **Multiple Destinations**: Load to DuckDB, Snowflake, BigQuery, Postgres, or Redshift
- **Insights Support**: Get detailed performance metrics with demographic breakdowns
- **Ad States Filtering**: Filter ads by status (ACTIVE, PAUSED, etc.)
- **Incremental Loading**: dlt automatically handles incremental updates on subsequent runs

## Data Extracted

### Standard Resources
- **Campaigns**: Marketing campaigns with objectives and budgets
- **Ad Sets**: Ad set configurations within campaigns
- **Ads**: Individual ad units
- **Creatives**: Visual and textual ad content
- **Ad Leads**: Lead generation form submissions

### Insights
Performance metrics including:
- Impressions
- Clicks
- Spend
- Reach
- CPM, CPC, CTR
- Conversions
- And 100+ more metrics

Insights can be broken down by:
- Age
- Gender
- Country/Region
- Platform
- Device
- Placement

## Prerequisites

### 1. Facebook Developer Account
Create a Facebook Business App at https://developers.facebook.com/

### 2. Access Token
Generate an access token with these permissions:
- `ads_read`
- `lead_retrieval` (if extracting leads)

### 3. Account ID
Find your Facebook Ads Account ID:
1. Go to Facebook Ads Manager
2. Look in the URL: `act_123456789`
3. Or check Account Settings

## Configuration

### Basic Example

```yaml
type: dagster_component_templates.FacebookAdsIngestionComponent
attributes:
  asset_name: facebook_ads_data
  account_id: "act_123456789"
  access_token: "${FACEBOOK_ACCESS_TOKEN}"
  resources: "campaigns,ads,insights"
  destination: "duckdb"
```

### Advanced Example

```yaml
type: dagster_component_templates.FacebookAdsIngestionComponent
attributes:
  asset_name: facebook_ads_insights
  account_id: "act_123456789"
  access_token: "${FACEBOOK_ACCESS_TOKEN}"
  app_id: "${FACEBOOK_APP_ID}"
  app_secret: "${FACEBOOK_APP_SECRET}"

  # Extract only insights with custom fields
  resources: "insights"
  insights_fields: "impressions,clicks,spend,reach,cpm,ctr,conversions"
  insights_breakdown: "age"

  # Load last 90 days of data
  initial_load_past_days: 90
  time_increment_days: 1

  # Load to Snowflake
  destination: "snowflake"
  destination_config: '{"credentials": "snowflake://user:pass@account/db/schema"}'

  group_name: "marketing_analytics"
```

## Environment Variables

Store sensitive credentials in environment variables:

```bash
export FACEBOOK_ACCESS_TOKEN="your_access_token"
export FACEBOOK_APP_ID="your_app_id"
export FACEBOOK_APP_SECRET="your_app_secret"
```

Reference in YAML with `${VAR_NAME}` syntax.

## Downstream Usage

The ingested data can be used by:
- **Marketing Data Standardizer**: Normalize Facebook Ads data into common schema
- **Attribution Modeling**: Multi-touch attribution analysis
- **Campaign Performance**: Cross-platform campaign dashboards
- **Customer 360**: Combine with CRM and analytics data

## Tips

1. **Long-lived Tokens**: Use `app_id` and `app_secret` to automatically refresh tokens
2. **Incremental Loading**: dlt handles incremental updates automatically using state management
3. **Custom Fields**: Use `insights_fields` to request only the metrics you need
4. **Breakdowns**: Add demographic breakdowns to insights for deeper analysis
5. **Rate Limits**: Facebook has API rate limits; dlt handles retries automatically

## Data Schema

### Campaigns Table
- `id` - Campaign ID
- `name` - Campaign name
- `objective` - Campaign objective (CONVERSIONS, LINK_CLICKS, etc.)
- `status` - ACTIVE, PAUSED, etc.
- `daily_budget` - Daily budget in cents
- `lifetime_budget` - Total budget in cents

### Ads Table
- `id` - Ad ID
- `name` - Ad name
- `adset_id` - Parent ad set ID
- `campaign_id` - Parent campaign ID
- `status` - Ad status
- `creative` - Creative configuration

### Insights Table
- `date_start` - Report date start
- `date_stop` - Report date end
- `campaign_id` - Campaign ID
- `ad_id` - Ad ID (if applicable)
- `impressions` - Number of impressions
- `clicks` - Number of clicks
- `spend` - Amount spent
- `reach` - Unique users reached
- `cpm` - Cost per 1000 impressions
- `cpc` - Cost per click
- `ctr` - Click-through rate
- Plus breakdown dimensions if configured

## Related Components

- **Google Ads Ingestion**: Ingest Google Ads data
- **Marketing Data Standardizer**: Normalize data across platforms
- **Attribution Modeling**: Analyze marketing attribution
- **DataFrame Transformer**: Transform and clean the data

## Support

For issues with:
- **Component**: Create issue in dagster-component-templates repo
- **dlt source**: Check [dlt Facebook Ads docs](https://dlthub.com/docs/dlt-ecosystem/verified-sources/facebook_ads)
- **Facebook API**: Refer to [Facebook Marketing API docs](https://developers.facebook.com/docs/marketing-api)
