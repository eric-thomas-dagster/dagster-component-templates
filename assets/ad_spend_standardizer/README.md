# Ad Spend Standardizer

Unifies advertising spend data from multiple platforms (Google Ads, Facebook Ads) into a standardized schema for cross-platform analysis and marketing attribution.

## Overview

Essential component for CDP marketing analytics. Transforms platform-specific ad data into a unified format, enabling cross-platform campaign comparison and accurate revenue attribution.

## Standard Output Schema

| Field | Description |
|-------|-------------|
| campaign_id | Unique campaign identifier |
| campaign_name | Campaign display name |
| platform | Platform (google_ads, facebook_ads) |
| campaign_type | Campaign type (search, display, social) |
| date | Campaign date |
| impressions | Number of ad impressions |
| clicks | Number of clicks |
| spend | Amount spent (normalized currency) |
| conversions | Number of conversions |
| conversion_value | Value of conversions |
| ctr | Click-through rate (%) |
| cpc | Cost per click |
| cpa | Cost per acquisition |
| roas | Return on ad spend |

## Configuration

```yaml
asset_name: standardized_ad_spend
currency_normalization: USD
aggregate_by_day: true
calculate_derived_metrics: true
```

##Input Sources (Via Visual Lineage)

- Google Ads ingestion component
- Facebook Ads ingestion component
- Other ad platforms (optional)

## Use Cases

1. **Cross-Platform Analysis**: Compare campaign performance across Google and Facebook
2. **Marketing Attribution**: Feed into revenue_attribution component for ROI calculation
3. **Budget Optimization**: Identify best-performing platforms and campaigns
4. **Trend Analysis**: Track spend, CTR, CPA trends over time

## Example Pipeline

```
google_ads_ingestion → ┐
                       ├→ ad_spend_standardizer → revenue_attribution
facebook_ads_ingestion →┘
```

## Key Metrics

- **CTR**: Click-through rate = (clicks / impressions) × 100
- **CPC**: Cost per click = spend / clicks
- **CPA**: Cost per acquisition = spend / conversions
- **ROAS**: Return on ad spend = conversion_value / spend

## Platform Support

- ✅ Google Ads (full support)
- ✅ Facebook Ads (full support)
- ⚠️  Other platforms (generic mapping, may need customization)

## Related Components

- Google Ads Ingestion
- Facebook Ads Ingestion
- Revenue Attribution
- Customer 360

