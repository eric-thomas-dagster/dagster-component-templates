# Campaign Performance Analytics Component

Analyze marketing campaign performance with ROI, ROAS, CPA, and conversion metrics. Compare against targets and identify top/bottom performers.

## Overview

Track and optimize marketing campaigns with key performance metrics:
- **ROAS** (Return on Ad Spend): Revenue per dollar spent
- **CPA** (Cost Per Acquisition): Cost to acquire a customer
- **ROI** (Return on Investment): Profitability percentage
- **CTR** (Click-Through Rate): Click engagement
- **CPC** (Cost Per Click): Click efficiency
- **Conversion Rate**: Click-to-conversion efficiency

## Use Cases

- **Budget Allocation**: Invest more in high-ROAS campaigns
- **Performance Monitoring**: Track campaigns against KPI targets
- **Channel Comparison**: Compare performance across platforms
- **Optimization**: Identify underperforming campaigns to pause/fix
- **Forecasting**: Project future performance based on trends
- **Executive Reporting**: Dashboard-ready metrics

## Input Requirements

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `campaign_id` | string | ✓ | campaignId, id, campaign | Campaign identifier |
| `spend` | number | ✓ | cost, budget, amount | Campaign spend |
| `campaign_name` | string | | name, campaignName | Campaign name |
| `channel` | string | | source, platform | Marketing channel |
| `impressions` | number | | views, reach | Ad impressions |
| `clicks` | number | | link_clicks | Click count |
| `conversions` | number | | purchases, leads | Conversion count |
| `revenue` | number | | sales, conversion_value | Revenue generated |

**Compatible Upstream Components:**
- `facebook_ads_ingestion`
- `google_ads_ingestion`
- `marketing_data_standardizer`
- Any component with campaign metrics

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `campaign_id` | string | Campaign identifier |
| `campaign_name` | string | Campaign name (if available) |
| `channel` | string | Marketing channel (if available) |
| `spend` | number | Total campaign spend |
| `impressions` | number | Total impressions |
| `clicks` | number | Total clicks |
| `conversions` | number | Total conversions |
| `revenue` | number | Total revenue |
| `ctr` | number | Click-through rate (%) |
| `cpc` | number | Cost per click ($) |
| `conversion_rate` | number | Conversion rate (%) |
| `cpa` | number | Cost per acquisition ($) |
| `roas` | number | Return on ad spend (x) |
| `roi` | number | Return on investment (%) |
| `roas_vs_target` | number | % above/below target ROAS |
| `roas_performance` | string | Performance rating |

## Configuration

```yaml
type: dagster_component_templates.CampaignPerformanceComponent
attributes:
  asset_name: campaign_performance
  target_roas: 3.0
  target_cpa: 50.0
  include_benchmarks: true
  description: Campaign performance analysis
```

## Best Practices

**Target Setting:**
- **E-commerce ROAS**: 3-5x
- **B2B ROAS**: 2-4x
- **Lead Gen CPA**: $20-$100 depending on LTV
- **E-commerce CPA**: $10-$50

**Optimization Strategy:**
- Campaigns with ROAS < 2x → Review/pause
- Campaigns with ROAS > 5x → Scale up
- CTR < 1% → Improve creative
- Conversion Rate < 2% → Improve landing page

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`
