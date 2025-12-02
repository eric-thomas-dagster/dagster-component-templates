# Funnel Analysis Component

Analyze user progression through defined funnel stages to identify conversion rates, drop-off points, and optimization opportunities across the customer journey.

## Overview

The Funnel Analysis component helps you understand how users move through your conversion funnel—from initial awareness to final conversion and beyond. By tracking user progression through defined stages, you can identify where users drop off, which channels perform best, and how to optimize your funnel for better conversion rates.

## Key Features

- **Multi-Stage Funnels**: Support for 2-5 stage funnels
- **Sequential Tracking**: Linear funnels with time-window validation
- **Conversion Metrics**: Stage-by-stage conversion and drop-off rates
- **Time Analysis**: Median time to convert between stages
- **Cohort Tracking**: Daily, weekly, or monthly cohort analysis
- **Source Segmentation**: Compare funnel performance by traffic source
- **Drop-off Identification**: Automatic flagging of problem stages
- **Flexible Configuration**: Customize stages for any business model

## Output Schema

| Field | Description |
|-------|-------------|
| segment | Segment name (overall, source, etc.) |
| cohort | Cohort period (all, or date for cohort analysis) |
| {stage}_count | Number of users who reached this stage |
| {stage}_conversion_rate | Conversion rate from previous stage |
| {stage}_drop_off_rate | Drop-off rate from previous stage |
| {stage}_median_hours | Median hours to convert from previous stage |
| {stage}_high_drop_off | Boolean flag if drop-off exceeds threshold |

## Common Funnel Types

### E-commerce Purchase Funnel

```yaml
stage_1_event: product_view
stage_1_name: Product View
stage_2_event: add_to_cart
stage_2_name: Add to Cart
stage_3_event: checkout_started
stage_3_name: Checkout
stage_4_event: purchase_completed
stage_4_name: Purchase
```

**Example Results**:
- Product View: 10,000 users
- Add to Cart: 2,000 users (20% conversion)
- Checkout: 1,200 users (60% conversion)
- Purchase: 800 users (67% conversion)

**Insight**: 80% drop-off from view to cart is the biggest problem.

### SaaS Signup Funnel

```yaml
stage_1_event: landing_page_view
stage_1_name: Awareness
stage_2_event: signup_started
stage_2_name: Signup Started
stage_3_event: signup_completed
stage_3_name: Signup Completed
stage_4_event: trial_activated
stage_4_name: Trial Activated
stage_5_event: converted_to_paid
stage_5_name: Paid Customer
```

### Lead Generation Funnel

```yaml
stage_1_event: ad_click
stage_1_name: Ad Click
stage_2_event: landing_page_view
stage_2_name: Landing Page
stage_3_event: form_started
stage_3_name: Form Started
stage_4_event: form_submitted
stage_4_name: Lead Captured
```

### Product Activation Funnel

```yaml
stage_1_event: account_created
stage_1_name: Account Created
stage_2_event: profile_completed
stage_2_name: Profile Setup
stage_3_event: first_action
stage_3_name: First Action
stage_4_event: feature_used
stage_4_name: Feature Adoption
stage_5_event: activated
stage_5_name: Activated User
```

## Configuration

### Basic Configuration

```yaml
asset_name: conversion_funnel
funnel_type: linear
funnel_window_days: 30
analysis_period_days: 90
```

### Input Sources (Connected via Visual Lineage)

**Event Data** (Required)
- Must include: user_id, event_name, timestamp
- Optional: source (for segmentation)

Connect by drawing an edge in Dagster Designer UI from `event_data` → `conversion_funnel`.

### Defining Funnel Stages

Minimum 2 stages, maximum 5 stages:

```yaml
# Required stages
stage_1_event: page_view
stage_1_name: Awareness
stage_2_event: signup
stage_2_name: Signup

# Optional stages
stage_3_event: trial_started
stage_3_name: Trial
stage_4_event: purchase
stage_4_name: Purchase
stage_5_event: retained
stage_5_name: Retained
```

**Event Names**: Must match exactly with event_name in your event data.

### Time Window

Control how long users have to complete the funnel:

```yaml
funnel_window_days: 30  # Users must complete funnel within 30 days
```

**Examples**:
- E-commerce: 7 days (short buying cycle)
- SaaS trial: 30 days (length of trial period)
- Enterprise sales: 180 days (long sales cycle)

### Cohort Analysis

Track how funnel performance changes over time:

```yaml
group_by_cohort: true
cohort_period: weekly  # daily, weekly, or monthly
```

**Output**: Separate funnel metrics for each cohort.

**Use Case**: Identify if product changes improved conversion rates.

### Segmentation

Compare funnel performance across segments:

```yaml
segment_by_source: true  # Compare by traffic source (google, facebook, etc.)
segment_by_attribute: plan_type  # Additional custom segmentation
```

**Output**: Separate funnel metrics for each segment.

**Use Case**: Identify which marketing channels drive best conversions.

### Drop-off Identification

Automatically flag problem stages:

```yaml
identify_drop_offs: true
drop_off_threshold: 0.5  # Flag stages with >50% drop-off
```

**Output**: Boolean column `{stage}_high_drop_off` for each stage.

### Time to Convert

Calculate how long users take between stages:

```yaml
calculate_time_to_convert: true
```

**Output**: Median hours between each stage.

**Use Case**: Identify slow stages that need optimization.

## Use Cases

### 1. Identify Biggest Drop-off Points

Find where you're losing the most users:

```python
df = context.load_asset_value("conversion_funnel")

# Get overall funnel
overall = df[df['segment'] == 'overall'].iloc[0]

# Find stages with high drop-off
stages = ['Awareness', 'Signup', 'Trial', 'Purchase']
for stage in stages:
    drop_off_col = f"{stage}_drop_off_rate"
    if drop_off_col in overall:
        drop_off = overall[drop_off_col]
        if drop_off > 0.5:
            print(f"⚠️  HIGH DROP-OFF: {stage} - {drop_off*100:.1f}%")
```

**Example Output**:
```
⚠️  HIGH DROP-OFF: Signup - 75.0%
⚠️  HIGH DROP-OFF: Trial - 60.0%
```

**Action**: Focus optimization efforts on Signup and Trial stages.

### 2. Compare Funnel Performance by Source

Identify which marketing channels drive best conversions:

```python
df = context.load_asset_value("conversion_funnel")

# Filter to source segments
sources = df[df['segment'] != 'overall']

# Compare overall conversion (first stage to last)
sources['overall_conversion'] = (
    sources['Purchase_count'] / sources['Awareness_count']
)

# Sort by conversion rate
best_sources = sources.sort_values('overall_conversion', ascending=False)

print("Top Performing Sources:")
print(best_sources[['segment', 'overall_conversion', 'Awareness_count']])
```

**Example Output**:
```
segment           overall_conversion  Awareness_count
organic_search    0.12               5000
paid_social       0.08               3000
email             0.15               1000
```

**Insight**: Email has highest conversion rate, but lowest volume. Consider increasing email marketing budget.

### 3. Cohort Analysis - Track Improvements Over Time

See if product changes improved conversion rates:

```python
df = context.load_asset_value("conversion_funnel")

# Filter to overall cohorts
cohorts = df[(df['segment'] == 'overall') & (df['cohort'] != 'all')]

# Calculate overall conversion for each cohort
cohorts['overall_conversion'] = (
    cohorts['Purchase_count'] / cohorts['Awareness_count']
)

# Plot over time
import matplotlib.pyplot as plt
plt.figure(figsize=(12, 6))
plt.plot(cohorts['cohort'], cohorts['overall_conversion'] * 100)
plt.xlabel('Cohort')
plt.ylabel('Conversion Rate (%)')
plt.title('Funnel Conversion Rate Over Time')
plt.xticks(rotation=45)
plt.show()
```

**Use Case**: A/B test launched in Week 3 → Did conversion rate improve?

### 4. Calculate Funnel Efficiency

Measure overall funnel health:

```python
df = context.load_asset_value("conversion_funnel")
overall = df[df['segment'] == 'overall'].iloc[0]

# Overall funnel conversion
first_stage_count = overall['Awareness_count']
last_stage_count = overall['Purchase_count']
overall_conversion = last_stage_count / first_stage_count

print(f"Funnel Efficiency:")
print(f"  Total entered: {first_stage_count}")
print(f"  Total converted: {last_stage_count}")
print(f"  Overall conversion: {overall_conversion*100:.1f}%")

# Benchmark against industry
if overall_conversion < 0.02:
    print("  Status: ⚠️  Below average (2%)")
elif overall_conversion < 0.05:
    print("  Status: ✓ Average (2-5%)")
else:
    print("  Status: ✅ Excellent (>5%)")
```

### 5. Optimize Slow Stages

Find stages where users take too long:

```python
df = context.load_asset_value("conversion_funnel")
overall = df[df['segment'] == 'overall'].iloc[0]

stages = ['Signup', 'Trial', 'Purchase']
for stage in stages:
    time_col = f"{stage}_median_hours"
    if time_col in overall:
        hours = overall[time_col]
        days = hours / 24
        print(f"{stage}: {days:.1f} days median time")

        # Flag if taking >14 days
        if days > 14:
            print(f"  ⚠️  Slow stage - consider intervention")
```

**Example Output**:
```
Signup: 0.5 days median time
Trial: 12.0 days median time
Purchase: 22.0 days median time
  ⚠️  Slow stage - consider intervention
```

**Action**: Send reminder emails or offer discounts to speed up purchase decision.

### 6. Stage-by-Stage Conversion Report

Generate executive summary:

```python
df = context.load_asset_value("conversion_funnel")
overall = df[df['segment'] == 'overall'].iloc[0]

stages = ['Awareness', 'Signup', 'Trial', 'Purchase']

print("Conversion Funnel Report")
print("=" * 60)

for i, stage in enumerate(stages):
    count = overall.get(f"{stage}_count", 0)
    print(f"\n{stage}: {count:,} users")

    if i > 0:
        conversion = overall.get(f"{stage}_conversion_rate", 0)
        drop_off = overall.get(f"{stage}_drop_off_rate", 0)
        time_hours = overall.get(f"{stage}_median_hours", 0)

        print(f"  Conversion from {stages[i-1]}: {conversion*100:.1f}%")
        print(f"  Drop-off rate: {drop_off*100:.1f}%")
        print(f"  Median time: {time_hours/24:.1f} days")

        # Assess stage health
        if drop_off > 0.7:
            health = "🔴 Critical"
        elif drop_off > 0.5:
            health = "🟡 Needs Attention"
        else:
            health = "🟢 Healthy"

        print(f"  Health: {health}")
```

## Best Practices

1. **Start Simple**: Begin with 2-3 key stages, add more as needed
2. **Match Your Business**: Use events that map to actual user journey
3. **Set Realistic Windows**: Funnel window should match your sales cycle
4. **Monitor Trends**: Track cohorts weekly to spot improvements/regressions
5. **Segment Smartly**: Compare apples-to-apples (similar traffic sources)
6. **Act on Insights**: Funnel analysis is useless without optimization actions
7. **Validate Data**: Check that event tracking is firing correctly
8. **Benchmark**: Compare to industry standards for your business model

## Troubleshooting

### No Users in Funnel

**Problem**: All stage counts are 0

**Solutions**:
- Verify event_data_asset is connected
- Check event names match exactly (case-sensitive)
- Ensure events exist in analysis_period_days window
- Verify user_id field is present and populated

### Low Conversion Rates Everywhere

**Problem**: Every stage has <10% conversion

**Solutions**:
- Check funnel_window_days - may be too short
- Verify stages are in correct order
- Ensure events represent actual user actions
- Look for data quality issues (duplicate events, missing timestamps)

### Funnel Window Too Restrictive

**Problem**: Many users dropped because window expired

**Solutions**:
- Increase funnel_window_days
- Consider flexible funnel (coming in future version)
- Split into multiple shorter funnels

### Cohort Analysis Shows No Variation

**Problem**: All cohorts have same conversion rates

**Solutions**:
- Use longer analysis_period_days to get more cohorts
- Switch to daily/weekly if using monthly cohorts
- Verify you've made product changes during analysis period

### Source Segmentation Not Working

**Problem**: All users showing as "unknown" source

**Solutions**:
- Ensure 'source' field exists in event data
- Check that source is populated for first-stage events
- Verify UTM parameters or tracking is capturing source

## Example Pipeline

```
┌─────────────┐
│   Website   │
│  Analytics  │
│   Events    │
└──────┬──────┘
       │
       │
       ▼
┌─────────────┐
│   Funnel    │
│  Analysis   │
└──────┬──────┘
       │
       ├──────────────────┬──────────────────┐
       │                  │                  │
┌──────▼──────┐    ┌──────▼──────┐   ┌──────▼──────┐
│  Drop-off   │    │   Source    │   │   Cohort    │
│  Analysis   │    │ Performance │   │   Trends    │
│   Report    │    │   Report    │   │   Report    │
└─────────────┘    └─────────────┘   └─────────────┘
```

## Industry Benchmarks

### E-commerce

**Add to Cart Conversion**: 10-15%
**Checkout Conversion**: 60-70%
**Purchase Conversion**: 40-60%
**Overall**: 2-5% view to purchase

### SaaS

**Signup Conversion**: 20-40%
**Trial Activation**: 40-60%
**Trial to Paid**: 10-25%
**Overall**: 1-5% visitor to customer

### Lead Generation

**Landing Page Conversion**: 10-30%
**Form Completion**: 50-70%
**MQL to SQL**: 20-40%
**Overall**: 2-10% visitor to SQL

## Related Components

- **Event Tracking Ingestion**: Source event data
- **Lead Scoring**: Qualify users based on funnel progression
- **Customer Health Score**: Monitor post-conversion engagement
- **Revenue Attribution**: Connect funnel to revenue outcomes

## Learn More

- [Funnel Analysis Guide](https://www.productplan.com/glossary/funnel-analysis/)
- [Conversion Rate Optimization](https://www.optimizely.com/optimization-glossary/conversion-rate-optimization/)
- [Cohort Analysis](https://mixpanel.com/topics/cohort-analysis/)
- [Drop-off Analysis](https://www.amplitude.com/blog/product-drop-off-analysis)
