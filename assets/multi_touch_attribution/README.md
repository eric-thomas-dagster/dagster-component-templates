# Multi-Touch Attribution Component

Attribute revenue and conversions across multiple marketing touchpoints using industry-standard attribution models. Understand which channels truly drive conversions and optimize marketing spend accordingly.

## Overview

Multi-touch attribution solves the problem of credit assignment in complex customer journeys. Instead of giving 100% credit to a single touchpoint (first or last click), this component distributes credit across all interactions based on proven attribution models:

- **First Touch**: All credit to the first interaction
- **Last Touch**: All credit to the last interaction before conversion
- **Linear**: Equal credit to all touchpoints
- **Time Decay**: More credit to recent interactions (exponential decay)
- **U-Shaped (Position-Based)**: 40% first, 40% last, 20% middle
- **W-Shaped**: 30% first, 30% key milestone, 30% last, 10% others

## Use Cases

- **Marketing Budget Allocation**: Invest more in high-performing channels
- **Channel Performance**: Understand true channel contribution vs. last-click bias
- **Customer Journey Analysis**: See common paths to conversion
- **Campaign ROI**: Calculate return on investment per channel
- **A/B Testing**: Compare attribution models to find best fit
- **Cross-Channel Optimization**: Optimize the full funnel, not individual channels

## Input Requirements

Marketing touchpoint data with customer interactions:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `customer_id` | string | ✓ | user_id, customerId, userId, id | Unique customer identifier |
| `date` | datetime | ✓ | timestamp, touchpoint_date, interaction_date, created_at | Touchpoint timestamp |
| `channel` | string | ✓ | source, marketing_channel, utm_source, campaign_source | Marketing channel name |
| `is_conversion` | boolean | | converted, conversion | Whether this touchpoint led to conversion (optional) |
| `conversion_value` | number | | revenue, value, amount | Revenue from conversion (optional, defaults to 1) |

**Compatible Upstream Components:**
- `event_data_standardizer`
- `marketing_data_standardizer`
- `google_analytics_ingestion`
- `facebook_ads_ingestion`
- Any component outputting marketing touchpoint data

## Output Schema

Returns one row per attributed touchpoint:

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | string | Customer identifier |
| `date` | datetime | Touchpoint timestamp |
| `channel` | string | Marketing channel |
| `conversion_id` | string | Unique conversion identifier |
| `conversion_date` | datetime | When conversion occurred |
| `total_conversion_value` | number | Full conversion value |
| `attribution_weight` | number | Credit weight (0-1) assigned to this touchpoint |
| `attributed_value` | number | Revenue attributed to this touchpoint |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for the output asset (e.g., `channel_attribution`)

### Optional Parameters

- **`touchpoint_data_asset`** (string): Upstream touchpoint data (auto-set via lineage)
- **`conversion_data_asset`** (string): Separate conversion data (optional)
- **`attribution_model`** (string): Model to use (default: `linear`)
  - `first_touch`
  - `last_touch`
  - `linear`
  - `time_decay`
  - `u_shaped`
  - `w_shaped`
- **`lookback_window_days`** (number): Days before conversion to attribute (default: 30)
- **`time_decay_half_life_days`** (number): Half-life for time decay model (default: 7)
- **`include_channel_performance`** (boolean): Aggregate metrics (default: true)
- **`include_journey_details`** (boolean): Journey path analysis (default: true)
- **`customer_id_field`** (string): Custom column name (auto-detected)
- **`touchpoint_date_field`** (string): Custom column name (auto-detected)
- **`channel_field`** (string): Custom column name (auto-detected)
- **`conversion_date_field`** (string): Custom column name (auto-detected)
- **`conversion_value_field`** (string): Custom column name (auto-detected)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `marketing_analytics`)

## Example Configuration

### Time Decay Attribution (Recommended)

```yaml
type: dagster_component_templates.MultiTouchAttributionComponent
attributes:
  asset_name: channel_attribution
  attribution_model: time_decay
  lookback_window_days: 30
  time_decay_half_life_days: 7
  description: Time-decay attribution analysis
```

### Compare Multiple Models

```yaml
# Linear model
type: dagster_component_templates.MultiTouchAttributionComponent
attributes:
  asset_name: attribution_linear
  attribution_model: linear
  lookback_window_days: 30

---
# U-shaped model
type: dagster_component_templates.MultiTouchAttributionComponent
attributes:
  asset_name: attribution_u_shaped
  attribution_model: u_shaped
  lookback_window_days: 30

---
# Last touch (for comparison)
type: dagster_component_templates.MultiTouchAttributionComponent
attributes:
  asset_name: attribution_last_touch
  attribution_model: last_touch
  lookback_window_days: 30
```

## Attribution Models Explained

### First Touch
**Best for:** Brand awareness, top-of-funnel campaigns

```
Touch 1 (Paid Search) → Touch 2 (Social) → Touch 3 (Email) → Conversion
100%                    0%                 0%
```

**When to use:**
- You want to optimize acquisition channels
- Understanding initial awareness is critical
- Attribution to campaigns that start the journey

### Last Touch (Last Click)
**Best for:** Direct response, bottom-of-funnel campaigns

```
Touch 1 (Social) → Touch 2 (Email) → Touch 3 (Paid Search) → Conversion
0%                 0%                100%
```

**When to use:**
- Quick decisions (e-commerce)
- You want to reward closing channels
- Comparing to Google Analytics (uses last non-direct click)

### Linear
**Best for:** Balanced view, all-touchpoint credit

```
Touch 1 (Social) → Touch 2 (Email) → Touch 3 (Paid) → Conversion
33.3%              33.3%              33.3%
```

**When to use:**
- Long consideration cycles
- All channels contribute equally
- Starting point for attribution analysis

### Time Decay
**Best for:** Recent interactions matter most (recommended default)

```
Touch 1 (Social) → Touch 2 (Email) → Touch 3 (Paid) → Conversion
15%                25%                60%
```

**When to use:**
- E-commerce and SaaS (most common)
- Recent interactions predict conversion
- Balances first and last touch

### U-Shaped (Position-Based)
**Best for:** Emphasizing awareness and conversion

```
Touch 1 (Social) → Touch 2 (Email) → Touch 3 (Paid) → Conversion
40%                10%                10%               40%
```

**When to use:**
- B2B sales with long cycles
- First impression and final decision both matter
- Multiple middle-funnel touchpoints

### W-Shaped
**Best for:** B2B with lead conversion milestone

```
Touch 1 (Ad) → Touch 2 (Content) → Touch 3 (Demo) → Touch 4 (Email) → Conversion
30%            5%                   30%               5%                30%
```

**When to use:**
- Complex B2B sales
- Clear milestone events (MQL, SQL, demo)
- Long sales cycles with key conversion points

## How It Works

### 1. Data Preparation

- Load touchpoint data with customer interactions
- Identify conversions (explicit flag or last touchpoint)
- Sort chronologically by customer and date

### 2. Journey Construction

For each conversion:
- Find all touchpoints within lookback window
- Build customer journey sequence
- Calculate days between touchpoint and conversion

### 3. Weight Calculation

Apply attribution model formula:
- **First/Last Touch**: Binary weights (0 or 1)
- **Linear**: Equal weights (1/N)
- **Time Decay**: Exponential decay based on recency
- **U/W-Shaped**: Position-based weights

### 4. Value Distribution

Distribute conversion value:
```
Attributed Value = Conversion Value × Attribution Weight
```

### 5. Aggregation

Summarize by channel:
- Total attributed value
- Number of conversions
- Contribution percentage
- Average value per conversion

## Reading the Results

### Channel Performance Table

| Channel | Attributed Value | Contribution % | Conversions | Avg Value/Conv |
|---------|-----------------|----------------|-------------|----------------|
| Paid Search | $45,000 | 35% | 150 | $300 |
| Email | $32,000 | 25% | 200 | $160 |
| Organic Social | $25,000 | 19% | 180 | $139 |
| Direct | $27,000 | 21% | 90 | $300 |

**Insights:**
- Paid Search drives highest value despite fewer conversions
- Email has most conversions but lower value per conversion
- Direct has high value/conversion (likely returning customers)

### Journey Path Analysis

Top conversion paths:
```
1. Organic Search → Email → Paid Search (45 conversions)
2. Social → Email → Direct (38 conversions)
3. Paid Search → Email (32 conversions)
4. Social → Organic Search → Email → Paid Search (28 conversions)
```

**Insights:**
- Email appears in most conversion paths (nurture role)
- Paid Search often closes deals (last touch)
- Social frequently starts journeys (awareness)

## Use Case Examples

### 1. Compare Attribution Models

```python
linear_df = context.load_asset_value("attribution_linear")
last_touch_df = context.load_asset_value("attribution_last_touch")

# Aggregate by channel
linear_summary = linear_df.groupby('channel')['attributed_value'].sum()
last_touch_summary = last_touch_df.groupby('channel')['attributed_value'].sum()

comparison = pd.DataFrame({
    'Linear': linear_summary,
    'Last Touch': last_touch_summary,
    'Difference': linear_summary - last_touch_summary,
    'Difference %': ((linear_summary - last_touch_summary) / last_touch_summary * 100).round(1)
})

print("Attribution Model Comparison:")
print(comparison.sort_values('Difference %', ascending=False))
```

### 2. Calculate Channel ROI

```python
# Load attribution results
attribution_df = context.load_asset_value("channel_attribution")

# Load channel costs
channel_costs = {
    'Paid Search': 15000,
    'Facebook Ads': 12000,
    'Email': 3000,
    'Content Marketing': 8000
}

# Calculate attributed value by channel
channel_value = attribution_df.groupby('channel')['attributed_value'].sum()

# Calculate ROI
roi_df = pd.DataFrame({
    'Attributed Revenue': channel_value,
    'Cost': pd.Series(channel_costs),
    'ROI': ((channel_value - pd.Series(channel_costs)) / pd.Series(channel_costs) * 100).round(1)
})

roi_df = roi_df.sort_values('ROI', ascending=False)

print("Channel ROI Analysis:")
print(roi_df)
```

### 3. Journey Length Analysis

```python
attribution_df = context.load_asset_value("channel_attribution")

# Calculate journey length (touchpoints per conversion)
journey_lengths = attribution_df.groupby('conversion_id').size()

print(f"Average touchpoints to conversion: {journey_lengths.mean():.1f}")
print(f"Median touchpoints: {journey_lengths.median():.0f}")

# Distribution
print("\nJourney Length Distribution:")
print(journey_lengths.value_counts().sort_index().head(10))
```

### 4. Top Performing Channel Combinations

```python
attribution_df = context.load_asset_value("channel_attribution")

# Get unique channel pairs in journeys
from itertools import combinations

journey_pairs = []
for conversion_id in attribution_df['conversion_id'].unique():
    channels = attribution_df[attribution_df['conversion_id'] == conversion_id]['channel'].unique()
    if len(channels) >= 2:
        for pair in combinations(sorted(channels), 2):
            journey_pairs.append(pair)

pair_counts = pd.Series(journey_pairs).value_counts().head(10)

print("Top Channel Combinations in Conversion Journeys:")
for pair, count in pair_counts.items():
    print(f"  {pair[0]} + {pair[1]}: {count} conversions")
```

### 5. Time to Conversion Analysis

```python
attribution_df = context.load_asset_value("channel_attribution")

# Calculate days from first touch to conversion per customer journey
first_touches = attribution_df.groupby('conversion_id').agg({
    'date': 'min',
    'conversion_date': 'first',
    'total_conversion_value': 'first'
})

first_touches['days_to_conversion'] = (
    first_touches['conversion_date'] - first_touches['date']
).dt.days

print(f"Average days to conversion: {first_touches['days_to_conversion'].mean():.1f}")
print(f"Median days to conversion: {first_touches['days_to_conversion'].median():.0f}")

# By conversion value
print("\nDays to Conversion by Value Tier:")
first_touches['value_tier'] = pd.qcut(
    first_touches['total_conversion_value'],
    q=4,
    labels=['Low', 'Medium', 'High', 'Very High']
)
print(first_touches.groupby('value_tier')['days_to_conversion'].mean().round(1))
```

## Best Practices

### Choosing an Attribution Model

| Business Type | Recommended Model | Why |
|--------------|-------------------|-----|
| E-commerce | Time Decay | Recent interactions predict purchase |
| SaaS (Short Cycle) | Time Decay or Linear | Balanced view, recent emphasis |
| SaaS (Long Cycle) | U-Shaped | First touch and decision matter |
| B2B Enterprise | W-Shaped | Multiple key milestones (MQL, SQL, demo) |
| Retail | Last Touch or Time Decay | Quick purchase decisions |
| Subscription | Linear or Time Decay | All nurture touches matter |

### Lookback Window Guidelines

- **E-commerce**: 7-30 days (short purchase cycles)
- **SaaS (Monthly)**: 30-60 days
- **SaaS (Annual)**: 90-180 days
- **B2B Enterprise**: 180-365 days (long sales cycles)
- **Mobile Apps**: 1-7 days (impulse downloads)

### Data Quality Tips

1. **Consistent Customer IDs**: Ensure same customer_id across touchpoints
2. **Clean Channel Names**: Standardize channel naming (e.g., "Google Ads" vs "google_ads")
3. **Deduplicate**: Remove duplicate touchpoints (same customer, channel, timestamp)
4. **Filter Bot Traffic**: Remove non-human interactions
5. **Handle Direct Traffic**: Consider removing or grouping "direct" traffic

### Validation

**Test Your Model:**
```python
# Compare total attributed value to actual conversion value
actual_total = attribution_df.groupby('conversion_id')['total_conversion_value'].first().sum()
attributed_total = attribution_df['attributed_value'].sum()

print(f"Actual Total: ${actual_total:,.2f}")
print(f"Attributed Total: ${attributed_total:,.2f}")
print(f"Difference: ${abs(actual_total - attributed_total):,.2f}")
```

Should be within $1-10 due to rounding.

## Limitations

1. **Correlation ≠ Causation**: Attribution shows correlation, not necessarily causation
2. **Offline Touchpoints**: Doesn't capture offline interactions (TV, radio, events)
3. **Cross-Device**: Limited to same customer_id (can't track device switching)
4. **View-Through Attribution**: Only tracks clicks, not ad impressions
5. **External Factors**: Doesn't account for seasonality, competition, market changes
6. **Historical Data**: All models are backward-looking, not predictive

For more sophisticated attribution:
- Machine learning attribution (data-driven)
- Incrementality testing (A/B tests)
- Marketing Mix Modeling (MMM)
- Multi-touch attribution platforms (Google Attribution, etc.)

## Integration Examples

### Full Marketing Analytics Pipeline

```
Marketing Touchpoints → Multi-Touch Attribution → Channel Performance Dashboard
Campaign Costs → ROI Calculation → Budget Optimization
Customer Segmentation → Attributed Value by Segment → Targeted Campaigns
```

### Combined with LTV

```
Attribution Data → Channel-Level LTV → Customer Acquisition Strategy
LTV Prediction → Channel LTV Prediction → Budget Allocation by Channel
```

## Key Metrics to Track

### Channel-Level
- Attributed revenue per channel
- Contribution percentage
- Conversions touched
- Average attributed value per conversion
- Cost per attributed conversion (CPA)
- Return on ad spend (ROAS)

### Journey-Level
- Average touchpoints to conversion
- Time to conversion
- Most common paths
- Channel sequence patterns

### Model Comparison
- Revenue variance across models
- Channel ranking stability
- Model sensitivity to parameters

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **Update Frequency**: Run weekly or monthly with new conversion data
- **Model Testing**: Compare multiple models to find best fit for your business
- **Incremental Analysis**: Use with holdout testing to validate channel impact
- **Cross-Functional**: Share results with marketing, finance, and executive teams
- **Iteration**: Attribution is a journey—start simple (linear/time decay) and iterate
