# Funnel Analysis Component

Track conversion rates through multi-step user journeys. Identify drop-off points and optimize your conversion funnel.

## Overview

Funnel analysis tracks how users progress through a sequence of steps toward a goal (e.g., purchase, signup, feature adoption). This component helps you:

- Measure conversion rates at each step
- Identify where users drop off
- Calculate time between steps
- Optimize the user journey

Common funnels:
- **E-commerce**: Homepage → Product Page → Cart → Checkout → Purchase
- **SaaS Signup**: Landing → Signup → Verify Email → Complete Profile → First Use
- **Content**: Article List → Article View → Newsletter Signup → Share

## Use Cases

- **Conversion Optimization**: Identify and fix friction points
- **A/B Testing**: Compare funnel performance across variants
- **User Journey Mapping**: Understand how users navigate your product
- **Drop-off Analysis**: Find where you're losing users
- **Time-to-Convert**: Measure how long conversions take
- **Product Analytics**: Track feature adoption funnels

## Input Requirements

The component expects event-level data:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `user_id` | string | ✓ | customer_id, userId, visitor_id, session_id | Unique user identifier |
| `event_name` | string | ✓ | event, event_type, action, page_path | Event or step name |
| `timestamp` | datetime | ✓ | event_time, created_at, date, time | Event timestamp |

**Compatible Upstream Components:**
- `google_analytics_4_ingestion`
- `matomo_ingestion`
- `product_analytics_standardizer`
- `event_data_standardizer`

## Output Schema

Returns one row per funnel step with metrics:

| Column | Type | Description |
|--------|------|-------------|
| `step_number` | number | Step position in funnel (0-based) |
| `step_name` | string | Name of the funnel step |
| `users_entered` | number | Users who reached this step |
| `conversion_rate_overall` | number | % of total users who reached this step |
| `conversion_rate_from_previous` | number | % of previous step users who progressed |
| `drop_off_rate` | number | % who dropped off after this step |
| `avg_time_to_next_hours` | number | Average hours to next step (optional) |

**Example Output:**

```
step  step_name     users  overall%  from_prev%  drop_off%  avg_time_hrs
0     Landing       10000  100.0     -           40.0       2.5
1     Product View   6000   60.0     60.0        33.3       1.2
2     Add to Cart    4000   40.0     66.7        50.0       0.5
3     Checkout       2000   20.0     50.0        20.0       0.3
4     Purchase       1600   16.0     80.0        -          -
```

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for the output asset (e.g., `checkout_funnel`)
- **`funnel_steps`** (string): Comma-separated ordered steps (e.g., "Landing,Cart,Checkout,Purchase")

### Optional Parameters

- **`source_asset`** (string): Upstream asset name (auto-set via lineage)
- **`conversion_window_hours`** (number): Max hours between steps (default: 24)
- **`require_sequential`** (boolean): Steps must occur in order (default: true)
- **`allow_skips`** (boolean): Users can skip intermediate steps (default: false)
- **`user_id_field`** (string): Custom column name (auto-detected)
- **`event_name_field`** (string): Custom column name (auto-detected)
- **`timestamp_field`** (string): Custom column name (auto-detected)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `product_analytics`)
- **`include_sample_metadata`** (boolean): Include data preview (default: true)

## Example Configuration

### E-commerce Checkout Funnel

```yaml
type: dagster_component_templates.FunnelAnalysisComponent
attributes:
  asset_name: checkout_funnel
  funnel_steps: "Product View,Add to Cart,Checkout,Payment,Purchase"
  conversion_window_hours: 24
  require_sequential: true
  description: E-commerce checkout conversion funnel
  group_name: product_analytics
```

### SaaS Signup Funnel (Flexible)

```yaml
type: dagster_component_templates.FunnelAnalysisComponent
attributes:
  asset_name: signup_funnel
  funnel_steps: "Landing,Signup,Email Verify,First Login"
  conversion_window_hours: 168  # 7 days
  require_sequential: false  # Can skip email verification
  allow_skips: true
  description: User onboarding funnel
```

### Content Engagement Funnel

```yaml
type: dagster_component_templates.FunnelAnalysisComponent
attributes:
  asset_name: content_engagement_funnel
  funnel_steps: "Article View,Scroll 50%,Scroll 100%,Share,Newsletter Signup"
  conversion_window_hours: 1  # Same session
  require_sequential: true
  description: Content engagement funnel
```

## How It Works

### 1. User Journey Tracking

For each user, the component:
1. Collects all events within the conversion window
2. Sorts events chronologically
3. Identifies which funnel steps the user completed
4. Records progression through the funnel

### 2. Sequential vs. Non-Sequential

**Sequential (require_sequential=true)**
- User must complete steps in exact order
- Example: Can't get to Checkout without Add to Cart

**Non-Sequential (require_sequential=false)**
- User can complete steps in any order
- Example: Can sign up via different paths

### 3. Skip Handling

**No Skips (allow_skips=false)**
- User must complete each step
- Drop-off calculated precisely

**Allow Skips (allow_skips=true)**
- User can skip intermediate steps
- Useful for multi-path funnels

### 4. Conversion Window

Users must complete all steps within the specified time window:
- **E-commerce**: 24-48 hours (typical shopping session)
- **SaaS Signup**: 7-30 days (account activation)
- **Content**: 1 hour (single session)

## Reading the Results

### Key Metrics

**Overall Conversion Rate**
- % of initial users who reach each step
- Identifies total funnel effectiveness

**Step Conversion Rate**
- % who progress from previous step
- Identifies specific problem steps

**Drop-off Rate**
- % who leave after a step
- High drop-off = friction point

**Time Between Steps**
- How long users take to progress
- Long times may indicate confusion or hesitation

### Identifying Problems

**High Drop-off Rate (>50%)**
- Indicates major friction
- Prioritize optimization here
- Common causes: complexity, unclear CTA, technical issues

**Long Time to Next Step**
- Users are hesitating
- May need more information or clarity
- Consider adding help text, examples, or social proof

**Low Overall Conversion**
- Check first step: are right users entering?
- Check last step: is the value proposition clear?

## Optimization Strategies

### Step 1: Identify the Bottleneck
Look for the step with highest drop-off rate

### Step 2: Hypothesize Why
- Too complex? → Simplify
- Unclear? → Add guidance
- Slow? → Improve performance
- Irrelevant? → Remove or make optional

### Step 3: Test Solutions
- A/B test changes
- Re-run funnel analysis
- Compare before/after metrics

### Step 4: Iterate
- Focus on one step at a time
- Compound improvements across steps

## Common Funnel Types

### Acquisition Funnel
```
Ad Click → Landing → Signup → Email Verify → First Use
```

### E-commerce Funnel
```
Homepage → Category → Product → Cart → Checkout → Purchase
```

### Feature Adoption Funnel
```
Feature Prompt → Info Modal → Enable → First Use → Second Use
```

### Content Funnel
```
Homepage → Article → Scroll 50% → Newsletter CTA → Subscribe
```

## Best Practices

### Funnel Design

1. **Keep it Simple**: 3-7 steps optimal
2. **Define Clearly**: Each step should be unambiguous
3. **Match User Intent**: Align with actual user goals
4. **Set Appropriate Windows**: Match your business cycle

### Analysis Tips

1. **Segment Funnels**: Compare by traffic source, user type, device
2. **Track Over Time**: Monitor trends, not just point-in-time
3. **A/B Test**: Compare variant performance
4. **Combine with Qualitative**: Use session recordings, surveys

### Common Pitfalls

❌ Too many steps (hard to analyze)
❌ Window too short (false negatives)
❌ Window too long (false positives)
❌ Wrong step order
❌ Steps that aren't causally linked

## Performance Considerations

- **Data Volume**: Handles millions of events
- **Memory**: Uses pandas groupby for efficiency
- **Speed**: Typical analysis completes in seconds
- **Scalability**: For billions of events, consider pre-aggregation

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **Data Quality**: Ensure event names are consistent and standardized
- **User Identity**: Use stable IDs (not session IDs for cross-session funnels)
- **Conversion Windows**: Test different windows to find optimal
- **Step Order**: Define logical, chronological order
- **Multiple Attempts**: Component tracks first completion of each step per user
- **Partial Funnels**: Users who don't complete are still counted at their furthest step
