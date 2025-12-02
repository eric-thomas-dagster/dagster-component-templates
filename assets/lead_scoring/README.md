# Lead Scoring Component

Intelligently score and qualify leads based on firmographic fit and behavioral intent to prioritize sales efforts and optimize the marketing-to-sales handoff.

## Overview

The Lead Scoring component automates lead qualification by combining two critical dimensions: **fit** (who they are) and **intent** (what they're doing). This dual-axis scoring ensures sales teams focus on leads that are both a good match for your product and actively showing buying signals.

## Key Features

- **Dual-Axis Scoring**: Combines firmographic fit and behavioral intent
- **Flexible Models**: Fit-only, intent-only, or combined scoring
- **Automatic Qualification**: MQL and SQL flags based on thresholds
- **Temperature Classification**: Hot, warm, and cold lead categorization
- **Letter Grades**: A-F grading system for quick prioritization
- **Time Decay**: Recent activity weighted more heavily than old signals
- **Configurable Weights**: Adjust scoring factors for your business
- **Score Breakdown**: See exactly what drives each lead's score

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| lead_id | string | Unique lead identifier |
| lead_score | float | Overall lead score (0-100) |
| fit_score | float | Firmographic fit score (0-100) |
| intent_score | float | Behavioral intent score (0-100) |
| lead_temperature | string | cold, warm, or hot |
| lead_grade | string | Letter grade (A, B, C, D, F) |
| is_mql | boolean | Marketing Qualified Lead flag |
| is_sql | boolean | Sales Qualified Lead flag |
| scored_at | timestamp | When the score was calculated |

## Scoring Models

### Combined Model (Default)

Weights both fit and intent to identify leads that match your ICP and are showing buying signals.

```
Lead Score = (Fit Score × 40%) + (Intent Score × 60%)
```

**Best for**: Most B2B SaaS companies

**Example**:
- Enterprise company (fit: 85) + high web activity (intent: 90) → Score: 88 (Hot SQL)
- SMB company (fit: 45) + minimal activity (intent: 20) → Score: 30 (Cold lead)

### Fit-Only Model

Scores based purely on demographic/firmographic data.

```
Lead Score = Fit Score
```

**Best for**:
- Early-stage marketing before sufficient behavioral data
- Account-based marketing (ABM) targeting
- Event/conference lead qualification

**Example**: Fortune 500 CTO → High score regardless of activity

### Intent-Only Model

Scores based purely on behavioral signals.

```
Lead Score = Intent Score
```

**Best for**:
- Product-led growth (PLG) where product fit matters less
- Freemium models with trial users
- Bottom-up sales motions

**Example**: User who requests demo → High score regardless of company size

## Fit Score (0-100)

Measures how well the lead matches your Ideal Customer Profile (ICP).

### Components

#### Company Size (25% default weight)

Scores based on employee count matching your ideal range.

**Scoring**:
- 50-500 employees: 100 (mid-market sweet spot)
- 20-50 or 500-1000: 75
- 1000+ employees: 80 (enterprise)
- 10-20 employees: 50
- <10 employees: 25

**Configure**: Adjust `company_size_weight`

#### Industry (25% default weight)

Matches lead's industry against target industries.

**Example Targets**:
- Technology, SaaS, Software
- Financial Services
- Healthcare
- E-commerce

**Scoring**:
- Target industry: 100
- Non-target industry: 30

**Configure**: Adjust `industry_weight` and target list in code

#### Job Title (25% default weight)

Evaluates decision-making authority.

**Scoring**:
- C-Level (CEO, CTO, CFO): 100
- VP/Director: 85
- Manager/Lead: 60
- Individual Contributor: 30

**Configure**: Adjust `job_title_weight`

#### Geography (15% default weight)

Matches lead location to target regions.

**Scoring**:
- Target countries (US, UK, Canada): 100
- Other countries: 50

**Configure**: Adjust `geography_weight` and target list

#### Budget Indicators (10% default weight)

Infers budget from company revenue.

**Scoring**:
- $10M+ annual revenue: 100
- $1-10M: 80
- $100K-1M: 60
- <$100K: 30

**Configure**: Adjust `budget_weight`

### Example Fit Calculation

**Lead Profile**:
- Company: 200 employees (100 × 0.25 = 25)
- Industry: SaaS (100 × 0.25 = 25)
- Title: VP Engineering (85 × 0.25 = 21.25)
- Geography: United States (100 × 0.15 = 15)
- Revenue: $5M (80 × 0.10 = 8)

**Fit Score**: 94.25 (Excellent fit!)

## Intent Score (0-100)

Measures buying interest through behavioral signals.

### Components

#### Email Engagement (25% default weight)

Tracks email opens and clicks.

**Scoring**:
- 5+ clicks: 100
- 10+ opens, 2-4 clicks: 75-90
- 3-9 opens, 1 click: 50-75
- 1-2 opens, no clicks: 25-50

**Configure**: Adjust `email_engagement_weight`

#### Website Activity (30% default weight)

Measures website visits and page views.

**Scoring**:
- 10+ sessions, 20+ page views: 100
- 5-9 sessions, 10-19 page views: 70-90
- 2-4 sessions, 5-9 page views: 40-70
- 1 session, <5 page views: 20-40

**Configure**: Adjust `website_activity_weight`

#### Content Consumption (20% default weight)

Tracks downloads of gated content (whitepapers, ebooks, case studies).

**Scoring**:
- 3+ downloads: 100
- 2 downloads: 75
- 1 download: 50
- 0 downloads: 0

**Configure**: Adjust `content_consumption_weight`

#### Product Interest (25% default weight)

Tracks high-intent pages (pricing, product, demo).

**Scoring**:
- Demo request: 100 (very high intent!)
- 3+ pricing page views: 90
- 1-2 pricing page views: 60
- Product page views only: 40

**Configure**: Adjust `product_interest_weight`

### Time Decay

Recent activity is weighted more heavily than old activity.

**Formula**: `Signal Strength × 0.5^(days_since_activity / decay_period)`

**Example** (30-day decay period):
- Activity today: 100% strength
- Activity 30 days ago: 50% strength
- Activity 60 days ago: 25% strength
- Activity 90 days ago: 12.5% strength

**Configure**: Set `time_decay_days` (default: 30)

### Example Intent Calculation

**Lead Activity** (past 30 days):
- Email: 8 opens, 3 clicks (90 × 0.25 = 22.5)
- Website: 6 sessions, 15 page views (80 × 0.30 = 24)
- Content: 2 whitepapers downloaded (75 × 0.20 = 15)
- Product: 1 demo request (100 × 0.25 = 25)

**Intent Score**: 86.5 (Very high intent!)

## Configuration

### Basic Configuration

```yaml
asset_name: lead_scores
scoring_model: combined
mql_threshold: 50
sql_threshold: 70
```

### Input Sources (Connected via Visual Lineage)

The component accepts 1-3 input data sources:

1. **Lead Data** (CRM contacts/leads)
   - Fields: lead_id, company_size, industry, job_title, country, annual_revenue

2. **Behavioral Data** (marketing automation, web analytics)
   - Fields: lead_id, email_opens, email_clicks, page_views, session_count, demo_requests, last_activity_date

3. **Company Data** (enrichment data - optional)
   - Fields: company_id, employee_count, annual_revenue, industry, funding_stage

Connect by drawing edges in Dagster Designer UI from data sources → `lead_scores`.

### Advanced Configuration - B2B SaaS

```yaml
asset_name: lead_scores
scoring_model: combined
fit_weight: 0.4
intent_weight: 0.6

# Fit scoring weights
company_size_weight: 0.30  # Size matters for B2B
industry_weight: 0.25
job_title_weight: 0.25
geography_weight: 0.15
budget_weight: 0.05

# Intent scoring weights
email_engagement_weight: 0.20
website_activity_weight: 0.30
content_consumption_weight: 0.25
product_interest_weight: 0.25

# Qualification thresholds
mql_threshold: 50
sql_threshold: 70
hot_lead_threshold: 75
warm_lead_threshold: 50

# Time decay
apply_time_decay: true
time_decay_days: 30

# Output
include_score_breakdown: true
calculate_lead_grade: true
```

### Advanced Configuration - Product-Led Growth

```yaml
scoring_model: intent_only  # Behavior matters most
intent_weight: 1.0

# Intent heavily weighted toward product usage
email_engagement_weight: 0.10
website_activity_weight: 0.20
content_consumption_weight: 0.10
product_interest_weight: 0.60  # Product trial/demo is key

# Lower thresholds for self-serve
mql_threshold: 40
sql_threshold: 60

# Shorter time decay (fast-moving)
time_decay_days: 14
```

### Advanced Configuration - Enterprise ABM

```yaml
scoring_model: fit_only  # Target account list matters most
fit_weight: 1.0

# Company attributes most important
company_size_weight: 0.40  # Must be enterprise
industry_weight: 0.30
job_title_weight: 0.20
budget_weight: 0.10

# High thresholds (quality over quantity)
mql_threshold: 70
sql_threshold: 85
```

## Use Cases

### 1. Sales Prioritization

Route hot leads to sales immediately:

```python
df = context.load_asset_value("lead_scores")

# Get SQLs sorted by score
sqls = df[df['is_sql'] == True].sort_values('lead_score', ascending=False)

# Prioritize hot leads
hot_sqls = sqls[sqls['lead_temperature'] == 'hot']

print(f"Hot SQLs ready for sales: {len(hot_sqls)}")
print("\nTop 10 leads:")
print(hot_sqls[['lead_id', 'lead_score', 'lead_grade']].head(10))
```

**Action**: Send hot SQLs to sales queue for immediate outreach

### 2. Marketing Nurture Segmentation

Build targeted nurture campaigns by score and weakness:

```python
df = context.load_asset_value("lead_scores")

# Segment leads
high_fit_low_intent = df[(df['fit_score'] > 70) & (df['intent_score'] < 40)]
low_fit_high_intent = df[(df['fit_score'] < 40) & (df['intent_score'] > 70)]
warm_leads = df[df['lead_temperature'] == 'warm']

print(f"High fit, low intent: {len(high_fit_low_intent)} - Needs activation")
print(f"Low fit, high intent: {len(low_fit_high_intent)} - Needs education on fit")
print(f"Warm leads: {len(warm_leads)} - Needs nurturing")
```

**Actions**:
- High fit, low intent → Send product education emails
- Low fit, high intent → May not convert, lower priority
- Warm leads → Regular nurture sequence

### 3. MQL to SQL Conversion Optimization

Identify what moves MQLs to SQLs:

```python
df = context.load_asset_value("lead_scores")

mqls = df[df['is_mql'] == True]
sqls = df[df['is_sql'] == True]

print(f"MQL → SQL conversion rate: {len(sqls) / len(mqls) * 100:.1f}%")

# What differentiates SQLs from MQLs?
mql_only = mqls[~mqls['is_sql']]

print(f"\nAverage scores:")
print(f"  MQL-only fit: {mql_only['fit_score'].mean():.1f}")
print(f"  MQL-only intent: {mql_only['intent_score'].mean():.1f}")
print(f"  SQL fit: {sqls['fit_score'].mean():.1f}")
print(f"  SQL intent: {sqls['intent_score'].mean():.1f}")

# Example output:
# MQL-only intent: 42 → Need more engagement to reach SQL
# SQL intent: 78 → Clear gap, need demo requests or pricing views
```

**Action**: Build campaigns to move MQLs toward SQL behaviors

### 4. Lead Score Monitoring Dashboard

Track scoring distribution and health:

```python
import matplotlib.pyplot as plt

df = context.load_asset_value("lead_scores")

# Score distribution
plt.figure(figsize=(12, 5))

plt.subplot(1, 2, 1)
plt.hist(df['lead_score'], bins=20, edgecolor='black')
plt.axvline(50, color='orange', linestyle='--', label='MQL Threshold')
plt.axvline(70, color='green', linestyle='--', label='SQL Threshold')
plt.xlabel('Lead Score')
plt.ylabel('Count')
plt.title('Lead Score Distribution')
plt.legend()

plt.subplot(1, 2, 2)
grade_counts = df['lead_grade'].value_counts().sort_index(ascending=False)
grade_counts.plot(kind='bar')
plt.xlabel('Lead Grade')
plt.ylabel('Count')
plt.title('Lead Grade Distribution')

plt.tight_layout()
plt.show()

# Summary metrics
print(f"\nLead Database Health:")
print(f"  Total leads: {len(df)}")
print(f"  Average score: {df['lead_score'].mean():.1f}")
print(f"  MQL rate: {df['is_mql'].mean() * 100:.1f}%")
print(f"  SQL rate: {df['is_sql'].mean() * 100:.1f}%")
print(f"  A+B grades: {((df['lead_grade'] == 'A') | (df['lead_grade'] == 'B')).mean() * 100:.1f}%")
```

### 5. Lead Routing Rules

Automatically route leads based on score:

```python
df = context.load_asset_value("lead_scores")

# Route by score and characteristics
for _, lead in df.iterrows():
    if lead['is_sql'] and lead['lead_temperature'] == 'hot':
        # Enterprise AE for hot SQLs
        route_to = "enterprise_ae_queue"
    elif lead['is_sql'] and lead['fit_score'] > 80:
        # High-fit SQLs to AE
        route_to = "ae_queue"
    elif lead['is_mql'] and lead['intent_score'] > 60:
        # High-intent MQLs to SDR
        route_to = "sdr_queue"
    elif lead['is_mql']:
        # Regular MQLs to marketing nurture
        route_to = "marketing_nurture"
    elif lead['lead_score'] > 25:
        # Warm prospects to drip campaign
        route_to = "drip_campaign"
    else:
        # Cold leads to database
        route_to = "database_only"

    # Update CRM with routing (pseudocode)
    # update_crm_lead(lead['lead_id'], {'queue': route_to})
```

## Qualification Definitions

### MQL (Marketing Qualified Lead)

**Definition**: Lead that meets basic qualification criteria and shows initial interest.

**Default Threshold**: 50

**Characteristics**:
- Matches ICP reasonably well (fit > 40)
- Shows some engagement (intent > 30)
- Ready for SDR outreach

**Typical Actions**:
- SDR qualification call
- Personalized email sequence
- Product education content

### SQL (Sales Qualified Lead)

**Definition**: Lead that's been vetted and is ready for sales conversation.

**Default Threshold**: 70

**Characteristics**:
- Good ICP fit (fit > 60)
- Strong buying signals (intent > 60)
- Has budget, authority, need, timing (BANT)

**Typical Actions**:
- Assign to AE
- Schedule discovery call
- Provide pricing/demo

## Temperature Classification

### Hot Lead (75-100)

**Definition**: High score, strong fit and intent, ready to buy now.

**Characteristics**:
- Excellent ICP match
- High engagement (demo request, pricing views)
- Recent activity (<7 days)

**SLA**: Contact within 24 hours
**Owner**: AE
**Priority**: Critical

### Warm Lead (50-75)

**Definition**: Moderate score, needs nurturing or qualification.

**Characteristics**:
- Decent ICP match
- Moderate engagement
- May need education

**SLA**: Contact within 3-5 days
**Owner**: SDR or marketing
**Priority**: Medium

### Cold Lead (0-50)

**Definition**: Low score, poor fit or no intent.

**Characteristics**:
- Weak ICP match or no engagement
- May be unresponsive
- Not ready to buy

**SLA**: Automated nurture only
**Owner**: Marketing automation
**Priority**: Low

## Best Practices

1. **Start with Defaults**: Use default weights until you have conversion data
2. **Calibrate to Your Data**: Analyze MQL→SQL→Customer conversion to tune thresholds
3. **Monitor Score Distribution**: Aim for normal distribution, not all high/low scores
4. **Track Predictiveness**: Does high score actually predict closed-won?
5. **Close the Loop**: Feed sales feedback back to scoring model
6. **Segment by Persona**: Different scoring for different buyer types
7. **Update Regularly**: Recalculate scores daily or weekly
8. **Act on Scores**: Scoring without action is useless

## Troubleshooting

### Everyone Has High/Low Scores

**Problem**: Scores clustered at one end of scale

**Solutions**:
- Check data quality (missing values, incorrect ranges)
- Adjust weights for more differentiation
- Verify threshold calibration
- Ensure both fit and intent data present

### MQL→SQL Conversion Too Low

**Problem**: Too many MQLs not converting to SQL

**Solutions**:
- Raise MQL threshold (try 60 instead of 50)
- Adjust intent weights toward high-value signals
- Implement lead nurture for low-intent MQLs
- Review SDR qualification criteria

### Sales Complaining About Lead Quality

**Problem**: High-scoring leads not converting

**Solutions**:
- Increase fit_weight in combined model
- Raise SQL threshold (try 80 instead of 70)
- Review fit scoring criteria (are targets right?)
- Close the loop: track which scores convert

### No Time Decay Effect

**Problem**: Old activity weighted same as new

**Solutions**:
- Enable apply_time_decay: true
- Decrease time_decay_days for faster decay
- Verify last_activity_date field is present
- Check date format and timezone

### Scores Not Updating

**Problem**: Lead scores stay static

**Solutions**:
- Verify materialization schedule
- Check that input data is updating
- Confirm behavioral data has recent timestamps
- Look for caching issues in pipeline

## Example Pipeline

```
┌─────────────┐      ┌─────────────┐
│     CRM     │      │  Marketing  │
│  Lead Data  │      │ Automation  │
└──────┬──────┘      └──────┬──────┘
       │                    │
       │                    │
       │             ┌──────▼──────┐
       │             │ Behavioral  │
       │             │    Data     │
       │             └──────┬──────┘
       │                    │
       └────────┬───────────┘
                │
         ┌──────▼──────┐
         │    Lead     │
         │   Scoring   │
         └──────┬──────┘
                │
                ├─────────────────┬─────────────────┐
                │                 │                 │
         ┌──────▼──────┐   ┌──────▼──────┐  ┌──────▼──────┐
         │     Hot     │   │    Warm     │  │    Cold     │
         │   SQLs to   │   │   MQLs to   │  │   Leads to  │
         │    Sales    │   │     SDR     │  │   Nurture   │
         └─────────────┘   └─────────────┘  └─────────────┘
```

## Related Components

- **CRM Ingestion**: Source lead data (Salesforce, HubSpot)
- **Marketing Automation Ingestion**: Source behavioral data (Marketo, Pardot)
- **Website Analytics**: Source web activity (Google Analytics, Segment)
- **Ad Spend Standardizer**: Track marketing spend per lead source
- **Revenue Attribution**: Calculate LTV and CAC per lead source

## Learn More

- [The Definitive Guide to Lead Scoring](https://www.marketo.com/definitive-guides/lead-scoring/)
- [MQL vs SQL Definitions](https://blog.hubspot.com/marketing/mql-vs-sql)
- [Lead Scoring Best Practices](https://www.salesforce.com/products/guide/lead-gen/scoring-models/)
- [Predictive Lead Scoring](https://www.6sense.com/platform/predictive-analytics/lead-scoring)
