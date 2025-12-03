# Anomaly Detection Component

Detect anomalies and outliers in customer behavior, transactions, and business metrics using statistical methods.

## Overview

Identify unusual patterns using proven statistical techniques:
- **Z-Score**: Detects values far from mean (standard deviations)
- **IQR (Interquartile Range)**: Robust outlier detection using quartiles
- **Moving Average**: Time-series deviation detection
- **Threshold**: Simple threshold-based alerts

## Use Cases

- **Fraud Detection**: Unusual transaction amounts or patterns
- **Quality Monitoring**: Detect data quality issues
- **System Monitoring**: Performance anomalies and errors
- **Business Metrics**: Revenue, conversion rate anomalies
- **Customer Behavior**: Unusual usage patterns
- **Security**: Detect suspicious activity

## Input Requirements

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `metric_column` | number | ✓ | (specified in config) | Metric to analyze |
| `id` | string | | record_id, transaction_id | Record identifier |
| `timestamp` | datetime | | date, created_at | Timestamp (for MA method) |
| `group_by_field` | string | | customer_id, category | Grouping field (optional) |

**Compatible Upstream Components:**
- Any component with numeric metrics to monitor
- `ecommerce_standardizer` (transaction amounts)
- `event_data_standardizer` (event metrics)
- System monitoring data

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `[all input columns]` | various | Original data preserved |
| `is_anomaly` | boolean | Anomaly flag |
| `anomaly_score` | number | Severity score |
| `anomaly_reason` | string | Why flagged as anomaly |

## Configuration

### Z-Score Method (Recommended)

```yaml
type: dagster_component_templates.AnomalyDetectionComponent
attributes:
  asset_name: transaction_anomalies
  detection_method: z_score
  metric_column: amount
  threshold: 3.0  # Standard deviations
  description: Transaction anomaly detection
```

### IQR Method (Robust to Outliers)

```yaml
type: dagster_component_templates.AnomalyDetectionComponent
attributes:
  asset_name: metric_anomalies
  detection_method: iqr
  metric_column: revenue
  threshold: 1.5  # IQR multiplier
```

### Moving Average (Time Series)

```yaml
type: dagster_component_templates.AnomalyDetectionComponent
attributes:
  asset_name: daily_metric_anomalies
  detection_method: moving_average
  metric_column: daily_active_users
  moving_average_window: 7
  threshold: 2.0
  timestamp_field: date
```

## Detection Methods Explained

### Z-Score Method
Measures how many standard deviations a value is from the mean.

**When to use:**
- Normally distributed data
- General purpose anomaly detection
- Known expected range

**Threshold guidance:**
- 2.0 = ~95% of data (5% flagged)
- 3.0 = ~99.7% of data (0.3% flagged, recommended)
- 4.0 = ~99.99% of data (very strict)

### IQR Method
Uses interquartile range, resistant to extreme outliers.

**When to use:**
- Skewed distributions
- Data with existing outliers
- Robust detection needed

**Threshold guidance:**
- 1.5 = Standard (typical outlier definition)
- 2.0 = Moderate (fewer false positives)
- 3.0 = Conservative

### Moving Average
Compares current value to recent trend.

**When to use:**
- Time-series data
- Trending metrics
- Detect sudden changes

**Threshold guidance:**
- 2.0 = Moderate sensitivity
- 3.0 = Standard
- 4.0 = Low sensitivity (major changes only)

### Threshold Method
Simple comparison to fixed value.

**When to use:**
- Known acceptable limits
- Business rules (e.g., max transaction $10,000)
- Simple alerts

## Best Practices

**Choose the Right Method:**
- **Fraud/Security**: Z-score or IQR
- **Time Series**: Moving average
- **Business Rules**: Threshold
- **Unknown distribution**: IQR (most robust)

**Set Appropriate Thresholds:**
- Start conservative (3.0 for Z-score, 1.5 for IQR)
- Monitor false positive rate
- Adjust based on domain knowledge

**Handle False Positives:**
- Review flagged anomalies manually
- Refine thresholds over time
- Consider business context

**Group-Level Detection:**
- Use `group_by_field` for per-customer/per-category analysis
- Detects anomalies relative to normal behavior per group

## Example Use Cases

### Fraud Detection
```yaml
# Flag unusually high transaction amounts per customer
detection_method: z_score
metric_column: transaction_amount
group_by_field: customer_id
threshold: 3.5
```

### Revenue Monitoring
```yaml
# Alert on daily revenue anomalies
detection_method: moving_average
metric_column: daily_revenue
moving_average_window: 7
threshold: 2.0
timestamp_field: date
```

### Data Quality
```yaml
# Detect invalid data (e.g., negative amounts)
detection_method: threshold
metric_column: amount
threshold: 0  # Flag negative values
```

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`
