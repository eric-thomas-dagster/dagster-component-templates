# Priority Scorer Component

Score ticket priority/urgency using LLM-based, ML-based (logistic regression, XGBoost), or rule-based methods with SLA breach prediction and escalation triggers.

## Overview

The Priority Scorer Component calculates priority scores for support tickets using multiple scoring methods. It considers ticket content, customer tier, sentiment, urgency, and SLA requirements to assign numeric scores (0-100) or categorical priorities (P1/P2/P3/P4).

## Features

- **Three Scoring Methods**: LLM (most nuanced), ML (trained models), or rules (fast and free)
- **Flexible Output**: Numeric scores (0-100) or priority levels (P1-P4)
- **SLA Breach Prediction**: Predict if tickets will miss SLA deadlines
- **Multi-Factor Scoring**: Combines text, customer tier, sentiment, urgency
- **Customer Tier Weighting**: Premium customers get higher priority
- **Escalation Triggers**: Automatic flagging of high-priority tickets
- **Explainability**: Shows factors contributing to each score
- **Cost Efficient**: Rules and ML methods are free

## Use Cases

1. **Ticket Prioritization**: Automatically prioritize support queue
2. **SLA Management**: Predict and prevent SLA breaches
3. **Resource Allocation**: Route high-priority tickets to senior staff
4. **Escalation Management**: Flag tickets requiring immediate attention
5. **Customer Success**: Give premium customers faster response
6. **Analytics**: Track priority trends and team capacity
7. **Response Time Optimization**: Improve first response times

## Prerequisites

### Rules Method (Recommended, Free)
- **Python Packages**: `pandas>=1.5.0`, `numpy>=1.20.0`
- **Cost**: $0 (keyword-based rules)
- **Speed**: Fastest (1-5ms per ticket)

### ML Method
- **Python Packages**: `scikit-learn>=1.0.0`, `xgboost>=1.7.0`, `pandas>=1.5.0`
- **Cost**: $0 (local models)
- **Speed**: Fast (5-20ms per ticket)

### LLM Method
- **Python Packages**: `openai>=1.0.0` or `anthropic>=0.18.0`, `pandas>=1.5.0`
- **API Key**: OpenAI or Anthropic API key
- **Cost**: $0.15-$5 per 1M input tokens
- **Speed**: Slower (200-500ms per ticket)

## Configuration

### Rules Method (Recommended for Production)

Fast, free, and effective:

```yaml
type: dagster_component_templates.PriorityScorerComponent
attributes:
  asset_name: prioritized_tickets

  method: rules

  input_columns: "ticket_text,ticket_category"
  customer_tier_column: customer_tier
  sentiment_score_column: sentiment_score
  urgency_column: ticket_urgency

  priority_levels: "P1,P2,P3,P4"

  sla_hours: '{"enterprise": 4, "pro": 24, "basic": 72}'
  tier_weights: '{"enterprise": 1.5, "pro": 1.2, "basic": 1.0}'
  urgency_weights: '{"critical": 2.0, "high": 1.5, "medium": 1.0, "low": 0.5}'

  escalation_threshold: 80
  predict_sla_breach: true
  include_explanation: true
```

### LLM Method (Most Nuanced)

For complex scoring logic:

```yaml
type: dagster_component_templates.PriorityScorerComponent
attributes:
  asset_name: llm_prioritized

  method: llm
  llm_provider: openai
  llm_model: gpt-4o-mini
  api_key: "${OPENAI_API_KEY}"

  input_columns: "ticket_text,ticket_category,customer_feedback"
  customer_tier_column: customer_tier
  sentiment_score_column: sentiment_score
  urgency_column: ticket_urgency

  priority_levels: "P1,P2,P3,P4"

  sla_hours: '{"enterprise": 4, "pro": 24, "basic": 72}'

  escalation_threshold: 85
  predict_sla_breach: true
  include_explanation: true

  temperature: 0.0
  max_tokens: 300
```

### ML Method

Using trained models:

```yaml
type: dagster_component_templates.PriorityScorerComponent
attributes:
  asset_name: ml_prioritized

  method: ml
  ml_model: xgboost

  input_columns: "ticket_text"
  customer_tier_column: customer_tier
  sentiment_score_column: sentiment_score
  urgency_column: ticket_urgency

  priority_levels: "P1,P2,P3,P4"

  tier_weights: '{"enterprise": 1.5, "pro": 1.2, "basic": 1.0}'
  urgency_weights: '{"critical": 2.0, "high": 1.5, "medium": 1.0, "low": 0.5}'

  predict_sla_breach: true
  include_explanation: true
```

### Numeric Scores (0-100)

Instead of categorical levels:

```yaml
type: dagster_component_templates.PriorityScorerComponent
attributes:
  asset_name: numeric_scores

  method: rules

  input_columns: "ticket_text"
  customer_tier_column: customer_tier
  sentiment_score_column: sentiment_score

  priority_levels: "numeric"  # Returns 0-100 score

  escalation_threshold: 80
  include_explanation: true
```

## Output Schema

The component adds the following columns:

### Always Included:
- `priority_score`: Numeric score 0-100 (0=lowest, 100=highest)

### Optional (based on configuration):
- `priority_level`: Categorical level (P1/P2/P3/P4) if priority_levels set
- `sla_breach_likely`: Boolean prediction (if predict_sla_breach=true)
- `priority_explanation`: Text explanation of score (if include_explanation=true)
- `requires_escalation`: Boolean flag (if escalation_threshold set)

### Example Output:

```
| ticket_text                     | customer_tier | urgency  | priority_score | priority_level | sla_breach_likely | requires_escalation | priority_explanation                           |
|---------------------------------|---------------|----------|----------------|----------------|-------------------|---------------------|------------------------------------------------|
| Site completely down            | enterprise    | critical | 95.0           | P1             | true              | true                | Score: 95.0 | Factors: 2 urgent keywords, enterprise tier   |
| Need password reset             | basic         | low      | 35.0           | P4             | false             | false               | Score: 35.0 | Factors: basic tier, low urgency              |
| Billing question about invoice  | pro           | medium   | 60.0           | P2             | false             | false               | Score: 60.0 | Factors: pro tier, medium urgency             |
```

## Method Comparison

| Feature | Rules | ML | LLM |
|---------|-------|-----|-----|
| **Speed** | Fastest (1-5ms) | Fast (5-20ms) | Slower (200-500ms) |
| **Cost** | $0 | $0 | $0.15-$5 per 1M tokens |
| **Accuracy** | Good (80-85%) | High (85-92%) | Highest (90-95%) |
| **Setup** | Easy (config only) | Medium (needs training) | Easy (API key) |
| **Explainability** | High (clear rules) | Medium (feature importance) | High (natural language) |
| **Customization** | Medium (keyword/weight tuning) | High (model training) | Highest (prompt engineering) |
| **Best For** | Production, high volume | Trained models, high volume | Complex logic, nuanced scoring |

## Scoring Logic

### Rules Method

Calculates score based on:

1. **Urgent Keywords**: "urgent", "critical", "emergency", "down", "broken" (+15 points each)
2. **Priority Keywords**: "important", "priority", "issue", "problem" (+5 points each)
3. **Customer Tier**: Multiplier (enterprise: 1.5x, pro: 1.2x, basic: 1.0x)
4. **Sentiment**: Negative sentiment increases priority (up to +15 points)
5. **Urgency Level**: Multiplier (critical: 2.0x, high: 1.5x, medium: 1.0x, low: 0.5x)

### ML Method

Uses trained model with features:
- Text length and word count
- Customer tier weight
- Sentiment score
- Urgency weight
- Historical patterns (if available)

### LLM Method

Natural language analysis considering:
- Issue severity and business impact
- Customer tier and SLA requirements
- Time sensitivity and urgency
- Sentiment and customer satisfaction
- Context from multiple fields

## Cost Estimation

### Rules & ML Costs
- **Cost**: $0 (runs locally)
- **Infrastructure**: Minimal (100 MB RAM, CPU only)

### LLM Costs (per 1,000 tickets)

Assuming average 200 tokens input, 80 tokens output per ticket:

| Model | Input Cost | Output Cost | Total per 1k | Monthly (10k/day) |
|-------|------------|-------------|--------------|-------------------|
| GPT-4o-mini | $0.030 | $0.048 | $0.078 | $23.40 |
| GPT-4o | $1.00 | $1.20 | $2.20 | $660 |
| GPT-4 | $6.00 | $4.80 | $10.80 | $3,240 |
| Claude 3 Haiku | $0.050 | $0.100 | $0.150 | $45 |

**Recommendation**: Use rules method for most cases, LLM only for complex scoring

## Advanced Patterns

### Pipeline: Classify -> Score -> Route

```yaml
# Step 1: Classify tickets (use ticket_classifier)
# Step 2: Score priority
- type: dagster_component_templates.PriorityScorerComponent
  attributes:
    asset_name: scored_tickets
    method: rules
    input_columns: "ticket_text,ticket_category"
    customer_tier_column: customer_tier
    urgency_column: ticket_urgency
    priority_levels: "P1,P2,P3,P4"
    escalation_threshold: 80

# Step 3: Route to appropriate queues based on priority
```

### SLA Monitoring Dashboard

```yaml
type: dagster_component_templates.PriorityScorerComponent
attributes:
  asset_name: sla_monitored

  method: rules

  input_columns: "ticket_text"
  customer_tier_column: customer_tier
  urgency_column: urgency

  priority_levels: "P1,P2,P3,P4"

  sla_hours: |
    {
      "enterprise": 4,
      "pro": 24,
      "basic": 72
    }

  predict_sla_breach: true
  include_explanation: true
```

### Dynamic Tier Weighting

```yaml
type: dagster_component_templates.PriorityScorerComponent
attributes:
  asset_name: dynamic_priority

  method: rules

  input_columns: "ticket_text,ticket_category"
  customer_tier_column: customer_tier

  # Custom tier weights based on revenue
  tier_weights: |
    {
      "platinum": 2.0,
      "gold": 1.5,
      "silver": 1.2,
      "bronze": 1.0,
      "trial": 0.8
    }

  priority_levels: "P1,P2,P3,P4"
  escalation_threshold: 75
```

## Integration Examples

### With Ticket Classifier

```yaml
# Combined pipeline
components:
  # Step 1: Classify
  - type: dagster_component_templates.TicketClassifierComponent
    attributes:
      asset_name: classified
      # ... classifier config

  # Step 2: Score Priority
  - type: dagster_component_templates.PriorityScorerComponent
    attributes:
      asset_name: prioritized
      source_asset: classified
      method: rules
      input_columns: "ticket_text"
      customer_tier_column: customer_tier
      urgency_column: ticket_urgency
      priority_levels: "P1,P2,P3,P4"
```

### With Sentiment Analyzer

```yaml
# Step 1: Analyze sentiment
- type: dagster_component_templates.SentimentAnalyzerComponent
  attributes:
    asset_name: sentiment_analyzed
    # ... sentiment config

# Step 2: Score with sentiment
- type: dagster_component_templates.PriorityScorerComponent
  attributes:
    asset_name: priority_with_sentiment
    source_asset: sentiment_analyzed
    method: rules
    sentiment_score_column: sentiment_score
    escalation_threshold: 80
```

## Performance Tuning

### For High Volume (>100k tickets/day)

```yaml
attributes:
  method: rules  # Fastest
  input_columns: "ticket_text"  # Minimal fields
  include_explanation: false  # Reduce overhead
  priority_levels: "P1,P2,P3,P4"
```

### For High Accuracy

```yaml
attributes:
  method: llm
  llm_model: gpt-4-turbo
  input_columns: "ticket_text,description,customer_feedback"
  include_explanation: true
  temperature: 0.0
```

### For Cost Optimization

```yaml
attributes:
  method: rules  # Free
  input_columns: "ticket_text"
  customer_tier_column: customer_tier
  urgency_column: urgency
  # Tune weights based on your data
```

## Monitoring & Analytics

Metadata provided:

```python
{
  "method": "rules",
  "model": "rules",
  "num_scored": 1000,
  "avg_priority_score": 58.3,
  "high_priority_count": 150,
  "escalation_count": 45,
  "priority_level_distribution": {
    "P1": 45,
    "P2": 150,
    "P3": 500,
    "P4": 305
  },
  "predicted_sla_breaches": 67,
  "breach_rate": "6.7%"
}
```

## Troubleshooting

### Scores Too High/Low

**Problem**: Most tickets get very high or very low scores

**Solutions**:
1. Adjust tier_weights and urgency_weights
2. Review keyword lists for rules method
3. Tune escalation_threshold
4. Use LLM method for better calibration

### Poor SLA Predictions

**Problem**: SLA breach predictions are inaccurate

**Solutions**:
1. Ensure sla_hours match actual SLAs
2. Include more contextual columns in input_columns
3. Use LLM method for nuanced predictions
4. Tune urgency_weights based on historical data

### All Tickets Same Priority

**Problem**: Not enough differentiation in scores

**Solutions**:
1. Add more input columns (sentiment, entities, etc.)
2. Increase tier_weights spread
3. Adjust urgency_weights for more variance
4. Use ML or LLM method for better differentiation

## Best Practices

1. **Start with Rules**: Test rules method first, use LLM only if needed
2. **Calibrate Weights**: Tune tier_weights and urgency_weights for your SLAs
3. **Monitor Accuracy**: Track actual vs predicted priorities
4. **Include Context**: Use sentiment, classification, entities for better scoring
5. **Set Clear SLAs**: Define realistic sla_hours per tier
6. **Review Escalations**: Regularly audit escalation_threshold
7. **Explain Scores**: Enable include_explanation for transparency
8. **Track Breaches**: Monitor predicted vs actual SLA breaches
9. **Iterate**: Adjust weights based on team feedback
10. **Document Logic**: Keep clear documentation of scoring rules

## Related Components

- **ticket_classifier**: Classify before scoring
- **entity_extractor**: Extract entities for scoring factors
- **sentiment_analyzer**: Sentiment influences priority
- **text_moderator**: Flag urgent/toxic content

## Support & Resources

- [Dagster Documentation](https://docs.dagster.io)
- [Scikit-learn Documentation](https://scikit-learn.org)
- [XGBoost Documentation](https://xgboost.readthedocs.io)
- [OpenAI API Documentation](https://platform.openai.com/docs)

## License

This component is part of the Dagster Components Templates library.
