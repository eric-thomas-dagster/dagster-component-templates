# Ticket Classifier Component

Classify support tickets by category, urgency, department, and sentiment using LLM-based (GPT/Claude) or transformer-based (BERT, RoBERTa) methods.

## Overview

The Ticket Classifier Component automatically classifies support tickets across multiple dimensions: category, urgency level, routing department, and sentiment. It supports both API-based LLM methods and local transformer models.

## Features

- **Multi-Class Classification**: Category, urgency, department, and sentiment in one pass
- **Dual Methods**: LLM-based (flexible, nuanced) or transformer-based (fast, local)
- **Custom Categories**: Define your own ticket categories and departments
- **Confidence Scores**: Get confidence scores for each classification
- **Multi-Label Support**: Allow tickets to have multiple categories
- **Batch Processing**: Efficient processing of large ticket volumes
- **Multi-Language Support**: Works with tickets in multiple languages
- **Cost Tracking**: Monitor API costs for budget management
- **Integration Ready**: Works with Zendesk, Intercom, Freshdesk formats

## Use Cases

1. **Automated Ticket Routing**: Route tickets to the right department automatically
2. **Priority Queue Management**: Identify urgent tickets for immediate attention
3. **SLA Management**: Classify urgency for SLA compliance
4. **Support Analytics**: Analyze ticket categories and trends
5. **Multi-Channel Support**: Classify tickets from email, chat, phone, social media
6. **Escalation Triggers**: Identify high-priority tickets for escalation
7. **Resource Planning**: Forecast support team needs by category

## Prerequisites

### LLM Method
- **Python Packages**: `openai>=1.0.0` or `anthropic>=0.18.0`, `pandas>=1.5.0`
- **API Key**: OpenAI or Anthropic API key
- **Cost**: $0.15-$30 per 1M input tokens (varies by model)

### Transformer Method
- **Python Packages**: `transformers>=4.30.0`, `torch>=2.0.0`, `pandas>=1.5.0`
- **No API Key Required**
- **Cost**: $0 (runs locally)

## Configuration

### LLM Method (Recommended for Flexibility)

Using OpenAI GPT-4o-mini (cost-effective):

```yaml
type: dagster_component_templates.TicketClassifierComponent
attributes:
  asset_name: classified_tickets

  method: llm
  llm_provider: openai
  llm_model: gpt-4o-mini
  api_key: "${OPENAI_API_KEY}"

  input_column: ticket_text

  categories: "technical,billing,account,product,general"
  urgency_levels: "low,medium,high,critical"
  departments: "engineering,finance,customer_success,sales"

  classify_sentiment: true
  include_confidence: true

  temperature: 0.0
  batch_size: 50
  track_costs: true
```

Using Anthropic Claude (high quality):

```yaml
type: dagster_component_templates.TicketClassifierComponent
attributes:
  asset_name: classified_tickets_claude

  method: llm
  llm_provider: anthropic
  llm_model: claude-3-haiku-20240307
  api_key: "${ANTHROPIC_API_KEY}"

  input_column: ticket_description

  categories: "bug,feature_request,question,documentation,integration"
  urgency_levels: "low,medium,high,critical"

  include_confidence: true
  include_reasoning: true

  temperature: 0.0
```

### Transformer Method (Local, No API)

Using zero-shot classification:

```yaml
type: dagster_component_templates.TicketClassifierComponent
attributes:
  asset_name: classified_tickets_local

  method: transformer
  transformer_model: facebook/bart-large-mnli

  input_column: ticket_text

  categories: "technical,billing,account,product,general"
  urgency_levels: "low,medium,high,critical"
  departments: "engineering,finance,support"

  classify_sentiment: true
  include_confidence: true
  batch_size: 32
```

### Multi-Label Classification

Allow tickets to have multiple categories:

```yaml
type: dagster_component_templates.TicketClassifierComponent
attributes:
  asset_name: multi_label_tickets

  method: llm
  llm_provider: openai
  llm_model: gpt-4o-mini
  api_key: "${OPENAI_API_KEY}"

  input_column: ticket_text

  categories: "technical,billing,account,product,security,compliance"
  urgency_levels: "low,medium,high,critical"

  multi_label: true  # Enable multi-label classification
  include_confidence: true

  temperature: 0.0
```

Output example:
```
ticket_category: "technical,security"
category_confidence: 0.92
```

### With Additional Context

Include metadata fields in classification:

```yaml
type: dagster_component_templates.TicketClassifierComponent
attributes:
  asset_name: context_aware_classification

  method: llm
  llm_provider: openai
  llm_model: gpt-4o-mini
  api_key: "${OPENAI_API_KEY}"

  input_column: ticket_text
  include_metadata_fields: "customer_tier,channel,previous_tickets"

  categories: "technical,billing,account,product"
  urgency_levels: "low,medium,high,critical"
  departments: "tier1,tier2,tier3,specialist"

  include_confidence: true
  include_reasoning: true
```

### Confidence Threshold Filtering

Filter out low-confidence predictions:

```yaml
type: dagster_component_templates.TicketClassifierComponent
attributes:
  asset_name: high_confidence_tickets

  method: llm
  llm_provider: openai
  llm_model: gpt-4-turbo
  api_key: "${OPENAI_API_KEY}"

  input_column: ticket_text

  categories: "technical,billing,account,product"
  urgency_levels: "low,medium,high,critical"

  include_confidence: true
  confidence_threshold: 0.8  # Mark predictions below 0.8 as "uncertain"
```

## Output Schema

The component adds the following columns to your DataFrame:

### Always Included:
- `ticket_category`: Classified category (or comma-separated for multi-label)
- `ticket_urgency`: Urgency level

### Optional (based on configuration):
- `ticket_department`: Assigned department (if departments configured)
- `ticket_sentiment`: Sentiment (positive/neutral/negative) (if classify_sentiment=true)
- `category_confidence`: Confidence score 0.0-1.0 (if include_confidence=true)
- `urgency_confidence`: Confidence score 0.0-1.0 (if include_confidence=true)
- `department_confidence`: Confidence score 0.0-1.0 (if include_confidence=true)
- `sentiment_confidence`: Confidence score 0.0-1.0 (if include_confidence=true)
- `classification_reasoning`: Explanation of classification (if include_reasoning=true)

### Example Output:

```
| ticket_text                           | ticket_category | ticket_urgency | ticket_department    | ticket_sentiment | category_confidence | urgency_confidence |
|---------------------------------------|-----------------|----------------|----------------------|------------------|---------------------|--------------------|
| Can't login, getting error 500        | technical       | high           | engineering          | negative         | 0.95                | 0.89               |
| Need refund for last month            | billing         | medium         | finance              | neutral          | 0.92                | 0.85               |
| How do I export my data?              | product         | low            | customer_success     | neutral          | 0.88                | 0.90               |
| URGENT: Site is down for all users    | technical       | critical       | engineering          | negative         | 0.98                | 0.97               |
```

## Method Comparison

### LLM Method

**Pros:**
- High accuracy (90-95%)
- Flexible with custom categories
- Works with any language
- Handles nuanced cases
- Can provide reasoning
- No training required

**Cons:**
- API costs ($50-200/month for typical volume)
- Slower (100-500ms per ticket)
- Requires internet connection
- Rate limits apply

**Best For:**
- Complex categorization schemes
- Multi-language support
- When accuracy is critical
- Small to medium volumes (< 10k/day)

### Transformer Method

**Pros:**
- No API costs
- Fast (10-50ms per ticket)
- Works offline
- No rate limits
- Privacy (data stays local)

**Cons:**
- Lower accuracy (75-85%)
- Requires more setup
- Limited customization
- Model downloads required (GB)

**Best For:**
- High-volume processing
- Cost-sensitive applications
- Privacy requirements
- Offline environments

## Cost Estimation

### LLM Costs (per 1,000 tickets)

Assuming average 100 tokens input, 50 tokens output per ticket:

| Model | Input Cost | Output Cost | Total per 1k | Monthly (10k/day) |
|-------|------------|-------------|--------------|-------------------|
| GPT-4o-mini | $0.015 | $0.030 | $0.045 | $13.50 |
| GPT-4o | $0.50 | $0.75 | $1.25 | $375 |
| GPT-4 | $3.00 | $3.00 | $6.00 | $1,800 |
| Claude 3 Haiku | $0.025 | $0.063 | $0.088 | $26.40 |
| Claude 3.5 Sonnet | $0.30 | $0.75 | $1.05 | $315 |

**Recommendation**: GPT-4o-mini or Claude 3 Haiku for production (best cost/performance)

### Transformer Costs

- **One-time**: Model download (1-5 GB)
- **Ongoing**: $0 (runs locally)
- **Infrastructure**: Requires GPU for best performance (~$0.50/hr cloud GPU)

## Integration Examples

### Zendesk Format

```yaml
type: dagster_component_templates.TicketClassifierComponent
attributes:
  asset_name: zendesk_classified

  method: llm
  llm_provider: openai
  llm_model: gpt-4o-mini
  api_key: "${OPENAI_API_KEY}"

  input_column: description  # Zendesk field
  include_metadata_fields: "subject,priority,tags"

  categories: "technical,billing,account,feature_request,bug,question"
  urgency_levels: "low,normal,high,urgent"
  departments: "support_tier1,support_tier2,engineering,sales"

  classify_sentiment: true
  include_confidence: true
```

### Intercom Format

```yaml
type: dagster_component_templates.TicketClassifierComponent
attributes:
  asset_name: intercom_classified

  method: llm
  llm_provider: anthropic
  llm_model: claude-3-haiku-20240307
  api_key: "${ANTHROPIC_API_KEY}"

  input_column: body  # Intercom field
  include_metadata_fields: "conversation_rating,user_type"

  categories: "technical,billing,onboarding,feedback,bug"
  urgency_levels: "low,medium,high,critical"

  include_confidence: true
```

### Freshdesk Format

```yaml
type: dagster_component_templates.TicketClassifierComponent
attributes:
  asset_name: freshdesk_classified

  method: llm
  llm_provider: openai
  llm_model: gpt-4o-mini
  api_key: "${OPENAI_API_KEY}"

  input_column: description
  include_metadata_fields: "type,source,priority"

  categories: "incident,service_request,problem,change"
  urgency_levels: "low,medium,high,critical"
  departments: "IT,HR,facilities,finance"
```

## Advanced Patterns

### Pipeline: Classify -> Route -> Prioritize

```yaml
# Step 1: Classify tickets
- type: dagster_component_templates.TicketClassifierComponent
  attributes:
    asset_name: step1_classified
    method: llm
    llm_provider: openai
    llm_model: gpt-4o-mini
    api_key: "${OPENAI_API_KEY}"
    input_column: ticket_text
    categories: "technical,billing,account,product"
    urgency_levels: "low,medium,high,critical"
    departments: "engineering,finance,support"

# Step 2: Calculate priority scores (use priority_scorer component)
- type: dagster_component_templates.PriorityScorerComponent
  attributes:
    asset_name: step2_prioritized
    source_asset: step1_classified
    # ... priority scoring config
```

### Hybrid: Transformer Pre-filter + LLM Refinement

Use fast transformer for initial classification, then LLM for uncertain cases:

```yaml
# Step 1: Fast transformer classification
- type: dagster_component_templates.TicketClassifierComponent
  attributes:
    asset_name: transformer_classified
    method: transformer
    input_column: ticket_text
    categories: "technical,billing,account,product"
    include_confidence: true

# Step 2: Filter low-confidence tickets for LLM review
# (Use a filter component or custom logic)

# Step 3: LLM classification for uncertain tickets
- type: dagster_component_templates.TicketClassifierComponent
  attributes:
    asset_name: llm_refined
    method: llm
    llm_provider: openai
    llm_model: gpt-4o-mini
    api_key: "${OPENAI_API_KEY}"
    input_column: ticket_text
    categories: "technical,billing,account,product"
    include_reasoning: true
```

## Performance Tuning

### For High Volume (>10k tickets/day)

```yaml
attributes:
  method: transformer  # Use local models
  batch_size: 100  # Larger batches
  include_confidence: false  # Reduce overhead
  classify_sentiment: false  # Only if needed
```

### For High Accuracy

```yaml
attributes:
  method: llm
  llm_model: gpt-4-turbo  # Or claude-3-5-sonnet
  temperature: 0.0  # Deterministic
  include_reasoning: true  # For validation
  confidence_threshold: 0.9  # High confidence only
```

### For Cost Optimization

```yaml
attributes:
  method: llm
  llm_model: gpt-4o-mini  # Most cost-effective
  enable_caching: true  # Cache similar tickets
  batch_size: 100  # Process efficiently
  max_tokens: 150  # Limit response length
```

## Monitoring & Analytics

The component provides metadata for monitoring:

```python
{
  "method": "llm",
  "model": "gpt-4o-mini",
  "num_classified": 1000,
  "category_distribution": {
    "technical": 450,
    "billing": 200,
    "account": 150,
    "product": 150,
    "other": 50
  },
  "urgency_distribution": {
    "low": 300,
    "medium": 500,
    "high": 150,
    "critical": 50
  },
  "department_distribution": {
    "engineering": 450,
    "finance": 200,
    "customer_success": 350
  },
  "sentiment_distribution": {
    "positive": 200,
    "neutral": 600,
    "negative": 200
  },
  "avg_category_confidence": 0.89,
  "avg_urgency_confidence": 0.87,
  "total_input_tokens": 150000,
  "total_output_tokens": 75000,
  "estimated_cost_usd": "$0.0338"
}
```

## Troubleshooting

### Low Accuracy

**Problem**: Classifications are incorrect or inconsistent

**Solutions**:
1. Use LLM method instead of transformer
2. Upgrade to better model (GPT-4 instead of GPT-4o-mini)
3. Provide more context via `include_metadata_fields`
4. Refine category definitions (too broad or overlapping)
5. Add `include_reasoning: true` to debug decisions

### High Costs

**Problem**: API bills are too high

**Solutions**:
1. Switch to gpt-4o-mini or claude-3-haiku
2. Enable caching: `enable_caching: true`
3. Reduce `max_tokens` to minimum needed
4. Use transformer method for high-volume
5. Implement pre-filtering to classify only necessary tickets

### Slow Processing

**Problem**: Classification takes too long

**Solutions**:
1. Increase `batch_size` (for transformer)
2. Reduce `rate_limit_delay` if not hitting limits
3. Use faster model (gpt-4o-mini vs gpt-4)
4. Switch to transformer method for large batches
5. Process tickets asynchronously

### Rate Limits

**Problem**: Hitting API rate limits

**Solutions**:
1. Increase `rate_limit_delay` (e.g., 0.5 seconds)
2. Reduce `batch_size`
3. Implement exponential backoff (automatically handled by `max_retries`)
4. Upgrade API tier with provider
5. Distribute across multiple API keys

## Best Practices

1. **Start Simple**: Begin with basic categories, add complexity as needed
2. **Monitor Accuracy**: Review sample classifications regularly
3. **Use Confidence Scores**: Filter or flag low-confidence predictions
4. **Enable Reasoning**: Use `include_reasoning` during development/testing
5. **Test Both Methods**: Compare LLM vs transformer for your use case
6. **Cache Aggressively**: Enable caching to reduce costs
7. **Define Clear Categories**: Avoid overlapping or ambiguous categories
8. **Include Context**: Use metadata fields for better accuracy
9. **Track Costs**: Monitor spending with `track_costs: true`
10. **Validate Regularly**: Spot-check classifications against human labels

## Migration from Manual Classification

### Phase 1: Parallel Run
- Run classifier alongside manual classification
- Compare results, measure accuracy
- Tune configuration based on discrepancies

### Phase 2: Assisted Classification
- Use classifier suggestions with human review
- Flag uncertain predictions (confidence_threshold)
- Collect feedback for model improvement

### Phase 3: Full Automation
- Automatically classify high-confidence tickets
- Human review only for uncertain cases
- Monitor and adjust over time

## Related Components

- **priority_scorer**: Calculate priority scores based on classification
- **entity_extractor**: Extract entities from tickets (order IDs, product names)
- **sentiment_analyzer**: More detailed sentiment analysis
- **text_moderator**: Flag inappropriate content in tickets

## Support & Resources

- [Dagster Documentation](https://docs.dagster.io)
- [OpenAI API Documentation](https://platform.openai.com/docs)
- [Anthropic API Documentation](https://docs.anthropic.com)
- [Hugging Face Transformers](https://huggingface.co/docs/transformers)

## License

This component is part of the Dagster Components Templates library.
