# Text Moderator Component

Detect toxic content, hate speech, PII, profanity, and inappropriate content using OpenAI Moderation API, Perspective API, transformer models, or LLM-based methods.

## Overview

The Text Moderator Component provides comprehensive content moderation and safety detection for user-generated content. It detects toxicity, hate speech, PII (Personally Identifiable Information), profanity, sexual content, violence, and other policy violations.

## Features

- **Multiple Detection Categories**: toxicity, hate_speech, PII, profanity, sexual, violence, threats, harassment
- **Four Methods**: OpenAI Moderation (free), Perspective API (free with approval), transformer (local), LLM (custom logic)
- **PII Detection & Redaction**: Detect and mask emails, phone numbers, SSN, credit cards
- **Confidence Scores**: Get scores for each category
- **Custom Profanity Lists**: Add domain-specific prohibited words
- **Multi-Language Support**: Depending on provider/model
- **Batch Processing**: Efficient handling of large datasets
- **Compliance Ready**: GDPR, COPPA, content policy enforcement

## Use Cases

1. **UGC Platforms**: Moderate user comments, posts, reviews
2. **Chat/Messaging**: Real-time content filtering
3. **Forum Moderation**: Automated screening of forum posts
4. **Review Platforms**: Filter inappropriate product/business reviews
5. **Social Media**: Content safety for social features
6. **Support Tickets**: Flag abusive or inappropriate messages
7. **Compliance**: GDPR PII detection, content policy enforcement

## Prerequisites

### OpenAI Moderation API (Recommended, Free)
- **Python Packages**: `openai>=1.0.0`, `pandas>=1.5.0`
- **API Key**: OpenAI API key (same as GPT)
- **Cost**: $0 (free endpoint)
- **Rate Limits**: Generous free tier

### Perspective API (Free with Approval)
- **Python Packages**: `requests>=2.31.0`, `pandas>=1.5.0`
- **API Key**: Google Perspective API key (requires application)
- **Cost**: $0 (free with approval)
- **Rate Limits**: 1 QPS free tier

### Transformer Method (Local, Free)
- **Python Packages**: `transformers>=4.30.0`, `torch>=2.0.0`, `pandas>=1.5.0`
- **Cost**: $0 (runs locally)
- **No API Key Required**

### LLM Method (Custom Logic)
- **Python Packages**: `openai>=1.0.0` or `anthropic>=0.18.0`, `pandas>=1.5.0`
- **API Key**: OpenAI or Anthropic API key
- **Cost**: $0.15-$5 per 1M input tokens

## Configuration

### OpenAI Moderation API (Recommended)

Free, fast, and accurate:

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: moderated_comments

  method: openai_moderation
  api_key: "${OPENAI_API_KEY}"

  input_column: user_comment

  categories: "toxicity,hate_speech,pii,profanity,sexual,violence,harassment"

  threshold: 0.7
  redact_pii: true
  include_scores: true

  flag_column: flagged
```

### Perspective API

Google's content safety API:

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: perspective_moderated

  method: perspective_api
  api_key: "${PERSPECTIVE_API_KEY}"

  input_column: post_content

  categories: "toxicity,severe_toxicity,hate_speech,insult,profanity,threat"

  threshold: 0.8
  include_scores: true
```

### Transformer Method (Local)

No API required:

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: local_moderated

  method: transformer
  transformer_model: unitary/toxic-bert

  input_column: text

  categories: "toxicity,hate_speech,pii,profanity"

  threshold: 0.7
  redact_pii: true
  include_scores: true
  batch_size: 16
```

### LLM Method (Custom Logic)

For specialized moderation needs:

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: llm_moderated

  method: llm
  llm_provider: openai
  llm_model: gpt-4o-mini
  api_key: "${OPENAI_API_KEY}"

  input_column: user_content

  categories: "toxicity,hate_speech,pii,profanity,sexual,violence,misinformation"

  threshold: 0.8
  redact_pii: true
  include_scores: true

  temperature: 0.0
  max_tokens: 300
```

### PII Detection & Redaction

Detect and redact sensitive information:

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: pii_protected

  method: openai_moderation
  api_key: "${OPENAI_API_KEY}"

  input_column: user_input

  categories: "pii,toxicity"

  threshold: 0.5  # Lower threshold for PII
  redact_pii: true  # Replace PII with [REDACTED]
  include_scores: true
```

Output example:
```
Original: "Contact me at john@example.com or 555-1234"
Redacted: "Contact me at [REDACTED] or [REDACTED]"
```

### Custom Profanity List

Add domain-specific prohibited words:

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: custom_moderated

  method: openai_moderation
  api_key: "${OPENAI_API_KEY}"

  input_column: comment

  categories: "toxicity,profanity"

  threshold: 0.7
  custom_profanity_list: "badword1,badword2,badword3,customword"

  include_scores: true
```

## Output Schema

The component adds the following columns:

### Always Included:
- `{flag_column}`: Boolean flag (true if any category exceeds threshold)

### Optional (based on configuration):
- `{category}_score`: Confidence score 0.0-1.0 for each category (if include_scores=true)
- `{input_column}_redacted`: Text with PII redacted (if redact_pii=true)

### Example Output:

```
| user_comment                        | flagged | toxicity_score | hate_speech_score | pii_score | profanity_score | user_comment_redacted              |
|-------------------------------------|---------|----------------|-------------------|-----------|-----------------|-------------------------------------|
| This is a great product!            | false   | 0.05           | 0.02              | 0.00      | 0.00            | This is a great product!            |
| You're an idiot!                    | true    | 0.92           | 0.15              | 0.00      | 0.30            | You're an idiot!                    |
| Email me at john@example.com        | true    | 0.01           | 0.00              | 1.00      | 0.00            | Email me at [REDACTED]              |
| F*** this garbage product!          | true    | 0.88           | 0.05              | 0.00      | 0.95            | F*** this garbage product!          |
```

## Method Comparison

| Feature | OpenAI Moderation | Perspective API | Transformer | LLM |
|---------|-------------------|-----------------|-------------|-----|
| **Cost** | $0 (free) | $0 (free w/ approval) | $0 (local) | $0.15-$5 per 1M |
| **Speed** | Fast (100-200ms) | Fast (100-300ms) | Medium (50-200ms) | Slower (200-500ms) |
| **Accuracy** | High (90-95%) | High (90-95%) | Good (80-88%) | Highest (92-97%) |
| **Categories** | 11 built-in | 7 built-in | Limited | Fully customizable |
| **PII Detection** | Manual (regex) | Manual (regex) | Manual (regex) | Built-in |
| **Setup** | Easy (API key) | Medium (approval needed) | Easy | Easy (API key) |
| **Best For** | Production use | Google ecosystem | Privacy/offline | Custom categories |

### Detailed Comparison

**OpenAI Moderation API** (Recommended):
- Pros: Free, fast, accurate, 11 categories, no approval needed
- Cons: Requires internet, limited to built-in categories
- Best for: Most production use cases

**Perspective API**:
- Pros: Free, accurate, Google-backed
- Cons: Requires API approval, 1 QPS free tier, limited categories
- Best for: Google Cloud users, low-volume applications

**Transformer**:
- Pros: Free, works offline, privacy-friendly, no rate limits
- Cons: Slower, less accurate, limited categories
- Best for: Privacy requirements, high-volume offline processing

**LLM**:
- Pros: Most accurate, custom categories, best PII detection, explainable
- Cons: API costs, slower, requires internet
- Best for: Complex/custom moderation rules, when accuracy is critical

## Detected Categories

### Toxicity
Rude, disrespectful, or unreasonable comments

### Hate Speech
Content attacking a person or group based on protected characteristics

### PII (Personally Identifiable Information)
- Email addresses
- Phone numbers
- SSN (Social Security Numbers)
- Credit card numbers
- IP addresses
- Physical addresses

### Profanity
Swear words and obscene language

### Sexual
Sexually explicit or suggestive content

### Violence
Content depicting or promoting violence

### Harassment
Content harassing, bullying, or threatening individuals

### Self-Harm
Content promoting self-harm or suicide

## Cost Estimation

### Free Methods (OpenAI Moderation, Perspective API, Transformer)
- **Cost**: $0
- **Limits**: Rate limits apply (but generous for free tiers)

### LLM Costs (per 1,000 texts)

Assuming average 150 tokens input, 100 tokens output per text:

| Model | Input Cost | Output Cost | Total per 1k | Monthly (10k/day) |
|-------|------------|-------------|--------------|-------------------|
| GPT-4o-mini | $0.023 | $0.060 | $0.083 | $24.90 |
| GPT-4o | $0.75 | $1.50 | $2.25 | $675 |
| Claude 3 Haiku | $0.038 | $0.125 | $0.163 | $48.90 |

**Recommendation**: Use OpenAI Moderation API (free) for most cases

## Advanced Patterns

### Pipeline: Moderate -> Filter -> Store

```yaml
# Step 1: Moderate content
- type: dagster_component_templates.TextModeratorComponent
  attributes:
    asset_name: moderated_comments
    method: openai_moderation
    categories: "toxicity,hate_speech,profanity"
    threshold: 0.7

# Step 2: Filter out flagged content
# (Custom filtering logic or SQL transform)

# Step 3: Store safe content
```

### Multi-Level Moderation

Use multiple methods for higher accuracy:

```yaml
# Level 1: Fast OpenAI Moderation
- type: dagster_component_templates.TextModeratorComponent
  attributes:
    asset_name: level1_moderated
    method: openai_moderation
    threshold: 0.8  # Higher threshold

# Level 2: LLM review for borderline cases
- type: dagster_component_templates.TextModeratorComponent
  attributes:
    asset_name: level2_moderated
    method: llm
    llm_model: gpt-4-turbo
    threshold: 0.7  # More nuanced
```

### PII Compliance Pipeline

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: pii_compliant

  method: openai_moderation
  api_key: "${OPENAI_API_KEY}"

  input_column: user_data

  # Focus on PII
  categories: "pii"

  threshold: 0.5  # Low threshold for PII detection
  redact_pii: true  # Automatically redact

  # Optional: Use Presidio for advanced PII detection
```

### Real-Time Comment Moderation

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: realtime_moderated

  method: openai_moderation  # Fast enough for real-time
  api_key: "${OPENAI_API_KEY}"

  input_column: comment_text

  categories: "toxicity,hate_speech,profanity,sexual,harassment"

  threshold: 0.75
  include_scores: true

  rate_limit_delay: 0.05  # Fast processing
```

## Integration Examples

### User Comments Platform

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: moderated_user_comments

  method: openai_moderation
  api_key: "${OPENAI_API_KEY}"

  input_column: comment_body

  categories: "toxicity,hate_speech,profanity,harassment"

  threshold: 0.7
  include_scores: true
```

### Product Review Filtering

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: clean_reviews

  method: perspective_api
  api_key: "${PERSPECTIVE_API_KEY}"

  input_column: review_text

  categories: "toxicity,profanity,insult"

  threshold: 0.8  # Stricter for public reviews
  include_scores: true
```

### Support Ticket Safety

```yaml
type: dagster_component_templates.TextModeratorComponent
attributes:
  asset_name: safe_tickets

  method: openai_moderation
  api_key: "${OPENAI_API_KEY}"

  input_column: ticket_description

  categories: "toxicity,harassment,threat,pii"

  threshold: 0.6  # Flag for review, not auto-block
  redact_pii: true
  include_scores: true
```

## Performance Tuning

### For High Volume (>100k texts/day)

```yaml
attributes:
  method: transformer  # No API rate limits
  transformer_model: unitary/toxic-bert
  batch_size: 32
  categories: "toxicity,profanity"  # Only essential
  include_scores: false  # Reduce overhead
```

### For High Accuracy

```yaml
attributes:
  method: llm
  llm_model: gpt-4-turbo
  categories: "all_needed_categories"
  threshold: 0.7
  include_scores: true
  temperature: 0.0
```

### For Cost Optimization

```yaml
attributes:
  method: openai_moderation  # Free
  # or
  method: transformer  # Free + offline
```

## Monitoring & Analytics

Metadata provided:

```python
{
  "method": "openai_moderation",
  "model": "openai_moderation",
  "num_texts_moderated": 10000,
  "flagged_count": 450,
  "flagged_rate": "4.5%",
  "threshold": 0.7,
  "categories": ["toxicity", "hate_speech", "pii", "profanity", "sexual", "violence"],
  "category_statistics": {
    "toxicity": {"avg_score": 0.12, "flagged_count": 200},
    "hate_speech": {"avg_score": 0.05, "flagged_count": 50},
    "pii": {"avg_score": 0.08, "flagged_count": 150},
    "profanity": {"avg_score": 0.06, "flagged_count": 100},
    "sexual": {"avg_score": 0.03, "flagged_count": 30},
    "violence": {"avg_score": 0.02, "flagged_count": 20}
  },
  "api_calls": 10000,
  "pii_redaction_enabled": true
}
```

## Troubleshooting

### High False Positive Rate

**Problem**: Too many safe posts being flagged

**Solutions**:
1. Increase threshold (e.g., 0.7 -> 0.85)
2. Reduce categories to only critical ones
3. Use LLM method for better context understanding
4. Review and tune custom_profanity_list

### Missing Violations

**Problem**: Inappropriate content not being detected

**Solutions**:
1. Decrease threshold (e.g., 0.8 -> 0.6)
2. Add more categories
3. Use LLM method for better detection
4. Add domain-specific words to custom_profanity_list
5. Consider multi-level moderation

### Rate Limit Errors

**Problem**: Hitting API rate limits

**Solutions**:
1. Increase rate_limit_delay
2. Use transformer method (no rate limits)
3. Upgrade API tier
4. Batch processing with lower throughput

### PII Not Detected

**Problem**: PII slipping through detection

**Solutions**:
1. Lower threshold for PII category
2. Use LLM method (better PII detection)
3. Add Presidio library for advanced PII detection
4. Add custom regex patterns for domain-specific PII

## Best Practices

1. **Start with Free APIs**: Use OpenAI Moderation or Perspective API first
2. **Tune Threshold**: Adjust based on false positive/negative rate
3. **Layer Detection**: Use multiple methods for critical applications
4. **Monitor Metrics**: Track flagged_rate and category statistics
5. **Human Review**: Have humans review borderline cases
6. **Redact PII**: Always enable PII redaction for compliance
7. **Custom Lists**: Maintain domain-specific profanity/allowed words
8. **Test Thoroughly**: Test with real user content before deployment
9. **Update Regularly**: Review and update moderation rules
10. **Document Policy**: Keep clear content policy documentation

## Compliance & Legal

### GDPR Compliance
- Enable PII detection and redaction
- Log moderation actions for audit trails
- Provide user data deletion capabilities

### COPPA Compliance (Children's Online Privacy)
- Lower threshold for all categories
- Enable aggressive PII filtering
- Consider requiring manual review

### Content Policy Enforcement
- Define clear content policies
- Tune thresholds to match policy
- Maintain audit logs
- Provide appeal mechanisms

## Related Components

- **ticket_classifier**: Classify moderated content
- **sentiment_analyzer**: Analyze sentiment of flagged content
- **entity_extractor**: Extract entities from moderated text
- **priority_scorer**: Prioritize moderation review queue

## Support & Resources

- [OpenAI Moderation API Documentation](https://platform.openai.com/docs/guides/moderation)
- [Google Perspective API Documentation](https://developers.perspectiveapi.com)
- [Hugging Face Toxicity Models](https://huggingface.co/models?search=toxicity)
- [Presidio PII Detection](https://microsoft.github.io/presidio/)

## License

This component is part of the Dagster Components Templates library.
