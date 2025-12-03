# Sentiment Analyzer Component

Analyze sentiment using LLMs (GPT/Claude) or transformer models (DistilBERT, RoBERTa). Supports aspect-based sentiment, emotion detection, and custom sentiment categories.

## Overview

The Sentiment Analyzer Component analyzes the emotional tone and sentiment of text. It supports both local transformer models (no API required) and LLM-based analysis for more nuanced, customizable sentiment detection.

## Features

- **Dual Methods**: Transformer-based (fast, local) or LLM-based (flexible, nuanced)
- **Custom Categories**: Define your own sentiment categories
- **Aspect-Based Sentiment**: Analyze sentiment for specific aspects (LLM only)
- **Emotion Detection**: Detect specific emotions beyond positive/negative (LLM only)
- **Confidence Scores**: Get confidence scores for predictions
- **Batch Processing**: Efficient processing of large datasets
- **Multi-language Support**: With appropriate models
- **Reasoning**: Get explanations for sentiment classifications (LLM only)

## Use Cases

1. **Customer Feedback Analysis**: Analyze reviews, surveys, support tickets
2. **Social Media Monitoring**: Track brand sentiment on social platforms
3. **Product Review Analysis**: Understand customer opinions
4. **Content Moderation**: Flag negative or toxic content
5. **Market Research**: Analyze consumer sentiment
6. **Employee Feedback**: Analyze employee satisfaction surveys

## Prerequisites

### Transformer Method (Local)
- **Python Packages**: `transformers>=4.30.0`, `torch>=2.0.0`, `pandas>=1.5.0`
- **No API Key Required**

### LLM Method
- **Python Packages**: `openai>=1.0.0` or `anthropic>=0.18.0`, `pandas>=1.5.0`
- **API Key**: OpenAI or Anthropic API key

## Configuration

### Transformer Method (Default, No API)

Fast, local sentiment analysis:

```yaml
type: dagster_component_templates.SentimentAnalyzerComponent
attributes:
  asset_name: review_sentiment
  method: transformer
  model: distilbert-base-uncased-finetuned-sst-2-english
  input_column: review_text
  sentiment_label_column: sentiment
  sentiment_score_column: confidence
  sentiment_categories: "positive,negative"
  batch_size: 32
```

### LLM Method (GPT-4)

More flexible and customizable:

```yaml
type: dagster_component_templates.SentimentAnalyzerComponent
attributes:
  asset_name: feedback_sentiment
  method: llm
  provider: openai
  model: gpt-4-turbo
  api_key: "${OPENAI_API_KEY}"

  input_column: feedback_text
  sentiment_label_column: sentiment
  sentiment_score_column: confidence

  sentiment_categories: "very positive,positive,neutral,negative,very negative"

  temperature: 0.0
  max_tokens: 100
```

### Aspect-Based Sentiment (LLM Only)

Analyze sentiment for specific aspects:

```yaml
type: dagster_component_templates.SentimentAnalyzerComponent
attributes:
  asset_name: product_sentiment
  method: llm
  provider: openai
  model: gpt-4-turbo
  api_key: "${OPENAI_API_KEY}"

  input_column: review_text

  sentiment_categories: "positive,negative,neutral"

  # Aspect-based analysis
  aspect_based: true
  aspects: "quality,price,design,customer_service,shipping"

  temperature: 0.0
```

Output includes `aspect_sentiments` column:
```json
{
  "quality": "positive",
  "price": "negative",
  "design": "positive",
  "customer_service": "neutral",
  "shipping": "positive"
}
```

### Emotion Detection (LLM Only)

Detect specific emotions:

```yaml
type: dagster_component_templates.SentimentAnalyzerComponent
attributes:
  asset_name: emotion_analysis
  method: llm
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  api_key: "${ANTHROPIC_API_KEY}"

  input_column: text

  sentiment_categories: "positive,negative,neutral"

  # Emotion detection
  detect_emotions: true
  emotion_categories: "joy,anger,sadness,fear,surprise,disgust,trust"

  # Get reasoning
  return_reasoning: true
```

### Custom Sentiment Categories

Define your own categories:

```yaml
type: dagster_component_templates.SentimentAnalyzerComponent
attributes:
  asset_name: custom_sentiment
  method: llm
  provider: openai
  model: gpt-4-turbo
  api_key: "${OPENAI_API_KEY}"

  input_column: text

  # Custom categories for your domain
  sentiment_categories: "enthusiastic,satisfied,neutral,disappointed,angry"
```

### With Confidence Threshold

Filter uncertain predictions:

```yaml
type: dagster_component_templates.SentimentAnalyzerComponent
attributes:
  asset_name: high_confidence_sentiment
  method: transformer
  model: cardiffnlp/twitter-roberta-base-sentiment

  input_column: tweet_text

  sentiment_categories: "positive,negative,neutral"

  # Only keep predictions with confidence > 0.7
  confidence_threshold: 0.7  # Below this = "uncertain"
```

## Model Recommendations

### Transformer Models (Local)

| Model | Best For | Speed | Quality |
|-------|----------|-------|---------|
| distilbert-base-uncased-finetuned-sst-2-english | General sentiment | Fast | Good |
| cardiffnlp/twitter-roberta-base-sentiment | Social media | Medium | Very Good |
| nlptown/bert-base-multilingual-uncased-sentiment | Multi-language | Medium | Good |

### LLM Models

| Model | Best For | Cost |
|-------|----------|------|
| GPT-4-turbo | Complex, nuanced analysis | High |
| GPT-3.5-turbo | Fast, cost-effective | Low |
| Claude 3.5 Sonnet | Detailed reasoning | Medium |
| Claude 3 Haiku | Fast, cheap | Very Low |

## Configuration Options

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `asset_name` | string | Name of output asset |

### Method Selection

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `method` | string | `transformer` | Method: transformer, llm |
| `model` | string | `None` | Model name |
| `provider` | string | `None` | LLM provider (openai, anthropic) |
| `api_key` | string | `None` | API key for LLM |

### Input/Output

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `input_column` | string | `text` | Column with text |
| `sentiment_label_column` | string | `sentiment_label` | Column for label |
| `sentiment_score_column` | string | `sentiment_score` | Column for confidence |
| `sentiment_categories` | string | `positive,negative,neutral` | Sentiment categories |

### Advanced Features

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `aspect_based` | bool | `false` | Enable aspect-based sentiment (LLM only) |
| `aspects` | string | `None` | Comma-separated aspects |
| `detect_emotions` | bool | `false` | Detect emotions (LLM only) |
| `emotion_categories` | string | `joy,anger,sadness...` | Emotions to detect |
| `return_reasoning` | bool | `false` | Return explanation (LLM only) |
| `confidence_threshold` | float | `None` | Min confidence (0.0-1.0) |

### Performance

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | int | `32` | Processing batch size |
| `temperature` | float | `0.0` | LLM temperature |
| `max_tokens` | int | `100` | Max LLM tokens |

## Choosing a Method

### Use Transformer When:
- You need fast processing
- You want no API costs
- You have standard sentiment categories
- You're processing large volumes

### Use LLM When:
- You need custom sentiment categories
- You want aspect-based sentiment
- You need emotion detection
- You want reasoning/explanations
- You need nuanced analysis

## Integration with Other Components

### Upstream Components

Works with any component producing text:
- CSV File Ingestion
- Database Query
- API Ingestion components
- OpenAI/Anthropic LLM outputs

### Downstream Components

Use sentiment results with:
- **DataFrame Transformer**: Filter by sentiment, aggregate
- **Database Writer**: Store results
- **Visualization**: Chart sentiment distribution
- **Alerting**: Alert on negative sentiment

## Example Pipelines

### Customer Feedback Pipeline

```yaml
# 1. Load feedback
- type: dagster_component_templates.CSVFileIngestionComponent
  attributes:
    asset_name: customer_feedback
    file_path: "data/feedback.csv"

# 2. Analyze sentiment
- type: dagster_component_templates.SentimentAnalyzerComponent
  attributes:
    asset_name: sentiment_analyzed
    method: transformer
    model: distilbert-base-uncased-finetuned-sst-2-english
    input_column: feedback_text
    sentiment_categories: "positive,negative,neutral"
    confidence_threshold: 0.7

# 3. Store results
- type: dagster_component_templates.DuckDBTableWriterComponent
  attributes:
    asset_name: feedback_stored
    table_name: analyzed_feedback
```

### Product Review Analysis with Aspects

```yaml
# 1. Load reviews
- type: dagster_component_templates.DatabaseQueryComponent
  attributes:
    asset_name: product_reviews
    query: "SELECT id, product_id, review_text FROM reviews WHERE created_at > NOW() - INTERVAL '7 days'"

# 2. Aspect-based sentiment
- type: dagster_component_templates.SentimentAnalyzerComponent
  attributes:
    asset_name: review_sentiment
    method: llm
    provider: openai
    model: gpt-4-turbo
    api_key: "${OPENAI_API_KEY}"
    input_column: review_text
    aspect_based: true
    aspects: "quality,price,performance,design,value_for_money"
    return_reasoning: true
```

## Best Practices

1. **Start with Transformers**: Use transformer models for prototyping (free, fast)
2. **Use LLMs for Complex Cases**: Switch to LLMs when you need customization
3. **Set Confidence Thresholds**: Filter low-confidence predictions
4. **Batch Processing**: Use appropriate batch sizes for your hardware/API
5. **Monitor Sentiment Trends**: Track sentiment over time
6. **Custom Categories**: Define categories that match your business needs
7. **Aspect-Based for Details**: Use aspect-based analysis for product feedback

## Troubleshooting

### Low Confidence Scores

- Use more specific sentiment categories
- Try a better model (e.g., GPT-4 instead of GPT-3.5)
- Increase training data for transformer models
- Check if text is ambiguous

### Incorrect Classifications

- Review sentiment categories (too broad/narrow?)
- Add examples in LLM prompts (few-shot learning)
- Try a different model
- Check for sarcasm or context-dependent language

### Slow Processing

- Use transformer method instead of LLM
- Increase `batch_size`
- Use faster models (DistilBERT, Claude Haiku)
- Process in parallel

### High Costs (LLM)

- Switch to transformer method
- Use GPT-3.5-turbo or Claude Haiku
- Reduce `max_tokens`
- Process only high-priority items

## Metadata

The component provides:

- `method`: Method used (transformer/llm)
- `model`: Model name
- `num_analyzed`: Number of texts analyzed
- `sentiment_distribution`: Count by sentiment
- `average_confidence`: Average confidence score
- `categories`: Sentiment categories used
- `aspect_based`: Whether aspect-based (if enabled)
- `emotion_detection`: Whether emotions detected (if enabled)

## Support

For issues or questions:
- Transformers Documentation: https://huggingface.co/docs/transformers
- OpenAI Documentation: https://platform.openai.com/docs
- Anthropic Documentation: https://docs.anthropic.com
- Dagster Documentation: https://docs.dagster.io
