# OpenAI LLM Component

Process text data using OpenAI's GPT models (GPT-4, GPT-3.5-turbo, GPT-4-turbo) with support for batch processing, streaming, function calling, and cost tracking.

## Overview

The OpenAI LLM Component enables you to leverage OpenAI's powerful language models within your data pipelines. It supports both single-prompt execution and batch processing of DataFrame columns, making it ideal for data enrichment, classification, summarization, and other text processing tasks.

## Features

- **Multiple Models**: Support for GPT-4, GPT-4-turbo, GPT-4o, GPT-4o-mini, GPT-3.5-turbo
- **Batch Processing**: Process entire DataFrame columns efficiently
- **Streaming**: Real-time response streaming for single prompts
- **Function Calling**: Define custom functions/tools for the model to use
- **Token Counting**: Track input/output tokens for each request
- **Cost Tracking**: Automatic cost estimation based on token usage
- **Response Caching**: Cache responses to avoid redundant API calls and reduce costs
- **Retry Logic**: Exponential backoff for rate limits and transient errors
- **Template Support**: Use dynamic prompts with DataFrame column placeholders
- **Configurable Parameters**: Temperature, max_tokens, top_p, penalties, and more

## Use Cases

1. **Data Enrichment**: Generate descriptions, summaries, or additional fields
2. **Classification**: Categorize text into predefined or dynamic categories
3. **Entity Extraction**: Extract structured information from unstructured text
4. **Sentiment Analysis**: Analyze sentiment and emotions in text
5. **Translation**: Translate text between languages
6. **Question Answering**: Answer questions based on context
7. **Content Generation**: Create marketing copy, product descriptions, etc.

## Prerequisites

1. **OpenAI API Key**: Obtain from [OpenAI Platform](https://platform.openai.com/)
2. **Python Packages**: `openai>=1.0.0`, `pandas>=1.5.0`

## Configuration

### Basic Configuration

```yaml
type: dagster_component_templates.OpenAILLMComponent
attributes:
  asset_name: enriched_products
  api_key: "${OPENAI_API_KEY}"
  model: gpt-3.5-turbo
  system_prompt: "You are a helpful assistant."
  input_column: product_name
  output_column: description
```

### Batch Processing with Template

Process multiple rows with a custom prompt template:

```yaml
type: dagster_component_templates.OpenAILLMComponent
attributes:
  asset_name: customer_feedback_analysis
  api_key: "${OPENAI_API_KEY}"
  model: gpt-4-turbo

  system_prompt: "You are a customer feedback analyst. Classify feedback as positive, negative, or neutral, and extract key themes."

  user_prompt_template: |
    Customer: {customer_name}
    Feedback: {feedback_text}
    Product: {product_name}

    Analyze this feedback and provide:
    1. Sentiment (positive/negative/neutral)
    2. Key themes (comma-separated)
    3. Action items (if any)

  input_column: feedback_text
  output_column: analysis

  temperature: 0.3
  max_tokens: 300
  batch_size: 20
  rate_limit_delay: 0.2
```

### JSON Structured Output

Request structured JSON responses:

```yaml
type: dagster_component_templates.OpenAILLMComponent
attributes:
  asset_name: extracted_entities
  api_key: "${OPENAI_API_KEY}"
  model: gpt-4-turbo

  system_prompt: "Extract entities from text and return as JSON."
  user_prompt_template: "Extract all entities from: {text}"

  response_format: json_object
  input_column: text
  output_column: entities_json

  temperature: 0.0
  max_tokens: 500
```

### Cost-Optimized Configuration

Minimize API costs with caching and cheaper models:

```yaml
type: dagster_component_templates.OpenAILLMComponent
attributes:
  asset_name: categorized_content
  api_key: "${OPENAI_API_KEY}"
  model: gpt-4o-mini  # Cheapest model

  system_prompt: "Categorize this content into one of: Technology, Business, Science, Entertainment, Sports"
  input_column: article_text
  output_column: category

  temperature: 0.0  # Deterministic for better caching
  max_tokens: 10

  enable_caching: true
  cache_dir: "/data/cache/openai_llm"
  track_costs: true

  batch_size: 50
  rate_limit_delay: 0.05
```

### Function Calling

Enable the model to call functions:

```yaml
type: dagster_component_templates.OpenAILLMComponent
attributes:
  asset_name: weather_enhanced
  api_key: "${OPENAI_API_KEY}"
  model: gpt-4-turbo

  system_prompt: "You are a weather assistant. Use the get_weather function when needed."
  input_column: query
  output_column: response

  functions: |
    [
      {
        "name": "get_weather",
        "description": "Get the current weather for a location",
        "parameters": {
          "type": "object",
          "properties": {
            "location": {
              "type": "string",
              "description": "City name"
            },
            "units": {
              "type": "string",
              "enum": ["celsius", "fahrenheit"]
            }
          },
          "required": ["location"]
        }
      }
    ]

  function_call: auto
```

## Configuration Options

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `asset_name` | string | Name of the output asset |
| `api_key` | string | OpenAI API key (use `${OPENAI_API_KEY}`) |

### Model Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model` | string | `gpt-3.5-turbo` | Model: gpt-4, gpt-4-turbo, gpt-4o, gpt-4o-mini, gpt-3.5-turbo |
| `temperature` | float | `0.7` | Response randomness (0.0-2.0). Lower = more deterministic |
| `max_tokens` | int | `None` | Maximum tokens in response |
| `top_p` | float | `None` | Nucleus sampling (alternative to temperature) |
| `frequency_penalty` | float | `0.0` | Reduce token repetition (-2.0 to 2.0) |
| `presence_penalty` | float | `0.0` | Encourage new topics (-2.0 to 2.0) |

### Prompt Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `system_prompt` | string | `None` | System prompt to set context |
| `user_prompt_template` | string | `None` | Template with `{column_name}` placeholders |
| `input_column` | string | `None` | Column containing input text for batch processing |
| `output_column` | string | `llm_response` | Column name for LLM responses |

### Performance & Cost

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | int | `10` | Rows to process in parallel |
| `rate_limit_delay` | float | `0.1` | Delay between API calls (seconds) |
| `max_retries` | int | `3` | Max retries for failed requests |
| `enable_caching` | bool | `true` | Cache responses to reduce costs |
| `cache_dir` | string | `/tmp/openai_llm_cache` | Cache directory |
| `track_costs` | bool | `true` | Track and report token usage/costs |

### Advanced Features

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stream` | bool | `false` | Use streaming (single prompts only) |
| `response_format` | string | `None` | Format: `text` or `json_object` |
| `functions` | string | `None` | JSON array of function definitions |
| `function_call` | string | `None` | Control function calling: `auto`, `none`, or function name |

## Cost Optimization Tips

1. **Use Cheaper Models**: Start with `gpt-4o-mini` or `gpt-3.5-turbo` for simple tasks
2. **Enable Caching**: Set `enable_caching: true` to avoid reprocessing identical prompts
3. **Lower Temperature**: Use `temperature: 0.0` for deterministic, cacheable responses
4. **Reduce max_tokens**: Set appropriate `max_tokens` limits to avoid unnecessary tokens
5. **Batch Similar Requests**: Group similar prompts to maximize cache hits
6. **Monitor Costs**: Use `track_costs: true` to monitor spending in metadata

### Approximate Costs (per 1M tokens, as of 2024)

| Model | Input Cost | Output Cost |
|-------|------------|-------------|
| GPT-4 | $30.00 | $60.00 |
| GPT-4-turbo | $10.00 | $30.00 |
| GPT-4o | $5.00 | $15.00 |
| GPT-4o-mini | $0.15 | $0.60 |
| GPT-3.5-turbo | $0.50 | $1.50 |

## Integration with Other Components

### Upstream Components

The OpenAI LLM component works with any component that outputs a DataFrame:

- CSV File Ingestion
- Database Query
- API Ingestion components
- Document Text Extractor
- Text Chunker

### Downstream Components

Process LLM outputs with:

- **DataFrame Transformer**: Parse JSON responses, extract fields
- **Vector Store Writer**: Store embeddings for RAG
- **Database Writer**: Persist enriched data
- **Sentiment Analyzer**: Further analyze LLM outputs

## Example Pipeline

```yaml
# 1. Load customer data
- type: dagster_component_templates.CSVFileIngestionComponent
  attributes:
    asset_name: customer_feedback
    file_path: "data/feedback.csv"

# 2. Analyze with GPT-4
- type: dagster_component_templates.OpenAILLMComponent
  attributes:
    asset_name: feedback_analyzed
    api_key: "${OPENAI_API_KEY}"
    model: gpt-4-turbo
    system_prompt: "Analyze customer feedback and extract insights."
    user_prompt_template: "Feedback: {feedback_text}\n\nProvide: sentiment, key issues, priority (low/medium/high)"
    input_column: feedback_text
    output_column: analysis
    temperature: 0.3
    enable_caching: true

# 3. Store results
- type: dagster_component_templates.DuckDBTableWriterComponent
  attributes:
    asset_name: feedback_stored
    table_name: analyzed_feedback
```

## API Rate Limits

OpenAI has rate limits based on your account tier:

- **Free Tier**: 3 RPM, 40,000 TPM
- **Tier 1**: 500 RPM, 200,000 TPM
- **Tier 2**: 5,000 RPM, 2,000,000 TPM

Configure `rate_limit_delay` and `batch_size` according to your tier to avoid hitting limits.

## Error Handling

The component includes robust error handling:

1. **Rate Limit Errors**: Automatic retry with exponential backoff
2. **Transient Errors**: Retry up to `max_retries` times
3. **Invalid Responses**: Logged warnings with graceful degradation
4. **Cache Errors**: Continues without cache if unavailable

## Metadata

The component provides rich metadata:

- `model`: Model used
- `rows_processed`: Number of rows processed
- `cache_hits`: Number of cached responses used
- `cache_hit_rate`: Percentage of cache hits
- `total_input_tokens`: Total input tokens used
- `total_output_tokens`: Total output tokens used
- `estimated_cost_usd`: Estimated API cost in USD

## Best Practices

1. **Test Prompts**: Test prompts on small datasets before running on full data
2. **Use System Prompts**: Set clear context with system prompts for consistent results
3. **Handle JSON**: If requesting JSON, validate and parse in downstream components
4. **Monitor Costs**: Always enable `track_costs` to monitor spending
5. **Version Control Prompts**: Store prompts in version control for reproducibility
6. **Cache Aggressively**: Use caching for expensive models and deterministic tasks

## Troubleshooting

### High Costs

- Use cheaper models (gpt-4o-mini instead of gpt-4)
- Enable caching
- Reduce max_tokens
- Lower temperature for better cache hits

### Rate Limit Errors

- Increase `rate_limit_delay`
- Decrease `batch_size`
- Upgrade OpenAI account tier

### Inconsistent Responses

- Lower `temperature` (0.0-0.3 for deterministic)
- Use more specific system prompts
- Add examples in the prompt (few-shot learning)

### Cache Not Working

- Ensure `cache_dir` is writable
- Check disk space
- Verify deterministic prompts (same input = same prompt)

## Support

For issues or questions:
- OpenAI API Documentation: https://platform.openai.com/docs
- Dagster Documentation: https://docs.dagster.io
- Component Issues: File an issue in the component repository
