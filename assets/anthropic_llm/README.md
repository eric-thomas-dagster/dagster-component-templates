# Anthropic Claude LLM Component

Process text data using Anthropic's Claude models (Claude 3.5 Sonnet, Claude 3 Opus, Claude 3 Haiku) with support for batch processing, prompt caching, tool use, and extended context windows.

## Overview

The Anthropic LLM Component enables you to leverage Claude's advanced capabilities within your data pipelines. Claude excels at complex reasoning, long document analysis, code generation, and nuanced understanding of instructions.

## Features

- **Latest Claude Models**: Claude 3.5 Sonnet, Claude 3 Opus, Claude 3 Sonnet, Claude 3 Haiku
- **Extended Context**: Up to 200K tokens context window for long documents
- **Prompt Caching**: Save 90% on repeated contexts with ephemeral prompt caching
- **Batch Processing**: Process entire DataFrame columns efficiently
- **Streaming**: Real-time response streaming for single prompts
- **Tool Use**: Define tools for Claude to use (function calling)
- **Token Counting**: Track input/output/cached tokens
- **Cost Tracking**: Automatic cost estimation including cache savings
- **Response Caching**: Cache responses to avoid redundant API calls
- **Retry Logic**: Exponential backoff for rate limits and errors
- **Template Support**: Dynamic prompts with DataFrame column placeholders

## Use Cases

1. **Long Document Analysis**: Analyze entire documents (up to 200K tokens)
2. **Complex Reasoning**: Multi-step analysis and decision making
3. **Code Generation**: Generate and review code with context
4. **Research**: Fact-checking and research assistance
5. **Summarization**: Extract key insights from lengthy content
6. **Creative Writing**: Content generation with nuanced understanding

## Prerequisites

1. **Anthropic API Key**: Obtain from [Anthropic Console](https://console.anthropic.com/)
2. **Python Packages**: `anthropic>=0.18.0`, `pandas>=1.5.0`

## Configuration

### Basic Configuration

```yaml
type: dagster_component_templates.AnthropicLLMComponent
attributes:
  asset_name: document_summaries
  api_key: "${ANTHROPIC_API_KEY}"
  model: claude-3-5-sonnet-20241022
  system_prompt: "You are a document summarization expert."
  input_column: document_text
  output_column: summary
  max_tokens: 1000
```

### Batch Processing with Prompt Caching

Save 90% on costs for repeated contexts:

```yaml
type: dagster_component_templates.AnthropicLLMComponent
attributes:
  asset_name: legal_document_analysis
  api_key: "${ANTHROPIC_API_KEY}"
  model: claude-3-5-sonnet-20241022

  # System prompt is cached across all requests
  system_prompt: |
    You are a legal document analyst. Analyze contracts for:
    - Key obligations and rights
    - Potential risks
    - Compliance requirements
    - Ambiguous language

    [Include your standard 50-page legal analysis guidelines here]

  user_prompt_template: "Analyze this contract:\n\n{contract_text}"

  input_column: contract_text
  output_column: legal_analysis

  max_tokens: 4000
  temperature: 0.3

  # Enable prompt caching - system prompt cached, saves 90%
  enable_prompt_caching: true

  enable_caching: true
  track_costs: true
```

### Long Document Processing

Claude supports up to 200K tokens:

```yaml
type: dagster_component_templates.AnthropicLLMComponent
attributes:
  asset_name: research_paper_analysis
  api_key: "${ANTHROPIC_API_KEY}"
  model: claude-3-5-sonnet-20241022

  system_prompt: "You are a research paper analyst. Extract methodology, findings, and implications."

  user_prompt_template: |
    Research Paper:
    {full_paper_text}

    Provide:
    1. Summary (3-5 sentences)
    2. Methodology
    3. Key Findings
    4. Limitations
    5. Future Research Directions

  input_column: full_paper_text
  output_column: analysis

  max_tokens: 4096
  temperature: 0.5
```

### Tool Use (Function Calling)

Define tools for Claude to use:

```yaml
type: dagster_component_templates.AnthropicLLMComponent
attributes:
  asset_name: data_lookup_enhanced
  api_key: "${ANTHROPIC_API_KEY}"
  model: claude-3-5-sonnet-20241022

  system_prompt: "You are a data assistant. Use the lookup_customer tool to get customer details."

  input_column: query
  output_column: response

  tools: |
    [
      {
        "name": "lookup_customer",
        "description": "Look up customer details by ID or email",
        "input_schema": {
          "type": "object",
          "properties": {
            "identifier": {
              "type": "string",
              "description": "Customer ID or email"
            },
            "fields": {
              "type": "array",
              "items": {"type": "string"},
              "description": "Fields to retrieve"
            }
          },
          "required": ["identifier"]
        }
      }
    ]

  tool_choice: auto
  max_tokens: 2000
```

### JSON Structured Output

Extract structured data:

```yaml
type: dagster_component_templates.AnthropicLLMComponent
attributes:
  asset_name: entity_extraction
  api_key: "${ANTHROPIC_API_KEY}"
  model: claude-3-haiku-20240307  # Fast and cheap for structured extraction

  system_prompt: |
    Extract entities and return as JSON with this structure:
    {
      "people": ["name1", "name2"],
      "organizations": ["org1", "org2"],
      "locations": ["loc1", "loc2"],
      "dates": ["date1", "date2"]
    }

  user_prompt_template: "Extract entities from: {text}"

  input_column: text
  output_column: entities_json

  max_tokens: 500
  temperature: 0.0
```

## Configuration Options

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `asset_name` | string | Name of the output asset |
| `api_key` | string | Anthropic API key (use `${ANTHROPIC_API_KEY}`) |
| `max_tokens` | int | Maximum tokens in response (required by API) |

### Model Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model` | string | `claude-3-5-sonnet-20241022` | Model: claude-3-5-sonnet, claude-3-opus, claude-3-sonnet, claude-3-haiku |
| `temperature` | float | `1.0` | Response randomness (0.0-1.0) |
| `top_p` | float | `None` | Nucleus sampling parameter |
| `top_k` | int | `None` | Top-K sampling parameter |

### Prompt Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `system_prompt` | string | `None` | System prompt to set context (can be cached) |
| `user_prompt_template` | string | `None` | Template with `{column_name}` placeholders |
| `input_column` | string | `None` | Column containing input text |
| `output_column` | string | `claude_response` | Column name for responses |

### Performance & Cost

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | int | `10` | Rows to process in parallel |
| `rate_limit_delay` | float | `0.2` | Delay between API calls (seconds) |
| `max_retries` | int | `3` | Max retries for failed requests |
| `enable_caching` | bool | `true` | Cache responses |
| `cache_dir` | string | `/tmp/anthropic_llm_cache` | Cache directory |
| `enable_prompt_caching` | bool | `false` | Enable prompt caching (90% savings) |
| `track_costs` | bool | `true` | Track and report costs |

### Advanced Features

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stream` | bool | `false` | Use streaming (single prompts only) |
| `tools` | string | `None` | JSON array of tool definitions |
| `tool_choice` | string | `None` | Control tool use: `auto`, `any`, or tool name |

## Cost Optimization Tips

1. **Use Haiku for Simple Tasks**: Claude 3 Haiku is 60x cheaper than Opus
2. **Enable Prompt Caching**: Save 90% on repeated contexts (great for system prompts)
3. **Lower Temperature**: Use `temperature: 0.0` for deterministic, cacheable responses
4. **Set Appropriate max_tokens**: Don't request more tokens than needed
5. **Batch Processing**: Process multiple items to amortize setup costs
6. **Monitor Costs**: Use `track_costs: true` to track spending

### Approximate Costs (per 1M tokens, as of 2024)

| Model | Input | Output | Cached Input (90% off) |
|-------|--------|--------|------------------------|
| Claude 3.5 Sonnet | $3.00 | $15.00 | $0.30 |
| Claude 3 Opus | $15.00 | $75.00 | $1.50 |
| Claude 3 Sonnet | $3.00 | $15.00 | $0.30 |
| Claude 3 Haiku | $0.25 | $1.25 | $0.03 |

## Prompt Caching Details

Anthropic's prompt caching can save 90% on input tokens for repeated contexts:

**How it works:**
1. System prompts are automatically marked for caching when `enable_prompt_caching: true`
2. First request: full cost for system prompt
3. Subsequent requests (within 5 minutes): 90% discount on cached tokens

**Best for:**
- Long system prompts with guidelines (e.g., 10K+ token instructions)
- Processing many documents with same instructions
- Repeated analysis with same context

**Example savings:**
- System prompt: 50K tokens @ $3/M = $0.15 per request
- With caching: 50K tokens @ $0.30/M = $0.015 per request
- **Savings: $0.135 per request (90%)**

## Integration with Other Components

### Upstream Components

Works with any component that outputs a DataFrame:
- CSV File Ingestion
- Database Query
- Document Text Extractor
- API Ingestion components

### Downstream Components

Process Claude outputs with:
- **DataFrame Transformer**: Parse structured responses
- **Sentiment Analyzer**: Analyze Claude's outputs
- **Database Writer**: Persist enriched data
- **Vector Store Writer**: Store for RAG

## Example Pipeline

```yaml
# 1. Load legal documents
- type: dagster_component_templates.CSVFileIngestionComponent
  attributes:
    asset_name: legal_contracts
    file_path: "data/contracts.csv"

# 2. Analyze with Claude
- type: dagster_component_templates.AnthropicLLMComponent
  attributes:
    asset_name: contract_analysis
    api_key: "${ANTHROPIC_API_KEY}"
    model: claude-3-5-sonnet-20241022
    system_prompt: "You are a legal analyst. Identify risks and obligations."
    user_prompt_template: "Contract:\n{contract_text}\n\nAnalyze for risks and key obligations."
    input_column: contract_text
    output_column: legal_analysis
    max_tokens: 3000
    enable_prompt_caching: true
    enable_caching: true

# 3. Store results
- type: dagster_component_templates.DuckDBTableWriterComponent
  attributes:
    asset_name: contracts_analyzed
    table_name: analyzed_contracts
```

## API Rate Limits

Anthropic rate limits vary by tier:

- **Free Tier**: 5 RPM, 25,000 TPM
- **Build Tier 1**: 50 RPM, 50,000 TPM
- **Build Tier 2**: 1,000 RPM, 100,000 TPM
- **Scale**: Custom limits

Configure `rate_limit_delay` and `batch_size` according to your tier.

## Error Handling

The component includes robust error handling:

1. **Rate Limit Errors**: Automatic retry with exponential backoff
2. **Transient Errors**: Retry up to `max_retries` times
3. **Invalid Responses**: Logged warnings
4. **Cache Errors**: Continues without cache if unavailable

## Metadata

The component provides rich metadata:

- `model`: Model used
- `rows_processed`: Number of rows processed
- `cache_hits`: Number of cached responses used
- `cache_hit_rate`: Percentage of cache hits
- `total_input_tokens`: Total input tokens used
- `total_output_tokens`: Total output tokens used
- `cache_creation_tokens`: Tokens written to prompt cache
- `cache_read_tokens`: Tokens read from prompt cache
- `prompt_cache_savings_usd`: Savings from prompt caching
- `estimated_cost_usd`: Estimated API cost in USD

## Best Practices

1. **Use Appropriate Models**: Haiku for simple tasks, Sonnet for complex, Opus for hardest tasks
2. **Enable Prompt Caching**: For long system prompts used repeatedly
3. **Leverage Context Window**: Claude supports up to 200K tokens
4. **Structure System Prompts**: Put stable instructions in system prompt (cacheable)
5. **Monitor Costs**: Always enable `track_costs`
6. **Test on Small Batches**: Validate prompts on small datasets first

## Troubleshooting

### High Costs

- Switch to Claude 3 Haiku for simpler tasks
- Enable prompt caching for repeated contexts
- Reduce `max_tokens`
- Use caching for deterministic tasks

### Rate Limit Errors

- Increase `rate_limit_delay`
- Decrease `batch_size`
- Upgrade Anthropic tier

### Inconsistent Responses

- Lower `temperature` for more deterministic output
- Make system prompt more specific
- Use examples in prompts

### Cache Not Working

- Ensure `cache_dir` is writable
- Check disk space
- Verify prompt consistency

## Support

For issues or questions:
- Anthropic API Documentation: https://docs.anthropic.com
- Dagster Documentation: https://docs.dagster.io
- Component Issues: File an issue in the component repository
