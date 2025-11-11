# LLM Prompt Executor Asset

Execute prompts against LLM APIs (OpenAI, Anthropic, Cohere, Hugging Face) using Dagster's IO manager pattern. Accepts prompt text from upstream assets and materializes LLM responses - just connect prompt-generating assets!

## Overview

This component executes prompts automatically passed from upstream assets via IO managers. Simply draw connections in the visual editor - no configuration needed for dependencies!

**Compatible with:**
- Text File Readers (output: string)
- Prompt Template Components (output: string or dict)
- User Input Components (output: string or dict)
- Any component that outputs text or dict with 'prompt'/'text' keys

Perfect for:
- Content generation (marketing copy, documentation)
- Code generation and review
- Data analysis and summarization
- Question answering
- Translation and text transformation
- Building AI-powered data pipelines

## Features

- **Multiple Providers**: OpenAI, Anthropic, Cohere, Hugging Face
- **Streaming Support**: Real-time response streaming
- **System Prompts**: Set context and behavior for the LLM
- **Temperature Control**: Fine-tune creativity vs consistency
- **Structured Outputs**: JSON mode with schema validation
- **Response Caching**: Cache responses to avoid redundant API calls
- **Token Limits**: Configure max tokens for responses
- **Format Options**: Text, JSON, or Markdown outputs

## Input and Output

### Input Requirements

This component accepts prompts from upstream assets via IO managers. The input can be:
- **String**: Direct prompt text (e.g., "Summarize this document")
- **Dict with 'prompt' key**: `{"prompt": "Your prompt here"}`
- **Dict with 'text' key**: `{"text": "Your prompt here"}`

### Output Format

The component outputs the LLM response in the specified format:
- **text** (default): Plain string response
- **json**: Parsed JSON dict (when `response_format: json`)
- **markdown**: Formatted markdown string

## Configuration

### Required Parameters

- **asset_name** (string) - Name of the asset
- **provider** (string) - LLM provider: `"openai"`, `"anthropic"`, `"cohere"`, `"huggingface"`
- **model** (string) - Model name (e.g., `"gpt-4"`, `"claude-3-5-sonnet-20241022"`)

### Optional Parameters

- **prompt** (string) - Static prompt (if not receiving from upstream assets)
- **system_prompt** (string) - System prompt to set context
- **temperature** (number) - Response randomness 0.0-2.0 (default: `0.7`)
- **max_tokens** (integer) - Maximum tokens in response
- **api_key** (string) - API key (use `${API_KEY}` for environment variable)
- **response_format** (string) - Format: `"text"`, `"json"`, `"markdown"` (default: `"text"`)
- **json_schema** (string) - JSON schema for structured output
- **streaming** (boolean) - Use streaming responses (default: `false`)
- **cache_responses** (boolean) - Cache LLM responses (default: `false`)
- **cache_path** (string) - Path to cache file
- **description** (string) - Asset description
- **group_name** (string) - Asset group

## Usage Examples

### Basic GPT-4 Prompt

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: product_description
  provider: openai
  model: gpt-4
  prompt: "Generate a creative product description for a new smartwatch with health tracking features."
  api_key: ${OPENAI_API_KEY}
```

### With System Prompt

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: code_review
  provider: openai
  model: gpt-4
  system_prompt: "You are an expert code reviewer focusing on security and performance."
  prompt: "Review this Python function for potential issues: ${CODE_SNIPPET}"
  temperature: 0.3
  api_key: ${OPENAI_API_KEY}
```

### Claude with Temperature Control

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: creative_writing
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  prompt: "Write a short story about space exploration"
  system_prompt: "You are a creative science fiction writer."
  temperature: 0.9
  max_tokens: 2000
  api_key: ${ANTHROPIC_API_KEY}
```

### Structured JSON Output

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: sentiment_analysis
  provider: openai
  model: gpt-4
  prompt: "Analyze the sentiment of this text: ${TEXT}"
  response_format: json
  json_schema: '{"type": "object", "properties": {"sentiment": {"type": "string"}, "score": {"type": "number"}, "reasoning": {"type": "string"}}}'
  api_key: ${OPENAI_API_KEY}
```

### With Streaming

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: story_generator
  provider: openai
  model: gpt-4
  prompt: "Write a detailed story about ${TOPIC}"
  streaming: true
  temperature: 0.8
  api_key: ${OPENAI_API_KEY}
```

### Cohere for Classification

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: topic_classification
  provider: cohere
  model: command-r-plus
  prompt: "Classify this text into one of these topics: technology, sports, politics, entertainment: ${TEXT}"
  temperature: 0.2
  api_key: ${COHERE_API_KEY}
```

### With Response Caching

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: cached_analysis
  provider: openai
  model: gpt-4
  prompt: "Analyze market trends for Q4 2024"
  cache_responses: true
  cache_path: /tmp/llm_cache.json
  api_key: ${OPENAI_API_KEY}
```

## Example Pipeline with IO Manager Pattern

### Pipeline 1: Text Processing Chain

```yaml
# Step 1: Read prompt from file or template
type: dagster_component_templates.TextFileReaderComponent
attributes:
  asset_name: user_prompt
  file_path: /data/prompts/analysis_prompt.txt

# Step 2: Execute prompt with LLM (automatically receives prompt from user_prompt)
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: llm_response
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}
  temperature: 0.7
```

**Visual Connection:**
```
user_prompt → llm_response
```

The IO manager automatically passes the prompt text from `user_prompt` to `llm_response`.

### Pipeline 2: Document Analysis

```yaml
# Step 1: Extract text from document
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: document_text
  file_path: /data/documents/report.pdf

# Step 2: Generate analysis prompt
type: dagster_component_templates.PromptTemplateComponent
attributes:
  asset_name: analysis_prompt
  template: "Analyze this document and extract key insights: {text}"

# Step 3: Execute LLM analysis
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: analysis_result
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  api_key: ${ANTHROPIC_API_KEY}
  temperature: 0.5
  max_tokens: 2000
```

**Visual Connections:**
```
document_text → analysis_prompt → analysis_result
```

### Pipeline 3: JSON Output for Structured Data

```yaml
# Step 1: Create prompt
type: dagster_component_templates.StaticTextComponent
attributes:
  asset_name: sentiment_prompt
  text: "Analyze the sentiment of this customer feedback: 'The product is amazing but shipping was slow'"

# Step 2: Execute with structured JSON output
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: sentiment_analysis
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}
  response_format: json
  json_schema: '{"type": "object", "properties": {"sentiment": {"type": "string", "enum": ["positive", "negative", "neutral"]}, "score": {"type": "number", "minimum": 0, "maximum": 1}, "reasoning": {"type": "string"}}}'
  temperature: 0.3
```

**Visual Connection:**
```
sentiment_prompt → sentiment_analysis
```

Output format: `{"sentiment": "positive", "score": 0.75, "reasoning": "..."}`

### Pipeline 4: Multi-Language Translation

```yaml
# Step 1: Source text
type: dagster_component_templates.TextFileReaderComponent
attributes:
  asset_name: english_text
  file_path: /data/content/english.txt

# Step 2: Translate to Spanish
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: spanish_translation
  provider: openai
  model: gpt-4
  system_prompt: "You are a professional translator. Translate accurately while preserving tone and context."
  prompt: "Translate the following text to Spanish: {text}"
  api_key: ${OPENAI_API_KEY}
  temperature: 0.3

# Step 3: Translate to French
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: french_translation
  provider: openai
  model: gpt-4
  system_prompt: "You are a professional translator. Translate accurately while preserving tone and context."
  prompt: "Translate the following text to French: {text}"
  api_key: ${OPENAI_API_KEY}
  temperature: 0.3
```

**Visual Connections:**
```
english_text → spanish_translation
english_text → french_translation
```

## Provider-Specific Configuration

### OpenAI

```yaml
provider: openai
model: gpt-4  # or gpt-4-turbo, gpt-3.5-turbo
api_key: ${OPENAI_API_KEY}
```

**Popular Models:**
- `gpt-4` - Most capable, higher cost
- `gpt-4-turbo` - Faster, lower cost
- `gpt-3.5-turbo` - Fast, economical

### Anthropic

```yaml
provider: anthropic
model: claude-3-5-sonnet-20241022  # or claude-3-opus, claude-3-haiku
api_key: ${ANTHROPIC_API_KEY}
```

**Popular Models:**
- `claude-3-5-sonnet-20241022` - Balanced performance
- `claude-3-opus-20240229` - Most capable
- `claude-3-haiku-20240307` - Fastest, economical

### Cohere

```yaml
provider: cohere
model: command-r-plus  # or command-r, command
api_key: ${COHERE_API_KEY}
```

### Hugging Face

```yaml
provider: huggingface
model: meta-llama/Llama-2-70b-chat-hf
api_key: ${HUGGINGFACE_TOKEN}
```

## Temperature Guide

Control the randomness/creativity of responses:

| Temperature | Use Case | Description |
|------------|----------|-------------|
| 0.0 - 0.3 | Factual, deterministic | Code generation, data extraction, classification |
| 0.4 - 0.7 | Balanced | General purpose, Q&A, summarization |
| 0.8 - 1.0 | Creative | Content generation, brainstorming, storytelling |
| 1.0+ | Highly creative | Experimental, very diverse outputs |

## Response Formats

### Text (Default)

Plain text response:
```yaml
response_format: text
```

Returns the raw text response from the LLM.

### JSON

Structured JSON output:
```yaml
response_format: json
```

The LLM will return valid JSON. Use with `json_schema` for validation.

### Markdown

Markdown-formatted response:
```yaml
response_format: markdown
```

Useful for documentation generation or formatted content.

## JSON Schema Validation

Define the structure of JSON responses:

```yaml
json_schema: '{
  "type": "object",
  "properties": {
    "summary": {"type": "string"},
    "key_points": {"type": "array", "items": {"type": "string"}},
    "confidence": {"type": "number", "minimum": 0, "maximum": 1}
  },
  "required": ["summary", "key_points"]
}'
```

## Common Use Cases

### 1. Content Generation

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: blog_post
  provider: openai
  model: gpt-4
  system_prompt: "You are a technical blog writer."
  prompt: "Write a blog post about ${TOPIC} aimed at ${AUDIENCE}"
  temperature: 0.7
  max_tokens: 2000
  api_key: ${OPENAI_API_KEY}
  description: Generate blog content
  group_name: content
```

### 2. Code Generation

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: generated_code
  provider: openai
  model: gpt-4
  system_prompt: "You are an expert Python developer. Write clean, documented code."
  prompt: "Write a Python function that ${REQUIREMENT}"
  temperature: 0.2
  response_format: markdown
  api_key: ${OPENAI_API_KEY}
  group_name: code_gen
```

### 3. Data Analysis

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: data_insights
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  prompt: "Analyze this dataset and provide key insights: ${DATA_SUMMARY}"
  temperature: 0.5
  max_tokens: 1500
  api_key: ${ANTHROPIC_API_KEY}
  group_name: analytics
```

### 4. Translation

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: translated_text
  provider: openai
  model: gpt-4
  prompt: "Translate this text to ${TARGET_LANGUAGE}: ${TEXT}"
  temperature: 0.3
  api_key: ${OPENAI_API_KEY}
  group_name: translation
```

### 5. Sentiment Analysis

```yaml
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: sentiment_score
  provider: openai
  model: gpt-4
  prompt: "Analyze the sentiment and return a JSON object: ${TEXT}"
  response_format: json
  json_schema: '{"type": "object", "properties": {"sentiment": {"type": "string", "enum": ["positive", "negative", "neutral"]}, "confidence": {"type": "number"}}}'
  temperature: 0.1
  api_key: ${OPENAI_API_KEY}
  group_name: nlp
```

## Prompt Engineering Tips

### 1. Be Specific

❌ Bad: "Write about AI"
✅ Good: "Write a 300-word introduction to machine learning for beginners"

### 2. Provide Context

```yaml
system_prompt: "You are a financial analyst with 10 years of experience."
prompt: "Analyze this company's Q4 performance: ${FINANCIAL_DATA}"
```

### 3. Use Examples

```yaml
prompt: |
  Classify these emails as spam or not spam.

  Example 1: "Congratulations! You won $1000000!" -> spam
  Example 2: "Meeting at 3pm tomorrow" -> not spam

  Now classify: "${EMAIL_TEXT}"
```

### 4. Specify Format

```yaml
prompt: "List 5 key features of ${PRODUCT}. Format as a numbered list."
```

## Environment Variables

Set API keys as environment variables:

```bash
# OpenAI
export OPENAI_API_KEY="sk-..."

# Anthropic
export ANTHROPIC_API_KEY="sk-ant-..."

# Cohere
export COHERE_API_KEY="..."

# Hugging Face
export HUGGINGFACE_TOKEN="hf_..."
```

Reference in configuration:
```yaml
api_key: ${OPENAI_API_KEY}
```

## Error Handling

The component handles common errors:

- **Invalid API Key**: Logs error and raises ValueError
- **Rate Limits**: Raised by provider SDK
- **Model Not Found**: Raised with clear error message
- **JSON Parse Errors**: Falls back to text with warning
- **Network Errors**: Logged and raised

## Metadata

Each materialization includes metadata:
- Provider and model used
- Temperature setting
- Response length in characters
- Streaming status
- Max tokens (if configured)

## Troubleshooting

### Issue: "API key not found"

**Solution**: Ensure environment variable is set:
```bash
export OPENAI_API_KEY="your-key-here"
```

### Issue: "Rate limit exceeded"

**Solution**:
- Reduce request frequency
- Upgrade API plan
- Add retry logic with exponential backoff

### Issue: "Response is not valid JSON"

**Solution**:
- Check that `response_format: json` is set
- Verify `json_schema` is valid
- Try adjusting the prompt to explicitly request JSON

### Issue: "Token limit exceeded"

**Solution**: Reduce prompt length or increase `max_tokens`:
```yaml
max_tokens: 4096
```

## Performance Tips

1. **Use Appropriate Models**: gpt-3.5-turbo for simple tasks, gpt-4 for complex reasoning
2. **Enable Caching**: Cache responses for repeated prompts
3. **Lower Temperature**: Use 0.0-0.3 for deterministic, faster responses
4. **Limit Tokens**: Set `max_tokens` to reduce cost and latency
5. **Batch Operations**: Process multiple prompts in parallel when possible

## Cost Optimization

1. **Choose Right Model**: Balance capability vs cost
   - gpt-3.5-turbo: ~$0.002/1K tokens
   - gpt-4: ~$0.03/1K tokens
   - claude-3-haiku: ~$0.0025/1K tokens

2. **Limit Output**: Use `max_tokens` to control costs
3. **Cache Results**: Enable caching for repeated operations
4. **Shorter Prompts**: Be concise while maintaining clarity

## Security Best Practices

1. **Use Environment Variables**: Never hard-code API keys
```yaml
api_key: ${OPENAI_API_KEY}
```

2. **Rotate Keys**: Regularly rotate API keys

3. **Monitor Usage**: Track API usage and set alerts

4. **Limit Permissions**: Use API keys with minimum required permissions

5. **Sanitize Inputs**: Validate and sanitize user inputs in prompts

## Requirements

- openai >= 1.0.0
- anthropic >= 0.18.0
- cohere >= 4.0.0
- huggingface-hub >= 0.20.0

## Contributing

Found a bug or have a feature request?
- GitHub Issues: https://github.com/eric-thomas-dagster/dagster-component-templates/issues

## License

MIT License
