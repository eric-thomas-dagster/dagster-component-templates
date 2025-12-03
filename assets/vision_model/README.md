# Vision Model Component

Analyze images using GPT-4 Vision and Claude 3 models for OCR, image captioning, object detection, visual question answering, and content moderation.

## Overview

The Vision Model Component enables image understanding and analysis using state-of-the-art vision-language models from OpenAI and Anthropic. It processes images through multimodal LLMs to extract text, generate descriptions, answer questions about visual content, and perform other vision tasks.

## Features

- **Multiple Providers**:
  - OpenAI: GPT-4 Vision, GPT-4o, GPT-4o-mini
  - Anthropic: Claude 3 Opus, Sonnet, Haiku

- **Flexible Image Input**:
  - Image URLs (public or signed)
  - Local file paths
  - Base64-encoded images
  - Batch processing support

- **Analysis Capabilities**:
  - OCR and text extraction
  - Image captioning and descriptions
  - Object detection descriptions
  - Visual question answering
  - Chart and diagram interpretation
  - Content moderation
  - Multi-image analysis

- **Production Features**:
  - Detail level control (low/high/auto for GPT-4)
  - Cost tracking and optimization
  - Retry logic with exponential backoff
  - Rate limiting
  - Image metadata extraction
  - Batch processing

## Use Cases

### 1. Document OCR & Text Extraction
Extract text from receipts, invoices, documents:
```yaml
type: dagster_component_templates.VisionModelComponent
attributes:
  asset_name: receipt_ocr
  source_asset: receipt_images
  provider: openai
  model: gpt-4o
  prompt: |
    Extract all text from this receipt/invoice.
    Return as structured JSON with:
    - merchant_name
    - date
    - total_amount
    - line_items (array of {item, quantity, price})
  image_column: receipt_url
  image_type: url
  max_tokens: 1000
  api_key: "${OPENAI_API_KEY}"
```

### 2. Product Image Analysis
Analyze product images for cataloging:
```yaml
type: dagster_component_templates.VisionModelComponent
attributes:
  asset_name: product_descriptions
  source_asset: product_images
  provider: openai
  model: gpt-4o
  prompt: |
    Describe this product in detail:
    1. Product category
    2. Color and materials
    3. Notable features
    4. Estimated condition (if used)
  image_column: image_path
  image_type: path
  detail_level: high
  api_key: "${OPENAI_API_KEY}"
```

### 3. Content Moderation
Screen images for inappropriate content:
```yaml
type: dagster_component_templates.VisionModelComponent
attributes:
  asset_name: moderated_images
  source_asset: user_uploaded_images
  provider: openai
  model: gpt-4o-mini  # Fast and cost-effective
  prompt: |
    Analyze this image for content safety.
    Respond with JSON:
    {
      "is_safe": true/false,
      "issues": ["list", "of", "issues"],
      "severity": "low/medium/high"
    }
  image_column: image_url
  temperature: 0.0  # Deterministic
  api_key: "${OPENAI_API_KEY}"
```

### 4. Visual Question Answering
Answer specific questions about images:
```yaml
type: dagster_component_templates.VisionModelComponent
attributes:
  asset_name: vqa_results
  source_asset: images_with_questions
  provider: anthropic
  model: claude-3-sonnet-20240229
  prompt: "{question}"  # Column substitution
  image_column: image_url
  api_key: "${ANTHROPIC_API_KEY}"
```

### 5. Chart & Diagram Analysis
Extract data from charts and diagrams:
```yaml
type: dagster_component_templates.VisionModelComponent
attributes:
  asset_name: chart_data
  source_asset: chart_images
  provider: openai
  model: gpt-4o
  prompt: |
    Analyze this chart/graph and extract:
    1. Chart type (line, bar, pie, etc.)
    2. Title and labels
    3. Data points and values
    4. Key insights
    Return as structured JSON.
  image_column: chart_url
  max_tokens: 1500
  api_key: "${OPENAI_API_KEY}"
```

### 6. Multi-Image Analysis
Compare or analyze multiple images together:
```yaml
type: dagster_component_templates.VisionModelComponent
attributes:
  asset_name: image_comparison
  source_asset: image_pairs
  provider: openai
  model: gpt-4o
  prompt: "Compare these two images and describe the differences."
  image_column: image_urls  # Column with list of URLs
  max_images_per_request: 2
  api_key: "${OPENAI_API_KEY}"
```

## Configuration

### Basic Configuration

```yaml
type: dagster_component_templates.VisionModelComponent
attributes:
  asset_name: image_analysis
  source_asset: images_df
  provider: openai
  model: gpt-4o
  prompt: "Describe this image."
  image_column: image_url
  api_key: "${OPENAI_API_KEY}"
```

### Full Configuration Options

```yaml
type: dagster_component_templates.VisionModelComponent
attributes:
  # Required
  asset_name: image_analysis_results
  source_asset: images_dataframe
  provider: openai  # openai | anthropic
  model: gpt-4o
  api_key: "${OPENAI_API_KEY}"
  prompt: "Describe this image in detail."
  
  # Image Configuration
  image_column: image_url
  image_type: url  # url | path | base64
  detail_level: high  # low | high | auto (GPT-4 only)
  max_images_per_request: 1  # 1-10 for multi-image analysis
  
  # Output Configuration
  output_column: vision_analysis
  
  # Model Parameters
  temperature: 0.3  # 0.0-1.0 (lower = more deterministic)
  max_tokens: 500
  
  # Performance
  batch_size: 5
  rate_limit_delay: 0.5  # Seconds between requests
  max_retries: 3
  
  # Cost & Metadata
  track_costs: true
  include_image_metadata: true  # Add image dimensions, format, size
  
  # Organization
  description: "Image analysis using vision models"
  group_name: "vision"
  include_sample_metadata: true
```

## Model Comparison

### OpenAI Models

#### GPT-4o (Recommended)
- **Best For**: General-purpose vision tasks, production use
- **Strengths**: Fast, accurate, cost-effective, multimodal
- **Cost**: $5/1M input tokens + $15/1M output tokens
- **Speed**: ~2-3 seconds per image
- **Context**: 128K tokens
- **Image Input**: URLs or base64

#### GPT-4 Vision Preview
- **Best For**: Complex visual reasoning
- **Strengths**: Highest accuracy, detailed analysis
- **Cost**: $10/1M input tokens + $30/1M output tokens
- **Speed**: ~3-5 seconds per image
- **Context**: 128K tokens
- **Note**: Being replaced by GPT-4o

#### GPT-4o-mini
- **Best For**: High-volume, cost-sensitive applications
- **Strengths**: Very fast, very cheap
- **Cost**: $0.15/1M input tokens + $0.6/1M output tokens
- **Speed**: ~1-2 seconds per image
- **Use Cases**: Content moderation, simple OCR, batch processing

### Anthropic Models

#### Claude 3 Opus
- **Best For**: Highest accuracy, complex analysis
- **Strengths**: Superior reasoning, detailed outputs
- **Cost**: $15/1M input tokens + $75/1M output tokens
- **Speed**: ~3-4 seconds per image
- **Context**: 200K tokens
- **Image Input**: Base64 only (component handles conversion)

#### Claude 3 Sonnet
- **Best For**: Balanced accuracy and cost
- **Strengths**: Good reasoning, fast, reliable
- **Cost**: $3/1M input tokens + $15/1M output tokens
- **Speed**: ~2-3 seconds per image
- **Context**: 200K tokens

#### Claude 3 Haiku
- **Best For**: Simple vision tasks, high volume
- **Strengths**: Very fast, very cheap
- **Cost**: $0.25/1M input tokens + $1.25/1M output tokens
- **Speed**: ~1-2 seconds per image
- **Context**: 200K tokens

## Image Input Types

### URL Images (`image_type: url`)
Images hosted on public or signed URLs:
```yaml
image_type: url
image_column: image_url  # Column with URLs
```
**Pros**: No preprocessing, fast
**Cons**: Images must be publicly accessible or use signed URLs
**Supported By**: OpenAI (native), Anthropic (component fetches and encodes)

### Local Path Images (`image_type: path`)
Images stored as local files:
```yaml
image_type: path
image_column: image_path  # Column with file paths
```
**Pros**: Works with local files, no hosting needed
**Cons**: Files must be accessible to Dagster process
**Component Behavior**: Auto-encodes to base64, detects MIME type

### Base64 Images (`image_type: base64`)
Pre-encoded base64 image strings:
```yaml
image_type: base64
image_column: image_base64  # Column with base64 strings
```
**Pros**: Full control over encoding
**Cons**: Large data in DataFrame
**Format**: `data:image/jpeg;base64,<encoded_data>` or just base64 string

## Detail Level (GPT-4 Only)

Controls image resolution sent to the model:

### `detail_level: low`
- **Resolution**: 512x512 (single tile)
- **Tokens**: ~85 tokens per image
- **Cost**: Lowest
- **Best For**: Simple tasks (is it a cat/dog?), content moderation
- **Speed**: Fastest

### `detail_level: high`
- **Resolution**: Up to 2048x2048 (multiple 512x512 tiles)
- **Tokens**: 170 tokens + (tiles × 170)
- **Cost**: Higher (proportional to tiles)
- **Best For**: OCR, detailed analysis, small text
- **Speed**: Slower

### `detail_level: auto` (Default)
- **Behavior**: Model decides based on image dimensions
- **Best For**: Mixed use cases
- **Recommendation**: Use this unless you have specific needs

## Cost Optimization

### 1. Choose the Right Model
```yaml
# For simple tasks (moderation, basic OCR)
model: gpt-4o-mini  # 10-20x cheaper than GPT-4o

# For balanced performance
model: gpt-4o  # Best price/performance

# For complex reasoning
model: claude-3-opus-20240229  # Highest accuracy
```

### 2. Optimize Detail Level
```yaml
# For low-resolution tasks
detail_level: low  # 85 tokens vs 170+ tokens

# For OCR and fine details
detail_level: high  # Worth the extra cost
```

### 3. Batch Processing
```yaml
# Process multiple images efficiently
batch_size: 10  # Balance throughput and memory
rate_limit_delay: 0.2  # Reduce delay for higher throughput
```

### 4. Limit Token Usage
```yaml
# Control output length
max_tokens: 300  # Enough for most descriptions
temperature: 0.3  # More focused responses = fewer tokens
```

### 5. Use Multi-Image Analysis
```yaml
# Analyze multiple images in one request (when appropriate)
max_images_per_request: 3  # Cheaper than 3 separate requests
```

## Cost Estimates

### Example: 10,000 Product Images (GPT-4o, high detail)
- **Input**: 10,000 images × 500 tokens = 5M tokens
- **Output**: 10,000 descriptions × 200 tokens = 2M tokens
- **Cost**: (5M × $5) + (2M × $15) / 1M = $55

### Example: 100,000 Receipt OCR (GPT-4o-mini, low detail)
- **Input**: 100,000 images × 100 tokens = 10M tokens
- **Output**: 100,000 extractions × 150 tokens = 15M tokens
- **Cost**: (10M × $0.15) + (15M × $0.6) / 1M = $10.50

### Example: 1,000 Complex Analysis (Claude 3 Opus)
- **Input**: 1,000 images × 800 tokens = 0.8M tokens
- **Output**: 1,000 analyses × 400 tokens = 0.4M tokens
- **Cost**: (0.8M × $15) + (0.4M × $75) / 1M = $42

## Prerequisites

1. **API Keys**:
   - OpenAI: [platform.openai.com](https://platform.openai.com/)
   - Anthropic: [console.anthropic.com](https://console.anthropic.com/)

2. **Python Packages**:
   - `openai>=1.0.0`
   - `anthropic>=0.18.0`
   - `pillow>=10.0.0` (for image metadata)
   - `requests>=2.31.0` (for URL fetching)

3. **Image Requirements**:
   - **Formats**: JPEG, PNG, GIF, WebP
   - **Max Size**: 20MB (OpenAI), 5MB per image (Claude)
   - **Max Dimensions**: No hard limit, but high-res images use more tokens
   - **URLs**: Must be publicly accessible or use signed URLs

## Performance Tuning

### Optimize Throughput
```yaml
batch_size: 20  # Process more images in parallel
rate_limit_delay: 0.1  # Reduce delay (watch for rate limits)
max_retries: 5  # Handle transient errors
```

### Optimize Latency
```yaml
model: gpt-4o-mini  # Fastest model
detail_level: low  # Faster processing
max_tokens: 200  # Shorter responses
```

### Optimize Accuracy
```yaml
model: claude-3-opus-20240229  # Best reasoning
detail_level: high  # See fine details
temperature: 0.1  # More deterministic
max_tokens: 1000  # Allow detailed responses
```

## Error Handling

The component handles common errors:

1. **Rate Limits**: Exponential backoff with configurable retries
2. **Invalid Images**: Logged as errors, processing continues
3. **API Timeouts**: Automatic retries
4. **Missing API Keys**: Clear error message
5. **Unsupported Formats**: Helpful error with supported formats

## Advanced Examples

### Structured JSON Output
```yaml
prompt: |
  Extract information from this image and return as JSON:
  {
    "type": "product/document/person/other",
    "description": "detailed description",
    "attributes": {
      "color": "...",
      "material": "...",
      "condition": "..."
    },
    "text_detected": ["list", "of", "text"]
  }
temperature: 0.0  # Deterministic for structured output
```

### Conditional Analysis with Column Substitution
```yaml
prompt: "Answer this question about the image: {user_question}"
# Assumes DataFrame has 'user_question' column
```

### Multi-Language OCR
```yaml
prompt: |
  Extract all text from this image.
  Detect the language and provide translation to English if not English.
  Return as: {"original_text": "...", "language": "...", "translation": "..."}
model: gpt-4o  # Good multilingual support
```

## Troubleshooting

### Issue: High Cost
**Solution**: Use `gpt-4o-mini` or `detail_level: low`, reduce `max_tokens`

### Issue: Poor OCR Accuracy
**Solution**: Use `detail_level: high`, increase image resolution, use GPT-4o or Claude 3 Opus

### Issue: Slow Processing
**Solution**: Increase `batch_size`, reduce `rate_limit_delay`, use faster model (gpt-4o-mini)

### Issue: Images Not Loading (URLs)
**Solution**: Verify URLs are publicly accessible, use signed URLs, or switch to `image_type: path`

### Issue: Out of Memory
**Solution**: Reduce `batch_size`, process images in smaller chunks

## Best Practices

1. **Choose Model Based on Task**: Use gpt-4o-mini for simple tasks, GPT-4o/Claude for complex
2. **Optimize Detail Level**: Use `low` for classification, `high` for OCR
3. **Write Clear Prompts**: Specific instructions get better results
4. **Structure Output**: Request JSON for downstream processing
5. **Monitor Costs**: Track usage, especially with large batches
6. **Test on Sample**: Start with small batch to validate before full run
7. **Use Appropriate Temperature**: 0.0-0.3 for factual tasks, 0.5-0.7 for creative

## Related Components

- **openai_llm**: Text-only LLM processing
- **anthropic_llm**: Text-only Claude processing
- **document_chunker**: Prepare documents for vision analysis
- **reranker**: Rank vision analysis results by relevance

## References

- [GPT-4 Vision Documentation](https://platform.openai.com/docs/guides/vision)
- [Claude 3 Vision Documentation](https://docs.anthropic.com/claude/docs/vision)
- [Vision Model Pricing](https://openai.com/pricing)
- [Best Practices for Vision Models](https://cookbook.openai.com/examples/gpt_with_vision_for_video_understanding)

## Changelog

### v1.0.0
- Initial release
- Support for OpenAI GPT-4 Vision and GPT-4o
- Support for Claude 3 Opus, Sonnet, Haiku
- URL, path, and base64 image input
- Detail level control
- Multi-image analysis
- Cost tracking and optimization
- Image metadata extraction
