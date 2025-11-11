# Text Classifier Asset

Classify text using LLMs with custom categories, confidence scores, and reasoning explanations.

## Overview

This component classifies text automatically passed from upstream assets via IO managers. Simply draw connections in the visual editor - no configuration needed for dependencies!

**Compatible with:**
- Document Text Extractor
- Document Summarizer
- Text Chunker
- Any text-producing asset (string output or dict with 'text' key)

Perfect for sentiment analysis, topic classification, and content moderation.

## Quick Start

### 1. Extract Text from Document

```yaml
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: feedback_text
  file_path: /path/to/feedback.txt
```

### 2. Classify the Text

```yaml
# No upstream_asset_key needed - set by drawing connections!
type: dagster_component_templates.TextClassifierComponent
attributes:
  asset_name: sentiment
  provider: openai
  model: gpt-4
  categories: '["positive", "negative", "neutral"]'
  classification_task: sentiment analysis
  api_key: ${OPENAI_API_KEY}
```

### 3. Draw Connection

In the visual editor: `feedback_text` → `sentiment`

That's it! The IO manager automatically passes the text.

## Features

- **Visual Dependencies**: Draw connections, no manual configuration
- **Custom Categories**: Define any categories
- **Confidence Scores**: Get classification confidence
- **Reasoning**: Optional explanation of classification
- **JSON Output**: Structured classification results
- **Multiple Providers**: OpenAI, Anthropic
- **Low Temperature**: Consistent, deterministic results

## Input Requirements

**Accepts text from upstream assets via IO manager:**
- String content (plain text)
- Dict with 'text' key (e.g., `{"text": "content here"}`)
- Any text-producing asset

## Output Format

**Returns dict with classification results:**
```json
{
  "category": "positive",
  "confidence": 0.95,
  "reasoning": "Text expresses clear satisfaction and enthusiasm"
}
```

## Configuration

### Required
- **asset_name** (string), **provider** (string), **model** (string)
- **categories** (string) - JSON array of categories

### Optional
- **classification_task** (string) - Task description (default: `"classification"`)
- **include_confidence** (boolean) - default: `true`
- **include_reasoning** (boolean) - default: `false`
- **api_key** (string) - `${API_KEY}`
- **temperature** (number) - default: `0.1` (low for consistency)

## Example Pipeline

```yaml
# Step 1: Extract text from document
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: feedback_text
  file_path: /path/to/feedback.txt

# Step 2: Classify the sentiment
type: dagster_component_templates.TextClassifierComponent
attributes:
  asset_name: sentiment
  provider: openai
  model: gpt-4
  categories: '["positive", "negative", "neutral"]'
  classification_task: sentiment analysis
  include_confidence: true
  include_reasoning: true
  api_key: ${OPENAI_API_KEY}
```

**Visual Connections:**
```
feedback_text → sentiment
```

## Alternative Pipeline: Extract, Summarize, Then Classify

```yaml
# Step 1: Extract text
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: report_text
  file_path: /path/to/report.pdf

# Step 2: Summarize
type: dagster_component_templates.DocumentSummarizerComponent
attributes:
  asset_name: report_summary
  provider: openai
  model: gpt-4
  summary_type: concise
  api_key: ${OPENAI_API_KEY}

# Step 3: Classify summary topic
type: dagster_component_templates.TextClassifierComponent
attributes:
  asset_name: report_topic
  provider: openai
  model: gpt-4
  categories: '["technology", "finance", "healthcare", "education"]'
  classification_task: topic classification
  api_key: ${OPENAI_API_KEY}
```

**Visual Connections:**
```
report_text → report_summary → report_topic
```

## Standalone Examples

### Sentiment Analysis
```yaml
type: dagster_component_templates.TextClassifierComponent
attributes:
  asset_name: sentiment
  provider: openai
  model: gpt-4
  categories: '["positive", "negative", "neutral"]'
  classification_task: sentiment analysis
  api_key: ${OPENAI_API_KEY}
```

### Topic Classification with Reasoning
```yaml
type: dagster_component_templates.TextClassifierComponent
attributes:
  asset_name: topic
  provider: openai
  model: gpt-4
  categories: '["technology", "sports", "politics", "entertainment", "business"]'
  classification_task: topic classification
  include_reasoning: true
  api_key: ${OPENAI_API_KEY}
```

### Content Moderation
```yaml
type: dagster_component_templates.TextClassifierComponent
attributes:
  asset_name: content_flag
  provider: openai
  model: gpt-4
  categories: '["safe", "spam", "inappropriate", "harmful"]'
  classification_task: content moderation
  temperature: 0.0
  api_key: ${OPENAI_API_KEY}
```

## Requirements
- openai >= 1.0.0, anthropic >= 0.18.0

## License
MIT License
