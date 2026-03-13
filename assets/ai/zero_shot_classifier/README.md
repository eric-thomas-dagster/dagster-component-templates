# Zero Shot Classifier Component

Classify text into arbitrary categories using HuggingFace zero-shot classification models. No labelled training data required.

## Overview

The Zero Shot Classifier Component uses Natural Language Inference (NLI) models to assign text to any set of categories you define. Because zero-shot classification requires no fine-tuning, you can experiment with new label sets instantly.

## Features

- **No Training Data**: Define categories at configuration time
- **Multi-label Mode**: Allow texts to belong to multiple categories
- **Per-label Scores**: Confidence scores for every candidate label
- **Batch Processing**: Configurable batch size for GPU/CPU throughput
- **Any NLI Model**: Works with any HuggingFace zero-shot-classification model

## Use Cases

1. **Support Ticket Routing**: Classify tickets by department
2. **Content Moderation**: Flag harmful, spam, or off-topic content
3. **Intent Detection**: Identify user intent in messages
4. **Topic Tagging**: Assign topic labels to articles
5. **Urgency Scoring**: Route critical issues first

## Prerequisites

- `transformers>=4.30.0`, `torch>=2.0.0`, `pandas>=1.5.0`
- Internet access to download model weights on first run

## Configuration

### Basic Classification

```yaml
type: dagster_component_templates.ZeroShotClassifierComponent
attributes:
  asset_name: classified_emails
  upstream_asset_key: raw_emails
  text_column: email_body
  candidate_labels:
    - "sales"
    - "support"
    - "spam"
    - "other"
  output_column: email_category
  output_scores: true
```

### Multi-label Classification

```yaml
type: dagster_component_templates.ZeroShotClassifierComponent
attributes:
  asset_name: tagged_articles
  upstream_asset_key: raw_articles
  text_column: content
  candidate_labels:
    - "technology"
    - "science"
    - "politics"
    - "sports"
  multi_label: true
  output_column: primary_topic
```

### Lightweight Model

```yaml
type: dagster_component_templates.ZeroShotClassifierComponent
attributes:
  asset_name: fast_classified
  upstream_asset_key: raw_text
  text_column: body
  candidate_labels:
    - "positive"
    - "negative"
    - "neutral"
  model_name: "cross-encoder/nli-MiniLM2-L6-H768"
  batch_size: 32
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `text_column` | string | required | Column with text |
| `candidate_labels` | list | required | Categories to classify into |
| `model_name` | string | `facebook/bart-large-mnli` | HuggingFace model |
| `output_column` | string | `predicted_label` | Column for top label |
| `output_scores` | bool | `true` | Add per-label score columns |
| `multi_label` | bool | `false` | Allow multiple labels |
| `batch_size` | int | `8` | Inference batch size |
| `group_name` | string | `None` | Asset group |

## Output Schema

| Column | Description |
|--------|-------------|
| `predicted_label` | Top predicted label |
| `score_{label}` | Confidence score per label (if output_scores=true) |

## Recommended Models

| Model | Speed | Quality | Notes |
|-------|-------|---------|-------|
| `facebook/bart-large-mnli` | Medium | High | Default, general purpose |
| `cross-encoder/nli-MiniLM2-L6-H768` | Fast | Good | Smaller, faster |
| `typeform/distilbart-mnli-12-3` | Fast | Good | Distilled BART |

## Troubleshooting

- **Slow inference**: Increase `batch_size`, use a smaller model, or enable GPU
- **ImportError**: Run `pip install transformers torch`
- **Low confidence**: Try rephrasing candidate labels to be more descriptive
