# Image Classifier Component

Classify images using pre-trained models. Supports CLIP zero-shot classification with custom candidate labels and torchvision models (ResNet50, EfficientNet) with ImageNet labels. Returns top-N predicted labels and confidence scores.

## Overview

The Image Classifier Component loads a pre-trained vision model and classifies each image in the upstream DataFrame. For CLIP models, it performs zero-shot classification against a configurable list of candidate labels. For torchvision models, it uses ImageNet class labels.

## Features

- **CLIP zero-shot**: Classify against any custom label list without fine-tuning
- **torchvision models**: ResNet50 and EfficientNet with 1000 ImageNet classes
- **Top-k predictions**: Return multiple predictions with scores
- **Full predictions column**: Optional list of all `{label, score}` pairs
- **Device support**: CPU, CUDA, and Apple MPS

## Use Cases

1. **Product categorization**: Tag e-commerce images with category labels
2. **Content moderation**: Classify images for policy compliance
3. **Dataset labeling**: Automatically label image collections
4. **Visual search indexing**: Generate labels for image search indexes

## Prerequisites

- `transformers>=4.30.0`, `Pillow>=9.0.0`, `torch>=2.0.0`

## Configuration

### CLIP Zero-shot with Custom Labels

```yaml
type: dagster_component_templates.ImageClassifierComponent
attributes:
  asset_name: product_categories
  upstream_asset_key: product_images
  image_column: image_path
  model_name: openai/clip-vit-base-patch32
  candidate_labels:
    - electronics
    - clothing
    - furniture
    - food
    - toys
```

### ResNet50 ImageNet Classification

```yaml
type: dagster_component_templates.ImageClassifierComponent
attributes:
  asset_name: imagenet_labels
  upstream_asset_key: raw_images
  image_column: file_path
  model_name: resnet50
  top_k: 5
  all_predictions_column: all_predictions
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `image_column` | string | required | Column with image paths or URLs |
| `output_column` | string | `predicted_class` | Top label column |
| `score_column` | string | `confidence_score` | Top confidence column |
| `top_k` | integer | `3` | Number of top predictions |
| `all_predictions_column` | string | `None` | Column for all predictions |
| `model_name` | string | `openai/clip-vit-base-patch32` | Model ID |
| `candidate_labels` | list | `None` | CLIP zero-shot label list |
| `device` | string | `cpu` | cpu, cuda, or mps |
| `group_name` | string | `None` | Asset group |

## Output

Original DataFrame plus `output_column`, `score_column`, and optionally `all_predictions_column`.
