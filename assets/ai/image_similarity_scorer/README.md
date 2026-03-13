# Image Similarity Scorer Component

Score visual similarity between images using CLIP embeddings and cosine similarity. Compare two image columns pairwise or compare all images against a fixed reference image.

## Overview

The Image Similarity Scorer Component encodes images using a CLIP model to produce high-dimensional embeddings, then computes cosine similarity between pairs. Scores range from 0 (completely dissimilar) to 1 (identical).

## Features

- **CLIP embeddings**: State-of-the-art visual similarity using contrastive learning
- **Pairwise comparison**: Compare two image columns row by row
- **Reference comparison**: Compare all images against a fixed reference
- **Cosine similarity**: Normalized 0-1 similarity score
- **Device support**: CPU, CUDA, and Apple MPS

## Use Cases

1. **Duplicate detection**: Find near-duplicate images in large datasets
2. **Product matching**: Compare product images across catalogs
3. **Before/after comparison**: Measure visual change between image pairs
4. **Quality control**: Compare production images against reference standards

## Prerequisites

- `transformers>=4.30.0`, `Pillow>=9.0.0`, `torch>=2.0.0`

## Configuration

### Pairwise Comparison

```yaml
type: dagster_component_templates.ImageSimilarityScorerComponent
attributes:
  asset_name: similarity_scores
  upstream_asset_key: image_pairs
  image_column_a: original_image
  image_column_b: candidate_image
  output_column: similarity_score
```

### Compare Against Reference Image

```yaml
type: dagster_component_templates.ImageSimilarityScorerComponent
attributes:
  asset_name: reference_similarity
  upstream_asset_key: product_images
  image_column_a: product_image
  reference_image: /data/reference_product.jpg
  output_column: similarity_to_reference
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `image_column_a` | string | required | First image column |
| `image_column_b` | string | `None` | Second image column |
| `reference_image` | string | `None` | Fixed reference image path |
| `output_column` | string | `image_similarity` | Similarity score column |
| `model_name` | string | `openai/clip-vit-base-patch32` | CLIP model ID |
| `device` | string | `cpu` | cpu, cuda, or mps |
| `group_name` | string | `None` | Asset group |

## Output

Original DataFrame plus `output_column` with cosine similarity scores (0-1).

## Notes

Either `image_column_b` or `reference_image` must be provided. If both are provided, `reference_image` takes precedence.
