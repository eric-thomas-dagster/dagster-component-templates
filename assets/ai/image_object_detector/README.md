# Image Object Detector Component

Detect objects in images using HuggingFace DETR models. Returns bounding boxes, labels, and confidence scores for each detected object. Supports confidence threshold filtering and label-based filtering.

## Overview

The Image Object Detector Component uses HuggingFace's `transformers` object-detection pipeline with DETR (Detection Transformer) models to identify and locate objects in images. Results are stored as structured lists of detection dictionaries with label, confidence, and bounding box coordinates.

## Features

- **DETR-based detection**: State-of-the-art transformer object detection
- **Confidence filtering**: Discard low-confidence detections
- **Label filtering**: Only return specific object classes (e.g. person, car)
- **Count column**: Convenient object count per image
- **Device support**: CPU, CUDA, and Apple MPS

## Use Cases

1. **Inventory counting**: Count products on shelves from photos
2. **Safety compliance**: Detect PPE presence in workplace images
3. **Traffic analysis**: Count vehicles in traffic camera frames
4. **Retail analytics**: Detect and count items in store images

## Prerequisites

- `transformers>=4.30.0`, `Pillow>=9.0.0`, `torch>=2.0.0`

## Configuration

### Detect All Objects

```yaml
type: dagster_component_templates.ImageObjectDetectorComponent
attributes:
  asset_name: all_objects
  upstream_asset_key: raw_images
  image_column: file_path
  confidence_threshold: 0.5
```

### Detect Only People and Vehicles

```yaml
type: dagster_component_templates.ImageObjectDetectorComponent
attributes:
  asset_name: people_and_vehicles
  upstream_asset_key: camera_frames
  image_column: frame_path
  target_labels:
    - person
    - car
    - truck
    - bicycle
  confidence_threshold: 0.7
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `image_column` | string | required | Column with image paths or URLs |
| `output_column` | string | `detected_objects` | Column for detection results |
| `count_column` | string | `object_count` | Column for object count |
| `model_name` | string | `facebook/detr-resnet-50` | HuggingFace model ID |
| `confidence_threshold` | float | `0.7` | Minimum confidence |
| `target_labels` | list | `None` | Filter to these labels only |
| `device` | string | `cpu` | cpu, cuda, or mps |
| `group_name` | string | `None` | Asset group |

## Output

Original DataFrame plus `output_column` (list of `{label, confidence, bbox}` dicts) and `count_column`.
