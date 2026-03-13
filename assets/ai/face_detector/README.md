# Face Detector Component

Detect faces in images and return bounding boxes, face count, and optional facial landmark positions. Supports OpenCV Haar cascades and MediaPipe face detection backends.

## Overview

The Face Detector Component processes images to detect human faces and return structured results. It supports two backends: OpenCV (fast, no extra dependencies beyond opencv-python) and MediaPipe (more accurate, returns facial landmarks).

## Features

- **OpenCV backend**: Fast Haar cascade detection, no GPU required
- **MediaPipe backend**: High-accuracy face detection with confidence scores and landmarks
- **Face count column**: Convenient count of detected faces per image
- **Optional bounding boxes**: Pixel coordinates for each detected face
- **Optional landmarks**: Facial keypoint positions (MediaPipe only)

## Use Cases

1. **Headcount estimation**: Count people in photos for crowd analytics
2. **Privacy compliance**: Detect faces before blurring for GDPR pipelines
3. **Photo quality assessment**: Filter photos with no detected faces
4. **Audience measurement**: Count faces in venue or event photography

## Prerequisites

- `opencv-python>=4.5.0`
- For MediaPipe backend: `pip install mediapipe`

## Configuration

### OpenCV Backend (Default)

```yaml
type: dagster_component_templates.FaceDetectorComponent
attributes:
  asset_name: face_counts
  upstream_asset_key: photos
  image_column: file_path
  count_column: face_count
  backend: opencv
```

### MediaPipe with Landmarks

```yaml
type: dagster_component_templates.FaceDetectorComponent
attributes:
  asset_name: face_details
  upstream_asset_key: headshots
  image_column: image_path
  count_column: face_count
  boxes_column: face_boxes
  landmarks_column: face_landmarks
  backend: mediapipe
  min_confidence: 0.7
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `image_column` | string | required | Column with image file paths |
| `count_column` | string | `face_count` | Column for face count |
| `boxes_column` | string | `None` | Column for bounding boxes |
| `landmarks_column` | string | `None` | Column for landmarks |
| `backend` | string | `opencv` | opencv, mediapipe, or retinaface |
| `min_confidence` | float | `0.5` | Min confidence (MediaPipe) |
| `group_name` | string | `None` | Asset group |

## Output

Original DataFrame plus `count_column`, and optionally `boxes_column` and `landmarks_column`.
