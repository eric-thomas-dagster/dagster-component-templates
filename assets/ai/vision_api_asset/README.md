# Cloud Vision API

Per-row image analysis via Google Cloud Vision. Pass a column of image references (local paths, `gs://` URIs, or HTTP URLs) and one or more feature types; the component returns the upstream DataFrame with new `vision_*` columns per feature.

```yaml
type: dagster_component_templates.VisionApiAssetComponent
attributes:
  asset_name: product_image_analysis
  upstream_asset_key: product_images
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  image_column: file_path
  features: [LABEL_DETECTION, OBJECT_LOCALIZATION, SAFE_SEARCH_DETECTION]
  max_results: 10
  group_name: vision
```

## Supported features

| Feature | What it does | Output column |
|---|---|---|
| `LABEL_DETECTION` | Whole-image labels with confidence | `vision_labels` (list of `{description, score}`) |
| `OBJECT_LOCALIZATION` | Object detection with bounding boxes | `vision_objects` (list of `{name, score}`) |
| `FACE_DETECTION` | Faces + emotion likelihoods | `vision_faces` (count) + `vision_face_emotions` (list) |
| `LANDMARK_DETECTION` | Famous landmarks | `vision_landmarks` |
| `LOGO_DETECTION` | Brand logos | `vision_logos` |
| `TEXT_DETECTION` | Basic OCR | `vision_text` (string) |
| `DOCUMENT_TEXT_DETECTION` | OCR optimized for documents | `vision_doc_text` (string) |
| `SAFE_SEARCH_DETECTION` | NSFW / violence / racy / spoof / medical | `vision_safesearch` (dict) |
| `IMAGE_PROPERTIES` | Dominant color | `vision_dominant_color_rgb` + `vision_dominant_color_score` |
| `WEB_DETECTION` | Reverse-image search | `vision_web_entities` |
| `CROP_HINTS` | Suggested crop boxes | `vision_crop_hints` (count) |

For form / table / structured-document parsing, use **`document_ai_extractor`** instead — Vision's OCR is fine for text on photos, but Document AI parses field structure.

## Required SA roles

`roles/serviceusage.serviceUsageConsumer` on the project + Vision API enabled.

## Sister components

- `document_ai_extractor` — structured document parsing (forms, tables).
- `gemini_image_generation` — generate images.
- `vision_model` — multi-vendor wrapper (OpenAI / Anthropic / Vertex via LiteLLM).
- `vertex_ai_text_embeddings_asset` — text embeddings (could chain after Vision OCR for RAG).
