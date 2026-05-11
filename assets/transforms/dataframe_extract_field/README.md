# Dataframe Extract Field

Pull one field out of a column whose values are lists or dicts. Common pattern after API-driven assets (Vision, DLP, OCR, embeddings) that emit nested response shapes.

```yaml
type: dagster_component_templates.DataframeExtractFieldComponent
attributes:
  asset_name: top_label_per_image
  upstream_asset_key: image_analysis
  source_column: vision_labels   # list[{description, score, ...}]
  target_column: top_label
  index: 0
  field: description
```

## Common patterns

| Upstream | Goal | Config |
|---|---|---|
| `vision_api_asset` — column of label dicts | Get top label per image | `index: 0`, `field: description` |
| `cloud_dlp_inspect_asset` — column of finding dicts | Get top PII type per row | `index: 0`, `field: info_type` |
| `document_ai_extractor` — column of entity dicts | Get first detected entity | `index: 0`, `field: type` |
| OpenAI / Anthropic message responses (list of choices) | Pluck first choice's content | `index: 0`, `field: content` |

## Behavior

- **List input + no `field`** → pluck the indexed item itself (works when list contains scalars like `["a", "b"]`)
- **List input + `field`** → pluck the dict field from the indexed item
- **Dict input** (no list wrapping) → `index` is ignored; `field` plucks directly
- **None / empty / missing** → output is `None` (the asset still materializes)

## When to use this vs. write SQL/pandas

If you're already running downstream SQL on a serialized JSON column (via `dataframe_flatten_nested_columns` → BQ), use `JSON_QUERY` / `JSON_EXTRACT_SCALAR` on the warehouse side. Use this component when the next step is another pandas-shape component (e.g. feeding `translation_api_asset` which takes a single text column).

## Sister components

- `dataframe_flatten_nested_columns` — opposite direction (stringify the nested column for warehouse loads)
- `dataframe_merge` — common downstream once you have flat fields to join on
