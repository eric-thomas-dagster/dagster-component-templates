# Cloud Translation API

Translate a column of text to one or more target languages via Google Cloud Translation v3. Returns the upstream DataFrame with one new column per target language.

```yaml
type: dagster_component_templates.TranslationApiAssetComponent
attributes:
  asset_name: ticket_translations
  upstream_asset_key: support_tickets
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  text_column: ticket_text
  target_languages: [es, fr, de, ja]
  output_prefix: text_
```

For 4 target languages on a 100-row DataFrame, you get 4 new columns: `text_es`, `text_fr`, `text_de`, `text_ja`.

## Auto vs explicit source

If `source_language:` is unset, Translation auto-detects the source language **per row** — useful for mixed-language input (e.g. customer support tickets in many languages, all translated to English).

## MIME type

- `text/plain` (default) — passthrough.
- `text/html` — preserves HTML tag structure during translation.

## Required SA roles

`roles/cloudtranslate.user` on the project + Cloud Translation API enabled.

## Sister components

- `vision_api_asset` (TEXT_DETECTION feature) — OCR an image first, then translate the result with this.
- `gemini_llm` / `openai_llm` — LLM-based translation (often higher quality for nuanced text but costlier and slower).
