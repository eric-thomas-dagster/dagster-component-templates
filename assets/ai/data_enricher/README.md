# DataEnricherComponent

LLM-driven row enrichment — for each row in an upstream DataFrame, send selected
context columns to an LLM and parse structured fields back. Useful for tagging,
sentiment, classification, summarization, entity extraction.

## Dependencies

- `openai` (or your LLM provider's client)
- `pandas`

## Authentication

Reads the API key from an env var (default `OPENAI_API_KEY`).

## Configuration

| Field | Type | Description |
|---|---|---|
| `asset_name` | str | Output asset name |
| `upstream_asset_key` | str | Source DataFrame |
| `context_columns` | list[str] | Columns concatenated into the prompt |
| `enrichment_fields` | dict[str, str] | `{output_field: instruction}` — LLM returns each |
| `model` | str | OpenAI model (e.g. `gpt-4o-mini`) |
| `output_prefix` | str | Optional prefix for new columns (default `""`) |
| `max_workers` | int | Concurrent LLM calls per row batch |
| `api_key_env_var` | str | Env var holding the API key |

## See also
- [Schema](schema.json) · [Example](example.yaml)
