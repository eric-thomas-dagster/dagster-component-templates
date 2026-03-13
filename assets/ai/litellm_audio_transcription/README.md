# LiteLLM Audio Transcription

Transcribe audio files using Whisper via LiteLLM. Processes a column of audio file paths and writes transcribed text to a new column.

## Overview

`LitellmAudioTranscriptionComponent` reads an upstream Dagster asset as a DataFrame, opens each audio file referenced in the path column, calls `litellm.transcription()` with the Whisper model, and writes the resulting text to a new column.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | string | required | Output Dagster asset name |
| `upstream_asset_key` | string | required | Upstream asset key providing a DataFrame |
| `audio_path_column` | string | required | Column containing local audio file paths |
| `output_column` | string | `"transcription"` | Column to write transcribed text |
| `model` | string | `"whisper-1"` | Transcription model |
| `language` | string | null | ISO 639-1 language code hint (e.g. en, es) |
| `api_key_env_var` | string | null | Env var name for API key |
| `group_name` | string | null | Dagster asset group name |

## Example

```yaml
type: dagster_component_templates.LitellmAudioTranscriptionComponent
attributes:
  asset_name: transcribed_customer_calls
  upstream_asset_key: customer_call_recordings
  audio_path_column: recording_path
  output_column: transcription
  language: en
  api_key_env_var: OPENAI_API_KEY
```

## Requirements

```
litellm>=1.0.0
pandas>=1.5.0
```
