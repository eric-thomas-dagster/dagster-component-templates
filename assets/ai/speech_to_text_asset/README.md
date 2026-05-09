# Cloud Speech-to-Text

Transcribe audio files via Cloud Speech-to-Text v2. Pass a column of audio references (local paths or `gs://` URIs); get transcripts back as a new column.

```yaml
type: dagster_component_templates.SpeechToTextAssetComponent
attributes:
  asset_name: call_transcripts
  upstream_asset_key: support_calls
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  audio_column: audio_path
  recognizer_model: latest_long
  language_codes: [en-US]
  enable_automatic_punctuation: true
```

## Recognizer models

| Model | When |
|---|---|
| `latest_long` (default) | Long-form audio (lectures, podcasts, calls) |
| `latest_short` | <60s clips |
| `chirp` / `chirp_2` | Multilingual / low-resource languages, best general accuracy |
| `phone_call` | 8 kHz call-center audio |
| `medical_conversation` | Clinical dictation |
| `medical_dictation` | Clinical single-speaker |

## Multilingual

Pass multiple `language_codes` for code-switching audio:

```yaml
language_codes: [en-US, es-US, fr-FR]
```

## Speaker diarization

```yaml
enable_speaker_diarization: true
diarization_speaker_count: 2          # min/max
```

## Required SA roles

`roles/speech.client` + Cloud Speech API enabled.

## Sister components

- `audio_transcriber` — local Whisper (no cloud required, $0).
- `litellm_audio_transcription` — multi-vendor wrapper.
- `vision_api_asset` — same shape, but for images.

## Notes

Synchronous `recognize` works for clips up to 60s. For longer audio, point `audio_column` at a `gs://` URI — the component routes through Speech v2's batch_recognize automatically. Files <60s can use either.
