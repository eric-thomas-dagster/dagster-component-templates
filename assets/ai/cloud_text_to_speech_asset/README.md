# Cloud Text-to-Speech Asset

Generate audio files from a text column via [Cloud Text-to-Speech](https://cloud.google.com/text-to-speech). Per-row: emit one `.mp3` / `.wav` / `.ogg` file. Counterpart of `speech_to_text_asset`.

```yaml
type: dagster_component_templates.CloudTextToSpeechAssetComponent
attributes:
  asset_name: announcements_audio
  upstream_asset_key: announcements
  text_column: text
  language_code: en-US
  voice_name: en-US-Neural2-D
  audio_encoding: MP3
```

## Output

Adds 2 columns to the upstream DataFrame:
- `audio_path` — filesystem path to the generated audio file
- `tts_error` — error string per row (None on success)

## Voice tiers (en-US example IDs; same shape in other languages)

| Tier | Example | Quality | Price |
|---|---|---|---|
| Standard | `en-US-Standard-A` | Basic, robotic | $4/1M chars |
| WaveNet | `en-US-Wavenet-D` | Natural | $16/1M chars |
| **Neural2** (recommended) | `en-US-Neural2-D` | High quality, expressive | $16/1M chars |
| Studio | `en-US-Studio-O` | Pro narrator, English only | $160/1M chars |
| Chirp 3 (preview) | `en-US-Chirp3-HD-*` | Newest, instruction-following | Preview pricing |

Browse the full list at <https://cloud.google.com/text-to-speech/docs/voices>.

## Typical chains

**TTS-only** (text → audio):
```
announcements        ← synthetic_data_generator (or any text source)
       └── announcements_audio  ← cloud_text_to_speech_asset
```

**Speech-to-speech translation** (audio → audio in another language):
```
audio_files          ← synthetic_data_generator (audio_samples)
       └── transcripts            ← speech_to_text_asset (English)
              └── translated      ← translation_api_asset (→ Spanish)
                     └── audio_out  ← cloud_text_to_speech_asset (Spanish voice)
```

## Auth

Service account needs `roles/texttospeech.user` (or `roles/cloudtts.user`).

## Required API

<https://console.cloud.google.com/apis/library/texttospeech.googleapis.com>

## Cost

Free tier: 1M characters/month of Standard voices, 0.1M of WaveNet/Neural2. Beyond that:
- Standard: $4/1M chars
- WaveNet / Neural2 / Polyglot: $16/1M chars
- Studio: $160/1M chars

## Sister components

- `speech_to_text_asset` — opposite direction (audio → text)
- `translation_api_asset` — translate text between languages (pair with this for speech-to-speech translation)
- `litellm_text_to_speech` — multi-provider TTS (OpenAI, ElevenLabs, Azure) for when you want one component to span vendors
- `gemini_image_generation` — sister "text → media" component for images
## ⚠️ Deployment note (Dagster+ / Kubernetes)

This component reads or writes local filesystem paths. Behavior across deployments:

| Environment | Works? |
|---|---|
| Local dev | ✅ Yes |
| Dagster+ Serverless (multiprocess executor, default) | ✅ Within a single run — `/tmp/...` is shared across ops in the same run. Files do **not** persist after the run ends. |
| Dagster Hybrid on k8s with `k8s_job` executor (op-per-pod) | ❌ Each op runs in its own pod with its own `/tmp` — files don't travel between ops, even within one run. Set the run to use the `in_process` executor as a workaround. |
| Cross-run reads (run N writes, run N+1 reads) | ❌ Anywhere — the local filesystem is ephemeral by definition. |

**Recommended alternatives for production:**

1. **Return bytes as the asset value** instead of writing a file. The default `PickledObjectFilesystemIOManager` (and the Dagster+ Serverless S3-backed IO manager) serialize binary data fine. Downstream ops read the bytes from the IO manager regardless of pod / run.
2. **Use a cloud-storage sink** for cross-run persistence: [`dataframe_to_s3`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_s3), [`dataframe_to_gcs`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_gcs), [`dataframe_to_adls`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_adls).
3. **Mount a shared volume** (k8s PVC / Cloud Run volumes) if you genuinely need a shared filesystem path across pods.
