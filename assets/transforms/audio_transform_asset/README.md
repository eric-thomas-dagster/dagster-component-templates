# Audio Transform Asset

ffmpeg-based audio resample / convert / trim / normalize. Reads a column of audio file paths, applies the configured ffmpeg ops per row, writes new files.

```yaml
type: dagster_component_templates.AudioTransformAssetComponent
attributes:
  asset_name: whisper_ready_audio
  upstream_asset_key: raw_audio
  audio_path_column: file_path
  target_format: wav
  sample_rate: 16000
  channels: 1
  normalize: true
```

## Requires `ffmpeg` in PATH

- macOS: `brew install ffmpeg`
- Debian/Ubuntu: `apt install ffmpeg`
- Alpine (Docker): `apk add ffmpeg`

The component fails fast at materialization time with a clear message if ffmpeg isn't available.

## Common configs

| Goal | Settings |
|---|---|
| Whisper / OpenAI STT preprocess | `target_format: wav`, `sample_rate: 16000`, `channels: 1` |
| Cloud Speech-to-Text v1 preprocess | `target_format: flac`, `sample_rate: 16000` |
| Standardize to 128k MP3 | `target_format: mp3`, `bitrate: 128k`, `sample_rate: 44100` |
| Trim first 30 seconds | `start_seconds: 0`, `end_seconds: 30` |
| Loudness-normalize mixed sources | `normalize: true` (EBU R128 -16 LUFS) |
| WebM/MP4 audio extract → MP3 | `target_format: mp3` (works on any container ffmpeg supports) |

## Output columns added

| Column | Meaning |
|---|---|
| `transformed_path` (configurable) | Path to the written audio file |
| `transform_error` | Per-row error string (None on success). The LAST line of ffmpeg stderr — usually enough to diagnose. |

## Typical chains

**Pre-STT normalization:**
```
raw_audio              ← synthetic_audio_generator (mixed format/rate sources)
   └── whisper_ready  ← audio_transform_asset (→ 16kHz mono WAV)
        └── transcripts  ← speech_to_text_asset / litellm_audio_transcription
```

**Speech-to-speech translation:**
```
audio_files            ← synthetic_data_generator (audio_samples)
   └── transcripts        ← speech_to_text_asset
        └── translated     ← translation_api_asset
             └── audio_out  ← cloud_text_to_speech_asset
                  └── normalized ← audio_transform_asset (loudness-match the output)
```

## Sister components

- `synthetic_audio_generator` — common upstream for demos (sine-tone WAVs)
- `speech_to_text_asset` / `litellm_audio_transcription` — common downstream
- `cloud_text_to_speech_asset` / `litellm_text_to_speech` — generates new audio that this can post-process
- `image_transform_asset` — image sibling (Pillow-based)
