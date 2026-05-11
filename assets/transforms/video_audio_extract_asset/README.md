# Video → Audio Extract Asset

Pull the audio track from each video file via ffmpeg. Optionally resample / downmix / re-encode in the same step to match what your STT expects.

```yaml
type: dagster_component_templates.VideoAudioExtractAssetComponent
attributes:
  asset_name: video_audio
  upstream_asset_key: sample_videos
  target_format: wav
  sample_rate: 16000      # Whisper / Cloud Speech v1 preset
  channels: 1             # mono
```

## Typical chain — video to transcript

```
sample_videos               ← synthetic_video_generator
       └── video_audio       ← video_audio_extract_asset (→ 16kHz mono WAV)
              └── transcripts ← speech_to_text_asset / litellm_audio_transcription
```

## Settings

| Setting | Common values |
|---|---|
| `target_format` | `wav` (Whisper / Cloud Speech), `mp3` (downstream archival), `flac` (lossless small) |
| `sample_rate` | 16000 for STT, 44100 for music, omit to preserve source |
| `channels` | 1 = mono (best for STT), 2 = stereo, omit to preserve |
| `bitrate` | `128k`, `192k` (lossy formats only) |

## Output columns added

| Column | Meaning |
|---|---|
| `audio_path` (configurable) | Path to the extracted audio |
| `audio_extract_error` | Per-row error (None on success). Last line of ffmpeg stderr. |

## Sister components

- [`synthetic_video_generator`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/source/synthetic_video_generator) — common upstream for demos
- [`video_metadata_extractor`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/video_metadata_extractor) — peek at codecs/sample_rate before extracting
- [`audio_transform_asset`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/audio_transform_asset) — further audio processing (loudness-normalize, trim)
- [`speech_to_text_asset`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/ai/speech_to_text_asset) — common downstream
- [`litellm_audio_transcription`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/ai/litellm_audio_transcription) — multi-provider STT (Whisper, Deepgram)
