# Synthetic Video Generator

Generate small test MP4s via ffmpeg's built-in `testsrc` (color-bar pattern) + `sine` audio generators. Emits a DataFrame with one row per video.

```yaml
type: dagster_component_templates.SyntheticVideoGeneratorComponent
attributes:
  asset_name: sample_videos
  samples: default
```

## Output shape

| Column | Type |
|---|---|
| `clip_id` | str |
| `duration_seconds`, `fps`, `width`, `height`, `tone_hz` | numeric |
| `file_path` | str — absolute path to the MP4 |
| `file_size_bytes` | int |

## Built-in clips

| clip_id | duration | size | fps | tone |
|---|---|---|---|---|
| vid-001 | 2.0s | 320×240 | 24 | A4 (440 Hz) |
| vid-002 | 1.0s | 640×480 | 30 | A5 (880 Hz) |

## Custom clips

```yaml
attributes:
  samples: custom
  clips:
    - { clip_id: short_4k, duration_seconds: 0.5, fps: 60, width: 3840, height: 2160, tone_hz: 1000.0 }
```

## Requires ffmpeg in PATH

Same as `audio_transform_asset`. macOS: `brew install ffmpeg`.

## Why this exists

Demos must be 100% components. This is the shared upstream for any video-processing demo:

```
sample_videos       ← synthetic_video_generator
       ├── video_meta    ← video_metadata_extractor
       ├── video_frames  ← video_frame_extract_asset
       └── video_audio   ← video_audio_extract_asset
```

## Sister components

- `synthetic_image_generator`, `synthetic_audio_generator`, `synthetic_pdf_generator` — same hermetic-test pattern for other media
- `video_metadata_extractor`, `video_frame_extract_asset`, `video_audio_extract_asset` — typical downstreams
