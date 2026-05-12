# Video Metadata Extractor

Pull container + per-stream metadata from video files via `ffprobe` (ships with ffmpeg). Adds video/audio codec, resolution, fps, duration, bitrate, sample rate, channels, plus the full `streams_raw` JSON.

```yaml
type: dagster_component_templates.VideoMetadataExtractorComponent
attributes:
  asset_name: video_meta
  upstream_asset_key: sample_videos
  video_path_column: file_path
```

## Added columns

| Column | Source |
|---|---|
| `video_format_name`, `video_duration_seconds`, `video_size_bytes`, `video_bit_rate` | Container (`format`) |
| `video_codec`, `video_width`, `video_height`, `video_fps`, `video_pix_fmt` | Primary video stream |
| `audio_codec`, `audio_sample_rate`, `audio_channels`, `audio_bit_rate` | Primary audio stream |
| `streams_raw` | Full per-stream JSON for ad-hoc inspection |
| `video_meta_error` | Per-row error (None on success) |

## Requires ffprobe in PATH

`ffprobe` ships with ffmpeg. `brew install ffmpeg` / `apt install ffmpeg`.

## Use cases

| Goal | What to do |
|---|---|
| Validate ingest | Asset check: `video_height < 720` → fail |
| Route by resolution | Filter into 4K / 1080p / SD subassets downstream |
| Codec audit | `GROUP BY video_codec` in a warehouse rollup |
| Catch deprecated formats | Asset check on `video_codec IN ('mjpeg', 'h263')` → warn |

## Sister components

- [`synthetic_video_generator`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/source/synthetic_video_generator) — common upstream for demos
- [`video_frame_extract_asset`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/video_frame_extract_asset) — extract frames for Vision API
- [`video_audio_extract_asset`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/video_audio_extract_asset) — pull audio track for STT
- [`image_exif_extractor`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/image_exif_extractor) — image-side sibling
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
