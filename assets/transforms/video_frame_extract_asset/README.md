# Video Frame Extract Asset

Pull N frames per video as image files via ffmpeg. Emits a NEW DataFrame with one row per extracted frame — fan-out from each upstream video.

```yaml
type: dagster_component_templates.VideoFrameExtractAssetComponent
attributes:
  asset_name: video_frames
  upstream_asset_key: sample_videos
  mode: every_seconds
  every_seconds: 1.0
```

## Three sampling modes

| mode | What | Knob |
|---|---|---|
| `every_seconds` (default) | One frame every N seconds of source | `every_seconds: 1.0` (1 fps) |
| `every_n_frames` | One frame every N frames of source | `every_n_frames: 30` |
| `fixed_count` | Spread N frames evenly across the duration | `fixed_count: 10` |

## Output

Each row of the new DataFrame:
- `video_id` — source clip id (from `video_id_column` or row index)
- `source_video` — path to the source video
- `frame_index` — sequential index in the extracted set
- `file_path` — absolute path to the JPG/PNG

Chain naturally into:
- [`vision_api_asset`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/ai/vision_api_asset) — label / OCR each frame
- [`image_exif_extractor`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/image_exif_extractor) — EXIF on each frame (rare on video frames, but valid)
- [`gemini_llm`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/ai/gemini_llm) (multimodal) — describe each frame

## Sister components

- [`synthetic_video_generator`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/source/synthetic_video_generator) — common upstream for demos
- [`video_metadata_extractor`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/video_metadata_extractor) — peek at codec/resolution BEFORE deciding to extract
- [`video_audio_extract_asset`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/transforms/video_audio_extract_asset) — pull audio track instead of frames
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
