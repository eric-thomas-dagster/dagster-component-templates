# Synthetic Audio Generator

Generate sample WAV files (sine tones) and emit a DataFrame describing them. Stdlib-only — no audio dependencies.

```yaml
type: dagster_component_templates.SyntheticAudioGeneratorComponent
attributes:
  asset_name: sample_audio
  samples: default
```

## Output shape

| Column | Type |
|---|---|
| `clip_id` | str |
| `kind` | str (`sine`) |
| `frequency_hz` | float |
| `duration_seconds` | float |
| `sample_rate` | int |
| `file_path` | str — absolute path to the WAV |
| `file_size_bytes` | int |

## Built-in sample set

Three short tones (1.0s, 1.0s, 0.5s) at A4 (440 Hz), A5 (880 Hz), A6 (1760 Hz). Useful for exercising `audio_transform_asset` (resample / format-convert) and the basic I/O path of audio sinks.

## Custom clips

```yaml
attributes:
  samples: custom
  clips:
    - { clip_id: middle_c, frequency_hz: 261.63, duration_seconds: 2.0, kind: sine }
    - { clip_id: e_above,  frequency_hz: 329.63, duration_seconds: 2.0, kind: sine }
```

Only `kind: sine` is implemented; other values fall back to sine. Fork the component for square / saw waves if needed.

## Why this exists (vs. `synthetic_data_generator` `audio_samples`)

| Component | Output |
|---|---|
| `synthetic_audio_generator` (this) | **Local WAV files** — for transforms, codec testing, file I/O |
| `synthetic_data_generator` `audio_samples` | **DataFrame of GCS URIs** to Google's public speech samples — for real STT testing |

Use this when you need local audio bytes; use the other when you need real speech for transcription.

## Sister components

- `synthetic_pdf_generator` — PDF sibling for OCR demos
- `synthetic_image_generator` — PNG sibling for vision demos
- `audio_transform_asset` — common downstream (ffmpeg ops)
- `cloud_text_to_speech_asset` — generates speech audio (real, not tones)
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
