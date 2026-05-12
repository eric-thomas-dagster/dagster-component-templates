# Image Transform Asset

Pillow-based image resize / crop / format-convert / grayscale. Reads a column of image file paths, applies the configured transforms per row, writes new files, and adds the output path back into the DataFrame.

```yaml
type: dagster_component_templates.ImageTransformAssetComponent
attributes:
  asset_name: thumbnails
  upstream_asset_key: sample_images
  image_path_column: file_path
  output_dir: /tmp/thumbnails
  resize_to: [128, 128]
  convert_to: webp
  quality: 80
```

## Ops (applied in order)

1. `grayscale: true` — single-channel
2. `resize_to: [w, h]` — `preserve_aspect_ratio: true` (default) uses `Image.thumbnail`; `false` uses absolute resize
3. `crop_to: [w, h]` — center crop after resize
4. Save as `convert_to` format (jpg / png / webp / bmp / tiff) at `quality` (1–100; JPEG/WebP only)

If `convert_to` is omitted, the output keeps the input format.

## Common configs

| Goal | Settings |
|---|---|
| 256px thumbnail in WebP | `resize_to: [256, 256]`, `convert_to: webp`, `quality: 80` |
| Strict 224×224 model input | `resize_to: [224, 224]`, `preserve_aspect_ratio: false` |
| Center 1:1 from any aspect | `crop_to: [1024, 1024]` |
| HEIC → JPEG | `convert_to: jpg` (Pillow handles HEIC if `pillow-heif` is installed) |
| Grayscale OCR preprocess | `grayscale: true`, `convert_to: png` |

## Output columns added

| Column | Meaning |
|---|---|
| `transformed_path` (configurable) | Path to the written file |
| `transform_error` | Per-row error string (None on success) |
| `size_before` / `size_after` | `WxH` strings for sanity checks |

## Sister components

- `synthetic_image_generator` — common upstream for demos
- `vision_api_asset` — common downstream (label / OCR the transformed images)
- `gemini_image_generation` — generates images; this component post-processes them
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
