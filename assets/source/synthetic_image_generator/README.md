# Synthetic Image Generator

Generate sample PNG images and emit a DataFrame describing them. PNG sibling of `synthetic_pdf_generator` — designed to exercise Cloud Vision label detection without committing binary fixtures.

```yaml
type: dagster_component_templates.SyntheticImageGeneratorComponent
attributes:
  asset_name: sample_images
  samples: default
```

## Output shape

| Column | Type |
|---|---|
| `sku` | str — provided id |
| `name` | str — short caption (also the render key for default mode) |
| `kind` | str — caller-provided category |
| `file_path` | str — absolute path to the written PNG |

## Built-in sample set

The default set has 3 images that Vision labels reliably:
- `FRUIT-1` (red apple-like circle + stem)
- `VEH-1` (blue car-like rectangle with two black wheels)
- `PLANT-1` (green leafy plant with brown stem)

## Custom images

```yaml
attributes:
  samples: custom
  images:
    - { sku: BANANA, name: apple, kind: fruit }   # name='apple' uses the built-in renderer
    - { sku: TRUCK,  name: blue car, kind: vehicle }
    - { sku: CUSTOM, name: my-thing, kind: misc } # unknown names get a gray placeholder
```

The default renderer recognizes `apple`, `blue car`, `green plant`. For custom shapes, fork the component or generate images upstream in a separate component.

## Typical chain

```
sample_images        ← synthetic_image_generator
       │
       └── image_analysis  ← vision_api_asset
```

## Why this exists

Demos must be 100% components — no inline `Pillow` rendering in `defs/`. This component is the shared upstream for any Vision / image-processing demo.

## Sister components

- `synthetic_pdf_generator` — PDF sibling for OCR / Document AI demos
- `synthetic_data_generator` — tabular synthetic DataFrames
- `vision_api_asset` — typical downstream (Cloud Vision)
- `gemini_image_generation` — generate REAL images via Gemini Imagen (this is for shapes-and-colors only)
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
