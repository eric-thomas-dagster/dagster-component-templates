"""GeminiImageGenerationComponent — native (no-LiteLLM) Gemini 2.5 Flash Image.

Generates or edits images per DataFrame row using Google's `google-genai`
SDK directly. The default model is `gemini-2.5-flash-image-preview`,
which the community calls "Nano Banana." Image-to-image edits are
supported by adding an `input_image_column`.

Sister of `litellm_image_generation`. Use this when your stack is
Google-only and you don't want a LiteLLM dependency.
"""

import base64
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Canonical partition factory shared across the registry."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date)."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values.")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class GeminiImageGenerationComponent(Component, Model, Resolvable):
    """Generate or edit images per DataFrame row using Google's Gemini 2.5 Flash Image
    (a.k.a. "Nano Banana"), via the native `google-genai` SDK — no LiteLLM dependency.

    For multi-vendor / model-switching workflows, see `litellm_image_generation`.
    """

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(
        description="Upstream DataFrame asset key. Each row triggers one image generation."
    )

    api_key_env_var: str = Field(
        default="GEMINI_API_KEY",
        description="Env var holding the Google AI Studio API key. Defaults to GEMINI_API_KEY (also accepts GOOGLE_API_KEY).",
    )
    image_model: str = Field(
        default="gemini-2.5-flash-image",
        description=(
            "Gemini image model id. Default is the GA 'Nano Banana' model "
            "(gemini-2.5-flash-image). For preview models use "
            "'nano-banana-pro-preview', 'gemini-3-pro-image-preview', or "
            "'gemini-3.1-flash-image-preview'."
        ),
    )

    prompt_column: Optional[str] = Field(
        default=None,
        description="Column with per-row prompt text. Mutually exclusive with prompt_template.",
    )
    prompt_template: Optional[str] = Field(
        default=None,
        description="Static prompt template; supports {column_name} placeholders. Mutually exclusive with prompt_column.",
    )

    input_image_column: Optional[str] = Field(
        default=None,
        description="Optional column of source-image file paths for image-to-image edits.",
    )

    output_dir: str = Field(
        default="/tmp/gemini_image_generation",
        description="Directory for generated PNGs. One file per successful row.",
    )
    output_path_column: str = Field(
        default="generated_image_path",
        description="Column added to the output DataFrame with the saved file path (None on failure).",
    )
    output_filename_template: str = Field(
        default="row_{idx}.png",
        description="Per-row filename. Supports {idx} (row index) and {column} placeholders.",
    )

    temperature: float = Field(default=1.0, description="Temperature (0.0–2.0).")
    rate_limit_delay: float = Field(default=0.5, description="Seconds between API calls.")
    max_retries: int = Field(default=3, description="Retries on transient errors.")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if not self.prompt_column and not self.prompt_template:
            raise ValueError(
                "GeminiImageGenerationComponent: set either prompt_column or prompt_template."
            )
        if self.prompt_column and self.prompt_template:
            raise ValueError(
                "GeminiImageGenerationComponent: set prompt_column OR prompt_template, not both."
            )

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )

        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        deps_keys = [AssetKey.from_user_string(k) for k in (self.deps or [])]

        api_key_env_var_local = self.api_key_env_var
        image_model_local = self.image_model
        prompt_column_local = self.prompt_column
        prompt_template_local = self.prompt_template
        input_image_column_local = self.input_image_column
        output_dir_local = self.output_dir
        output_path_column_local = self.output_path_column
        output_filename_template_local = self.output_filename_template
        temperature_local = self.temperature
        rate_limit_delay_local = self.rate_limit_delay
        max_retries_local = self.max_retries

        @asset(
            name=self.asset_name,
            description=self.description or f"Gemini image generation per row from {self.upstream_asset_key}.",
            group_name=self.group_name,
            kinds={"gemini", "image"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=deps_keys or None,
            partitions_def=partitions_def,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            try:
                from google import genai
                from google.genai import types as genai_types
            except ImportError as e:
                raise ImportError(
                    "google-genai required: pip install google-genai>=0.3.0"
                ) from e

            api_key = os.environ.get(api_key_env_var_local) or os.environ.get("GOOGLE_API_KEY")
            if not api_key:
                raise ValueError(
                    f"{api_key_env_var_local} not set in environment "
                    "(also tried GOOGLE_API_KEY as fallback)."
                )

            os.makedirs(output_dir_local, exist_ok=True)
            client = genai.Client(api_key=api_key)

            df = upstream.copy().reset_index(drop=True)
            if df.empty:
                df[output_path_column_local] = []
                return df

            paths: List[Optional[str]] = []
            errors: List[Optional[str]] = []
            success_count = 0

            for idx, row in df.iterrows():
                if prompt_column_local:
                    if prompt_column_local not in df.columns:
                        raise ValueError(
                            f"prompt_column={prompt_column_local!r} not in upstream columns: {list(df.columns)}"
                        )
                    prompt = str(row[prompt_column_local])
                else:
                    template = prompt_template_local or ""
                    try:
                        prompt = template.format(**row.to_dict())
                    except KeyError as e:
                        raise ValueError(
                            f"prompt_template references missing column {e}; row columns: {list(df.columns)}"
                        )

                contents: List[Any] = [prompt]

                if input_image_column_local:
                    img_path = row.get(input_image_column_local)
                    if img_path and isinstance(img_path, str) and os.path.exists(img_path):
                        with open(img_path, "rb") as fh:
                            img_bytes = fh.read()
                        ext = Path(img_path).suffix.lstrip(".").lower() or "png"
                        mime = {
                            "jpg": "image/jpeg", "jpeg": "image/jpeg",
                            "png": "image/png", "webp": "image/webp",
                        }.get(ext, "image/png")
                        contents.append(
                            genai_types.Part.from_bytes(data=img_bytes, mime_type=mime)
                        )

                attempt = 0
                last_err: Optional[Exception] = None
                resp = None
                while attempt <= max_retries_local:
                    try:
                        resp = client.models.generate_content(
                            model=image_model_local,
                            contents=contents,
                            config=genai_types.GenerateContentConfig(
                                temperature=temperature_local,
                                response_modalities=["IMAGE", "TEXT"],
                            ),
                        )
                        last_err = None
                        break
                    except Exception as e:
                        # Don't retry on 404 (model not found) — retrying won't help.
                        err_str = str(e)
                        is_not_found = "404" in err_str or "NOT_FOUND" in err_str
                        last_err = e
                        attempt += 1
                        if is_not_found or attempt > max_retries_local:
                            break
                        wait = (2 ** attempt) * 0.5
                        context.log.warning(
                            f"row {idx}: gemini call failed ({e!r}), retrying in {wait}s "
                            f"(attempt {attempt}/{max_retries_local})"
                        )
                        time.sleep(wait)

                if last_err is not None or resp is None:
                    paths.append(None)
                    err_str = str(last_err) if last_err else "no response"
                    errors.append(err_str)
                    if "404" in err_str or "NOT_FOUND" in err_str:
                        context.log.error(
                            f"row {idx}: model {image_model_local!r} returned 404 NOT_FOUND. "
                            f"Set `image_model:` in your defs.yaml to a current model. "
                            f"Known-good ids (as of writing): "
                            f"gemini-2.5-flash-image (GA), nano-banana-pro-preview, "
                            f"gemini-3-pro-image-preview, gemini-3.1-flash-image-preview. "
                            f"Run `client.models.list()` against your key to see what's available."
                        )
                    elif "429" in err_str or "RESOURCE_EXHAUSTED" in err_str or "quota" in err_str.lower():
                        context.log.error(
                            f"row {idx}: quota exhausted for {image_model_local!r}. "
                            f"Image generation often requires a billed account on Google AI Studio "
                            f"(free-tier quota is 0 for image models). "
                            f"Enable billing at console.cloud.google.com or check "
                            f"https://ai.dev/rate-limit."
                        )
                    else:
                        context.log.error(f"row {idx}: gemini call ultimately failed: {last_err}")
                    time.sleep(rate_limit_delay_local)
                    continue

                # Find the first inline image part in the response.
                img_bytes_out: Optional[bytes] = None
                for cand in (resp.candidates or []):
                    parts = getattr(cand.content, "parts", None) or []
                    for part in parts:
                        inline = getattr(part, "inline_data", None)
                        if inline and getattr(inline, "data", None):
                            data = inline.data
                            if isinstance(data, str):
                                data = base64.b64decode(data)
                            img_bytes_out = data
                            break
                    if img_bytes_out is not None:
                        break

                if img_bytes_out is None:
                    paths.append(None)
                    errors.append("no inline image returned")
                    context.log.warning(f"row {idx}: response had no inline image")
                    time.sleep(rate_limit_delay_local)
                    continue

                fname = output_filename_template_local.format(
                    idx=idx, **{k: row.get(k, "") for k in df.columns}
                )
                out_path = os.path.join(output_dir_local, fname)
                with open(out_path, "wb") as fh:
                    fh.write(img_bytes_out)
                paths.append(out_path)
                errors.append(None)
                success_count += 1
                time.sleep(rate_limit_delay_local)

            df[output_path_column_local] = paths
            if any(errors):
                df[f"{output_path_column_local}_error"] = errors

            n_404 = sum(1 for e in errors if e and ("404" in e or "NOT_FOUND" in e))
            n_429 = sum(1 for e in errors if e and ("429" in e or "RESOURCE_EXHAUSTED" in e or "quota" in e.lower()))

            preview_md = df.head(5).to_markdown(index=False) or ""
            md_metadata: Dict[str, Any] = {
                "rows":           MetadataValue.int(len(df)),
                "images_saved":   MetadataValue.int(success_count),
                "output_dir":     MetadataValue.path(output_dir_local),
                "model":          MetadataValue.text(image_model_local),
                "preview":        MetadataValue.md(preview_md),
            }
            if n_404:
                md_metadata["model_not_found_count"] = MetadataValue.int(n_404)
                md_metadata["hint"] = MetadataValue.text(
                    f"Model {image_model_local!r} returned 404. Set `image_model:` "
                    f"to one of: gemini-2.5-flash-image (GA), nano-banana-pro-preview, "
                    f"gemini-3-pro-image-preview, gemini-3.1-flash-image-preview."
                )
            if n_429:
                md_metadata["quota_exhausted_count"] = MetadataValue.int(n_429)
                md_metadata["hint"] = MetadataValue.text(
                    "Quota exhausted — image gen typically requires a billed Google AI account. "
                    "Enable billing at console.cloud.google.com."
                )
            context.add_output_metadata(md_metadata)
            return df

        return Definitions(assets=[_asset])
