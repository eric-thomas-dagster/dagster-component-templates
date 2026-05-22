"""HuggingFace text-to-image generation component.

Generates images from text prompts via ``huggingface_hub.InferenceClient.text_to_image()``.
Supports multi-provider routing (``provider="wavespeed"``, ``"falai"``,
``"replicate"``, etc.) so customers can get to the cheapest / fastest
backend without rewriting Dagster code.

Each prompt becomes one PNG saved to ``output_dir``. The asset emits a
``MaterializeResult`` with the list of file paths in metadata.
"""

import os
import re
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import ConfigDict, Field


def _slug(s: str, max_len: int = 40) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", s.lower()).strip("_")
    return cleaned[:max_len] or "image"


class HuggingfaceTextToImageComponent(Component, Model, Resolvable):
    # `model` shadows Pydantic / Dagster Resolvable parent attr; alias in YAML.
    model_config = ConfigDict(populate_by_name=True)
    """Generate images from text prompts via HuggingFace InferenceClient.

    Example (FLUX.1-dev via wavespeed provider):
        ```yaml
        type: dagster_community_components.HuggingfaceTextToImageComponent
        attributes:
          asset_key: hf/images/airship
          model: black-forest-labs/FLUX.1-dev
          provider: wavespeed
          prompts:
            - "A steampunk airship in the clouds"
            - "A cyberpunk city at night, neon reflections in rain puddles"
          output_dir: ./generated_images
          hf_token_env_var: HF_TOKEN
        ```

    Example (default provider, single prompt):
        ```yaml
        type: dagster_community_components.HuggingfaceTextToImageComponent
        attributes:
          asset_key: hf/images/sample
          model: stabilityai/stable-diffusion-xl-base-1.0
          prompts:
            - "A serene mountain landscape at sunset, photorealistic"
          output_dir: ./generated_images
          hf_token_env_var: HF_TOKEN
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'hf/images/marketing_hero').")
    model_id: str = Field(
        alias="model",
        description=(
            "Text-to-image HF Hub model id (e.g. 'black-forest-labs/FLUX.1-dev', "
            "'stabilityai/stable-diffusion-xl-base-1.0'). Aliased to `model:` in YAML."
        ),
    )
    prompts: List[str] = Field(description="List of text prompts. One PNG per prompt.")
    provider: Optional[str] = Field(
        default=None,
        description=(
            "Inference provider to route through. Common options: 'wavespeed', "
            "'falai', 'replicate', 'together'. Leave unset for HF default routing."
        ),
    )
    output_dir: str = Field(
        default="./generated_images",
        description="Directory to write generated PNGs (relative to working dir).",
    )
    filename_template: str = Field(
        default="{asset}_{index}_{slug}.png",
        description=(
            "Filename template. Placeholders: {asset} (sanitized asset_key), "
            "{index} (zero-padded), {slug} (slugged prompt)."
        ),
    )
    hf_token_env_var: str = Field(
        default="HF_TOKEN",
        description="Env var with HF token.",
        json_schema_extra={"ui:widget": "env_var"},
    )
    generation_kwargs: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Extra kwargs for text_to_image() — e.g. {'guidance_scale': 7.5, "
            "'num_inference_steps': 30, 'width': 1024, 'height': 1024}."
        ),
    )

    group_name: Optional[str] = Field(default="huggingface", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Key-value catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'huggingface').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys this asset depends on.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("huggingface")

        @asset(
            key=dg.AssetKey(self.asset_key.split("/")),
            description=self.description or f"HF text-to-image via {self.model_id}",
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.asset_tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def huggingface_text_to_image_asset(context: AssetExecutionContext) -> MaterializeResult:
            try:
                from huggingface_hub import InferenceClient
            except ImportError:
                raise ImportError(
                    "huggingface_text_to_image requires the 'huggingface_hub' package. "
                    "Install with: pip install huggingface-hub"
                )

            token = os.environ.get(_self.hf_token_env_var, "")
            if not token:
                raise ValueError(
                    f"HuggingFace token not found in env var {_self.hf_token_env_var!r}. "
                    f"Text-to-image requires authentication."
                )

            client_kwargs: Dict[str, Any] = {"api_key": token}
            if _self.provider:
                client_kwargs["provider"] = _self.provider

            client = InferenceClient(**client_kwargs)

            output_dir = os.path.abspath(_self.output_dir)
            os.makedirs(output_dir, exist_ok=True)

            asset_slug = re.sub(r"[^a-zA-Z0-9_]+", "_", _self.asset_key.lower()).strip("_") or "image"
            gkw = dict(_self.generation_kwargs or {})

            saved_paths: List[str] = []
            errors: List[Dict[str, str]] = []

            for i, prompt in enumerate(_self.prompts):
                try:
                    context.log.info(
                        f"[{i+1}/{len(_self.prompts)}] generating: {prompt[:80]!r} "
                        f"via model={_self.model_id} provider={_self.provider or '(default)'}"
                    )
                    image = client.text_to_image(prompt, model=_self.model_id, **gkw)
                    filename = _self.filename_template.format(
                        asset=asset_slug,
                        index=f"{i:03d}",
                        slug=_slug(prompt),
                    )
                    path = os.path.join(output_dir, filename)
                    image.save(path, format="PNG")
                    saved_paths.append(path)
                    context.log.info(f"  → saved {path}")
                except Exception as e:
                    context.log.warning(f"  → failed: {e}")
                    errors.append({"prompt": prompt, "error": str(e)})

            metadata: Dict[str, Any] = {
                "model": _self.model_id,
                "provider": _self.provider or "(default)",
                "output_dir": output_dir,
                "prompt_count": len(_self.prompts),
                "saved_count": len(saved_paths),
                "failed_count": len(errors),
                "saved_paths": MetadataValue.md(
                    "\n".join(f"- `{p}`" for p in saved_paths) or "_(no images saved)_"
                ),
            }
            if errors:
                metadata["errors"] = MetadataValue.md(
                    "\n".join(f"- `{e['prompt'][:60]}` → {e['error']}" for e in errors)
                )

            return MaterializeResult(metadata=metadata)

        return Definitions(assets=[huggingface_text_to_image_asset])
