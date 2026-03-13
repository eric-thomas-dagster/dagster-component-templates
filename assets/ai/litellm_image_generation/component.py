"""LiteLLM Image Generation Component.

Generate images from text prompts in a DataFrame column using LiteLLM.
Supports DALL-E, Stable Diffusion, and other image generation models.
"""
from dataclasses import dataclass
from typing import Optional
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


@dataclass
class LitellmImageGenerationComponent(Component, Model, Resolvable):
    """Generate images from text prompts in a DataFrame column using LiteLLM.

    Supports DALL-E 3, DALL-E 2, Stable Diffusion, and other image generation models.
    Writes image URLs or base64-encoded data to the output column.

    Example:
        ```yaml
        type: dagster_component_templates.LitellmImageGenerationComponent
        attributes:
          asset_name: generated_product_images
          upstream_asset_key: product_descriptions
          prompt_column: description
          model: dall-e-3
          size: 1024x1024
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    prompt_column: str = Field(description="Column containing image prompts")
    output_column: str = Field(default="image_url", description="Column to write image URLs or base64 data")
    model: str = Field(default="dall-e-3", description='Model to use (e.g. "dall-e-3", "dall-e-2", "stability/stable-diffusion-xl-1024-v1-0")')
    size: str = Field(default="1024x1024", description="Image size: 256x256, 512x512, 1024x1024, 1792x1024, or 1024x1792")
    quality: str = Field(default="standard", description="Image quality: standard or hd")
    response_format: str = Field(default="url", description="Response format: url or b64_json")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var name for API key")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        prompt_column = self.prompt_column
        output_column = self.output_column
        model = self.model
        size = self.size
        quality = self.quality
        response_format = self.response_format
        api_key_env_var = self.api_key_env_var
        group_name = self.group_name

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
            kinds={"ai", "image-generation"},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            try:
                import litellm
            except ImportError:
                raise ImportError("pip install litellm>=1.0.0")

            import os

            df = upstream.copy()
            context.log.info(f"Generating {len(df)} images with model={model}")

            kwargs: dict = {
                "model": model,
                "size": size,
                "quality": quality,
                "response_format": response_format,
                "n": 1,
            }
            if api_key_env_var:
                kwargs["api_key"] = os.environ[api_key_env_var]

            results = []
            for i, row in df.iterrows():
                prompt = str(row[prompt_column])
                response = litellm.image_generation(prompt=prompt, **kwargs)
                image_data = response.data[0]

                if response_format == "url":
                    results.append(image_data.url)
                else:
                    results.append(image_data.b64_json)

                if (i + 1) % 10 == 0:
                    context.log.info(f"Generated {i + 1}/{len(df)} images")

            df[output_column] = results

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "model": MetadataValue.text(model),
                "size": MetadataValue.text(size),
                "response_format": MetadataValue.text(response_format),
            })
            return df

        return Definitions(assets=[_asset])
