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
class ImageCaptionerComponent(Component, Model, Resolvable):
    """Generate captions for images from a file path column using a vision-capable LLM."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
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
    image_path_column: str = Field(description="Column containing local image file paths or URLs")
    output_column: str = Field(default="caption", description="Column to write generated caption text")
    model: str = Field(default="gpt-4o-mini", description="Vision-capable model to use for captioning")
    prompt: str = Field(
        default="Describe this image concisely.",
        description="Prompt to send with each image",
    )
    max_tokens: int = Field(default=200, description="Maximum tokens in the caption response")
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable name containing the API key",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        image_path_column = self.image_path_column
        output_column = self.output_column
        model = self.model
        prompt = self.prompt
        max_tokens = self.max_tokens
        api_key_env_var = self.api_key_env_var

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
            import base64

            if image_path_column not in upstream.columns:
                raise ValueError(f"Column '{image_path_column}' not found in DataFrame.")

            api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
            df = upstream.copy()
            captions = []

            for image_ref in df[image_path_column]:
                if image_ref is None or (isinstance(image_ref, float) and pd.isna(image_ref)):
                    captions.append(None)
                    continue
                try:
                    image_str = str(image_ref)
                    if image_str.startswith("http://") or image_str.startswith("https://"):
                        image_content = {"url": image_str}
                    else:
                        with open(image_str, "rb") as f:
                            b64 = base64.b64encode(f.read()).decode("utf-8")
                        ext = image_str.rsplit(".", 1)[-1].lower()
                        mime = f"image/{ext}" if ext in ("png", "jpg", "jpeg", "gif", "webp") else "image/jpeg"
                        image_content = {"url": f"data:{mime};base64,{b64}"}

                    response = litellm.completion(
                        model=model,
                        messages=[
                            {
                                "role": "user",
                                "content": [
                                    {"type": "text", "text": prompt},
                                    {"type": "image_url", "image_url": image_content},
                                ],
                            }
                        ],
                        max_tokens=max_tokens,
                                    api_key=api_key,
            )
                    captions.append(response.choices[0].message.content)
                except Exception as e:
                    context.log.warning(f"Failed to caption image '{image_ref}': {e}")
                    captions.append(None)

            df[output_column] = captions

            successful = sum(1 for c in captions if c is not None)
            context.add_output_metadata({
                "row_count": MetadataValue.int(len(df)),
                "successful_captions": MetadataValue.int(successful),
                "model": MetadataValue.text(model),
            })
            return df

        return Definitions(assets=[_asset])
