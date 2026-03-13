"""Image LLM Extractor Component.

Send images to a vision LLM and extract structured fields as new columns.
"""

from dataclasses import dataclass
from typing import Optional, Dict
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
class ImageLlmExtractorComponent(Component, Model, Resolvable):
    """Component for extracting structured fields from images using vision LLMs.

    Sends each image to a vision-capable LLM (GPT-4o, Claude, Gemini) and extracts
    the requested fields as new DataFrame columns. Supports file paths (base64-encoded)
    or direct URLs.

    Features:
    - Works with any vision-capable model via litellm (GPT-4o, Claude, Gemini)
    - Configurable extraction fields with descriptions
    - File path (base64) or URL input modes
    - Optional prompt prefix for domain-specific instructions
    - Null-safe: missing fields become None

    Use Cases:
    - Product image data extraction (name, price, brand)
    - Receipt / label parsing
    - Real estate image analysis
    - Medical image metadata extraction
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame"
    )
    image_column: str = Field(description="Column with image file paths or URLs")
    extraction_fields: Dict[str, str] = Field(
        description=(
            "Mapping of column_name -> description, "
            "e.g. {'product_name': 'name of product shown', 'price': 'price if visible'}"
        )
    )
    prompt_prefix: Optional[str] = Field(
        default=None,
        description="Extra instruction prepended to the extraction prompt",
    )
    model: str = Field(
        default="gpt-4o-mini",
        description="Vision-capable model name (litellm format)",
    )
    max_tokens: int = Field(default=500, description="Maximum tokens in LLM response")
    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Environment variable holding the API key",
    )
    input_type: str = Field(
        default="file",
        description="Input type: 'file' (base64 encode) or 'url' (pass directly)",
    )
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
        image_column = self.image_column
        extraction_fields = self.extraction_fields
        prompt_prefix = self.prompt_prefix
        model = self.model
        max_tokens = self.max_tokens
        api_key_env_var = self.api_key_env_var
        input_type = self.input_type

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
            group_name=self.group_name,
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
            import os
            import json
            import base64

            try:
                from litellm import completion
            except ImportError:
                raise ImportError("litellm required: pip install litellm")

            df = upstream.copy()
            field_names = list(extraction_fields.keys())
            field_descriptions = "\n".join(
                f"- {k}: {v}" for k, v in extraction_fields.items()
            )

            results = []
            for path in df[image_column]:
                path_str = str(path)
                try:
                    if input_type == "file":
                        with open(path_str, "rb") as f:
                            img_data = base64.b64encode(f.read()).decode("utf-8")
                        # Detect mime type
                        ext = path_str.lower().rsplit(".", 1)[-1]
                        mime = {
                            "jpg": "image/jpeg", "jpeg": "image/jpeg",
                            "png": "image/png", "gif": "image/gif",
                            "webp": "image/webp",
                        }.get(ext, "image/jpeg")
                        image_content = {
                            "type": "image_url",
                            "image_url": {"url": f"data:{mime};base64,{img_data}"},
                        }
                    else:
                        image_content = {
                            "type": "image_url",
                            "image_url": {"url": path_str},
                        }

                    prompt_parts = []
                    if prompt_prefix:
                        prompt_parts.append(prompt_prefix)
                    prompt_parts.append(
                        f"Extract the following fields from this image as JSON:\n{field_descriptions}\n\n"
                        "Return only a valid JSON object with the requested field names as keys. "
                        "Use null for fields that cannot be determined."
                    )

                    resp = completion(
                        model=model,
                        messages=[
                            {
                                "role": "user",
                                "content": [
                                    {"type": "text", "text": "\n\n".join(prompt_parts)},
                                    image_content,
                                ],
                            }
                        ],
                        max_tokens=max_tokens,
                        api_key=os.environ.get(api_key_env_var),
                    )
                    raw = resp.choices[0].message.content.strip()
                    if raw.startswith("```"):
                        raw = raw.split("```")[1]
                        if raw.startswith("json"):
                            raw = raw[4:]
                    extracted = json.loads(raw)
                except Exception as e:
                    context.log.warning(f"Extraction failed for {path}: {e}")
                    extracted = {f: None for f in field_names}

                results.append(extracted)

            extracted_df = pd.DataFrame(results)
            for col in field_names:
                df[col] = extracted_df.get(col, None)

            context.add_output_metadata(
                {
                    "row_count": MetadataValue.int(len(df)),
                    "extracted_fields": field_names,
                    "model": model,
                    "preview": MetadataValue.md(df.head(5).to_markdown()),
                }
            )
            return df

        return Definitions(assets=[_asset])
