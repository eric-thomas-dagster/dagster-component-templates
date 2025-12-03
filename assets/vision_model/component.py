"""Vision Model Component.

Analyze images using GPT-4 Vision and Claude 3 models for OCR, captioning,
object detection, visual question answering, and content moderation.
"""

import os
import time
import base64
from typing import Optional, Dict, Any, List
from pathlib import Path
import pandas as pd

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


class VisionModelComponent(Component, Model, Resolvable):
    """Component for image analysis using vision-capable LLMs.

    This component enables image understanding using GPT-4 Vision and Claude 3 models.
    It supports batch processing of images with various analysis tasks including OCR,
    captioning, object detection descriptions, visual question answering, and content moderation.

    Features:
    - Multiple providers: OpenAI (GPT-4 Vision, GPT-4o), Anthropic (Claude 3)
    - Image input: URLs or local file paths
    - Base64 encoding support
    - Batch image processing
    - Multiple images per request
    - Custom prompts for analysis
    - Detail level control (GPT-4)
    - Cost tracking and optimization
    - Retry logic with exponential backoff

    Use Cases:
    - Document OCR and text extraction
    - Product image analysis and cataloging
    - Content moderation and safety checks
    - Visual question answering
    - Image captioning and descriptions
    - Chart and diagram interpretation
    - Receipt and invoice processing
    - Medical image analysis (descriptions)

    Example:
        ```yaml
        type: dagster_component_templates.VisionModelComponent
        attributes:
          asset_name: product_image_analysis
          source_asset: product_images
          provider: openai
          model: gpt-4o
          prompt: "Describe this product image in detail"
          image_column: image_url
          api_key: "${OPENAI_API_KEY}"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold image analysis results"
    )

    source_asset: str = Field(
        description="Name of upstream asset containing image references"
    )

    provider: str = Field(
        description="Vision model provider: openai, anthropic"
    )

    model: str = Field(
        description="Model name (e.g., 'gpt-4o', 'gpt-4-vision-preview', 'claude-3-opus-20240229')"
    )

    api_key: str = Field(
        description="API key for provider. Use ${API_KEY_NAME} for env vars."
    )

    prompt: str = Field(
        description="Analysis prompt/instructions for the vision model"
    )

    image_column: str = Field(
        default="image_url",
        description="Column name containing image URLs or file paths"
    )

    output_column: str = Field(
        default="vision_analysis",
        description="Column name for analysis results"
    )

    image_type: str = Field(
        default="url",
        description="Image input type: url, path, base64"
    )

    detail_level: str = Field(
        default="auto",
        description="Detail level for GPT-4 Vision: low, high, auto"
    )

    max_images_per_request: int = Field(
        default=1,
        description="Maximum images per API request (1-10 for multi-image analysis)"
    )

    temperature: float = Field(
        default=0.3,
        description="Temperature for response randomness (0.0-1.0)"
    )

    max_tokens: Optional[int] = Field(
        default=500,
        description="Maximum tokens in response"
    )

    batch_size: int = Field(
        default=5,
        description="Number of images to process in parallel"
    )

    rate_limit_delay: float = Field(
        default=0.5,
        description="Delay between API calls in seconds"
    )

    max_retries: int = Field(
        default=3,
        description="Maximum retries for failed requests"
    )

    track_costs: bool = Field(
        default=True,
        description="Track token usage and estimated costs"
    )

    include_image_metadata: bool = Field(
        default=True,
        description="Include image metadata (size, format) in results"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="vision",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_asset = self.source_asset
        provider = self.provider
        model = self.model
        api_key = self.api_key
        prompt = self.prompt
        image_column = self.image_column
        output_column = self.output_column
        image_type = self.image_type
        detail_level = self.detail_level
        max_images_per_request = self.max_images_per_request
        temperature = self.temperature
        max_tokens = self.max_tokens
        batch_size = self.batch_size
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries
        track_costs = self.track_costs
        include_image_metadata = self.include_image_metadata
        description = self.description or f"Vision analysis using {provider}/{model}"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Cost per 1M tokens (approximate, as of 2024)
        COST_PER_1M_INPUT = {
            "gpt-4-vision-preview": 10.0,
            "gpt-4o": 5.0,
            "gpt-4o-mini": 0.15,
            "claude-3-opus-20240229": 15.0,
            "claude-3-sonnet-20240229": 3.0,
            "claude-3-haiku-20240307": 0.25,
        }
        COST_PER_1M_OUTPUT = {
            "gpt-4-vision-preview": 30.0,
            "gpt-4o": 15.0,
            "gpt-4o-mini": 0.6,
            "claude-3-opus-20240229": 75.0,
            "claude-3-sonnet-20240229": 15.0,
            "claude-3-haiku-20240307": 1.25,
        }

        # Image token costs (approximate)
        IMAGE_TOKEN_COST_LOW = 85  # Low detail
        IMAGE_TOKEN_COST_HIGH = 170  # High detail per 512x512 tile

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def vision_model_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that analyzes images using vision models."""

            context.log.info(f"Starting vision analysis with {provider}/{model}")

            # Get input DataFrame from upstream assets
            input_df = None
            for key, value in kwargs.items():
                if isinstance(value, pd.DataFrame):
                    input_df = value
                    context.log.info(f"Received DataFrame from '{key}': {len(value)} rows")
                    break

            if input_df is None:
                raise ValueError("Vision Model requires an upstream DataFrame with image references")

            # Validate image column
            if image_column not in input_df.columns:
                raise ValueError(f"Image column '{image_column}' not found. Available: {list(input_df.columns)}")

            # Expand API key
            expanded_api_key = os.path.expandvars(api_key)
            if expanded_api_key == api_key and api_key.startswith('${'):
                var_name = api_key.strip('${}')
                raise ValueError(f"Environment variable not set: {var_name}")

            # Helper function to encode image
            def encode_image(image_ref: str) -> str:
                """Encode image to base64 or return URL."""
                if image_type == "url":
                    return image_ref
                elif image_type == "base64":
                    return image_ref
                elif image_type == "path":
                    # Read local file and encode
                    try:
                        with open(image_ref, "rb") as image_file:
                            encoded = base64.b64encode(image_file.read()).decode('utf-8')
                            # Detect image format from extension
                            ext = Path(image_ref).suffix.lower()
                            mime_type = {
                                '.jpg': 'jpeg', '.jpeg': 'jpeg',
                                '.png': 'png', '.gif': 'gif', '.webp': 'webp'
                            }.get(ext, 'jpeg')
                            return f"data:image/{mime_type};base64,{encoded}"
                    except Exception as e:
                        raise ValueError(f"Failed to read image file '{image_ref}': {e}")
                else:
                    raise ValueError(f"Invalid image_type: {image_type}")

            # Helper function to get image metadata
            def get_image_metadata(image_ref: str) -> Dict[str, Any]:
                """Get image metadata (size, format)."""
                if image_type == "path" and os.path.exists(image_ref):
                    try:
                        from PIL import Image
                        with Image.open(image_ref) as img:
                            return {
                                "width": img.width,
                                "height": img.height,
                                "format": img.format,
                                "mode": img.mode,
                                "size_kb": os.path.getsize(image_ref) / 1024
                            }
                    except ImportError:
                        context.log.warning("PIL not installed, skipping image metadata")
                    except Exception as e:
                        context.log.warning(f"Failed to get metadata for {image_ref}: {e}")
                return {}

            # Helper function to call vision API
            def call_vision_api(image_refs: List[str], attempt: int = 0) -> tuple[str, int, int]:
                """Call vision API with retry logic."""
                try:
                    if provider == "openai":
                        import openai
                        client = openai.OpenAI(api_key=expanded_api_key)

                        # Prepare content
                        content = [{"type": "text", "text": prompt}]

                        for image_ref in image_refs:
                            encoded_image = encode_image(image_ref)

                            if image_type == "url":
                                content.append({
                                    "type": "image_url",
                                    "image_url": {
                                        "url": encoded_image,
                                        "detail": detail_level
                                    }
                                })
                            else:
                                content.append({
                                    "type": "image_url",
                                    "image_url": {
                                        "url": encoded_image,
                                        "detail": detail_level
                                    }
                                })

                        # Call API
                        response = client.chat.completions.create(
                            model=model,
                            messages=[{
                                "role": "user",
                                "content": content
                            }],
                            temperature=temperature,
                            max_tokens=max_tokens
                        )

                        response_text = response.choices[0].message.content
                        tokens_in = response.usage.prompt_tokens
                        tokens_out = response.usage.completion_tokens

                        return response_text, tokens_in, tokens_out

                    elif provider == "anthropic":
                        import anthropic
                        client = anthropic.Anthropic(api_key=expanded_api_key)

                        # Prepare content
                        content = []

                        for image_ref in image_refs:
                            encoded_image = encode_image(image_ref)

                            # Extract base64 data and media type
                            if encoded_image.startswith("data:"):
                                # Format: data:image/jpeg;base64,<data>
                                header, data = encoded_image.split(",", 1)
                                media_type = header.split(";")[0].split(":")[1]
                                content.append({
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": media_type,
                                        "data": data
                                    }
                                })
                            elif image_type == "url":
                                # Claude 3 requires base64, so we need to fetch and encode URLs
                                import requests
                                response = requests.get(encoded_image)
                                img_data = base64.b64encode(response.content).decode('utf-8')
                                media_type = response.headers.get('content-type', 'image/jpeg')
                                content.append({
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": media_type,
                                        "data": img_data
                                    }
                                })

                        # Add text prompt
                        content.append({
                            "type": "text",
                            "text": prompt
                        })

                        # Call API
                        response = client.messages.create(
                            model=model,
                            max_tokens=max_tokens or 1024,
                            temperature=temperature,
                            messages=[{
                                "role": "user",
                                "content": content
                            }]
                        )

                        response_text = response.content[0].text
                        tokens_in = response.usage.input_tokens
                        tokens_out = response.usage.output_tokens

                        return response_text, tokens_in, tokens_out

                    else:
                        raise ValueError(f"Unsupported provider: {provider}")

                except Exception as e:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * rate_limit_delay
                        context.log.warning(f"API error: {e}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        return call_vision_api(image_refs, attempt + 1)
                    raise

            # Process images
            results = []
            metadata_list = []
            total_input_tokens = 0
            total_output_tokens = 0
            total_images = 0

            # Group images for multi-image requests
            image_refs = input_df[image_column].tolist()

            for i in range(0, len(image_refs), max_images_per_request):
                batch_end = min(i + max_images_per_request, len(image_refs))
                batch_images = image_refs[i:batch_end]

                context.log.info(f"Processing images {i + 1}-{batch_end}/{len(image_refs)}")

                # Get image metadata if requested
                batch_metadata = []
                if include_image_metadata:
                    for img_ref in batch_images:
                        batch_metadata.append(get_image_metadata(img_ref))

                # Call vision API
                try:
                    response_text, tokens_in, tokens_out = call_vision_api(batch_images)
                    results.append(response_text)
                    metadata_list.append(batch_metadata[0] if batch_metadata else {})
                    total_input_tokens += tokens_in
                    total_output_tokens += tokens_out
                    total_images += len(batch_images)

                    # If multi-image request, duplicate result for each image
                    if len(batch_images) > 1:
                        for _ in range(len(batch_images) - 1):
                            results.append(response_text)
                            metadata_list.append({})

                except Exception as e:
                    context.log.error(f"Failed to process images {i + 1}-{batch_end}: {e}")
                    # Add error placeholder
                    for _ in range(len(batch_images)):
                        results.append(f"ERROR: {str(e)}")
                        metadata_list.append({})

                # Rate limiting
                if rate_limit_delay > 0 and batch_end < len(image_refs):
                    time.sleep(rate_limit_delay)

            # Add results to DataFrame
            result_df = input_df.copy()
            result_df[output_column] = results

            # Add image metadata columns if requested
            if include_image_metadata and metadata_list:
                for key in ["width", "height", "format", "size_kb"]:
                    if any(key in m for m in metadata_list):
                        result_df[f"image_{key}"] = [m.get(key) for m in metadata_list]

            elapsed_time = time.time()
            context.log.info(f"Completed processing {total_images} images")

            # Calculate costs
            cost_input = 0.0
            cost_output = 0.0
            if track_costs and total_input_tokens > 0:
                for key in COST_PER_1M_INPUT.keys():
                    if key in model:
                        cost_input = (total_input_tokens / 1_000_000) * COST_PER_1M_INPUT[key]
                        cost_output = (total_output_tokens / 1_000_000) * COST_PER_1M_OUTPUT[key]
                        break

            total_cost = cost_input + cost_output

            # Metadata
            metadata = {
                "provider": provider,
                "model": model,
                "images_processed": total_images,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "estimated_cost_usd": f"${total_cost:.4f}",
                "avg_tokens_per_image": f"{total_input_tokens / total_images:.0f}" if total_images > 0 else "0",
            }

            if include_sample and len(result_df) > 0:
                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(result_df.head(10))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        return Definitions(assets=[vision_model_asset])
