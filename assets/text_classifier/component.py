"""Text Classifier Asset Component.

Classify text using LLMs with predefined categories or custom classification tasks.
"""

import os
import json
from typing import Optional, List

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
)
from pydantic import Field


class TextClassifierComponent(Component, Model, Resolvable):
    """Component for classifying text with LLMs.

    Example:
        ```yaml
        type: dagster_component_templates.TextClassifierComponent
        attributes:
          asset_name: text_classification
          provider: openai
          model: gpt-4
          categories: '["positive", "negative", "neutral"]'
          classification_task: "sentiment analysis"
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    provider: str = Field(description="LLM provider")
    model: str = Field(description="Model name")
    categories: str = Field(description="JSON array of categories")
    classification_task: str = Field(default="classification", description="Task description")
    include_confidence: bool = Field(default=True, description="Include confidence scores")
    include_reasoning: bool = Field(default=False, description="Include reasoning")
    api_key: Optional[str] = Field(default=None, description="API key with ${VAR_NAME} syntax")
    temperature: float = Field(default=0.1, description="Temperature (low for consistency)")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        provider = self.provider
        model = self.model
        categories_str = self.categories
        classification_task = self.classification_task
        include_confidence = self.include_confidence
        include_reasoning = self.include_reasoning
        api_key = self.api_key
        temperature = self.temperature
        description = self.description or f"Classify text using {provider}"
        group_name = self.group_name

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def text_classifier_asset(ctx: AssetExecutionContext, **kwargs):
            """Classify text.

            Accepts text from upstream assets via IO manager.
            Compatible with: Document Text Extractor, Text Chunker, or any text-producing asset.
            """

            # Get text from upstream assets
            upstream_assets = {k: v for k, v in kwargs.items()}

            if not upstream_assets:
                raise ValueError(
                    f"Text Classifier '{asset_name}' requires at least one upstream asset "
                    "that produces text. Connect a text-producing asset."
                )

            ctx.log.info(f"Received {len(upstream_assets)} upstream asset(s)")

            # Extract text from first upstream asset
            text = None
            for key, value in upstream_assets.items():
                if isinstance(value, str):
                    text = value
                    ctx.log.info(f"Using text from '{key}' (string, {len(value)} chars)")
                    break
                elif isinstance(value, dict) and 'text' in value:
                    text = value['text']
                    ctx.log.info(f"Using text from '{key}' dict['text']")
                    break
                elif isinstance(value, list) and value and isinstance(value[0], str):
                    text = value[0]
                    ctx.log.info(f"Using first text from '{key}' (list)")
                    break

            if not text:
                raise ValueError(
                    f"No text found in upstream assets. Received: {list(upstream_assets.keys())}. "
                    f"Ensure upstream assets produce text as string or dict with 'text' key."
                )

            categories = json.loads(categories_str)

            # Expand environment variables in API key
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and '${' in api_key:
                    raise ValueError(f"Environment variable in api_key '{api_key}' is not set")

            ctx.log.info(f"Classifying text into {len(categories)} categories")

            # Build prompt
            prompt = f"Perform {classification_task} on the following text.\n\n"
            prompt += f"Categories: {', '.join(categories)}\n\n"
            prompt += f"Text: {text}\n\n"
            prompt += "Respond with a JSON object containing:\n"
            prompt += "- category: the selected category\n"

            if include_confidence:
                prompt += "- confidence: confidence score between 0 and 1\n"

            if include_reasoning:
                prompt += "- reasoning: brief explanation of the classification\n"

            # Call LLM
            if provider == "openai":
                import openai
                client = openai.OpenAI(api_key=expanded_api_key)
                response = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=temperature,
                    response_format={"type": "json_object"}
                )
                result_text = response.choices[0].message.content
            elif provider == "anthropic":
                import anthropic
                client = anthropic.Anthropic(api_key=expanded_api_key)
                message = client.messages.create(
                    model=model,
                    max_tokens=1024,
                    temperature=temperature,
                    messages=[{"role": "user", "content": prompt}]
                )
                result_text = message.content[0].text
            else:
                raise ValueError(f"Unsupported provider: {provider}")

            # Parse result
            result = json.loads(result_text)

            ctx.log.info(f"Classification: {result.get('category')}")

            ctx.add_output_metadata({
                "category": result.get('category'),
                "confidence": result.get('confidence', 'N/A')
            })

            return result

        return Definitions(assets=[text_classifier_asset])
