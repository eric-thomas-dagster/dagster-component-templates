"""Document Summarizer Asset Component.

Summarize documents using LLMs with support for long documents via chunking and map-reduce.
"""

import os
from typing import Optional

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


class DocumentSummarizerComponent(Component, Model, Resolvable):
    """Component for summarizing documents with LLMs.

    Example:
        ```yaml
        type: dagster_component_templates.DocumentSummarizerComponent
        attributes:
          asset_name: document_summary
          provider: openai
          model: gpt-4
          summary_type: concise
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    provider: str = Field(description="LLM provider")
    model: str = Field(description="Model name")
    summary_type: str = Field(default="concise", description="Type: 'concise', 'detailed', 'bullet_points', 'executive'")
    max_length: Optional[int] = Field(default=None, description="Max summary length in words")
    chunk_size: int = Field(default=3000, description="Chunk size for long documents")
    use_map_reduce: bool = Field(default=True, description="Use map-reduce for long documents")
    api_key: Optional[str] = Field(default=None, description="API key with ${VAR_NAME} syntax")
    temperature: float = Field(default=0.3, description="Temperature")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        provider = self.provider
        model = self.model
        summary_type = self.summary_type
        max_length = self.max_length
        chunk_size = self.chunk_size
        use_map_reduce = self.use_map_reduce
        api_key = self.api_key
        temperature = self.temperature
        description = self.description or f"Summarize document with {provider}"
        group_name = self.group_name

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def document_summarizer_asset(ctx: AssetExecutionContext, **kwargs):
            """Summarize document.

            Accepts text from upstream assets via IO manager.
            Compatible with: Document Text Extractor, Text Chunker, or any text-producing asset.
            """

            # Get text from upstream assets
            upstream_assets = {k: v for k, v in kwargs.items()}

            if not upstream_assets:
                raise ValueError(
                    f"Document Summarizer '{asset_name}' requires at least one upstream asset "
                    "that produces text. Connect a text-producing asset like Document Text Extractor."
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
                    ctx.log.info(f"Using text from '{key}' dict['text'] ({len(text)} chars)")
                    break

            if not text:
                raise ValueError(
                    f"No text found in upstream assets. Received: {list(upstream_assets.keys())}. "
                    f"Ensure upstream assets produce text as string or dict with 'text' key."
                )

            # Expand environment variables in API key
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and '${' in api_key:
                    raise ValueError(f"Environment variable in api_key '{api_key}' is not set")

            # Build prompt based on summary type
            prompts = {
                "concise": "Provide a concise summary of the following text:\n\n{text}",
                "detailed": "Provide a detailed summary covering all key points:\n\n{text}",
                "bullet_points": "Summarize the following text as bullet points:\n\n{text}",
                "executive": "Provide an executive summary suitable for leadership:\n\n{text}"
            }

            base_prompt = prompts.get(summary_type, prompts["concise"])

            if max_length:
                base_prompt += f"\n\nLimit the summary to approximately {max_length} words."

            ctx.log.info(f"Summarizing {len(text)} characters")

            # Handle long documents
            if len(text) > chunk_size and use_map_reduce:
                ctx.log.info("Using map-reduce for long document")

                # Split into chunks
                chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

                # Map: Summarize each chunk
                chunk_summaries = []
                for i, chunk in enumerate(chunks):
                    ctx.log.info(f"Summarizing chunk {i+1}/{len(chunks)}")
                    prompt = f"Summarize this section:\n\n{chunk}"

                    if provider == "openai":
                        import openai
                        client = openai.OpenAI(api_key=expanded_api_key)
                        response = client.chat.completions.create(
                            model=model,
                            messages=[{"role": "user", "content": prompt}],
                            temperature=temperature
                        )
                        chunk_summary = response.choices[0].message.content
                    elif provider == "anthropic":
                        import anthropic
                        client = anthropic.Anthropic(api_key=expanded_api_key)
                        message = client.messages.create(
                            model=model,
                            max_tokens=4096,
                            temperature=temperature,
                            messages=[{"role": "user", "content": prompt}]
                        )
                        chunk_summary = message.content[0].text
                    else:
                        raise ValueError(f"Unsupported provider: {provider}")

                    chunk_summaries.append(chunk_summary)

                # Reduce: Combine summaries
                combined = "\n\n".join(chunk_summaries)
                final_prompt = base_prompt.format(text=combined)
            else:
                final_prompt = base_prompt.format(text=text)

            # Generate final summary
            ctx.log.info("Generating final summary")

            if provider == "openai":
                import openai
                client = openai.OpenAI(api_key=expanded_api_key)
                response = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": final_prompt}],
                    temperature=temperature
                )
                summary = response.choices[0].message.content
            elif provider == "anthropic":
                import anthropic
                client = anthropic.Anthropic(api_key=expanded_api_key)
                message = client.messages.create(
                    model=model,
                    max_tokens=4096,
                    temperature=temperature,
                    messages=[{"role": "user", "content": final_prompt}]
                )
                summary = message.content[0].text
            else:
                raise ValueError(f"Unsupported provider: {provider}")

            ctx.log.info(f"Generated summary ({len(summary)} characters)")

            ctx.add_output_metadata({
                "original_length": len(text),
                "summary_length": len(summary),
                "compression_ratio": round(len(text) / len(summary), 2)
            })

            return {
                "summary": summary,
                "summary_type": summary_type,
                "original_length": len(text),
                "summary_length": len(summary)
            }

        return Definitions(assets=[document_summarizer_asset])
