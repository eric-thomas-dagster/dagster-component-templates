"""Embedding Generator Asset Component.

Generate embeddings for text using various embedding models (OpenAI, Sentence Transformers, Cohere, etc.).
Supports batch processing and multiple output formats.
"""

import os
from typing import Optional, List, Union
import pandas as pd
import numpy as np

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


class EmbeddingGeneratorComponent(Component, Model, Resolvable):
    """Component for generating text embeddings.

    This asset generates embeddings for text using various embedding models.
    Supports batch processing of text chunks and multiple providers.
    Accepts text from upstream assets via IO manager.

    Example:
        ```yaml
        type: dagster_component_templates.EmbeddingGeneratorComponent
        attributes:
          asset_name: text_embeddings
          provider: openai
          model: text-embedding-3-small
          api_key: ${OPENAI_API_KEY}
        ```
    """

    asset_name: str = Field(
        description="Name of the asset"
    )

    provider: str = Field(
        description="Embedding provider: 'openai', 'sentence_transformers', 'cohere', 'huggingface'"
    )

    model: str = Field(
        description="Model name (e.g., 'text-embedding-3-small', 'all-MiniLM-L6-v2')"
    )

    text_column: str = Field(
        default="text",
        description="Column name containing text (if upstream is DataFrame)"
    )

    batch_size: int = Field(
        default=100,
        description="Batch size for processing multiple texts"
    )

    api_key: Optional[str] = Field(
        default=None,
        description="API key (use ${VAR_NAME} for environment variable, e.g., ${OPENAI_API_KEY})"
    )

    normalize_embeddings: bool = Field(
        default=True,
        description="Normalize embeddings to unit length"
    )

    output_format: str = Field(
        default="array",
        description="Output format: 'array' (numpy), 'list', 'dataframe'"
    )

    add_text_to_output: bool = Field(
        default=True,
        description="Include original text in output (for dataframe format)"
    )

    save_to_file: bool = Field(
        default=False,
        description="Save embeddings to file"
    )

    output_path: Optional[str] = Field(
        default=None,
        description="Path to save embeddings"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        provider = self.provider
        model = self.model
        text_column = self.text_column
        batch_size = self.batch_size
        api_key = self.api_key
        normalize_embeddings = self.normalize_embeddings
        output_format = self.output_format
        add_text_to_output = self.add_text_to_output
        save_to_file = self.save_to_file
        output_path = self.output_path
        description = self.description or f"Generate embeddings using {provider}/{model}"
        group_name = self.group_name

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def embedding_generator_asset(context: AssetExecutionContext, **kwargs) -> Union[np.ndarray, List, pd.DataFrame]:
            """Asset that generates embeddings for text.

            Accepts text from upstream assets via IO manager.
            Compatible with: Text Chunker, Document Text Extractor, or any text-producing asset.
            """

            # Get text from upstream assets
            upstream_assets = {k: v for k, v in kwargs.items()}

            if not upstream_assets:
                raise ValueError(
                    f"Embedding Generator '{asset_name}' requires at least one upstream asset "
                    "that produces text. Connect a text-producing asset like Text Chunker."
                )

            context.log.info(f"Received {len(upstream_assets)} upstream asset(s)")

            # Extract texts from upstream assets
            texts = []
            for key, value in upstream_assets.items():
                if isinstance(value, str):
                    texts.append(value)
                    context.log.info(f"Using text from '{key}' (single string)")
                elif isinstance(value, list):
                    if all(isinstance(item, str) for item in value):
                        texts.extend(value)
                        context.log.info(f"Using text from '{key}' (list of {len(value)} strings)")
                    elif all(isinstance(item, dict) and text_column in item for item in value):
                        texts.extend([item[text_column] for item in value])
                        context.log.info(f"Using text from '{key}' (list of {len(value)} dicts)")
                elif isinstance(value, pd.DataFrame):
                    if text_column in value.columns:
                        texts.extend(value[text_column].tolist())
                        context.log.info(f"Using text from '{key}' DataFrame column '{text_column}' ({len(value)} rows)")
                    else:
                        raise ValueError(f"DataFrame from '{key}' missing column '{text_column}'. Available: {list(value.columns)}")
                elif isinstance(value, dict) and text_column in value:
                    texts.append(value[text_column])
                    context.log.info(f"Using text from '{key}' dict")

            if not texts:
                raise ValueError(
                    f"No text found in upstream assets. Received: {list(upstream_assets.keys())}. "
                    f"Ensure upstream assets produce text strings, lists, or DataFrames with '{text_column}' column."
                )

            context.log.info(f"Generating embeddings for {len(texts)} text(s) using {provider}/{model}")

            # Expand environment variables in API key (supports ${VAR_NAME} pattern)
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    context.log.error(f"Environment variable not set: {var_name}")
                    raise ValueError(f"Environment variable not set: {var_name}")

            embeddings = []

            # Generate embeddings based on provider
            if provider == "openai":
                try:
                    import openai
                    client = openai.OpenAI(api_key=expanded_api_key)

                    # Process in batches
                    for i in range(0, len(texts), batch_size):
                        batch = texts[i:i + batch_size]
                        context.log.info(f"Processing batch {i // batch_size + 1} ({len(batch)} texts)")

                        response = client.embeddings.create(
                            model=model,
                            input=batch
                        )

                        batch_embeddings = [data.embedding for data in response.data]
                        embeddings.extend(batch_embeddings)

                except ImportError:
                    raise ImportError("OpenAI package not installed. Install with: pip install openai")

            elif provider == "sentence_transformers":
                try:
                    from sentence_transformers import SentenceTransformer
                    model_obj = SentenceTransformer(model)

                    # Process in batches
                    for i in range(0, len(texts), batch_size):
                        batch = texts[i:i + batch_size]
                        context.log.info(f"Processing batch {i // batch_size + 1} ({len(batch)} texts)")

                        batch_embeddings = model_obj.encode(
                            batch,
                            normalize_embeddings=normalize_embeddings,
                            show_progress_bar=False
                        )
                        embeddings.extend(batch_embeddings.tolist())

                except ImportError:
                    raise ImportError("sentence-transformers not installed. Install with: pip install sentence-transformers")

            elif provider == "cohere":
                try:
                    import cohere
                    client = cohere.Client(api_key=expanded_api_key)

                    # Process in batches
                    for i in range(0, len(texts), batch_size):
                        batch = texts[i:i + batch_size]
                        context.log.info(f"Processing batch {i // batch_size + 1} ({len(batch)} texts)")

                        response = client.embed(
                            texts=batch,
                            model=model,
                            input_type="search_document"
                        )
                        embeddings.extend(response.embeddings)

                except ImportError:
                    raise ImportError("Cohere package not installed. Install with: pip install cohere")

            elif provider == "huggingface":
                try:
                    from huggingface_hub import InferenceClient
                    client = InferenceClient(token=expanded_api_key)

                    # Process one at a time (API limitation)
                    for i, text in enumerate(texts):
                        if i % 10 == 0:
                            context.log.info(f"Processing text {i + 1}/{len(texts)}")

                        embedding = client.feature_extraction(text, model=model)
                        embeddings.append(embedding)

                except ImportError:
                    raise ImportError("Hugging Face package not installed. Install with: pip install huggingface-hub")

            else:
                raise ValueError(f"Unsupported provider: {provider}")

            context.log.info(f"Generated {len(embeddings)} embeddings")

            # Normalize if requested
            if normalize_embeddings and provider != "sentence_transformers":
                embeddings_array = np.array(embeddings)
                norms = np.linalg.norm(embeddings_array, axis=1, keepdims=True)
                embeddings_array = embeddings_array / norms
                embeddings = embeddings_array.tolist()

            # Format output
            result = embeddings
            if output_format == "array":
                result = np.array(embeddings)
            elif output_format == "dataframe":
                df_data = {"embedding": embeddings}
                if add_text_to_output:
                    df_data["text"] = texts
                result = pd.DataFrame(df_data)
            elif output_format == "list":
                result = embeddings

            # Save to file
            if save_to_file and output_path:
                import pickle
                context.log.info(f"Saving embeddings to {output_path}")
                os.makedirs(os.path.dirname(output_path), exist_ok=True)

                if output_format == "dataframe":
                    result.to_parquet(output_path, index=False)
                elif output_format == "array":
                    np.save(output_path, result)
                else:
                    with open(output_path, 'wb') as f:
                        pickle.dump(result, f)

            # Add metadata
            embedding_dim = len(embeddings[0]) if embeddings else 0
            metadata = {
                "provider": provider,
                "model": model,
                "num_embeddings": len(embeddings),
                "embedding_dimension": embedding_dim,
                "normalized": normalize_embeddings,
            }

            context.add_output_metadata(metadata)

            return result

        return Definitions(assets=[embedding_generator_asset])
