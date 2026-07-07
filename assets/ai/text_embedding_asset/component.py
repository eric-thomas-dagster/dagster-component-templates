"""Text Embedding Asset — turn one or more texts into a DataFrame of embeddings.

Useful when you need a query embedding for vector search (typical RAG pattern)
or when you're pre-embedding a small curated set of prompts / labels.

For row-wise embedding of an upstream DataFrame column, prefer
``embeddings_generator``. This component is for the "I have literal text
strings in my config and want them embedded into an asset" shape.

Providers supported (via the OpenAI-compatible embeddings API):
  - openai (default) — text-embedding-3-small, text-embedding-3-large
  - vercel_ai_gateway — same protocol, different base_url
  - any other OpenAI-compatible embedding endpoint via api_base_env_var

Returns a pandas DataFrame with columns: text, embedding (list[float]).
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class TextEmbeddingAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materialize a small set of literal texts as an embedding DataFrame.

    Example — one-row RAG query embedding:

        ```yaml
        type: dagster_community_components.TextEmbeddingAssetComponent
        attributes:
          asset_name: rag_query_embedding
          texts:
            - "How does Dagster orchestrate around long-running Temporal workflows?"
          model: text-embedding-3-small
          api_key_env_var: OPENAI_API_KEY
          dimensions: 1536
        ```

    Example — pre-embed a set of intent labels for classification:

        ```yaml
        attributes:
          asset_name: intent_label_embeddings
          texts:
            - "billing question"
            - "shipping status"
            - "product return"
            - "technical support"
          model: text-embedding-3-small
        ```

    Downstream `SupabaseVectorSearchAssetComponent` (or any pgvector /
    Pinecone / Weaviate consumer) reads the ``embedding`` column via
    ``query_embedding_column: embedding``.
    """

    asset_name: str = Field(description="Output asset name.")
    texts: List[str] = Field(
        description=(
            "Literal text strings to embed. Each becomes one row in the "
            "output DataFrame. For a single-question RAG query, pass a "
            "list with one element."
        ),
    )
    model_name: str = Field(
        default="text-embedding-3-small",
        alias="model",
        description="Embedding model — 'text-embedding-3-small' / 'text-embedding-3-large' or any OpenAI-compatible model at api_base_url.",
    )
    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Env var containing the API key.",
    )
    api_base_env_var: Optional[str] = Field(
        default=None,
        description="Optional env var containing a custom base URL — e.g. https://ai-gateway.vercel.sh/v1 for Vercel AI Gateway.",
    )
    dimensions: Optional[int] = Field(
        default=None,
        description="Optional embedding dimensionality (only text-embedding-3-* models support this). Defaults to model default.",
    )
    text_column: str = Field(default="text", description="Column name for the source text.")
    embedding_column: str = Field(default="embedding", description="Column name for the embedding vector.")

    group_name: Optional[str] = Field(default=None, description="Asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'ai', 'embedding').",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"ai", "embedding"})

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=_kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Embed {len(_self.texts)} text(s) via {_self.model_name}"
            ),
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext):
            import os
            import pandas as pd
            try:
                from openai import OpenAI
            except ImportError:
                raise ImportError("text_embedding_asset requires: pip install 'openai>=1.0.0'")

            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                raise RuntimeError(f"{_self.api_key_env_var!r} env var not set.")
            client_kwargs: Dict[str, Any] = {"api_key": api_key}
            if _self.api_base_env_var:
                base_url = os.environ.get(_self.api_base_env_var)
                if base_url:
                    client_kwargs["base_url"] = base_url
            client = OpenAI(**client_kwargs)

            call_kwargs: Dict[str, Any] = {"model": _self.model_name, "input": list(_self.texts)}
            if _self.dimensions:
                call_kwargs["dimensions"] = _self.dimensions

            context.log.info(
                f"[text_embedding] embedding {len(_self.texts)} text(s) via {_self.model_name}"
            )
            resp = client.embeddings.create(**call_kwargs)
            embeddings = [e.embedding for e in resp.data]
            df = pd.DataFrame({_self.text_column: _self.texts, _self.embedding_column: embeddings})

            context.add_output_metadata({
                "text_count": dg.MetadataValue.int(len(_self.texts)),
                "model": dg.MetadataValue.text(_self.model_name),
                "dimensions": dg.MetadataValue.int(len(embeddings[0]) if embeddings else 0),
                "usage_tokens": dg.MetadataValue.int(getattr(resp.usage, "total_tokens", 0) or 0),
                "preview": dg.MetadataValue.md(
                    df[[_self.text_column]].head(5).to_markdown(index=False)
                ),
            })
            return df

        return dg.Definitions(assets=[_asset])
