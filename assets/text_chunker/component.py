"""Text Chunker Asset Component.

Split text into chunks for embedding generation, LLM processing, or other downstream tasks.
Supports multiple chunking strategies including character-based, token-based, and semantic chunking.
"""

import re
from typing import Optional, List, Dict, Any
import pandas as pd

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


class TextChunkerComponent(Component, Model, Resolvable):
    """Component for chunking text into smaller pieces.

    This asset splits text into chunks using various strategies. Useful for preparing
    text for embedding generation, LLM context windows, or batch processing.

    Example:
        ```yaml
        type: dagster_component_templates.TextChunkerComponent
        attributes:
          asset_name: text_chunks
          chunking_strategy: fixed_tokens
          chunk_size: 512
          chunk_overlap: 50
        ```
    """

    asset_name: str = Field(
        description="Name of the asset"
    )

    chunking_strategy: str = Field(
        default="fixed_chars",
        description="Strategy: 'fixed_chars', 'fixed_tokens', 'sentence', 'paragraph', 'semantic', 'recursive'"
    )

    chunk_size: int = Field(
        default=1000,
        description="Target size of each chunk (characters or tokens depending on strategy)"
    )

    chunk_overlap: int = Field(
        default=200,
        description="Overlap between consecutive chunks"
    )

    separator: Optional[str] = Field(
        default=None,
        description="Custom separator for splitting (for recursive strategy)"
    )

    preserve_sentences: bool = Field(
        default=True,
        description="Try to avoid breaking sentences in the middle"
    )

    add_metadata: bool = Field(
        default=True,
        description="Add metadata to each chunk (index, character positions)"
    )

    output_format: str = Field(
        default="list",
        description="Output format: 'list' (list of strings), 'dataframe', 'json'"
    )

    save_to_file: bool = Field(
        default=False,
        description="Save chunks to a file"
    )

    output_path: Optional[str] = Field(
        default=None,
        description="Path to save chunks"
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
        chunking_strategy = self.chunking_strategy
        chunk_size = self.chunk_size
        chunk_overlap = self.chunk_overlap
        separator = self.separator
        preserve_sentences = self.preserve_sentences
        add_metadata = self.add_metadata
        output_format = self.output_format
        save_to_file = self.save_to_file
        output_path = self.output_path
        description = self.description or f"Chunk text using {chunking_strategy} strategy"
        group_name = self.group_name

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def text_chunker_asset(context: AssetExecutionContext, **kwargs) -> Any:
            """Asset that chunks text into smaller pieces.

            Accepts text from upstream assets via IO manager.
            Compatible with: Document Text Extractor, other text-producing assets.
            """

            # Get text from upstream assets
            upstream_assets = {k: v for k, v in kwargs.items()}

            if not upstream_assets:
                raise ValueError(
                    f"Text Chunker '{asset_name}' requires at least one upstream asset "
                    "that produces text. Connect a text-producing asset like Document Text Extractor."
                )

            context.log.info(f"Received {len(upstream_assets)} upstream asset(s)")

            # Get text from first upstream asset
            text = None
            for key, value in upstream_assets.items():
                if isinstance(value, str):
                    text = value
                    context.log.info(f"Using text from '{key}' ({len(value)} characters)")
                    break
                elif isinstance(value, dict) and 'text' in value:
                    text = value['text']
                    context.log.info(f"Using text from '{key}' dict ({len(text)} characters)")
                    break

            if not text:
                raise ValueError(f"No text found in upstream assets. Received: {list(upstream_assets.keys())}")

            context.log.info(f"Chunking {len(text)} characters using {chunking_strategy} strategy")
            context.log.info(f"Chunk size: {chunk_size}, overlap: {chunk_overlap}")

            chunks = []

            if chunking_strategy == "fixed_chars":
                chunks = self._chunk_fixed_chars(text, chunk_size, chunk_overlap, preserve_sentences)

            elif chunking_strategy == "fixed_tokens":
                chunks = self._chunk_fixed_tokens(text, chunk_size, chunk_overlap, preserve_sentences)

            elif chunking_strategy == "sentence":
                chunks = self._chunk_sentences(text, chunk_size)

            elif chunking_strategy == "paragraph":
                chunks = self._chunk_paragraphs(text, chunk_size)

            elif chunking_strategy == "recursive":
                separators = [separator] if separator else ["\n\n", "\n", ". ", " ", ""]
                chunks = self._chunk_recursive(text, chunk_size, chunk_overlap, separators)

            elif chunking_strategy == "semantic":
                # Simplified semantic chunking (would need embeddings for true semantic chunking)
                chunks = self._chunk_semantic_simple(text, chunk_size, chunk_overlap)

            else:
                raise ValueError(f"Unknown chunking strategy: {chunking_strategy}")

            context.log.info(f"Created {len(chunks)} chunks")

            # Add metadata
            chunk_data = []
            if add_metadata:
                current_pos = 0
                for i, chunk in enumerate(chunks):
                    start_pos = text.find(chunk, current_pos)
                    end_pos = start_pos + len(chunk)
                    chunk_data.append({
                        "chunk_index": i,
                        "text": chunk,
                        "char_start": start_pos,
                        "char_end": end_pos,
                        "char_length": len(chunk),
                        "word_count": len(chunk.split()),
                    })
                    current_pos = start_pos
            else:
                chunk_data = [{"text": chunk} for chunk in chunks]

            # Format output
            result = chunks
            if output_format == "dataframe":
                result = pd.DataFrame(chunk_data)
            elif output_format == "json":
                result = {
                    "chunks": chunk_data,
                    "total_chunks": len(chunks),
                    "original_length": len(text),
                    "chunking_strategy": chunking_strategy,
                }
            elif output_format == "list":
                result = chunks

            # Save to file
            if save_to_file and output_path:
                import json
                import os
                context.log.info(f"Saving chunks to {output_path}")
                os.makedirs(os.path.dirname(output_path), exist_ok=True)

                if output_format == "dataframe":
                    result.to_parquet(output_path, index=False)
                elif output_format == "json":
                    with open(output_path, 'w') as f:
                        json.dump(result, f, indent=2)
                else:
                    with open(output_path, 'w') as f:
                        json.dump({"chunks": chunks}, f, indent=2)

            # Add metadata
            metadata = {
                "num_chunks": len(chunks),
                "chunking_strategy": chunking_strategy,
                "chunk_size": chunk_size,
                "chunk_overlap": chunk_overlap,
                "original_length": len(text),
                "avg_chunk_length": sum(len(c) for c in chunks) // len(chunks) if chunks else 0,
            }

            context.add_output_metadata(metadata)

            return result

        return Definitions(assets=[text_chunker_asset])

    def _chunk_fixed_chars(self, text: str, size: int, overlap: int, preserve_sentences: bool) -> List[str]:
        """Chunk by fixed character count."""
        chunks = []
        start = 0

        while start < len(text):
            end = start + size

            # Try to end at sentence boundary if preserve_sentences
            if preserve_sentences and end < len(text):
                # Look for sentence endings
                sentence_endings = ['. ', '! ', '? ', '.\n', '!\n', '?\n']
                best_end = end
                for i in range(end, max(start + size // 2, start), -1):
                    for ending in sentence_endings:
                        if text[i:i+len(ending)] == ending:
                            best_end = i + len(ending)
                            break
                    if best_end != end:
                        break
                end = best_end

            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)

            start = end - overlap if overlap < end - start else end

        return chunks

    def _chunk_fixed_tokens(self, text: str, token_size: int, overlap: int, preserve_sentences: bool) -> List[str]:
        """Chunk by fixed token count (approximate using whitespace)."""
        # Simplified token counting (words)
        words = text.split()
        chunks = []
        start = 0

        while start < len(words):
            end = min(start + token_size, len(words))
            chunk_words = words[start:end]
            chunk = ' '.join(chunk_words)

            if preserve_sentences and end < len(words):
                # Try to end at sentence boundary
                for i in range(len(chunk) - 1, max(len(chunk) // 2, 0), -1):
                    if chunk[i] in '.!?':
                        chunk = chunk[:i + 1]
                        break

            chunks.append(chunk.strip())
            start = end - overlap if overlap < end - start else end

        return chunks

    def _chunk_sentences(self, text: str, max_chars: int) -> List[str]:
        """Chunk by sentences, combining until size limit."""
        # Simple sentence splitting
        sentences = re.split(r'([.!?]+[\s\n]+)', text)
        sentences = [''.join(sentences[i:i+2]).strip() for i in range(0, len(sentences), 2)]

        chunks = []
        current_chunk = ""

        for sentence in sentences:
            if len(current_chunk) + len(sentence) <= max_chars:
                current_chunk += " " + sentence if current_chunk else sentence
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = sentence

        if current_chunk:
            chunks.append(current_chunk.strip())

        return chunks

    def _chunk_paragraphs(self, text: str, max_chars: int) -> List[str]:
        """Chunk by paragraphs, combining until size limit."""
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]

        chunks = []
        current_chunk = ""

        for para in paragraphs:
            if len(current_chunk) + len(para) <= max_chars:
                current_chunk += "\n\n" + para if current_chunk else para
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = para

        if current_chunk:
            chunks.append(current_chunk.strip())

        return chunks

    def _chunk_recursive(self, text: str, size: int, overlap: int, separators: List[str]) -> List[str]:
        """Recursively chunk using hierarchical separators."""
        if not separators:
            return [text]

        separator = separators[0]
        remaining_separators = separators[1:]

        parts = text.split(separator)
        chunks = []
        current_chunk = ""

        for part in parts:
            if len(current_chunk) + len(part) <= size:
                current_chunk += separator + part if current_chunk else part
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                # If part is too large, recursively split
                if len(part) > size and remaining_separators:
                    sub_chunks = self._chunk_recursive(part, size, overlap, remaining_separators)
                    chunks.extend(sub_chunks)
                    current_chunk = ""
                else:
                    current_chunk = part

        if current_chunk:
            chunks.append(current_chunk.strip())

        return chunks

    def _chunk_semantic_simple(self, text: str, size: int, overlap: int) -> List[str]:
        """Simplified semantic chunking based on paragraph and sentence structure."""
        # This is a simplified version - true semantic chunking would use embeddings
        paragraphs = text.split('\n\n')
        chunks = []

        for para in paragraphs:
            if len(para) <= size:
                chunks.append(para.strip())
            else:
                # Split long paragraphs by sentences
                sentences = re.split(r'([.!?]+\s+)', para)
                current_chunk = ""
                for i in range(0, len(sentences), 2):
                    sentence = ''.join(sentences[i:i+2])
                    if len(current_chunk) + len(sentence) <= size:
                        current_chunk += sentence
                    else:
                        if current_chunk:
                            chunks.append(current_chunk.strip())
                        current_chunk = sentence

                if current_chunk:
                    chunks.append(current_chunk.strip())

        return [c for c in chunks if c]
