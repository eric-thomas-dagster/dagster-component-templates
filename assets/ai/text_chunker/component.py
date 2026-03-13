"""Text Chunker Asset Component.

Split text into chunks for embedding generation, LLM processing, or other downstream tasks.
Supports multiple chunking strategies including character-based, token-based, and semantic chunking.
"""

import re
from typing import Optional, List, Dict, Any
import pandas as pd

from dagster import (
    AssetIn,
    AssetKey,
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

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with text to chunk"
    )

    source_column: str = Field(
        default="text",
        description="Column name containing text to chunk"
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        source_column = self.source_column
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
            description=description,
            partitions_def=partitions_def,
            group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def text_chunker_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            """Asset that chunks text from upstream DataFrame into smaller pieces."""

            input_df = upstream

            if source_column not in input_df.columns:
                raise ValueError(f"Source column '{source_column}' not found. Available: {list(input_df.columns)}")

            context.log.info(f"Chunking {len(input_df)} rows using {chunking_strategy} strategy")
            context.log.info(f"Chunk size: {chunk_size}, overlap: {chunk_overlap}")

            all_chunk_records = []
            total_chunks = 0

            for doc_idx, row in input_df.iterrows():
                text = str(row[source_column])

                if not text.strip():
                    context.log.warning(f"Skipping empty text at row {doc_idx}")
                    continue

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
                    chunks = self._chunk_semantic_simple(text, chunk_size, chunk_overlap)

                else:
                    raise ValueError(f"Unknown chunking strategy: {chunking_strategy}")

                total_chunks += len(chunks)

                # Build chunk records
                if add_metadata:
                    current_pos = 0
                    for i, chunk in enumerate(chunks):
                        start_pos = text.find(chunk, current_pos)
                        end_pos = start_pos + len(chunk)
                        record = {
                            "source_row_index": doc_idx,
                            "chunk_index": i,
                            "total_chunks": len(chunks),
                            "text": chunk,
                            "char_start": start_pos,
                            "char_end": end_pos,
                            "char_length": len(chunk),
                            "word_count": len(chunk.split()),
                        }
                        current_pos = start_pos
                        all_chunk_records.append(record)
                else:
                    for i, chunk in enumerate(chunks):
                        all_chunk_records.append({
                            "source_row_index": doc_idx,
                            "chunk_index": i,
                            "text": chunk,
                        })

            result_df = pd.DataFrame(all_chunk_records)

            # Save to file
            if save_to_file and output_path:
                import json as _json
                import os as _os
                context.log.info(f"Saving chunks to {output_path}")
                _os.makedirs(_os.path.dirname(output_path), exist_ok=True)
                result_df.to_parquet(output_path, index=False)

            context.log.info(f"Created {total_chunks} total chunks from {len(input_df)} documents")

            context.add_output_metadata({
                "num_chunks": total_chunks,
                "source_documents": len(input_df),
                "chunking_strategy": chunking_strategy,
                "chunk_size": chunk_size,
                "chunk_overlap": chunk_overlap,
                "avg_chunk_length": int(result_df["char_length"].mean()) if "char_length" in result_df.columns and len(result_df) > 0 else 0,
            })

            return result_df

        return Definitions(assets=[text_chunker_asset])

    def _chunk_fixed_chars(self, text: str, size: int, overlap: int, preserve_sentences: bool) -> List[str]:
        """Chunk by fixed character count."""
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
