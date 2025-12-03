"""Document Chunker Component.

Split documents into chunks for RAG (Retrieval Augmented Generation) and embeddings.
Supports multiple chunking strategies: fixed-size, semantic, recursive, sentence-based, and token-aware.
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
    Output,
    MetadataValue,
)
from pydantic import Field


class DocumentChunkerComponent(Component, Model, Resolvable):
    """Component for splitting documents into chunks for embeddings and RAG.

    This component splits long documents into smaller chunks suitable for vector embeddings
    and retrieval. It supports multiple chunking strategies and preserves metadata.

    Features:
    - Multiple chunking strategies (fixed, semantic, recursive, sentence, token-aware)
    - Configurable chunk size and overlap
    - Metadata preservation (source, page, position, etc.)
    - Token-aware chunking for LLM context limits
    - Sentence boundary detection
    - Semantic chunking based on meaning
    - Batch processing of multiple documents

    Use Cases:
    - Prepare documents for vector embeddings
    - RAG (Retrieval Augmented Generation) pipelines
    - Long document processing
    - Knowledge base indexing
    - Document search systems

    Example:
        ```yaml
        type: dagster_component_templates.DocumentChunkerComponent
        attributes:
          asset_name: document_chunks
          strategy: recursive
          chunk_size: 1000
          chunk_overlap: 200
          source_column: document_text
          metadata_columns: "doc_id,title,author"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the chunked data"
    )

    strategy: str = Field(
        default="recursive",
        description="Chunking strategy: fixed, semantic, recursive, sentence, token_aware"
    )

    chunk_size: int = Field(
        default=1000,
        description="Target chunk size in characters (or tokens for token_aware strategy)"
    )

    chunk_overlap: int = Field(
        default=200,
        description="Number of characters (or tokens) to overlap between chunks"
    )

    source_column: str = Field(
        default="text",
        description="Column name containing document text"
    )

    output_column: str = Field(
        default="chunk",
        description="Column name for chunk text"
    )

    metadata_columns: Optional[str] = Field(
        default=None,
        description="Comma-separated list of columns to preserve as metadata (e.g., 'doc_id,title,author')"
    )

    add_chunk_metadata: bool = Field(
        default=True,
        description="Add chunk metadata (chunk_index, total_chunks, start_char, end_char)"
    )

    separators: Optional[str] = Field(
        default=None,
        description="Custom separators for recursive strategy (comma-separated). Default: '\\n\\n,\\n, ,'"
    )

    sentence_tokenizer: str = Field(
        default="simple",
        description="Sentence tokenizer: simple (regex), nltk, spacy"
    )

    tokenizer_model: Optional[str] = Field(
        default=None,
        description="Model for token counting (e.g., 'gpt-3.5-turbo', 'cl100k_base'). Required for token_aware strategy."
    )

    min_chunk_size: int = Field(
        default=100,
        description="Minimum chunk size to avoid very small chunks"
    )

    merge_small_chunks: bool = Field(
        default=True,
        description="Merge chunks smaller than min_chunk_size with adjacent chunks"
    )

    preserve_sentences: bool = Field(
        default=True,
        description="Avoid splitting sentences when possible (for fixed and token_aware)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="document_processing",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        strategy = self.strategy
        chunk_size = self.chunk_size
        chunk_overlap = self.chunk_overlap
        source_column = self.source_column
        output_column = self.output_column
        metadata_columns_str = self.metadata_columns
        add_chunk_metadata = self.add_chunk_metadata
        separators_str = self.separators
        sentence_tokenizer = self.sentence_tokenizer
        tokenizer_model = self.tokenizer_model
        min_chunk_size = self.min_chunk_size
        merge_small_chunks = self.merge_small_chunks
        preserve_sentences = self.preserve_sentences
        description = self.description or f"Document chunks using {strategy} strategy"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def document_chunker_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that splits documents into chunks."""

            context.log.info(f"Starting document chunking with strategy: {strategy}")

            # Get input DataFrame from upstream assets
            input_df = None
            for key, value in kwargs.items():
                if isinstance(value, pd.DataFrame):
                    input_df = value
                    context.log.info(f"Received DataFrame from '{key}': {len(value)} rows")
                    break

            if input_df is None:
                raise ValueError("Document Chunker requires an upstream DataFrame with document text")

            # Validate source column exists
            if source_column not in input_df.columns:
                raise ValueError(f"Source column '{source_column}' not found. Available: {list(input_df.columns)}")

            # Parse metadata columns
            metadata_cols = []
            if metadata_columns_str:
                metadata_cols = [col.strip() for col in metadata_columns_str.split(',')]
                for col in metadata_cols:
                    if col not in input_df.columns:
                        context.log.warning(f"Metadata column '{col}' not found, skipping")
                        metadata_cols.remove(col)

            # Parse separators
            separators = None
            if separators_str:
                separators = [s.strip() for s in separators_str.split(',')]
                # Unescape newlines
                separators = [s.replace('\\n', '\n') for s in separators]
            else:
                separators = ['\n\n', '\n', '. ', ' ', '']

            # Initialize tokenizer if needed
            tokenizer = None
            if strategy == "token_aware":
                if not tokenizer_model:
                    raise ValueError("tokenizer_model required for token_aware strategy")

                try:
                    import tiktoken
                    if tokenizer_model in ["gpt-4", "gpt-3.5-turbo", "gpt-4-turbo"]:
                        tokenizer = tiktoken.encoding_for_model(tokenizer_model)
                    else:
                        tokenizer = tiktoken.get_encoding(tokenizer_model)
                    context.log.info(f"Using tiktoken tokenizer: {tokenizer_model}")
                except ImportError:
                    raise ImportError("tiktoken not installed. Install with: pip install tiktoken")

            def count_tokens(text: str) -> int:
                """Count tokens in text."""
                if tokenizer:
                    return len(tokenizer.encode(text))
                return len(text)

            def split_by_sentences(text: str) -> List[str]:
                """Split text into sentences."""
                if sentence_tokenizer == "nltk":
                    try:
                        import nltk
                        try:
                            nltk.data.find('tokenizers/punkt')
                        except LookupError:
                            nltk.download('punkt', quiet=True)
                        from nltk.tokenize import sent_tokenize
                        return sent_tokenize(text)
                    except ImportError:
                        context.log.warning("NLTK not available, falling back to simple tokenizer")
                elif sentence_tokenizer == "spacy":
                    try:
                        import spacy
                        nlp = spacy.load("en_core_web_sm")
                        doc = nlp(text)
                        return [sent.text for sent in doc.sents]
                    except:
                        context.log.warning("spaCy not available, falling back to simple tokenizer")

                # Simple regex-based sentence splitter
                sentences = re.split(r'(?<=[.!?])\s+', text)
                return sentences

            def chunk_fixed(text: str) -> List[str]:
                """Fixed-size chunking with optional overlap."""
                chunks = []
                start = 0
                text_len = len(text)

                while start < text_len:
                    end = start + chunk_size

                    # Try to end at sentence boundary if preserve_sentences
                    if preserve_sentences and end < text_len:
                        # Look for sentence ending near the boundary
                        boundary_window = text[max(0, end - 100):min(text_len, end + 100)]
                        sentence_ends = [m.end() for m in re.finditer(r'[.!?]\s', boundary_window)]
                        if sentence_ends:
                            # Find closest to desired end
                            closest = min(sentence_ends, key=lambda x: abs(x - 100))
                            end = max(0, end - 100) + closest

                    chunk = text[start:end].strip()
                    if len(chunk) >= min_chunk_size or not chunks:
                        chunks.append(chunk)

                    start = end - chunk_overlap

                return chunks

            def chunk_recursive(text: str, seps: List[str]) -> List[str]:
                """Recursive chunking using hierarchical separators."""
                if not seps:
                    return [text]

                separator = seps[0]
                remaining_seps = seps[1:]

                splits = text.split(separator)
                chunks = []
                current_chunk = []
                current_size = 0

                for split in splits:
                    split_size = len(split)

                    if current_size + split_size + len(separator) <= chunk_size:
                        current_chunk.append(split)
                        current_size += split_size + len(separator)
                    else:
                        # Finalize current chunk
                        if current_chunk:
                            chunk_text = separator.join(current_chunk).strip()
                            if len(chunk_text) >= min_chunk_size:
                                chunks.append(chunk_text)

                        # Handle large split
                        if split_size > chunk_size and remaining_seps:
                            # Recurse with next separator
                            chunks.extend(chunk_recursive(split, remaining_seps))
                        else:
                            current_chunk = [split]
                            current_size = split_size

                # Add remaining
                if current_chunk:
                    chunk_text = separator.join(current_chunk).strip()
                    if len(chunk_text) >= min_chunk_size or not chunks:
                        chunks.append(chunk_text)

                return chunks

            def chunk_sentence(text: str) -> List[str]:
                """Sentence-based chunking."""
                sentences = split_by_sentences(text)
                chunks = []
                current_chunk = []
                current_size = 0

                for sentence in sentences:
                    sentence_size = len(sentence)

                    if current_size + sentence_size <= chunk_size:
                        current_chunk.append(sentence)
                        current_size += sentence_size
                    else:
                        # Finalize current chunk
                        if current_chunk:
                            chunks.append(' '.join(current_chunk).strip())
                        current_chunk = [sentence]
                        current_size = sentence_size

                # Add remaining
                if current_chunk:
                    chunks.append(' '.join(current_chunk).strip())

                return chunks

            def chunk_token_aware(text: str) -> List[str]:
                """Token-aware chunking for LLM context limits."""
                if preserve_sentences:
                    sentences = split_by_sentences(text)
                else:
                    # Split by words
                    sentences = text.split(' ')

                chunks = []
                current_chunk = []
                current_tokens = 0

                for sentence in sentences:
                    sentence_tokens = count_tokens(sentence)

                    if current_tokens + sentence_tokens <= chunk_size:
                        current_chunk.append(sentence)
                        current_tokens += sentence_tokens
                    else:
                        # Finalize current chunk
                        if current_chunk:
                            chunk_text = ' '.join(current_chunk).strip() if not preserve_sentences else ' '.join(current_chunk).strip()
                            chunks.append(chunk_text)
                        current_chunk = [sentence]
                        current_tokens = sentence_tokens

                # Add remaining
                if current_chunk:
                    chunk_text = ' '.join(current_chunk).strip()
                    chunks.append(chunk_text)

                return chunks

            def chunk_semantic(text: str) -> List[str]:
                """Semantic chunking based on paragraph structure."""
                # Split by paragraphs first
                paragraphs = text.split('\n\n')
                chunks = []
                current_chunk = []
                current_size = 0

                for para in paragraphs:
                    para = para.strip()
                    if not para:
                        continue

                    para_size = len(para)

                    if current_size + para_size <= chunk_size:
                        current_chunk.append(para)
                        current_size += para_size
                    else:
                        # Finalize current chunk
                        if current_chunk:
                            chunks.append('\n\n'.join(current_chunk))

                        # If paragraph too large, split it
                        if para_size > chunk_size:
                            chunks.extend(chunk_fixed(para))
                        else:
                            current_chunk = [para]
                            current_size = para_size

                # Add remaining
                if current_chunk:
                    chunks.append('\n\n'.join(current_chunk))

                return chunks

            # Process each document
            all_chunks = []
            total_documents = len(input_df)

            for idx, row in input_df.iterrows():
                doc_text = str(row[source_column])

                # Skip empty documents
                if not doc_text.strip():
                    context.log.warning(f"Skipping empty document at index {idx}")
                    continue

                # Choose chunking strategy
                if strategy == "fixed":
                    chunks = chunk_fixed(doc_text)
                elif strategy == "recursive":
                    chunks = chunk_recursive(doc_text, separators)
                elif strategy == "sentence":
                    chunks = chunk_sentence(doc_text)
                elif strategy == "token_aware":
                    chunks = chunk_token_aware(doc_text)
                elif strategy == "semantic":
                    chunks = chunk_semantic(doc_text)
                else:
                    raise ValueError(f"Unknown strategy: {strategy}")

                # Merge small chunks if enabled
                if merge_small_chunks and len(chunks) > 1:
                    merged = []
                    i = 0
                    while i < len(chunks):
                        chunk = chunks[i]
                        if len(chunk) < min_chunk_size and i + 1 < len(chunks):
                            # Merge with next chunk
                            chunk = chunk + " " + chunks[i + 1]
                            i += 2
                        else:
                            i += 1
                        merged.append(chunk)
                    chunks = merged

                # Create chunk records
                total_chunks = len(chunks)
                for chunk_idx, chunk_text in enumerate(chunks):
                    chunk_record = {output_column: chunk_text}

                    # Add metadata from original row
                    for col in metadata_cols:
                        chunk_record[col] = row[col]

                    # Add chunk metadata
                    if add_chunk_metadata:
                        chunk_record['chunk_index'] = chunk_idx
                        chunk_record['total_chunks'] = total_chunks
                        chunk_record['chunk_size'] = len(chunk_text)
                        if strategy == "token_aware" and tokenizer:
                            chunk_record['chunk_tokens'] = count_tokens(chunk_text)

                    all_chunks.append(chunk_record)

                if idx % 100 == 0 and idx > 0:
                    context.log.info(f"Processed {idx}/{total_documents} documents")

            # Create result DataFrame
            result_df = pd.DataFrame(all_chunks)

            context.log.info(f"Chunking complete: {len(result_df)} chunks from {total_documents} documents")
            context.log.info(f"Average chunks per document: {len(result_df) / total_documents:.1f}")

            # Calculate statistics
            chunk_sizes = result_df['chunk_size'].tolist() if 'chunk_size' in result_df.columns else [len(c) for c in result_df[output_column]]
            avg_chunk_size = sum(chunk_sizes) / len(chunk_sizes)
            min_chunk = min(chunk_sizes)
            max_chunk = max(chunk_sizes)

            # Metadata
            metadata = {
                "strategy": strategy,
                "total_chunks": len(result_df),
                "source_documents": total_documents,
                "avg_chunks_per_doc": len(result_df) / total_documents,
                "avg_chunk_size": int(avg_chunk_size),
                "min_chunk_size": int(min_chunk),
                "max_chunk_size": int(max_chunk),
                "target_chunk_size": chunk_size,
                "chunk_overlap": chunk_overlap,
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

        return Definitions(assets=[document_chunker_asset])
