"""Text Chunker Asset Component.

Split text into chunks for embedding generation, LLM processing, or other downstream tasks.
Supports multiple chunking strategies including character-based, token-based, and semantic chunking.
"""

import re
from typing import Any, Dict, List, Optional, Union
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
    MetadataValue,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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

    source_column: Union[str, int] = Field(
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
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 25 "
            "rows or a sample) for builder UIs."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata. For long DataFrames "
            "(>10x preview_rows), a random sample is used; otherwise head()."
        ),
    )

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
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

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "text_chunker"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
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


            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(result_df.dtypes[col]))
                for col in result_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(result_df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Add column lineage if defined
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if 'upstream_asset_key' in dir() else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(result_df) > 0:
                try:
                    _prev = result_df.sample(min(preview_rows, len(result_df))) if len(result_df) > preview_rows * 10 else result_df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)

            return result_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[text_chunker_asset])


        return Definitions(assets=[text_chunker_asset], asset_checks=list(_schema_checks))

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
