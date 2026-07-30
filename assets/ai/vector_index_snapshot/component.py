"""VectorIndexSnapshotComponent — immutable, versioned vector-index snapshots.

Every materialization writes to its own subdirectory
(``{snapshot_root_dir}/{snapshot_id}/``) and updates a ``latest`` symlink.
Old snapshots are preserved on disk so downstream RAG can point at a specific
past version — that's the rollback path when today's index has retrieval-quality
regressions.

Backed by ChromaDB (local persistent client). The default embedder is the
built-in ONNX Sentence-Transformers MiniLM — no API key needed for the demo
path. Pass ``embedder_provider: openai`` (plus ``api_key_env_var``) to swap
to OpenAI embeddings.
"""

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


def _chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
    """Simple fixed-size chunker with paragraph-preferred split points.

    Not the fanciest strategy — the community `document_chunker` component
    covers semantic / recursive / token-aware splits. For a rag-state demo
    we want deterministic, dep-free chunking that lets `corpus_hash → chunks`
    be reproducible."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if chunk_overlap < 0 or chunk_overlap >= chunk_size:
        raise ValueError("chunk_overlap must be >= 0 and < chunk_size")

    # Normalize whitespace but keep paragraph breaks.
    text = re.sub(r"[ \t]+", " ", text).strip()
    if not text:
        return []

    step = chunk_size - chunk_overlap
    chunks: List[str] = []
    i = 0
    while i < len(text):
        chunk = text[i : i + chunk_size]
        chunks.append(chunk.strip())
        if i + chunk_size >= len(text):
            break
        i += step
    return [c for c in chunks if c]


class VectorIndexSnapshotComponent(dg.Component, dg.Model, dg.Resolvable):
    """Chunk + embed an upstream corpus into a versioned ChromaDB snapshot.

    Each materialization creates a new snapshot directory under
    ``snapshot_root_dir`` and updates the ``latest`` symlink to point at it.
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(
        description="Upstream document_corpus asset key (supports slash-paths for cross-loc)."
    )

    snapshot_root_dir: str = Field(
        description="Directory that holds all snapshots. Each materialization writes a new subdir."
    )

    chunk_size: int = Field(default=800, description="Max chars per chunk")
    chunk_overlap: int = Field(default=100, description="Overlap chars between adjacent chunks")

    embedder_provider: str = Field(
        default="chromadb_default",
        description="'chromadb_default' (local ONNX MiniLM, no API key) | 'openai' (needs api_key_env_var)",
    )
    embedder_model: Optional[str] = Field(
        default=None,
        description="Provider-specific model name. Defaults: chromadb_default=all-MiniLM-L6-v2, openai=text-embedding-3-small",
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="For openai provider: env var holding the API key (e.g. OPENAI_API_KEY)",
    )

    collection_name: str = Field(
        default="rag_corpus",
        description="ChromaDB collection name inside each snapshot",
    )

    dynamic_partition_name: str = Field(
        default="rag_snapshot",
        description=(
            "Name of the DynamicPartitionsDefinition used to track snapshot ids. "
            "Downstream assets bind to a specific snapshot by partition key = snapshot_id, "
            "so rollback is 'materialize downstream against the older partition' — no rebuild."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (defaults to ['rag', 'chromadb'])")
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None, description="Freshness policy: max lag before this snapshot is considered stale."
    )
    freshness_cron: Optional[str] = Field(
        default=None, description="Cron schedule for the freshness policy."
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None, description="Max retries on asset failure (opt-in)."
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None, description="Seconds between retries (default 1)."
    )
    retry_policy_backoff: str = Field(
        default="exponential", description="Backoff strategy: 'linear' or 'exponential'."
    )

    # Partitioning — when set, the asset itself is partitioned. Each partition
    # key becomes the snapshot_id (no auto-generation). If unset, keeps the
    # v0 behavior: unpartitioned, one auto-id snapshot per materialization,
    # still registers the id on the downstream dynamic partitions def.
    partition_this_asset: bool = Field(
        default=False,
        description=(
            "When True, partition this asset by the same dynamic-partitions "
            "def named by `dynamic_partition_name` (default 'rag_snapshot'). "
            "Materialization takes the snapshot_id from context.partition_key "
            "instead of auto-generating a timestamp id. Enables graph-native "
            "rollback: `dg launch --assets docs_index_snapshot --partition snap_v3`."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        snapshot_root_dir = self.snapshot_root_dir
        chunk_size = self.chunk_size
        chunk_overlap = self.chunk_overlap
        embedder_provider = self.embedder_provider
        embedder_model = self.embedder_model
        api_key_env_var = self.api_key_env_var
        collection_name = self.collection_name
        dynamic_partition_name = self.dynamic_partition_name

        kinds = self.kinds or ["rag", "chromadb"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            freshness_policy = dg.FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        # When partition_this_asset=True, the snapshot asset itself is
        # partitioned by the same dynamic-partitions def it registers keys
        # on for downstream. The partition_key IS the snapshot_id.
        partitions_def = (
            dg.DynamicPartitionsDefinition(name=dynamic_partition_name)
            if self.partition_this_asset
            else None
        )
        _partition_this = self.partition_this_asset

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Vector index snapshot from {upstream_asset_key}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            deps=[dg.AssetKey.from_user_string(upstream_asset_key)],
            partitions_def=partitions_def,
            freshness_policy=freshness_policy,
            retry_policy=retry_policy,
        )
        def _snapshot_asset(context: dg.AssetExecutionContext) -> dict:
            import chromadb

            upstream_key = dg.AssetKey.from_user_string(upstream_asset_key)
            corpus_df = context.instance.get_latest_materialization_event(upstream_key)  # sanity: existence
            df = context.load_asset_value(upstream_key)
            if not isinstance(df, pd.DataFrame):
                raise dg.Failure(description=f"Upstream {upstream_asset_key} is not a DataFrame: {type(df).__name__}")

            required = {"doc_id", "content", "content_hash"}
            missing = required - set(df.columns)
            if missing:
                raise dg.Failure(description=f"Upstream corpus missing columns: {missing}")

            # Try to lift corpus_hash from the upstream materialization metadata.
            corpus_hash = "unknown"
            try:
                if corpus_df is not None:
                    md = corpus_df.asset_materialization.metadata  # type: ignore[union-attr]
                    if md and "corpus_hash" in md:
                        raw = md["corpus_hash"].value  # MetadataValue.text
                        corpus_hash = str(raw)
            except Exception:  # noqa: BLE001
                pass

            # snapshot_id: either the partition_key (when partitioned) or auto-generated timestamp id
            if _partition_this and context.has_partition_key:
                snapshot_id = context.partition_key
            else:
                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                snapshot_id = f"{ts}-{corpus_hash[:8] if corpus_hash != 'unknown' else 'nohash'}"
            snapshot_root = Path(snapshot_root_dir).expanduser().resolve()
            snapshot_root.mkdir(parents=True, exist_ok=True)
            snapshot_dir = snapshot_root / snapshot_id
            snapshot_dir.mkdir(parents=True, exist_ok=True)

            context.log.info(f"Writing snapshot to {snapshot_dir}")

            # Configure embedder
            if embedder_provider == "chromadb_default":
                from chromadb.utils import embedding_functions

                model_name = embedder_model or "all-MiniLM-L6-v2"
                embedding_fn = embedding_functions.DefaultEmbeddingFunction()
                embedder_label = f"chromadb_default:{model_name}"
            elif embedder_provider == "openai":
                from chromadb.utils import embedding_functions

                if not api_key_env_var:
                    raise dg.Failure(description="embedder_provider=openai requires api_key_env_var")
                api_key = os.environ.get(api_key_env_var)
                if not api_key:
                    raise dg.Failure(description=f"env var {api_key_env_var} is not set")
                model_name = embedder_model or "text-embedding-3-small"
                embedding_fn = embedding_functions.OpenAIEmbeddingFunction(
                    api_key=api_key, model_name=model_name
                )
                embedder_label = f"openai:{model_name}"
            else:
                raise dg.Failure(description=f"unknown embedder_provider: {embedder_provider}")

            client = chromadb.PersistentClient(path=str(snapshot_dir))
            collection = client.get_or_create_collection(
                name=collection_name,
                embedding_function=embedding_fn,
            )

            # Chunk + upsert
            chunk_ids: List[str] = []
            chunk_texts: List[str] = []
            chunk_metas: List[dict] = []
            for _, row in df.iterrows():
                doc_id = str(row["doc_id"])
                content = str(row["content"])
                pieces = _chunk_text(content, chunk_size, chunk_overlap)
                for idx, piece in enumerate(pieces):
                    chunk_id = f"{doc_id}::c{idx}"
                    chunk_ids.append(chunk_id)
                    chunk_texts.append(piece)
                    chunk_metas.append(
                        {
                            "doc_id": doc_id,
                            "chunk_idx": idx,
                            "content_hash": str(row["content_hash"]),
                            "corpus_hash": corpus_hash,
                        }
                    )

            if not chunk_ids:
                raise dg.Failure(description="No chunks produced — upstream corpus was empty or all content was whitespace.")

            # Batch-add to Chroma
            BATCH = 128
            for start in range(0, len(chunk_ids), BATCH):
                collection.add(
                    ids=chunk_ids[start : start + BATCH],
                    documents=chunk_texts[start : start + BATCH],
                    metadatas=chunk_metas[start : start + BATCH],
                )

            # Detect embedding dimension by peeking at a probe
            probe = collection.get(ids=[chunk_ids[0]], include=["embeddings"])
            dim = 0
            try:
                embs = probe.get("embeddings")
                if embs and len(embs) > 0 and embs[0] is not None:
                    dim = len(embs[0])
            except Exception:  # noqa: BLE001
                pass

            # Update `latest` symlink for easy downstream access
            latest_link = snapshot_root / "latest"
            try:
                if latest_link.is_symlink() or latest_link.exists():
                    latest_link.unlink()
                latest_link.symlink_to(snapshot_dir, target_is_directory=True)
            except OSError as e:
                # Windows / restricted FS: leave a marker file with the snapshot id instead.
                context.log.warning(f"Could not create 'latest' symlink ({e}); writing latest.txt marker instead.")
                (snapshot_root / "latest.txt").write_text(snapshot_id)

            # Register this snapshot_id as a new dynamic-partition key. Downstream
            # `rag_eval` / `rag_query` assets partitioned by the same dynamic name
            # can then be materialized against a specific snapshot — that's the
            # graph-native rollback path.
            try:
                context.instance.add_dynamic_partitions(
                    partitions_def_name=dynamic_partition_name,
                    partition_keys=[snapshot_id],
                )
                context.log.info(
                    f"Registered dynamic partition '{snapshot_id}' on '{dynamic_partition_name}' "
                    f"— downstream can rollback via partition selection."
                )
            except Exception as e:  # noqa: BLE001
                # Don't fail the materialization if dynamic-partition registration
                # errors out — the snapshot is on disk regardless.
                context.log.warning(f"Could not register dynamic partition: {e}")

            result = {
                "snapshot_path": str(snapshot_dir),
                "snapshot_id": snapshot_id,
                "collection_name": collection_name,
                "embedder": embedder_label,
                "chunk_count": len(chunk_ids),
                "dimension": dim,
                "corpus_hash": corpus_hash,
                "dynamic_partition_name": dynamic_partition_name,
            }

            context.log.info(
                f"Snapshot {snapshot_id}: {len(chunk_ids)} chunks, dim={dim}, embedder={embedder_label}"
            )
            context.add_output_metadata(
                {
                    "snapshot_id": dg.MetadataValue.text(snapshot_id),
                    "snapshot_path": dg.MetadataValue.path(str(snapshot_dir)),
                    "chunk_count": dg.MetadataValue.int(len(chunk_ids)),
                    "dimension": dg.MetadataValue.int(dim),
                    "embedder": dg.MetadataValue.text(embedder_label),
                    "corpus_hash": dg.MetadataValue.text(corpus_hash),
                    "collection_name": dg.MetadataValue.text(collection_name),
                }
            )
            return result

        return dg.Definitions(assets=[_snapshot_asset])
