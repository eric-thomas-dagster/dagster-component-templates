"""DocumentCorpusComponent — the corpus AS state.

A `document_corpus` asset represents "the set of documents at time T." Each
materialization is a versioned snapshot of the corpus: doc count, content
hashes, total bytes. Downstream vector-index snapshots pin to a specific
corpus materialization, so a corpus change is a lineage event, not silent
drift under a stable name.

This is the entry point for the "RAG as state, not as a pipeline" story.
"""

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class DocumentCorpusComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materialize a directory of documents as a versioned corpus asset.

    Emits a DataFrame with one row per document: content, hash, size, path.
    Corpus-level metadata (doc_count, total_bytes, corpus_hash) surfaces on
    the materialization so downstream vector-index snapshots can pin to a
    specific corpus version and detect drift.
    """

    asset_name: str = Field(description="Dagster asset name")
    source_dir: str = Field(description="Absolute path to the directory of docs")
    file_glob: str = Field(default="**/*.md", description="Glob relative to source_dir (default '**/*.md')")
    encoding: str = Field(default="utf-8", description="Text file encoding")

    min_doc_count: int = Field(
        default=1,
        description="Asset check fails if fewer docs than this — guards against empty corpus regressions.",
    )

    group_name: Optional[str] = Field(default=None, description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (defaults to ['rag'])")
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None, description="Freshness policy: max lag before stale."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        source_dir = self.source_dir
        file_glob = self.file_glob
        encoding = self.encoding
        min_doc_count = self.min_doc_count

        kinds = self.kinds or ["rag"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        freshness = None
        if self.freshness_max_lag_minutes is not None:
            freshness = dg.FreshnessPolicy(maximum_lag_minutes=self.freshness_max_lag_minutes)

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Document corpus from {source_dir}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            freshness_policy=freshness,
        )
        def _corpus_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
            root = Path(source_dir).expanduser().resolve()
            if not root.is_dir():
                raise dg.Failure(description=f"source_dir does not exist or is not a directory: {root}")

            paths = sorted(root.glob(file_glob))
            context.log.info(f"Scanning {root} with glob {file_glob!r} — found {len(paths)} files")

            rows = []
            now_iso = datetime.now(timezone.utc).isoformat()
            for p in paths:
                if not p.is_file():
                    continue
                try:
                    content = p.read_text(encoding=encoding)
                except UnicodeDecodeError:
                    context.log.warning(f"Skipping (encoding): {p}")
                    continue
                content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
                rows.append(
                    {
                        "doc_id": str(p.relative_to(root)),
                        "content": content,
                        "source_path": str(p),
                        "content_hash": content_hash,
                        "byte_size": len(content.encode("utf-8")),
                        "ingested_at": now_iso,
                    }
                )

            df = pd.DataFrame(
                rows,
                columns=["doc_id", "content", "source_path", "content_hash", "byte_size", "ingested_at"],
            )

            # corpus_hash = hash of the sorted content hashes. Stable identifier
            # for "this exact set of doc contents" — downstream snapshots pin here.
            corpus_hash = hashlib.sha256(
                "\n".join(sorted(df["content_hash"].astype(str).tolist())).encode("utf-8")
            ).hexdigest() if len(df) else "empty"

            context.log.info(f"Corpus: {len(df)} docs, {int(df['byte_size'].sum())} bytes, corpus_hash={corpus_hash[:12]}…")

            context.add_output_metadata(
                {
                    "doc_count": dg.MetadataValue.int(len(df)),
                    "total_bytes": dg.MetadataValue.int(int(df["byte_size"].sum()) if len(df) else 0),
                    "corpus_hash": dg.MetadataValue.text(corpus_hash),
                    "source_dir": dg.MetadataValue.path(str(root)),
                    "file_glob": dg.MetadataValue.text(file_glob),
                }
            )
            return df

        @dg.asset_check(asset=_corpus_asset, name=f"{asset_name}_min_doc_count")
        def _min_doc_check(context: dg.AssetCheckExecutionContext, df: pd.DataFrame) -> dg.AssetCheckResult:
            n = int(len(df))
            passed = n >= min_doc_count
            return dg.AssetCheckResult(
                passed=passed,
                metadata={
                    "doc_count": dg.MetadataValue.int(n),
                    "min_required": dg.MetadataValue.int(min_doc_count),
                },
                description=(
                    f"Corpus has {n} doc(s); requires >= {min_doc_count}."
                    if passed
                    else f"Corpus regressed to {n} docs (< {min_doc_count})."
                ),
            )

        return dg.Definitions(assets=[_corpus_asset], asset_checks=[_min_doc_check])
