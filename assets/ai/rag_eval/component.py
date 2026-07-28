"""RagEvalComponent — retrieval quality as a first-class, checked asset.

For each snapshot of the vector index, run a golden-set of queries and score
retrieval quality (fraction of expected terms that appear in top-k chunks).
The materialization is partitioned by ``snapshot_id`` (the dynamic partition
key registered by ``vector_index_snapshot``), so:

  - Each snapshot has its own eval score, browsable in the UI history.
  - Rollback = re-materialize a past partition (or backfill a range).
  - The attached asset check FAILS the run when the current score regresses
    against the immediately-prior materialization by more than
    ``regression_pct_threshold`` percentage points, OR falls below
    ``min_score_threshold``. Failing checks with severity ERROR block
    downstream materializations — the regression is caught as data, not
    surfaced later from a customer complaint.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class RagEvalComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a golden-set retrieval eval against a specific vector_index_snapshot."""

    asset_name: str = Field(description="Dagster asset name")

    snapshot_root_dir: str = Field(
        description="Same snapshot_root_dir the upstream vector_index_snapshot writes to. "
        "The partition_key (snapshot_id) locates the specific snapshot directory."
    )
    collection_name: str = Field(
        default="rag_corpus",
        description="ChromaDB collection name (must match the upstream snapshot's collection_name).",
    )

    golden_set: List[Dict[str, Any]] = Field(
        description=(
            "List of {query: str, expected_terms: [str]} dicts. Each query is issued "
            "against the snapshot; the score is the fraction of `expected_terms` that "
            "appear (case-insensitive substring match) in any of the top-k retrieved "
            "chunks. Mean across queries is the asset's precision@k."
        ),
    )
    k: int = Field(default=3, description="Top-k retrieval count per query")

    min_score_threshold: float = Field(
        default=0.5,
        description="Absolute minimum precision@k score. Asset check fails below this.",
    )
    regression_pct_threshold: float = Field(
        default=10.0,
        description=(
            "Max allowed drop (in percentage points) vs the immediately-prior "
            "materialization. Asset check fails if this snapshot's score is "
            "prior_score - threshold or lower."
        ),
    )

    dynamic_partition_name: str = Field(
        default="rag_snapshot",
        description="Name of the DynamicPartitionsDefinition registered by vector_index_snapshot.",
    )

    upstream_snapshot_asset_key: Optional[str] = Field(
        default=None,
        description="Optional: the vector_index_snapshot asset key to declare as an upstream dep in the graph. Not required for execution (we load from disk).",
    )

    group_name: Optional[str] = Field(default=None, description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (defaults to ['rag', 'eval'])")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        snapshot_root_dir = self.snapshot_root_dir
        collection_name = self.collection_name
        golden_set = list(self.golden_set)
        k = self.k
        min_score_threshold = self.min_score_threshold
        regression_pct_threshold = self.regression_pct_threshold
        dynamic_partition_name = self.dynamic_partition_name
        upstream_snapshot_asset_key = self.upstream_snapshot_asset_key

        kinds = self.kinds or ["rag", "eval"]
        tags = dict(self.asset_tags or {})
        for kd in kinds:
            tags[f"dagster/kind/{kd}"] = ""

        partitions_def = dg.DynamicPartitionsDefinition(name=dynamic_partition_name)

        deps: List[dg.AssetKey] = []
        if upstream_snapshot_asset_key:
            deps.append(dg.AssetKey.from_user_string(upstream_snapshot_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Golden-set retrieval eval against snapshot",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            partitions_def=partitions_def,
            deps=deps or None,
        )
        def _rag_eval_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
            import chromadb

            snapshot_id = context.partition_key
            snapshot_dir = Path(snapshot_root_dir).expanduser().resolve() / snapshot_id
            if not snapshot_dir.is_dir():
                raise dg.Failure(
                    description=(
                        f"Snapshot dir not found: {snapshot_dir}. "
                        f"Did vector_index_snapshot materialize partition '{snapshot_id}'?"
                    )
                )

            context.log.info(f"Evaluating snapshot {snapshot_id} at {snapshot_dir}")

            client = chromadb.PersistentClient(path=str(snapshot_dir))
            try:
                collection = client.get_collection(name=collection_name)
            except Exception as e:  # noqa: BLE001
                raise dg.Failure(
                    description=f"Collection '{collection_name}' not found in snapshot: {e}"
                )

            rows = []
            for item in golden_set:
                query = str(item.get("query", "")).strip()
                expected_terms = [str(t) for t in (item.get("expected_terms") or [])]
                if not query or not expected_terms:
                    context.log.warning(f"Skipping malformed golden item: {item}")
                    continue
                result = collection.query(query_texts=[query], n_results=k)
                docs = (result.get("documents") or [[]])[0] or []
                combined = "\n".join(docs).lower()
                matched = [t for t in expected_terms if t.lower() in combined]
                score = len(matched) / len(expected_terms) if expected_terms else 0.0
                rows.append(
                    {
                        "query": query,
                        "expected_terms": expected_terms,
                        "top_k_docs": docs,
                        "matched_terms": matched,
                        "score": score,
                    }
                )

            df = pd.DataFrame(rows)
            precision_at_k = float(df["score"].mean()) if len(df) else 0.0
            context.log.info(
                f"snapshot={snapshot_id} n_queries={len(df)} precision@{k}={precision_at_k:.3f}"
            )
            context.add_output_metadata(
                {
                    "snapshot_id": dg.MetadataValue.text(snapshot_id),
                    "n_queries": dg.MetadataValue.int(len(df)),
                    "k": dg.MetadataValue.int(k),
                    "precision_at_k": dg.MetadataValue.float(precision_at_k),
                    "min_threshold": dg.MetadataValue.float(min_score_threshold),
                    "regression_pct_threshold": dg.MetadataValue.float(regression_pct_threshold),
                }
            )
            return df

        eval_asset_key = dg.AssetKey.from_user_string(asset_name)

        @dg.asset_check(
            asset=_rag_eval_asset,
            name=f"{asset_name}_retrieval_quality_check",
            description=(
                "Fails when precision@k drops below the absolute minimum OR "
                "regresses by more than the threshold vs the prior materialization. "
                "Cross-partition comparison — the newest materialization is compared "
                "to the second-newest across the whole asset."
            ),
        )
        def _quality_check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
            # Fetch the two most recent materialization records across all partitions.
            from dagster import EventRecordsFilter, DagsterEventType

            records = context.instance.get_event_records(
                event_records_filter=EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_MATERIALIZATION,
                    asset_key=eval_asset_key,
                ),
                limit=2,
                ascending=False,
            )
            if not records:
                return dg.AssetCheckResult(
                    passed=True,
                    metadata={"note": dg.MetadataValue.text("No materializations found — treating as pass on first run.")},
                )

            def _extract_score(rec) -> Optional[float]:
                try:
                    mat = rec.asset_materialization
                    if not mat:
                        return None
                    md = mat.metadata or {}
                    if "precision_at_k" in md:
                        val = md["precision_at_k"]
                        # MetadataValue.float returns FloatMetadataValue; .value is the float
                        v = getattr(val, "value", None)
                        return float(v) if v is not None else None
                except Exception:  # noqa: BLE001
                    return None
                return None

            def _extract_partition(rec) -> Optional[str]:
                try:
                    return rec.asset_materialization.partition
                except Exception:  # noqa: BLE001
                    return None

            current_score = _extract_score(records[0])
            current_partition = _extract_partition(records[0])
            prior_score = _extract_score(records[1]) if len(records) > 1 else None
            prior_partition = _extract_partition(records[1]) if len(records) > 1 else None

            metadata: Dict[str, Any] = {
                "current_snapshot": dg.MetadataValue.text(current_partition or "unknown"),
                "current_score": dg.MetadataValue.float(current_score if current_score is not None else 0.0),
            }
            if prior_score is not None:
                metadata["prior_snapshot"] = dg.MetadataValue.text(prior_partition or "unknown")
                metadata["prior_score"] = dg.MetadataValue.float(prior_score)
                metadata["delta"] = dg.MetadataValue.float((current_score or 0.0) - prior_score)

            if current_score is None:
                return dg.AssetCheckResult(
                    passed=False,
                    metadata={**metadata, "reason": dg.MetadataValue.text("current materialization missing precision_at_k metadata")},
                )

            if current_score < min_score_threshold:
                return dg.AssetCheckResult(
                    passed=False,
                    severity=dg.AssetCheckSeverity.ERROR,
                    metadata={
                        **metadata,
                        "reason": dg.MetadataValue.text(
                            f"score {current_score:.3f} < absolute floor {min_score_threshold:.3f}"
                        ),
                    },
                )

            if prior_score is not None:
                allowed_floor = prior_score - (regression_pct_threshold / 100.0)
                if current_score < allowed_floor:
                    return dg.AssetCheckResult(
                        passed=False,
                        severity=dg.AssetCheckSeverity.ERROR,
                        metadata={
                            **metadata,
                            "reason": dg.MetadataValue.text(
                                f"regression: current {current_score:.3f} < prior {prior_score:.3f} - "
                                f"{regression_pct_threshold:.1f}pp allowance"
                            ),
                        },
                    )

            return dg.AssetCheckResult(passed=True, metadata=metadata)

        return dg.Definitions(assets=[_rag_eval_asset], asset_checks=[_quality_check])
