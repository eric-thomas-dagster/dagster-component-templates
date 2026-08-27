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
from typing import Any, Dict, List, Optional, Union

import dagster as dg
import pandas as pd
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Canonical `_build_partitions_def` shared across the registry."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
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
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_values + partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class DocumentCorpusComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materialize a directory of documents as a versioned corpus asset."""

    asset_name: str = Field(description="Dagster asset name")
    source_dir: str = Field(description="Absolute path to the directory of docs")
    file_glob: str = Field(default="**/*.md", description="Glob relative to source_dir (default '**/*.md')")
    encoding: str = Field(default="utf-8", description="Text file encoding")
    min_doc_count: int = Field(
        default=1,
        description="Asset check fails if fewer docs than this — guards against empty corpus regressions.",
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None, description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (defaults to ['rag'])")
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None, description="Freshness policy: max lag before stale."
    )
    freshness_cron: Optional[str] = Field(
        default=None, description="Cron schedule string for the freshness policy."
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None, description="Column-level lineage (output col → list of upstream cols)."
    )

    # Preview metadata
    include_preview_metadata: bool = Field(
        default=False, description="Include a `preview` metadata key (markdown table of first rows)."
    )
    preview_rows: int = Field(
        default=25, ge=1, le=500, description="Rows in the preview when include_preview_metadata=True."
    )

    # Partitioning
    partition_type: Optional[str] = Field(
        default=None,
        description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'multi' | None",
    )
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partition types.")
    partition_values: Optional[str] = Field(
        default=None, description="Comma-separated values for static/multi partitioning."
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None, description="Name for DynamicPartitionsDefinition when partition_type='dynamic'."
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None, description="Column to filter to the current date partition."
    )
    partition_static_dim: Optional[str] = Field(
        default=None, description="Dimension name for static axis in multi-partitioning."
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None, description="Column to filter to the current static partition."
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name}.",
    )

    # Retry policy
    retry_policy_max_retries: Optional[int] = Field(
        default=None, description="Max retries on asset failure (opt-in)."
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None, description="Seconds between retries (default 1)."
    )
    retry_policy_backoff: str = Field(
        default="exponential", description="Backoff strategy: 'linear' or 'exponential'."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        source_dir = self.source_dir
        file_glob = self.file_glob
        encoding = self.encoding
        min_doc_count = self.min_doc_count
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        kinds = self.kinds or ["rag"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        freshness = None
        if self.freshness_max_lag_minutes is not None:
            from datetime import timedelta
            if self.freshness_cron:
                freshness = dg.FreshnessPolicy.cron(
                    deadline_cron=self.freshness_cron,
                    lower_bound_delta=timedelta(minutes=self.freshness_max_lag_minutes),
                )
            else:
                freshness = dg.FreshnessPolicy.time_window(
                    fail_window=timedelta(minutes=self.freshness_max_lag_minutes),
                )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Document corpus from {source_dir}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            partitions_def=partitions_def,
            freshness_policy=freshness,
            retry_policy=retry_policy,
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
                rows.append({
                    "doc_id": str(p.relative_to(root)),
                    "content": content,
                    "source_path": str(p),
                    "content_hash": content_hash,
                    "byte_size": len(content.encode("utf-8")),
                    "ingested_at": now_iso,
                })

            df = pd.DataFrame(
                rows,
                columns=["doc_id", "content", "source_path", "content_hash", "byte_size", "ingested_at"],
            )
            corpus_hash = hashlib.sha256(
                "\n".join(sorted(df["content_hash"].astype(str).tolist())).encode("utf-8")
            ).hexdigest() if len(df) else "empty"

            context.log.info(f"Corpus: {len(df)} docs, {int(df['byte_size'].sum()) if len(df) else 0} bytes, corpus_hash={corpus_hash[:12]}…")

            metadata: Dict[str, Any] = {
                "doc_count": dg.MetadataValue.int(len(df)),
                "total_bytes": dg.MetadataValue.int(int(df["byte_size"].sum()) if len(df) else 0),
                "corpus_hash": dg.MetadataValue.text(corpus_hash),
                "source_dir": dg.MetadataValue.path(str(root)),
                "file_glob": dg.MetadataValue.text(file_glob),
            }
            if include_preview and len(df) > 0:
                try:
                    _prev = df[["doc_id", "byte_size", "content_hash", "ingested_at"]]
                    _prev = _prev.sample(min(preview_rows, len(_prev))) if len(_prev) > preview_rows * 10 else _prev.head(preview_rows)
                    metadata["preview"] = dg.MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"preview emission failed: {e}")
            context.add_output_metadata(metadata)
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
