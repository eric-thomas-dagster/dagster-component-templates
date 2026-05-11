import os
from typing import Any, Dict, List, Optional

import dagster as dg
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
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
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


class PineconeAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert, query, or manage a Pinecone vector index as a Dagster asset.

    Accepts a pandas DataFrame from an upstream Dagster asset containing pre-computed
    vector embeddings, then performs the requested operation against a Pinecone
    serverless index.  Designed for RAG (Retrieval-Augmented Generation) pipelines
    where embedding generation and vector storage are separate, observable pipeline steps.

    Supported operations:

    - ``upsert`` — batch-upsert vectors from the upstream DataFrame into the index.
    - ``delete`` — delete vectors by ID from a column in the upstream DataFrame.
    - ``query``  — run a nearest-neighbour query using the first row's vector and
      surface results as metadata.
    - ``create_index`` — provision a new Pinecone serverless index (upstream DataFrame
      is not used).
    """

    # --- Identity -------------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")

    # --- Pinecone connection --------------------------------------------------
    api_key_env_var: str = Field(
        description="Name of the environment variable containing the Pinecone API key."
    )
    index_name: str = Field(description="Name of the Pinecone index to operate on.")

    # --- Operation ------------------------------------------------------------
    operation: str = Field(
        default="upsert",
        description=(
            "Operation to perform. One of: ``upsert``, ``delete``, ``query``, "
            "``create_index``."
        ),
    )

    # --- Upstream asset -------------------------------------------------------
    upstream_asset_key: str = Field(
        description=(
            "Asset key whose materialized output provides a pandas DataFrame with "
            "vector data. The DataFrame must contain at least the columns referenced "
            "by ``id_column`` and ``vector_column``."
        ),
    )

    # --- Column mapping -------------------------------------------------------
    id_column: str = Field(
        default="id",
        description="DataFrame column whose values become Pinecone vector IDs.",
    )
    vector_column: str = Field(
        default="embedding",
        description=(
            "DataFrame column containing the embedding arrays. Each value must be "
            "a list or numpy array of floats."
        ),
    )
    metadata_columns: Optional[list[str]] = Field(
        default=None,
        description=(
            "Additional DataFrame columns to store as Pinecone vector metadata. "
            "Useful for metadata-filtered similarity searches."
        ),
    )

    # --- Upsert / namespace ---------------------------------------------------
    namespace: Optional[str] = Field(
        default=None,
        description=(
            "Pinecone namespace to upsert into or query within. "
            "When None, the default namespace is used."
        ),
    )
    batch_size: int = Field(
        default=100,
        description=(
            "Number of vectors per upsert batch. Tune this to stay within "
            "Pinecone's per-request size limits (recommended: 100–1 000)."
        ),
    )

    # --- create_index parameters ----------------------------------------------
    dimension: Optional[int] = Field(
        default=None,
        description=(
            "Vector dimensionality for ``create_index``. Must match the output "
            "dimension of your embedding model (e.g. 1 536 for text-embedding-ada-002)."
        ),
    )
    metric: str = Field(
        default="cosine",
        description=(
            "Distance metric for ``create_index``. One of: ``cosine``, "
            "``euclidean``, ``dotproduct``."
        ),
    )
    cloud: str = Field(
        default="aws",
        description="Cloud provider for the serverless index: ``aws``, ``gcp``, ``azure``.",
    )
    region: str = Field(
        default="us-east-1",
        description="Cloud region for the serverless index (e.g. ``us-east-1``).",
    )

    # --- Asset metadata -------------------------------------------------------
    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output DataFrame in metadata (for builder UIs).",
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview when include_preview_metadata=True.",
    )

    group_name: str = Field(
        default="vector_store",
        description="Dagster asset group name.",
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
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description surfaced in the Dagster UI.",
    )

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _build_vectors(df, id_col: str, vec_col: str, meta_cols: Optional[list[str]]):
        """Convert a DataFrame into a list of Pinecone upsert dicts."""
        vectors = []
        for _, row in df.iterrows():
            vec_id = str(row[id_col])
            values = list(map(float, row[vec_col]))
            metadata = {}
            if meta_cols:
                for col in meta_cols:
                    val = row.get(col)
                    # Pinecone metadata values must be str, int, float, or bool
                    if val is not None:
                        metadata[col] = val if isinstance(val, (str, int, float, bool)) else str(val)
            vectors.append({"id": vec_id, "values": values, "metadata": metadata})
        return vectors

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

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



    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Standard catalog fields — phase 2 wiring
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        component = self


        # Build partition definition (auto-generated; supports daily, weekly, monthly,

        # hourly partitions out of the box).
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @dg.asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=component.asset_name,
            group_name=component.group_name,
            description=component.description,
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(component.upstream_asset_key))},
            freshness_policy=_freshness_policy,
            owners=self.owners or [],
            tags=_all_tags,
        )
        def _pinecone_asset(
            context: dg.AssetExecutionContext,
            upstream,
        ) -> dg.MaterializeResult:
            from pinecone import Pinecone  # type: ignore[import]

            api_key = os.environ.get(component.api_key_env_var)
            if not api_key:
                raise ValueError(
                    f"Environment variable '{component.api_key_env_var}' "
                    "is not set or is empty."
                )

            pc = Pinecone(api_key=api_key)
            operation = component.operation.lower()

            # ------------------------------------------------------------------
            # create_index
            # ------------------------------------------------------------------
            if operation == "create_index":
                if component.dimension is None:
                    raise ValueError(
                        "'dimension' must be set for the 'create_index' operation."
                    )
                from pinecone import ServerlessSpec  # type: ignore[import]

                context.log.info(
                    f"[Pinecone] Creating index '{component.index_name}' "
                    f"(dim={component.dimension}, metric={component.metric}, "
                    f"cloud={component.cloud}, region={component.region}) ..."
                )
                pc.create_index(
                    name=component.index_name,
                    dimension=component.dimension,
                    metric=component.metric,
                    spec=ServerlessSpec(
                        cloud=component.cloud,
                        region=component.region,
                    ),
                )
                context.log.info(
                    f"[Pinecone] Index '{component.index_name}' created."
                )
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "operation": "create_index",
                        "dimension": component.dimension,
                        "metric": component.metric,
                        "cloud": component.cloud,
                        "region": component.region,
                    }
                )

            # ------------------------------------------------------------------
            # Operations that require the index to exist
            # ------------------------------------------------------------------
            import pandas as pd  # type: ignore[import]

            df = upstream
            if not isinstance(df, pd.DataFrame):
                raise ValueError("Upstream asset did not provide a pandas DataFrame.")

            index = pc.Index(component.index_name)

            # ------------------------------------------------------------------
            # upsert
            # ------------------------------------------------------------------
            if operation == "upsert":
                vectors = component._build_vectors(
                    df,
                    component.id_column,
                    component.vector_column,
                    component.metadata_columns,
                )
                total = len(vectors)
                upserted = 0
                batch_size = component.batch_size

                context.log.info(
                    f"[Pinecone] Upserting {total:,} vectors into index "
                    f"'{component.index_name}'"
                    + (f" (namespace='{component.namespace}')" if component.namespace else "")
                    + f" in batches of {batch_size} ..."
                )

                for i in range(0, total, batch_size):
                    batch = vectors[i : i + batch_size]
                    upsert_kwargs: dict = {"vectors": batch}
                    if component.namespace:
                        upsert_kwargs["namespace"] = component.namespace
                    index.upsert(**upsert_kwargs)
                    upserted += len(batch)
                    context.log.info(
                        f"[Pinecone] Upserted {upserted:,}/{total:,} vectors ..."
                    )

                context.log.info(
                    f"[Pinecone] Upsert complete: {upserted:,} vectors."
                )
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "operation": "upsert",
                        "vectors_upserted": upserted,
                        "namespace": component.namespace or "(default)",
                        "batch_size": batch_size,
                    }
                )

            # ------------------------------------------------------------------
            # delete
            # ------------------------------------------------------------------
            if operation == "delete":
                ids = [str(v) for v in df[component.id_column].tolist()]
                context.log.info(
                    f"[Pinecone] Deleting {len(ids):,} vectors from index "
                    f"'{component.index_name}' ..."
                )
                delete_kwargs: dict = {"ids": ids}
                if component.namespace:
                    delete_kwargs["namespace"] = component.namespace
                index.delete(**delete_kwargs)
                context.log.info(
                    f"[Pinecone] Deleted {len(ids):,} vectors."
                )
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "operation": "delete",
                        "vectors_deleted": len(ids),
                        "namespace": component.namespace or "(default)",
                    }
                )

            # ------------------------------------------------------------------
            # query
            # ------------------------------------------------------------------
            if operation == "query":
                # Use first row as query vector
                query_vector = list(map(float, df.iloc[0][component.vector_column]))
                context.log.info(
                    f"[Pinecone] Querying index '{component.index_name}' "
                    f"with vector of dimension {len(query_vector)} ..."
                )
                query_kwargs: dict = {
                    "vector": query_vector,
                    "top_k": 10,
                    "include_metadata": True,
                }
                if component.namespace:
                    query_kwargs["namespace"] = component.namespace
                results = index.query(**query_kwargs)
                matches = results.get("matches", [])
                context.log.info(f"[Pinecone] Query returned {len(matches)} matches.")
                return dg.MaterializeResult(
                    metadata={
                        "index": component.index_name,
                        "operation": "query",
                        "matches_returned": len(matches),
                        "namespace": component.namespace or "(default)",
                        "top_match_id": matches[0]["id"] if matches else None,
                        "top_match_score": matches[0]["score"] if matches else None,
                    }
                )

            raise ValueError(
                f"Unsupported operation '{component.operation}'. "
                "Must be one of: upsert, delete, query, create_index."
            )

        return dg.Definitions(assets=[_pinecone_asset])
