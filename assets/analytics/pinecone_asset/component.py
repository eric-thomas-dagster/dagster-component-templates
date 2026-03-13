import os
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
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

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        @dg.asset(
            name=component.asset_name,
            group_name=component.group_name,
            description=component.description,
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(component.upstream_asset_key))},
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
