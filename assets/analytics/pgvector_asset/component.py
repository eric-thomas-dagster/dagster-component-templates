import os
from datetime import datetime, timezone
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
class PgvectorAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Generate text embeddings via OpenAI and upsert them into a PostgreSQL table
    using the ``pgvector`` extension.

    Accepts a pandas DataFrame from an upstream Dagster asset, calls the OpenAI
    Embeddings API in configurable batches, and upserts the resulting vectors into a
    target table with the schema ``(id, text, embedding vector(N), embedded_at)``.

    Typical use:

    1. A staging or raw asset produces a DataFrame with text that needs to be vectorised.
    2. This component materialises an embeddings table that downstream RAG or
       similarity-search assets can query directly with ``<->`` / ``<=>`` operators.
    """

    # --- Identity -------------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")

    # --- Upstream asset -------------------------------------------------------
    upstream_asset_key: str = Field(
        description=(
            "Asset key of the upstream asset providing a pandas DataFrame with "
            "at least the columns referenced by ``id_column`` and ``text_column``."
        )
    )

    # --- Column mapping -------------------------------------------------------
    id_column: str = Field(
        description="Primary key column in the upstream DataFrame. Used as the embedding row ID."
    )
    text_column: str = Field(
        description="Column containing the raw text to embed."
    )

    # --- Database connection (write target) -----------------------------------
    database_url_env_var: str = Field(
        description=(
            "Env var containing a PostgreSQL connection string compatible with "
            "psycopg2 and the pgvector extension "
            "(e.g. ``postgresql://user:pass@host:5432/db``)."
        )
    )

    # --- Target ---------------------------------------------------------------
    target_table: str = Field(
        description=(
            "Table to write embeddings into. Created automatically on first run if "
            "it does not exist."
        )
    )
    if_exists: str = Field(
        default="upsert",
        description=(
            "Behaviour when the target table already contains rows. "
            "``upsert`` — INSERT … ON CONFLICT DO UPDATE; "
            "``replace`` — DROP and recreate the table before inserting."
        ),
    )

    # --- Embedding model ------------------------------------------------------
    embedding_model: str = Field(
        default="text-embedding-3-small",
        description="OpenAI embedding model name.",
    )
    openai_api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Env var containing the OpenAI API key.",
    )
    dimensions: int = Field(
        default=1536,
        description=(
            "Vector dimensionality. Must match the chosen model's output size "
            "(1 536 for text-embedding-3-small, 3 072 for text-embedding-3-large)."
        ),
    )
    batch_size: int = Field(
        default=100,
        description="Number of texts sent to the OpenAI API per request.",
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

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        @dg.asset(
            name=component.asset_name,
            group_name=component.group_name,
            kinds={"sql", "vector"},
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(component.upstream_asset_key))},
        )
        def _pgvector_asset(
            context: dg.AssetExecutionContext,
            upstream,
        ) -> dg.MaterializeResult:
            import openai  # type: ignore[import]
            import pandas as pd  # type: ignore[import]
            from sqlalchemy import create_engine, text  # type: ignore[import]

            # ------------------------------------------------------------------
            # Validate environment
            # ------------------------------------------------------------------
            db_url = os.environ.get(component.database_url_env_var)
            if not db_url:
                raise ValueError(
                    f"Environment variable '{component.database_url_env_var}' "
                    "is not set or is empty."
                )
            openai_api_key = os.environ.get(component.openai_api_key_env_var)
            if not openai_api_key:
                raise ValueError(
                    f"Environment variable '{component.openai_api_key_env_var}' "
                    "is not set or is empty."
                )

            df = upstream
            if not isinstance(df, pd.DataFrame):
                raise ValueError("Upstream asset did not provide a pandas DataFrame.")

            context.log.info(f"[pgvector] Received {len(df):,} rows from upstream asset.")

            if df.empty:
                context.log.warning("[pgvector] Upstream DataFrame is empty — nothing to embed.")
                return dg.MaterializeResult(
                    metadata={
                        "num_rows": 0,
                        "dimensions": component.dimensions,
                        "model": component.embedding_model,
                    }
                )

            ids = df[component.id_column].tolist()
            texts = df[component.text_column].fillna("").tolist()

            openai_client = openai.OpenAI(api_key=openai_api_key)
            engine = create_engine(db_url)

            # ------------------------------------------------------------------
            # Generate embeddings in batches
            # ------------------------------------------------------------------
            context.log.info(
                f"[pgvector] Generating embeddings with model "
                f"'{component.embedding_model}' in batches of {component.batch_size} ..."
            )
            all_embeddings: list[list[float]] = []
            for i in range(0, len(texts), component.batch_size):
                batch_texts = texts[i : i + component.batch_size]
                response = openai_client.embeddings.create(
                    input=batch_texts,
                    model=component.embedding_model,
                )
                batch_embeddings = [item.embedding for item in response.data]
                all_embeddings.extend(batch_embeddings)
                context.log.info(
                    f"[pgvector] Embedded {min(i + component.batch_size, len(texts)):,}"
                    f"/{len(texts):,} rows ..."
                )

            # ------------------------------------------------------------------
            # Ensure pgvector extension and target table exist
            # ------------------------------------------------------------------
            dim = component.dimensions
            target = component.target_table

            with engine.begin() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))

                if component.if_exists == "replace":
                    context.log.info(
                        f"[pgvector] Dropping and recreating table '{target}' ..."
                    )
                    conn.execute(text(f'DROP TABLE IF EXISTS "{target}"'))

                conn.execute(
                    text(
                        f"""
                        CREATE TABLE IF NOT EXISTS "{target}" (
                            id TEXT PRIMARY KEY,
                            text TEXT,
                            embedding vector({dim}),
                            embedded_at TIMESTAMPTZ
                        )
                        """
                    )
                )

                # ------------------------------------------------------------------
                # Upsert embeddings
                # ------------------------------------------------------------------
                now = datetime.now(timezone.utc).isoformat()
                context.log.info(
                    f"[pgvector] Upserting {len(all_embeddings):,} rows into '{target}' ..."
                )

                for row_id, row_text, embedding in zip(ids, texts, all_embeddings):
                    embedding_str = "[" + ",".join(str(v) for v in embedding) + "]"
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO "{target}" (id, text, embedding, embedded_at)
                            VALUES (:id, :text, :embedding::vector, :embedded_at)
                            ON CONFLICT (id) DO UPDATE
                                SET text = EXCLUDED.text,
                                    embedding = EXCLUDED.embedding,
                                    embedded_at = EXCLUDED.embedded_at
                            """
                        ),
                        {
                            "id": str(row_id),
                            "text": row_text,
                            "embedding": embedding_str,
                            "embedded_at": now,
                        },
                    )

            context.log.info(
                f"[pgvector] Upsert complete: {len(all_embeddings):,} rows in '{target}'."
            )
            return dg.MaterializeResult(
                metadata={
                    "num_rows": len(all_embeddings),
                    "dimensions": dim,
                    "model": component.embedding_model,
                    "target_table": target,
                    "if_exists": component.if_exists,
                }
            )

        return dg.Definitions(assets=[_pgvector_asset])
