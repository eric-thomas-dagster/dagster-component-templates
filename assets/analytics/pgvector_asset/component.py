import os
from datetime import datetime, timezone
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



    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
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
            kinds={"sql", "vector"},
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(component.upstream_asset_key))},
            freshness_policy=_freshness_policy,
            owners=self.owners or [],
            tags=_all_tags,
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
