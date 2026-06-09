"""Dataframe To Snowflake (Bulk).

Memory-efficient bulk load of a pandas DataFrame into a Snowflake table via
chunked parquet → PUT to internal stage → COPY INTO. Designed for medium-large
DataFrames (~1M–50M rows) where pd.to_sql would be slow and a `df.copy()`
inside write_pandas would double memory.
"""
import os
import tempfile
import time
import uuid
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class DataframeToSnowflakeBulkComponent(Component, Model, Resolvable):
    """Bulk-load a DataFrame into a Snowflake table via internal stage + COPY INTO.

    Streams the DataFrame to local parquet chunks, PUTs each chunk to a
    transient Snowflake internal stage, then COPY INTO the target table.
    10-100x faster than pd.to_sql for medium-large tables, and uses far less
    memory than write_pandas (which calls df.copy() internally).
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    table: str = Field(description="Destination Snowflake table name")
    database: Optional[str] = Field(default=None, description="Snowflake database (overrides connection default)")
    schema_: Optional[str] = Field(default=None, alias="schema", description="Snowflake schema")
    warehouse: Optional[str] = Field(default=None, description="Snowflake warehouse to use")
    role: Optional[str] = Field(default=None, description="Snowflake role")
    account_env_var: str = Field(default="SNOWFLAKE_ACCOUNT", description="Env var containing Snowflake account identifier")
    user_env_var: str = Field(default="SNOWFLAKE_USER", description="Env var containing Snowflake user")
    password_env_var: str = Field(default="SNOWFLAKE_PASSWORD", description="Env var containing Snowflake password")

    mode: str = Field(
        default="replace",
        description="Load mode: 'replace' (drop+recreate), 'append' (insert into existing), 'merge' (upsert via primary_key)",
    )
    primary_key: Optional[List[str]] = Field(
        default=None,
        description="Column(s) used for upsert when mode='merge'. Required for merge mode.",
    )
    chunk_rows: int = Field(
        default=500_000,
        ge=10_000,
        description="Rows per parquet chunk streamed to stage. Larger = fewer files but more memory per chunk.",
    )
    on_error: str = Field(
        default="ABORT_STATEMENT",
        description="Snowflake COPY ON_ERROR option: ABORT_STATEMENT, CONTINUE, SKIP_FILE, SKIP_FILE_<num>, SKIP_FILE_<num>%",
    )
    purge_stage: bool = Field(
        default=True,
        description="Delete staged files after successful COPY (recommended).",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional asset tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (default: ['snowflake', 'python'])")
    description: Optional[str] = Field(default=None, description="Asset description")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on failure")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff: 'linear' or 'exponential'")

    @classmethod
    def get_description(cls) -> str:
        return "Bulk-load a DataFrame to Snowflake via internal stage + COPY INTO (memory-efficient, chunked parquet)."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        table = self.table.upper()
        database = self.database
        schema = self.schema_
        warehouse = self.warehouse
        role = self.role
        account_env_var = self.account_env_var
        user_env_var = self.user_env_var
        password_env_var = self.password_env_var
        mode = self.mode.lower()
        primary_key = [c.upper() for c in (self.primary_key or [])]
        chunk_rows = self.chunk_rows
        on_error = self.on_error
        purge_stage = self.purge_stage
        group_name = self.group_name

        if mode == "merge" and not primary_key:
            raise ValueError("mode='merge' requires primary_key (list of column names)")
        if mode not in {"replace", "append", "merge"}:
            raise ValueError(f"unknown mode={mode!r}; must be replace, append, or merge")

        kinds = self.kinds or ["snowflake", "python"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            owners=self.owners or [],
            tags=all_tags,
            group_name=group_name,
            description=self.description or self.get_description(),
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            retry_policy=retry_policy,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            try:
                import snowflake.connector
            except ImportError as e:
                raise ImportError(
                    "snowflake-connector-python required: pip install snowflake-connector-python[pandas]"
                ) from e

            conn_kwargs = {
                "account": os.environ[account_env_var],
                "user": os.environ[user_env_var],
                "password": os.environ[password_env_var],
            }
            if database:
                conn_kwargs["database"] = database
            if schema:
                conn_kwargs["schema"] = schema
            if warehouse:
                conn_kwargs["warehouse"] = warehouse
            if role:
                conn_kwargs["role"] = role

            n_rows = len(upstream)
            n_cols = len(upstream.columns)
            if n_rows == 0:
                context.log.warning("Upstream DataFrame is empty; nothing to load.")
                return MaterializeResult(metadata={
                    "row_count": MetadataValue.int(0),
                    "dagster/row_count": MetadataValue.int(0),
                    "table": MetadataValue.text(table),
                })

            qualified_table = ".".join(
                p for p in (database, schema, table) if p
            )
            stage_name = f"@~/{table.lower()}_{uuid.uuid4().hex[:12]}"

            conn = snowflake.connector.connect(**conn_kwargs)
            cur = conn.cursor()

            t0 = time.time()
            stage_files: List[str] = []
            stage_seconds = 0.0
            copy_seconds = 0.0

            try:
                with tempfile.TemporaryDirectory() as tmpdir:
                    n_chunks = (n_rows + chunk_rows - 1) // chunk_rows
                    for i in range(n_chunks):
                        lo = i * chunk_rows
                        hi = min(lo + chunk_rows, n_rows)
                        chunk = upstream.iloc[lo:hi]
                        chunk.columns = [c.upper() for c in chunk.columns]
                        path = os.path.join(tmpdir, f"chunk_{i:05d}.parquet")
                        chunk.to_parquet(path, compression="snappy", index=False)
                        ts = time.time()
                        cur.execute(
                            f"PUT file://{path} {stage_name} "
                            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL=4"
                        )
                        stage_seconds += time.time() - ts
                        stage_files.append(os.path.basename(path))
                        context.log.info(
                            f"Staged chunk {i + 1}/{n_chunks} ({hi - lo} rows) → {stage_name}"
                        )

                    if mode == "replace":
                        cur.execute(f"DROP TABLE IF EXISTS {qualified_table}")

                    if mode in ("replace", "append"):
                        cur.execute(
                            f"CREATE TABLE IF NOT EXISTS {qualified_table} "
                            f"USING TEMPLATE ("
                            f"  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE("
                            f"    INFER_SCHEMA("
                            f"      LOCATION => '{stage_name}', "
                            f"      FILE_FORMAT => '(TYPE => PARQUET, USE_LOGICAL_TYPE => TRUE)'"
                            f"    )"
                            f"  )"
                            f")"
                        )
                        tc = time.time()
                        cur.execute(
                            f"COPY INTO {qualified_table} FROM {stage_name} "
                            f"FILE_FORMAT = (TYPE = PARQUET, USE_LOGICAL_TYPE = TRUE) "
                            f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
                            f"ON_ERROR = {on_error} "
                            f"PURGE = {'TRUE' if purge_stage else 'FALSE'}"
                        )
                        copy_seconds = time.time() - tc

                    elif mode == "merge":
                        staging = f"{table}_STG_{uuid.uuid4().hex[:8].upper()}"
                        qualified_staging = ".".join(
                            p for p in (database, schema, staging) if p
                        )
                        cur.execute(
                            f"CREATE TRANSIENT TABLE {qualified_staging} "
                            f"USING TEMPLATE ("
                            f"  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE("
                            f"    INFER_SCHEMA("
                            f"      LOCATION => '{stage_name}', "
                            f"      FILE_FORMAT => '(TYPE => PARQUET, USE_LOGICAL_TYPE => TRUE)'"
                            f"    )"
                            f"  )"
                            f")"
                        )
                        cur.execute(
                            f"COPY INTO {qualified_staging} FROM {stage_name} "
                            f"FILE_FORMAT = (TYPE = PARQUET, USE_LOGICAL_TYPE = TRUE) "
                            f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
                            f"ON_ERROR = {on_error}"
                        )
                        cur.execute(f"DESC TABLE {qualified_staging}")
                        cols = [row[0].upper() for row in cur.fetchall()]
                        non_pk = [c for c in cols if c not in primary_key]
                        on_clause = " AND ".join(f"t.{c} = s.{c}" for c in primary_key)
                        set_clause = ", ".join(f"t.{c} = s.{c}" for c in non_pk)
                        insert_cols = ", ".join(cols)
                        insert_vals = ", ".join(f"s.{c}" for c in cols)
                        tc = time.time()
                        cur.execute(
                            f"MERGE INTO {qualified_table} t USING {qualified_staging} s "
                            f"ON {on_clause} "
                            + (f"WHEN MATCHED THEN UPDATE SET {set_clause} " if non_pk else "")
                            + f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
                        )
                        copy_seconds = time.time() - tc
                        cur.execute(f"DROP TABLE IF EXISTS {qualified_staging}")
                        if purge_stage:
                            cur.execute(f"REMOVE {stage_name}")

                total_seconds = time.time() - t0
                context.log.info(
                    f"Bulk-loaded {n_rows} rows into {qualified_table} "
                    f"(mode={mode}, stage={stage_seconds:.1f}s, copy={copy_seconds:.1f}s, total={total_seconds:.1f}s)"
                )
            finally:
                try:
                    if not purge_stage:
                        cur.execute(f"REMOVE {stage_name}")
                except Exception:
                    pass
                cur.close()
                conn.close()

            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(n_rows),
                    "dagster/row_count": MetadataValue.int(n_rows),
                    "column_count": MetadataValue.int(n_cols),
                    "table": MetadataValue.text(qualified_table),
                    "mode": MetadataValue.text(mode),
                    "chunks": MetadataValue.int(len(stage_files)),
                    "stage_seconds": MetadataValue.float(round(stage_seconds, 2)),
                    "copy_seconds": MetadataValue.float(round(copy_seconds, 2)),
                    "total_seconds": MetadataValue.float(round(total_seconds, 2)),
                    "rows_per_second": MetadataValue.int(int(n_rows / total_seconds) if total_seconds > 0 else 0),
                }
            )

        return Definitions(assets=[_asset])
