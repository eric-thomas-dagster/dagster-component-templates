"""Document Chunker Component.

Split documents into chunks for RAG (Retrieval Augmented Generation) and embeddings.
Supports multiple chunking strategies: fixed-size, semantic, recursive, sentence-based, and token-aware.
"""

import re
from typing import Any, Dict, List, Optional, Union
from typing import Literal  # noqa: E402,F401
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
    Output,
    MetadataValue,
)
from pydantic import Field


def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine() (SQLAlchemy)
    OR .get_connection() (DB-API) -- works out of the box with duckdb_resource,
    postgres_resource, snowflake_resource, bigquery_resource, and any custom
    resource implementing the same duck-typed interface. Falls back to a bare
    SQLAlchemy engine via `database_url_env_var` when no Dagster resource is
    registered -- same dual pattern already used by this repo's reverse_etl
    components (e.g. greenhouse_candidate_update)."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            # get_connection() is a @contextmanager (confirmed live against
            # dagster_duckdb.DuckDBResource) -- calling it without `with` hands
            # back a _GeneratorContextManager, not a connection, and pd.read_sql
            # fails with AttributeError. Must be entered via `with`.
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        if hasattr(resource, "get_client"):
            # "get_client" means something different per vendor -- there's no
            # universal calling convention, so dispatch on the CLIENT's own
            # shape rather than assume one. Verified against the real APIs,
            # not guessed:
            client = resource.get_client()
            if hasattr(client, "query"):
                # BigQuery (google.cloud.bigquery.Client): .query(sql) returns
                # a QueryJob; .to_dataframe() blocks until done and returns a
                # pandas DataFrame directly -- no .result() call needed first.
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                # Redshift Data API (dagster_aws RedshiftClient):
                # execute_query(sql, fetch_results=True) returns bare
                # List[Tuple] with NO column names attached -- a
                # RealDictCursor factory is required to get dict rows a
                # DataFrame can use with correct column names.
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(sql, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this helper doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found). Add a dispatch branch for it."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        import os
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class DocumentChunkerComponent(Component, Model, Resolvable):
    """Component for splitting documents into chunks for embeddings and RAG.

    This component splits long documents into smaller chunks suitable for vector embeddings
    and retrieval. It supports multiple chunking strategies and preserves metadata.

    Features:
    - Multiple chunking strategies (fixed, semantic, recursive, sentence, token-aware)
    - Configurable chunk size and overlap
    - Metadata preservation (source, page, position, etc.)
    - Token-aware chunking for LLM context limits
    - Sentence boundary detection
    - Semantic chunking based on meaning
    - Batch processing of multiple documents

    Use Cases:
    - Prepare documents for vector embeddings
    - RAG (Retrieval Augmented Generation) pipelines
    - Long document processing
    - Knowledge base indexing
    - Document search systems

    Example:
        ```yaml
        type: dagster_community_components.DocumentChunkerComponent
        attributes:
          asset_name: document_chunks
          strategy: recursive
          chunk_size: 1000
          chunk_overlap: 200
          source_column: document_text
          metadata_columns: "doc_id,title,author"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the chunked data"
    )

    strategy: str = Field(
        default="recursive",
        description=(
            "Chunking strategy: fixed, semantic, recursive, sentence, token_aware, qa. "
            "'qa': not a text-splitting strategy at all -- produces question/answer pairs indexed "
            "by question (see qa_source/qa_question_column/qa_answer_column/qa_model/qa_num_pairs)."
        )
    )

    qa_source: Literal["column", "llm"] = Field(
        default="column",
        description=(
            "Only used when strategy='qa'. 'column': qa_question_column/qa_answer_column already "
            "hold the question/answer pairs (one row in, one row out -- no actual splitting). "
            "'llm': generates qa_num_pairs question/answer pairs per document by calling an LLM "
            "(qa_model) on its full text."
        ),
    )
    qa_question_column: Optional[str] = Field(
        default=None,
        description="Required when strategy='qa' and qa_source='column': column already holding each row's question text.",
    )
    qa_answer_column: Optional[str] = Field(
        default=None,
        description="Required when strategy='qa' and qa_source='column': column already holding each row's answer text.",
    )
    qa_model: Optional[str] = Field(
        default=None,
        description="Required when strategy='qa' and qa_source='llm': any litellm-supported model name, e.g. 'gpt-4o-mini'.",
    )
    qa_num_pairs: int = Field(
        default=5,
        ge=1,
        le=50,
        description="Only used when strategy='qa' and qa_source='llm': how many question/answer pairs to generate per document.",
    )
    qa_api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Only used when strategy='qa' and qa_source='llm': env var holding the LLM API key.",
    )
    qa_prompt_prefix: Optional[str] = Field(
        default=None,
        description="Only used when strategy='qa' and qa_source='llm': extra instructions prepended to the generation prompt (e.g. domain context).",
    )

    chunk_size: int = Field(
        default=1000,
        description="Target chunk size in characters (or tokens for token_aware strategy)"
    )

    chunk_overlap: int = Field(
        default=200,
        description="Number of characters (or tokens) to overlap between chunks"
    )

    parent_chunk_size: Optional[int] = Field(
        default=None,
        description=(
            "Parent-child (small-to-big) chunking: when set, each document is first split into "
            "larger parent passages of this size (using the same `strategy`), then each parent is "
            "split again into the regular, smaller `chunk_size` child chunks. Every child chunk "
            "carries `parent_id` and `parent_text` alongside its own chunk fields -- embed/index "
            "the small, precise child chunks for retrieval matching, but return each hit's larger "
            "`parent_text` for the context an LLM actually needs. Must be larger than `chunk_size`. "
            "Unset (default): no parent-child chunking, matching today's behavior."
        ),
    )

    parent_chunk_overlap: int = Field(
        default=0,
        description="Overlap between parent passages, in the same units as parent_chunk_size. Only used when parent_chunk_size is set.",
    )

    source_column: Union[str, int] = Field(
        default="text",
        description="Column name containing document text"
    )

    output_column: Union[str, int] = Field(
        default="chunk",
        description="Column name for chunk text"
    )

    metadata_columns: Optional[Union[str, int]] = Field(
        default=None,
        description="Comma-separated list of columns to preserve as metadata (e.g., 'doc_id,title,author')"
    )

    add_chunk_metadata: bool = Field(
        default=True,
        description="Add chunk metadata (chunk_index, total_chunks, start_char, end_char)"
    )

    separators: Optional[str] = Field(
        default=None,
        description="Custom separators for recursive strategy (comma-separated). Default: '\\n\\n,\\n, ,'"
    )

    sentence_tokenizer: str = Field(
        default="simple",
        description="Sentence tokenizer: simple (regex), nltk, spacy"
    )

    tokenizer_model: Optional[str] = Field(
        default=None,
        description="Model for token counting (e.g., 'gpt-3.5-turbo', 'cl100k_base'). Required for token_aware strategy."
    )

    min_chunk_size: int = Field(
        default=100,
        description="Minimum chunk size to avoid very small chunks"
    )

    merge_small_chunks: bool = Field(
        default=True,
        description="Merge chunks smaller than min_chunk_size with adjacent chunks"
    )

    preserve_sentences: bool = Field(
        default=True,
        description="Avoid splitting sentences when possible (for fixed and token_aware)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="document_processing",
        description="Asset group for organization"
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
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

    partition_values: Optional[Union[str, List[str]]] = Field(
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
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' filters and processes an entire backfill range in ONE run (the upstream DataFrame is filtered to the union of every partition key in the range, then processed once -- the total rows sent to the model/API are identical to running one run per partition, so it does not multiply paid calls, but a single failure retries the whole range). 'multi_run' launches one run per partition (optionally capped via backfill_policy_max_partitions_per_run). Leave unset for Dagster's default (one run per partition, uncapped). Ignored when partition_type/partition_dimensions is not set. 'single_run' returns one DataFrame spanning multiple partitions, which requires a partition-range-aware IO manager (e.g. a DB-table IO manager with partition_expr set) -- the default filesystem IO manager cannot persist it and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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
        default=True,
        description="Include sample data preview in metadata"
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame with document text to chunk. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource>, sql: <query>}. "
            "resource_key must point at a resource exposing .get_engine() (SQLAlchemy) or "
            ".get_connection() (DB-API) -- works out of the box with duckdb_resource, "
            "postgres_resource, snowflake_resource, bigquery_resource, etc. Alternatively "
            "set `database_url_env_var` to a bare SQLAlchemy connection string from an "
            "environment variable when no Dagster resource is registered. Mutually "
            "exclusive with `upstream_asset_key` -- set exactly one."
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Document Chunker', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        strategy = self.strategy
        chunk_size = self.chunk_size
        chunk_overlap = self.chunk_overlap
        parent_chunk_size = self.parent_chunk_size
        parent_chunk_overlap = self.parent_chunk_overlap
        if strategy != "qa":
            if chunk_overlap >= chunk_size:
                raise ValueError("DocumentChunkerComponent: chunk_overlap must be smaller than chunk_size.")
            if parent_chunk_size is not None:
                if parent_chunk_size <= chunk_size:
                    raise ValueError("DocumentChunkerComponent: parent_chunk_size must be larger than chunk_size.")
                if parent_chunk_overlap >= parent_chunk_size:
                    raise ValueError("DocumentChunkerComponent: parent_chunk_overlap must be smaller than parent_chunk_size.")
        elif parent_chunk_size is not None:
            raise ValueError("DocumentChunkerComponent: parent_chunk_size is not supported with strategy='qa'.")
        qa_source = self.qa_source
        qa_question_column = self.qa_question_column
        qa_answer_column = self.qa_answer_column
        qa_model = self.qa_model
        qa_num_pairs = self.qa_num_pairs
        qa_api_key_env_var = self.qa_api_key_env_var
        qa_prompt_prefix = self.qa_prompt_prefix
        if strategy == "qa":
            if qa_source == "column" and not (qa_question_column and qa_answer_column):
                raise ValueError(
                    "DocumentChunkerComponent: strategy='qa' with qa_source='column' requires "
                    "both qa_question_column and qa_answer_column."
                )
            if qa_source == "llm" and not qa_model:
                raise ValueError("DocumentChunkerComponent: strategy='qa' with qa_source='llm' requires qa_model.")
        source_column = self.source_column
        output_column = self.output_column
        metadata_columns_str = self.metadata_columns
        add_chunk_metadata = self.add_chunk_metadata
        separators_str = self.separators
        sentence_tokenizer = self.sentence_tokenizer
        tokenizer_model = self.tokenizer_model
        min_chunk_size = self.min_chunk_size
        merge_small_chunks = self.merge_small_chunks
        preserve_sentences = self.preserve_sentences
        description = self.description or f"Document chunks using {strategy} strategy"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        source_cfg = self.source

        if bool(upstream_asset_key) == bool(source_cfg):
            raise ValueError(
                "DocumentChunkerComponent: set exactly one of `upstream_asset_key` or `source`."
            )

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim
        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        # Infer kinds from component name if not explicitly set
        _comp_name = "document_chunker"  # component directory name
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
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

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



        _asset_kwargs: Dict[str, Any] = dict(
            retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        if upstream_asset_key:
            _asset_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}
        if source_cfg and source_cfg.get("resource_key"):
            _asset_kwargs["required_resource_keys"] = {source_cfg["resource_key"]}

        @asset(**_asset_kwargs)
        def document_chunker_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            upstream = kwargs.get("upstream")
            if upstream is None:
                upstream = _ingest_warehouse_query(source_cfg, context)
            # Filter to current partition if partitioned
            if context.has_partition_key or context.has_partition_key_range:
                # Union of row filters over every partition key this call covers: a
                # normal tick has exactly one key; a single_run backfill has the range.
                import numpy as _np
                _keep = _np.zeros(len(upstream), dtype=bool)
                for _pk in context.partition_keys:
                    if partitions_def is not None and not hasattr(_pk, "keys_by_dimension") and type(partitions_def).__name__ == "MultiPartitionsDefinition":
                        _pk = partitions_def.get_partition_key_from_str(str(_pk))
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                    _m = _np.ones(len(upstream), dtype=bool)
                    if partition_date_column and partition_date_column in upstream.columns and _date_key:
                        _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                        _m &= (_col_dates == _date_key).to_numpy()
                    if partition_static_column and partition_static_column in upstream.columns and _static_key:
                        _m &= (upstream[partition_static_column].astype(str) == _static_key).to_numpy()
                    elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                        _m &= (upstream[partition_static_column].astype(str) == str(_pk)).to_numpy()
                    _keep |= _m
                upstream = upstream[_keep]
            """Asset that splits documents into chunks."""

            context.log.info(f"Starting document chunking with strategy: {strategy}")

            input_df = upstream
            context.log.info(f"Received DataFrame: {len(input_df)} rows")

            # Validate source column exists -- not needed for strategy='qa' with qa_source='column'
            # (question/answer are already in their own columns; there's no document text to split).
            if not (strategy == "qa" and qa_source == "column") and source_column not in input_df.columns:
                raise ValueError(f"Source column '{source_column}' not found. Available: {list(input_df.columns)}")

            # Parse metadata columns
            metadata_cols = []
            if metadata_columns_str:
                metadata_cols = [col.strip() for col in metadata_columns_str.split(',')]
                for col in metadata_cols:
                    if col not in input_df.columns:
                        context.log.warning(f"Metadata column '{col}' not found, skipping")
                        metadata_cols.remove(col)

            # Parse separators
            separators = None
            if separators_str:
                separators = [s.strip() for s in separators_str.split(',')]
                # Unescape newlines
                separators = [s.replace('\\n', '\n') for s in separators]
            else:
                separators = ['\n\n', '\n', '. ', ' ', '']

            # Initialize tokenizer if needed
            tokenizer = None
            if strategy == "token_aware":
                if not tokenizer_model:
                    raise ValueError("tokenizer_model required for token_aware strategy")

                try:
                    import tiktoken
                    if tokenizer_model in ["gpt-4", "gpt-3.5-turbo", "gpt-4-turbo"]:
                        tokenizer = tiktoken.encoding_for_model(tokenizer_model)
                    else:
                        tokenizer = tiktoken.get_encoding(tokenizer_model)
                    context.log.info(f"Using tiktoken tokenizer: {tokenizer_model}")
                except ImportError:
                    raise ImportError("tiktoken not installed. Install with: pip install tiktoken")

            def count_tokens(text: str) -> int:
                """Count tokens in text."""
                if tokenizer:
                    return len(tokenizer.encode(text))
                return len(text)

            def split_by_sentences(text: str) -> List[str]:
                """Split text into sentences."""
                if sentence_tokenizer == "nltk":
                    try:
                        import nltk
                        try:
                            nltk.data.find('tokenizers/punkt')
                        except LookupError:
                            nltk.download('punkt', quiet=True)
                        from nltk.tokenize import sent_tokenize
                        return sent_tokenize(text)
                    except ImportError:
                        context.log.warning("NLTK not available, falling back to simple tokenizer")
                elif sentence_tokenizer == "spacy":
                    try:
                        import spacy
                        nlp = spacy.load("en_core_web_sm")
                        doc = nlp(text)
                        return [sent.text for sent in doc.sents]
                    except Exception:
                        context.log.warning("spaCy not available, falling back to simple tokenizer")

                # Simple regex-based sentence splitter
                sentences = re.split(r'(?<=[.!?])\s+', text)
                return sentences

            def chunk_fixed(text: str, size: int = None, overlap: int = None) -> List[str]:
                """Fixed-size chunking with optional overlap. `size`/`overlap` default to the
                component's own chunk_size/chunk_overlap -- overridable so parent-child chunking
                (parent_chunk_size/parent_chunk_overlap) can reuse this same splitter at a larger
                granularity without a second, duplicated implementation."""
                size = chunk_size if size is None else size
                overlap = chunk_overlap if overlap is None else overlap
                chunks = []
                start = 0
                text_len = len(text)

                while start < text_len:
                    end = start + size

                    # Try to end at sentence boundary if preserve_sentences
                    if preserve_sentences and end < text_len:
                        # Look for sentence ending near the boundary
                        boundary_window = text[max(0, end - 100):min(text_len, end + 100)]
                        sentence_ends = [m.end() for m in re.finditer(r'[.!?]\s', boundary_window)]
                        if sentence_ends:
                            # Find closest to desired end
                            closest = min(sentence_ends, key=lambda x: abs(x - 100))
                            end = max(0, end - 100) + closest

                    chunk = text[start:end].strip()
                    if len(chunk) >= min_chunk_size or not chunks:
                        chunks.append(chunk)

                    start = end - overlap

                return chunks

            def chunk_recursive(text: str, seps: List[str], size: int = None) -> List[str]:
                """Recursive chunking using hierarchical separators. `size` defaults to the
                component's own chunk_size -- overridable for parent-child chunking."""
                size = chunk_size if size is None else size
                if not seps:
                    return [text]

                separator = seps[0]
                remaining_seps = seps[1:]

                splits = text.split(separator)
                chunks = []
                current_chunk = []
                current_size = 0

                for split in splits:
                    split_size = len(split)

                    if current_size + split_size + len(separator) <= size:
                        current_chunk.append(split)
                        current_size += split_size + len(separator)
                    else:
                        # Finalize current chunk
                        if current_chunk:
                            chunk_text = separator.join(current_chunk).strip()
                            if len(chunk_text) >= min_chunk_size:
                                chunks.append(chunk_text)

                        # Handle large split
                        if split_size > size and remaining_seps:
                            # Recurse with next separator
                            chunks.extend(chunk_recursive(split, remaining_seps, size=size))
                        else:
                            current_chunk = [split]
                            current_size = split_size

                # Add remaining
                if current_chunk:
                    chunk_text = separator.join(current_chunk).strip()
                    if len(chunk_text) >= min_chunk_size or not chunks:
                        chunks.append(chunk_text)

                return chunks

            def chunk_sentence(text: str, size: int = None) -> List[str]:
                """Sentence-based chunking. `size` defaults to the component's own chunk_size --
                overridable for parent-child chunking."""
                size = chunk_size if size is None else size
                sentences = split_by_sentences(text)
                chunks = []
                current_chunk = []
                current_size = 0

                for sentence in sentences:
                    sentence_size = len(sentence)

                    if current_size + sentence_size <= size:
                        current_chunk.append(sentence)
                        current_size += sentence_size
                    else:
                        # Finalize current chunk
                        if current_chunk:
                            chunks.append(' '.join(current_chunk).strip())
                        current_chunk = [sentence]
                        current_size = sentence_size

                # Add remaining
                if current_chunk:
                    chunks.append(' '.join(current_chunk).strip())

                return chunks

            def chunk_token_aware(text: str, size: int = None) -> List[str]:
                """Token-aware chunking for LLM context limits. `size` defaults to the
                component's own chunk_size -- overridable for parent-child chunking."""
                size = chunk_size if size is None else size
                if preserve_sentences:
                    sentences = split_by_sentences(text)
                else:
                    # Split by words
                    sentences = text.split(' ')

                chunks = []
                current_chunk = []
                current_tokens = 0

                for sentence in sentences:
                    sentence_tokens = count_tokens(sentence)

                    if current_tokens + sentence_tokens <= size:
                        current_chunk.append(sentence)
                        current_tokens += sentence_tokens
                    else:
                        # Finalize current chunk
                        if current_chunk:
                            chunk_text = ' '.join(current_chunk).strip() if not preserve_sentences else ' '.join(current_chunk).strip()
                            chunks.append(chunk_text)
                        current_chunk = [sentence]
                        current_tokens = sentence_tokens

                # Add remaining
                if current_chunk:
                    chunk_text = ' '.join(current_chunk).strip()
                    chunks.append(chunk_text)

                return chunks

            def chunk_semantic(text: str, size: int = None) -> List[str]:
                """Semantic chunking based on paragraph structure. `size` defaults to the
                component's own chunk_size -- overridable for parent-child chunking."""
                size = chunk_size if size is None else size
                # Split by paragraphs first
                paragraphs = text.split('\n\n')
                chunks = []
                current_chunk = []
                current_size = 0

                for para in paragraphs:
                    para = para.strip()
                    if not para:
                        continue

                    para_size = len(para)

                    if current_size + para_size <= size:
                        current_chunk.append(para)
                        current_size += para_size
                    else:
                        # Finalize current chunk
                        if current_chunk:
                            chunks.append('\n\n'.join(current_chunk))

                        # If paragraph too large, split it
                        if para_size > size:
                            chunks.extend(chunk_fixed(para, size=size))
                        else:
                            current_chunk = [para]
                            current_size = para_size

                # Add remaining
                if current_chunk:
                    chunks.append('\n\n'.join(current_chunk))

                return chunks

            def split_at(text: str, size: int, overlap: int) -> List[str]:
                """Dispatches to the configured strategy at an explicit size/overlap -- shared by
                the regular (child) split and, when parent_chunk_size is set, the larger parent
                split, so parent-child chunking reuses the exact same strategy instead of a second,
                separately-maintained implementation."""
                if strategy == "fixed":
                    return chunk_fixed(text, size=size, overlap=overlap)
                elif strategy == "recursive":
                    return chunk_recursive(text, separators, size=size)
                elif strategy == "sentence":
                    return chunk_sentence(text, size=size)
                elif strategy == "token_aware":
                    return chunk_token_aware(text, size=size)
                elif strategy == "semantic":
                    return chunk_semantic(text, size=size)
                else:
                    raise ValueError(f"Unknown strategy: {strategy}")

            def merge_small(chunks: List[str]) -> List[str]:
                if not (merge_small_chunks and len(chunks) > 1):
                    return chunks
                merged = []
                i = 0
                while i < len(chunks):
                    chunk = chunks[i]
                    if len(chunk) < min_chunk_size and i + 1 < len(chunks):
                        # Merge with next chunk
                        chunk = chunk + " " + chunks[i + 1]
                        i += 2
                    else:
                        i += 1
                    merged.append(chunk)
                return merged

            def generate_qa_pairs_llm(text: str) -> List[Dict[str, str]]:
                """Calls qa_model (litellm) once per document to generate qa_num_pairs
                question/answer pairs from its full text -- same litellm.completion +
                JSON-with-markdown-fence-stripping convention as context_engineering_pipeline's
                classify op mode='llm'."""
                import os
                import json
                try:
                    from litellm import completion
                except ImportError:
                    raise ImportError("strategy='qa' with qa_source='llm' requires litellm: pip install litellm")
                prompt_parts = []
                if qa_prompt_prefix:
                    prompt_parts.append(qa_prompt_prefix)
                prompt_parts.append(
                    f"Generate exactly {qa_num_pairs} question/answer pairs that a reader could ask "
                    "and answer using ONLY the following text. Return only a JSON array like "
                    '[{"question": "...", "answer": "..."}, ...], no other text.\n\n'
                    f"Text:\n{text}"
                )
                resp = completion(
                    model=qa_model,
                    messages=[{"role": "user", "content": "\n\n".join(prompt_parts)}],
                    api_key=os.environ.get(qa_api_key_env_var),
                )
                raw = resp.choices[0].message.content.strip()
                if raw.startswith("```"):
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                parsed = json.loads(raw)
                if not isinstance(parsed, list):
                    raise ValueError("expected a JSON array of {question, answer} objects")
                return [
                    {"question": str(item["question"]), "answer": str(item["answer"])}
                    for item in parsed
                    if isinstance(item, dict) and "question" in item and "answer" in item
                ]

            # Process each document
            all_chunks = []
            total_documents = len(input_df)

            if strategy == "qa":
                # Not a text-splitting strategy: 'column' reshapes existing question/answer
                # columns 1:1 (no document text involved at all); 'llm' generates qa_num_pairs
                # pairs per document from its full text. Either way, output_column holds the
                # QUESTION (so downstream embedding/indexing is keyed by question, matching the
                # retrieval pattern this mode exists for), and 'answer' holds the answer.
                for idx, row in input_df.iterrows():
                    if qa_source == "column":
                        pairs = [{"question": str(row[qa_question_column]), "answer": str(row[qa_answer_column])}]
                    else:
                        doc_text = str(row[source_column])
                        if not doc_text.strip():
                            context.log.warning(f"Skipping empty document at index {idx}")
                            continue
                        try:
                            pairs = generate_qa_pairs_llm(doc_text)
                        except Exception as e:
                            context.log.warning(f"qa (llm): generation failed for document at index {idx}: {e}")
                            pairs = []
                    for pair_idx, pair in enumerate(pairs):
                        chunk_record = {output_column: pair["question"], "answer": pair["answer"]}
                        for col in metadata_cols:
                            chunk_record[col] = row[col]
                        if add_chunk_metadata:
                            chunk_record['chunk_index'] = pair_idx
                            chunk_record['total_chunks'] = len(pairs)
                            chunk_record['chunk_size'] = len(pair["question"])
                        all_chunks.append(chunk_record)
                    if idx % 100 == 0 and idx > 0:
                        context.log.info(f"Processed {idx}/{total_documents} documents")
            else:
                for idx, row in input_df.iterrows():
                    doc_text = str(row[source_column])

                    # Skip empty documents
                    if not doc_text.strip():
                        context.log.warning(f"Skipping empty document at index {idx}")
                        continue

                    # parent_chunk_size unset (default): one flat level of chunks, exactly today's
                    # behavior. Set: split into larger parent passages first, then each parent into
                    # the regular, smaller child chunks -- every child carries parent_id/parent_text.
                    if parent_chunk_size is None:
                        parents = [(None, None, doc_text)]
                    else:
                        parent_passages = merge_small(split_at(doc_text, parent_chunk_size, parent_chunk_overlap))
                        parents = [
                            (f"{idx}_p{p_idx}", parent_text, parent_text)
                            for p_idx, parent_text in enumerate(parent_passages)
                        ]

                    doc_chunk_idx = 0
                    doc_total_chunks = 0
                    doc_chunk_records = []
                    for parent_id, parent_text, split_source in parents:
                        chunks = merge_small(split_at(split_source, chunk_size, chunk_overlap))
                        for chunk_text in chunks:
                            chunk_record = {output_column: chunk_text}
                            for col in metadata_cols:
                                chunk_record[col] = row[col]
                            if parent_id is not None:
                                chunk_record['parent_id'] = parent_id
                                chunk_record['parent_text'] = parent_text
                            if add_chunk_metadata:
                                chunk_record['chunk_index'] = doc_chunk_idx
                                chunk_record['chunk_size'] = len(chunk_text)
                                if strategy == "token_aware" and tokenizer:
                                    chunk_record['chunk_tokens'] = count_tokens(chunk_text)
                            doc_chunk_records.append(chunk_record)
                            doc_chunk_idx += 1
                        doc_total_chunks += len(chunks)

                    if add_chunk_metadata:
                        for chunk_record in doc_chunk_records:
                            chunk_record['total_chunks'] = doc_total_chunks
                    all_chunks.extend(doc_chunk_records)

                    if idx % 100 == 0 and idx > 0:
                        context.log.info(f"Processed {idx}/{total_documents} documents")

            # Create result DataFrame
            result_df = pd.DataFrame(all_chunks)

            context.log.info(f"Chunking complete: {len(result_df)} chunks from {total_documents} documents")
            context.log.info(f"Average chunks per document: {len(result_df) / total_documents:.1f}")

            # Calculate statistics -- guarded: a real, reachable empty result (e.g. strategy='qa'
            # with qa_source='llm' where every document's generation call failed) must report
            # clearly, not crash the whole asset on min()/division-by-zero.
            if len(result_df) > 0:
                chunk_sizes = result_df['chunk_size'].tolist() if 'chunk_size' in result_df.columns else [len(c) for c in result_df[output_column]]
                avg_chunk_size = int(sum(chunk_sizes) / len(chunk_sizes))
                min_chunk = int(min(chunk_sizes))
                max_chunk = int(max(chunk_sizes))
            else:
                avg_chunk_size = min_chunk = max_chunk = 0

            # Metadata
            metadata = {
                "strategy": strategy,
                "total_chunks": len(result_df),
                "source_documents": total_documents,
                "avg_chunks_per_doc": len(result_df) / total_documents if total_documents else 0,
                "avg_chunk_size": avg_chunk_size,
                "min_chunk_size": min_chunk,
                "max_chunk_size": max_chunk,
                "target_chunk_size": chunk_size,
                "chunk_overlap": chunk_overlap,
            }

            if include_preview and len(result_df) > 0:
                try:
                    _prev = result_df.sample(min(preview_rows, len(result_df))) if len(result_df) > preview_rows * 10 else result_df.head(preview_rows)
                    context.add_output_metadata({
                            **metadata,
                            "sample_chunks": MetadataValue.md(_prev.to_markdown(index=False) or ""),
                        })
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
                    context.add_output_metadata(metadata)
                return result_df
            else:
                context.add_output_metadata(metadata)
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
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col.name: [col.name] for col in _col_schema.columns
                        if col.name in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return result_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[document_chunker_asset])


        return Definitions(assets=[document_chunker_asset], asset_checks=list(_schema_checks))


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
