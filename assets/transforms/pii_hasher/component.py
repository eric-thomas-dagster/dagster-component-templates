"""PII Hasher Asset Component.

General-purpose transform: normalize + SHA-256 hash PII identifier columns
(email, phone, or any other free-form identifier) in an upstream DataFrame,
producing a new DataFrame safe to feed into ANY downstream destination --
not just one ad platform -- or to use for a user's own compliance /
anonymization pipeline.

Self-contained, independent implementation (per this repo's no-shared-
helpers rule) -- this does NOT import from any reverse_etl component or
other transform. It duplicates the hashing/normalization logic this repo's
ad-platform audience-activation components (google_ads_customer_match_upsert,
meta_custom_audience_upsert, tiktok_custom_audience_upsert,
linkedin_matched_audience_upsert, twitter_ads_tailored_audience_upsert,
pinterest_audience_upsert) each independently proved correct per-platform --
see README.md for the "why normalization differs by platform" explanation
with citations back to those components.

Supported identifier types (per-column, via `column_identifiers`):
  - "email": trim + lowercase (default), or with
    `email_strip_all_whitespace=true`, lowercase + strip ALL whitespace
    (LinkedIn's broader documented rule).
  - "phone": either `e164_with_plus` (Google Ads / TikTok / X convention --
    keep a leading '+', digits only) or `digits_only_no_plus` (Meta /
    Pinterest convention -- strip the '+' and every non-digit character),
    selected via `phone_mode`.
  - "generic": trim only, then hash -- for any identifier that doesn't have
    a platform-specific normalization rule.

This component never references any specific destination/ad-platform API --
it is a pure, destination-agnostic building block. Columns not listed in
`column_identifiers` pass through untouched.
"""
import hashlib
import math
import re
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field

_SUPPORTED_IDENTIFIER_TYPES = {"email", "phone", "generic"}
_SUPPORTED_PHONE_MODES = {"e164_with_plus", "digits_only_no_plus"}


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
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
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


def _normalize_email(value: str, strip_all_whitespace: bool) -> str:
    """Default: trim + lowercase -- matches Google Ads / Meta / TikTok / X /
    Pinterest's simple `.strip().lower()` convention. With
    strip_all_whitespace=True: lowercase, then remove ALL whitespace (not
    just leading/trailing) -- LinkedIn's documented, broader rule."""
    if strip_all_whitespace:
        return re.sub(r"\s+", "", value.lower())
    return value.strip().lower()


def _normalize_phone(value: str, mode: str) -> str:
    """'e164_with_plus': keep a single leading '+' plus digits only -- the
    Google Ads Customer Match / TikTok Custom Audiences / X Tailored
    Audiences convention (must include country code, e.g. '+14155552671').
    'digits_only_no_plus': strip the '+' and every non-digit character --
    the Meta Custom Audiences / Pinterest Customer Lists convention (e.g.
    '14155552671'). Neither is hardcoded as "the" phone convention: this
    repo's own ad-platform components proved it genuinely differs by
    destination (see README.md for citations)."""
    stripped = value.strip()
    if mode == "e164_with_plus":
        cleaned = re.sub(r"[^\d+]", "", stripped)
        digits_only = cleaned.lstrip("+")
        return "+" + digits_only
    if mode == "digits_only_no_plus":
        return re.sub(r"\D", "", stripped)
    raise ValueError(f"unknown phone_mode: {mode!r}")


def _normalize_generic(value: str) -> str:
    """No platform-specific rule applies -- trim only, then hash as-is."""
    return value.strip()


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_identifier(
    raw: Any, id_type: str, email_strip_all_whitespace: bool, phone_mode: str
) -> Optional[str]:
    """Pure function: raw cell value -> hashed hex string, or None for
    null/NaN/blank-after-normalization input (so the output column can mark
    "no value" instead of hashing an empty string or the literal text
    "nan"). Isolated from pandas so it's trivially unit-testable."""
    if raw is None:
        return None
    if isinstance(raw, float) and math.isnan(raw):
        return None
    raw_str = str(raw)
    if id_type == "email":
        normalized = _normalize_email(raw_str, email_strip_all_whitespace)
    elif id_type == "phone":
        normalized = _normalize_phone(raw_str, phone_mode)
    elif id_type == "generic":
        normalized = _normalize_generic(raw_str)
    else:
        raise ValueError(f"unsupported identifier type: {id_type!r}")
    if not normalized or normalized == "+":
        return None
    return _sha256_hex(normalized)



def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine()
    (SQLAlchemy), .get_connection() (DBAPI), or .get_client() (vendor
    client -- dispatched by the client's own shape since "get_client" means
    something different per vendor: BigQuery's .query(sql).to_dataframe(),
    Redshift's .execute_query(sql, fetch_results=True, cursor_factory=
    RealDictCursor)), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no Dagster resource is registered. Same
    helper, same contract, as every other dual-ingestion component in this
    repo (e.g. automl_asset, logistic_regression_model, churn_prediction)."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        if hasattr(resource, "get_client"):
            client = resource.get_client()
            if hasattr(client, "query"):
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
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


class PiiHasherComponent(Component, Model, Resolvable):
    """Normalize + SHA-256 hash PII identifier columns (email, phone, or a
    generic free-form identifier) in an upstream DataFrame. A pure
    DataFrame-in / DataFrame-out building block -- not tied to any one
    downstream destination, usable ahead of ANY reverse-ETL sink or for a
    standalone compliance/anonymization pipeline.

    Example:
        ```yaml
        type: dagster_component_templates.PiiHasherComponent
        attributes:
          asset_name: hashed_customers
          upstream_asset_key: raw_customers
          column_identifiers:
            email: email
            phone_number: phone
            loyalty_id: generic
          phone_mode: e164_with_plus
          drop_original_columns: true
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )

    column_identifiers: Dict[str, str] = Field(
        description=(
            "Mapping of column name -> identifier type. Supported types: "
            "'email', 'phone', 'generic'. Each listed column is normalized "
            "per its type then SHA-256 hashed into a new "
            "'{column}{hashed_column_suffix}' column. Columns not listed "
            "here pass through unchanged, e.g. {'email': 'email', "
            "'phone_number': 'phone', 'loyalty_id': 'generic'}."
        )
    )
    email_strip_all_whitespace: bool = Field(
        default=False,
        description=(
            "Email normalization mode. Default (false): trim + lowercase -- "
            "the convention Google Ads, Meta, TikTok, X/Twitter, and "
            "Pinterest all document. Set true to additionally strip ALL "
            "whitespace (not just leading/trailing) before lowercasing -- "
            "LinkedIn's documented, broader rule."
        ),
    )
    phone_mode: str = Field(
        default="e164_with_plus",
        description=(
            "Phone normalization convention -- pick whichever your "
            "downstream destination expects; this repo's own ad-platform "
            "activation components proved this genuinely differs by "
            "platform. 'e164_with_plus': keep a leading '+' plus "
            "country-code digits only (Google Ads Customer Match, TikTok "
            "Custom Audiences, X/Twitter Tailored Audiences convention). "
            "'digits_only_no_plus': strip the '+' and every non-digit "
            "character (Meta Custom Audiences, Pinterest Customer Lists "
            "convention)."
        ),
    )
    hashed_column_suffix: str = Field(
        default="_hashed",
        description=(
            "Suffix appended to each identifier column's name to form its "
            "hashed output column name, e.g. 'email' -> 'email_hashed'."
        ),
    )
    drop_original_columns: bool = Field(
        default=True,
        description=(
            "Drop the original plaintext identifier columns after hashing "
            "(default: true -- the whole point of this component is to "
            "not leak plaintext PII downstream). Set to false to KEEP the "
            "plaintext columns alongside the hashed ones, for local "
            "debugging/testing only -- WARNING: enabling this in a "
            "production pipeline defeats the purpose of hashing, since the "
            "plaintext PII remains in the output DataFrame and flows to "
            "every downstream consumer of this asset."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
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

    partition_values: Optional[str] = Field(
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
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
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
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'email_hashed': ['email']}",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 5 rows "
            "as a markdown table). Used by builder UIs to render asset shape "
            "without warehouse access. NOTE: since hashed values are the "
            "whole point, this preview only ever shows hashed digests (or "
            "passthrough/non-identifier columns) unless drop_original_columns "
            "is disabled."
        ),
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

    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    @classmethod
    def get_description(cls) -> str:
        return (
            "Normalize + SHA-256 hash PII identifier columns (email, phone, "
            "generic) in a DataFrame — a destination-agnostic building block "
            "for safe downstream activation or compliance/anonymization."
        )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("PiiHasherComponent: set exactly one of `upstream_asset_key` or `source`.")
        column_identifiers = self.column_identifiers

        bad_types = set(column_identifiers.values()) - _SUPPORTED_IDENTIFIER_TYPES
        if bad_types:
            raise ValueError(
                f"PiiHasherComponent: column_identifiers has unsupported "
                f"identifier type(s) {sorted(bad_types)}. Supported: "
                f"{sorted(_SUPPORTED_IDENTIFIER_TYPES)}."
            )
        if self.phone_mode not in _SUPPORTED_PHONE_MODES:
            raise ValueError(
                f"PiiHasherComponent: phone_mode must be one of "
                f"{sorted(_SUPPORTED_PHONE_MODES)}, got {self.phone_mode!r}."
            )

        # Standard catalog fields — phase 2 wiring
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        group_name = self.group_name
        email_strip_all_whitespace = self.email_strip_all_whitespace
        phone_mode = self.phone_mode
        hashed_column_suffix = self.hashed_column_suffix
        drop_original_columns = self.drop_original_columns

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
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
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage

        # Build-time column lineage default: hashed column <- its source column
        if not column_lineage and column_identifiers:
            column_lineage = {
                f"{col}{hashed_column_suffix}": [col] for col in column_identifiers
            }

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins=({"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))} if upstream_asset_key else None),
            required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
            partitions_def=partitions_def,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            description=self.description or PiiHasherComponent.get_description(),
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            upstream = kwargs.get("upstream")
            if upstream is None:
                upstream = _ingest_warehouse_query(self.source, context)
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                    upstream = upstream[_col_dates == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            df = upstream.copy()

            missing_cols = [c for c in column_identifiers if c not in df.columns]
            if missing_cols:
                context.log.warning(
                    f"Column(s) not found in DataFrame, skipping: {missing_cols}"
                )

            _hashed_counts: Dict[str, int] = {}
            for col, id_type in column_identifiers.items():
                if col not in df.columns:
                    continue
                hashed_col = f"{col}{hashed_column_suffix}"
                df[hashed_col] = df[col].apply(
                    lambda v, _id_type=id_type: _hash_identifier(
                        v, _id_type, email_strip_all_whitespace, phone_mode
                    )
                )
                _hashed_counts[hashed_col] = int(df[hashed_col].notna().sum())
                if drop_original_columns:
                    df = df.drop(columns=[col])

            context.log.info(
                f"PII hashing complete. Rows: {len(df)}, columns hashed: "
                f"{list(_hashed_counts.keys())}, non-null hashed values per "
                f"column: {_hashed_counts}, drop_original_columns={drop_original_columns}."
            )

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "columns_hashed": MetadataValue.int(len(_hashed_counts)),
            }
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        if out_col not in df.columns:
                            continue
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    if _lineage_deps:
                        _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                            TableColumnLineage(_lineage_deps)
                        )
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks

        _schema_checks = build_column_schema_change_checks(assets=[_asset])

        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
