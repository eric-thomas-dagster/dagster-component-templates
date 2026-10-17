"""Dataframe To Snowflake.

Write a DataFrame to a Snowflake table.
"""
import os
from typing import Any, Dict, List, Optional, Union
from typing import Literal

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
from pydantic import ConfigDict, Field


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value



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


class DataframeToSnowflakeComponent(Component, Model, Resolvable):
    """Write a DataFrame to a Snowflake table."""

    # Internal field is `schema_name` (avoids shadowing BaseModel.schema()).
    # YAML still accepts the Snowflake-native `schema:` key via the alias.
    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame")
    table: str = Field(description="Destination Snowflake table name")
    database: Optional[str] = Field(default=None, description="Snowflake database (overrides connection default)")
    schema_name: Optional[str] = Field(default=None, alias="schema", description="Snowflake schema")
    warehouse: Optional[str] = Field(default=None, description="Snowflake warehouse to use")
    role: Optional[str] = Field(default=None, description="Snowflake role")
    account_env_var: str = Field(default="SNOWFLAKE_ACCOUNT", description="Env var containing Snowflake account identifier")
    user_env_var: str = Field(default="SNOWFLAKE_USER", description="Env var containing Snowflake user")
    password_env_var: Optional[str] = Field(default="SNOWFLAKE_PASSWORD", description="Env var containing Snowflake password. Leave None / empty when using SSO or keypair.")
    # SSO / keypair / PAT alternatives — for accounts where password auth
    # is disabled. Leave password_env_var unset and use one of these.
    authenticator: Optional[str] = Field(
        default=None,
        description="Snowflake authenticator: 'SNOWFLAKE_JWT' (keypair), 'externalbrowser' (SSO), 'oauth', etc.",
    )
    private_key_file_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the path to a PEM RSA private key file (for authenticator='SNOWFLAKE_JWT').",
    )
    private_key_file_pwd_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the passphrase for an encrypted private key file (optional).",
    )
    token_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding an OAuth / PAT token (with authenticator='oauth' or PAT).",
    )
    if_exists: str = Field(default="replace", description="Behavior if table exists: 'replace', 'append', 'fail'")
    chunksize: Optional[int] = Field(default=None, description="Number of rows per write chunk")
    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the DataFrame about to be written, in "
            "metadata, so builder UIs can show 'what's being sunk' without "
            "warehouse access."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows in the preview when include_preview_metadata=True. Random "
            "sample if len > 10x preview_rows; else head."
        ),
    )

    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional key of a registered SnowflakeResourceComponent (resources/snowflake_resource). When set, connection "
            "credentials are read from that resource at run time instead of the inline "
            "account/user/password/authenticator env-var fields -- lets one credential serve this sink and other components "
            "without configuring it twice. Leave unset to use the inline fields "
            "(existing behavior, unchanged)."
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
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in ONE run: the upstream frame "
            "(loaded once, spanning the whole range) is filtered to every partition "
            "key in the range via partition_date_column/partition_static_column and "
            "written together. 'multi_run' launches one run per partition (optionally "
            "capped via backfill_policy_max_partitions_per_run). Leave unset for "
            "Dagster's default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. Requires a partition-range-aware IO manager "
            "for the upstream asset (e.g. a DB-table IO manager with partition_expr "
            "set) -- a generic filesystem IO manager cannot persist/load a single_run "
            "output spanning multiple partitions and raises a clear error at "
            "materialization time."
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


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
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
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_description(cls) -> str:
        return "Write a DataFrame to a Snowflake table."

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


    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        table = self.table
        database = self.database
        schema = self.schema_name
        warehouse = self.warehouse
        role = self.role
        account_env_var = self.account_env_var
        user_env_var = self.user_env_var
        password_env_var = self.password_env_var
        authenticator = self.authenticator
        private_key_file_env_var = self.private_key_file_env_var
        private_key_file_pwd_env_var = self.private_key_file_pwd_env_var
        token_env_var = self.token_env_var
        if_exists = self.if_exists
        chunksize = self.chunksize
        group_name = self.group_name

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "dataframe_to_snowflake"  # component directory name
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



        resource_key = self.resource_key

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @asset(retry_policy=_retry_policy, backfill_policy=_backfill_policy_obj, required_resource_keys=({resource_key} if resource_key else None),
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            description=DataframeToSnowflakeComponent.get_description(),
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> MaterializeResult:
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
            # A single_run/multi_run backfill range spans several partition keys:
            # keep rows matching ANY key in the range (one key on a normal run).
            if context.has_partition_key or context.has_partition_key_range:
                import numpy as _np
                _keys = [context.partition_key] if context.has_partition_key else list(context.partition_keys)
                _keep = _np.zeros(len(upstream), dtype=bool)
                for _pk in _keys:
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
            try:
                from snowflake.connector.pandas_tools import write_pandas
                import snowflake.connector
            except ImportError:
                raise ImportError(
                    "snowflake-connector-python required: pip install snowflake-connector-python[pandas]"
                )

            import contextlib

            _snow_stack = contextlib.ExitStack()
            if resource_key:
                # Credentials/account/warehouse come from the registered dagster-snowflake
                # SnowflakeResource; its get_connection() context manager yields the connection.
                conn = _snow_stack.enter_context(getattr(context.resources, resource_key).get_connection())
            else:
                conn_kwargs = {
                    "account": _require_env(account_env_var),
                    "user": _require_env(user_env_var),
                }
                # Auth: keypair / SSO / token (preferred for accounts where password
                # auth is disabled) takes precedence over password if BOTH are set.
                if authenticator:
                    conn_kwargs["authenticator"] = authenticator
                    if private_key_file_env_var:
                        pk_path = os.environ.get(private_key_file_env_var)
                        if pk_path:
                            conn_kwargs["private_key_file"] = pk_path
                        if private_key_file_pwd_env_var:
                            pk_pwd = os.environ.get(private_key_file_pwd_env_var)
                            if pk_pwd:
                                conn_kwargs["private_key_file_pwd"] = pk_pwd
                    elif token_env_var:
                        tok = os.environ.get(token_env_var)
                        if tok:
                            conn_kwargs["token"] = tok
                elif password_env_var and os.environ.get(password_env_var):
                    conn_kwargs["password"] = _require_env(password_env_var)
                else:
                    raise EnvironmentError(
                        "dataframe_to_snowflake: no auth configured. Set either "
                        "password_env_var (with password in env) OR authenticator + "
                        "private_key_file_env_var (keypair) / token_env_var (PAT)."
                    )
                if database:
                    conn_kwargs["database"] = database
                if schema:
                    conn_kwargs["schema"] = schema
                if warehouse:
                    conn_kwargs["warehouse"] = warehouse
                if role:
                    conn_kwargs["role"] = role

                conn = snowflake.connector.connect(**conn_kwargs)
            try:
                df_write = upstream.copy()
                df_write.columns = [c.upper() for c in df_write.columns]

                if if_exists == "replace":
                    cursor = conn.cursor()
                    full_table = (
                        f"{database + '.' if database else ''}"
                        f"{schema + '.' if schema else ''}"
                        f"{table.upper()}"
                    )
                    cursor.execute(f"DROP TABLE IF EXISTS {full_table}")
                    cursor.close()

                # use_logical_type=True is required so write_pandas emits
                # Parquet TIMESTAMP logical types instead of raw INT64. Without
                # it, COPY into an existing TIMESTAMP_NTZ column fails with
                # 002023 "expecting TIMESTAMP_NTZ(9) but got NUMBER(38,0)".
                # Available since snowflake-connector-python 3.4.0.
                success, nchunks, nrows, _ = write_pandas(
                    conn,
                    df_write,
                    table.upper(),
                    auto_create_table=True,
                    chunk_size=chunksize,
                    use_logical_type=True,
                )
            finally:  # always release the connection, even if the write fails
                conn.close()
                _snow_stack.close()

            context.log.info(f"Wrote {nrows} rows to Snowflake table {table} in {nchunks} chunks")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(nrows),
                    "column_count": MetadataValue.int(len(upstream.columns)),
                    "table": MetadataValue.text(table),
                    "success": MetadataValue.bool(success),
                
                    "dagster/row_count": MetadataValue.int(nrows),
                    **({"preview": MetadataValue.md((upstream.sample(preview_rows) if len(upstream) > preview_rows * 10 else upstream.head(preview_rows)).to_markdown(index=False))} if include_preview and len(upstream) > 0 else {}),
                }
            )

        return Definitions(assets=[_asset])


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
