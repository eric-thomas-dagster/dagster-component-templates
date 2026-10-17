"""FixMessageParserComponent — parse FIX (Financial Information eXchange) messages.

FIX is the dominant electronic-trading protocol. Messages are `tag=value`
pairs delimited by SOH (0x01, often displayed as `|`). Each message has a
`MsgType` (FIX tag 35) that determines its meaning.

Common message types:
  - **D** — NewOrderSingle (buy / sell)
  - **F** — OrderCancelRequest
  - **G** — OrderCancelReplaceRequest
  - **8** — ExecutionReport (fill / partial fill / rejection)
  - **9** — OrderCancelReject
  - **3** — Reject (session-level)
  - **0** — Heartbeat
  - **2** — Resend Request
  - **A** — Logon
  - **5** — Logout
  - **W** — MarketDataSnapshotFullRefresh

This component takes a column of FIX messages and emits ONE ROW PER
message with the most commonly-needed fields broken out (symbol, side,
quantity, price, order id, exec id, etc.). The full `tags_raw` dict is
preserved for ad-hoc analysis.

Auto-detects the SOH delimiter (0x01 or pipe `|`).
"""

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
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


# Common FIX tag → human name mapping (subset of the full ~1500-tag spec)
FIX_TAGS = {
    "6":   "AvgPx",
    "8":   "BeginString",
    "9":   "BodyLength",
    "10":  "CheckSum",
    "11":  "ClOrdID",
    "14":  "CumQty",
    "17":  "ExecID",
    "20":  "ExecTransType",
    "22":  "SecurityIDSource",
    "31":  "LastPx",
    "32":  "LastQty",
    "34":  "MsgSeqNum",
    "35":  "MsgType",
    "37":  "OrderID",
    "38":  "OrderQty",
    "39":  "OrdStatus",
    "40":  "OrdType",
    "44":  "Price",
    "48":  "SecurityID",
    "49":  "SenderCompID",
    "52":  "SendingTime",
    "54":  "Side",
    "55":  "Symbol",
    "56":  "TargetCompID",
    "59":  "TimeInForce",
    "60":  "TransactTime",
    "150": "ExecType",
    "151": "LeavesQty",
}

# Per-spec mappings for enum-like fields
_SIDE = {"1": "buy", "2": "sell", "3": "buy_minus", "4": "sell_plus",
         "5": "sell_short", "6": "sell_short_exempt"}
_ORD_TYPE = {"1": "market", "2": "limit", "3": "stop", "4": "stop_limit"}
_TIF = {"0": "day", "1": "gtc", "2": "opg", "3": "ioc", "4": "fok",
        "5": "gtx", "6": "gtd"}
_ORD_STATUS = {"0": "new", "1": "partially_filled", "2": "filled", "3": "done_for_day",
               "4": "canceled", "5": "replaced", "6": "pending_cancel", "7": "stopped",
               "8": "rejected", "9": "suspended", "A": "pending_new", "B": "calculated",
               "C": "expired", "D": "accepted_for_bidding", "E": "pending_replace"}
_EXEC_TYPE = {"0": "new", "1": "partial_fill", "2": "fill", "3": "done_for_day",
              "4": "canceled", "5": "replace", "6": "pending_cancel",
              "7": "stopped", "8": "rejected", "9": "suspended",
              "A": "pending_new", "B": "calculated", "C": "expired",
              "F": "trade", "G": "trade_correct", "H": "trade_cancel"}
_MSG_TYPE = {"D": "NewOrderSingle", "F": "OrderCancelRequest", "G": "OrderCancelReplaceRequest",
             "8": "ExecutionReport", "9": "OrderCancelReject", "3": "Reject",
             "0": "Heartbeat", "1": "TestRequest", "2": "ResendRequest",
             "A": "Logon", "5": "Logout", "W": "MarketDataSnapshotFullRefresh"}


def _detect_delimiter(raw: str) -> str:
    """SOH (`\\x01`) is the canonical FIX delimiter, but log files often render
    it as `|`. Auto-detect."""
    if "\x01" in raw:
        return "\x01"
    if "|" in raw:
        return "|"
    return "\x01"  # fall back; will likely produce a 1-row error


def _parse_message(raw: str) -> Dict[str, Any]:
    """Parse a single FIX message → one flat dict."""
    raw = raw.strip().rstrip("\x01").rstrip("|")
    if not raw:
        return {"_error": "empty message"}

    delim = _detect_delimiter(raw)
    tags_raw: Dict[str, str] = {}
    for kv in raw.split(delim):
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        tags_raw[k.strip()] = v.strip()

    if not tags_raw:
        return {"_error": "no tag=value pairs found", "raw_preview": raw[:80]}

    # Resolve common tag IDs to friendly columns
    out: Dict[str, Any] = {
        "begin_string":    tags_raw.get("8"),
        "msg_type":        tags_raw.get("35"),
        "msg_type_name":   _MSG_TYPE.get(tags_raw.get("35", ""), None),
        "sender":          tags_raw.get("49"),
        "target":          tags_raw.get("56"),
        "msg_seq_num":     tags_raw.get("34"),
        "sending_time":    tags_raw.get("52"),
        "cl_ord_id":       tags_raw.get("11"),
        "order_id":        tags_raw.get("37"),
        "exec_id":         tags_raw.get("17"),
        "symbol":          tags_raw.get("55"),
        "side_code":       tags_raw.get("54"),
        "side":            _SIDE.get(tags_raw.get("54", ""), None),
        "ord_type_code":   tags_raw.get("40"),
        "ord_type":        _ORD_TYPE.get(tags_raw.get("40", ""), None),
        "time_in_force_code": tags_raw.get("59"),
        "time_in_force":   _TIF.get(tags_raw.get("59", ""), None),
        "ord_status_code": tags_raw.get("39"),
        "ord_status":      _ORD_STATUS.get(tags_raw.get("39", ""), None),
        "exec_type_code":  tags_raw.get("150"),
        "exec_type":       _EXEC_TYPE.get(tags_raw.get("150", ""), None),
        "transact_time":   tags_raw.get("60"),
    }
    # Numeric tags
    for k_in, k_out in [("38", "order_qty"), ("44", "price"), ("31", "last_px"),
                        ("32", "last_qty"), ("14", "cum_qty"), ("151", "leaves_qty"),
                        ("6", "avg_px")]:
        v = tags_raw.get(k_in)
        try:
            out[k_out] = float(v) if v not in (None, "") else None
        except (ValueError, TypeError):
            out[k_out] = None

    # Preserve full raw tag map for fields we didn't break out
    out["tags_raw"] = tags_raw
    return out



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


class FixMessageParserComponent(Component, Model, Resolvable):
    """Parse FIX trading-protocol messages into a flat DataFrame."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream DataFrame asset key.. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )

    message_column: Union[str, int] = Field(
        default="message",
        description="Column holding the raw FIX message (tag=value pairs separated by SOH or `|`).",
    )

    msg_type_filter: Optional[List[Union[str, int]]] = Field(
        default=None,
        description=(
            "Optional list of MsgType codes to emit. E.g. [D, 8] for orders + executions only. "
            "Default: all. Numeric codes like 8 may be passed as ints in YAML — they're coerced to str at runtime."
        ),
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key (e.g. 'customer_id').",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), csv=('partition_values',), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("FixMessageParserComponent: set exactly one of `upstream_asset_key` or `source`.")
        partitions_def = None
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key) if self.upstream_asset_key else None
        message_column = self.message_column
        msg_type_filter = {str(x) for x in self.msg_type_filter} if self.msg_type_filter else None

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Parse FIX trading messages → flat DataFrame.",
            group_name=self.group_name,
            kinds={"fix", "fintech", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins=({"upstream": AssetIn(key=upstream_key)} if upstream_key else None),
            required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, **kwargs):
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
            if context.has_partition_key or context.has_partition_key_range:
                # One filter pass per partition key: a normal tick gives exactly one,
                # a single_run backfill gives the whole range. Rows are selected by
                # position so keys that match overlapping rows never duplicate them.
                _pk_base = upstream
                _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                _pk_keep = set()
                _pk_filtered = False
                for _pk_iter in context.partition_keys:
                    upstream = _pk_pos
                    _pk = _pk_iter
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                    if partition_date_column and partition_date_column in upstream.columns and _date_key:
                        _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                        upstream = upstream[_col_dates == _date_key]
                    if partition_static_column and partition_static_column in upstream.columns and _static_key:
                        upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                        upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                    if upstream is not _pk_pos:
                        _pk_filtered = True
                        _pk_keep.update(upstream.index)
                upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
            if message_column not in upstream.columns:
                raise ValueError(
                    f"message_column={message_column!r} not in upstream: {list(upstream.columns)}"
                )

            all_rows: List[Dict[str, Any]] = []
            type_counts: Dict[str, int] = {}
            error_count = 0
            for _, src in upstream.iterrows():
                raw = src[message_column]
                if not isinstance(raw, str) or not raw.strip():
                    error_count += 1
                    continue
                row = _parse_message(raw)
                if "_error" in row:
                    error_count += 1
                if msg_type_filter and row.get("msg_type") not in msg_type_filter:
                    continue
                # Carry over non-message columns
                for c in upstream.columns:
                    if c != message_column and c not in row:
                        row[c] = src[c]
                mt = row.get("msg_type_name") or row.get("msg_type") or "?"
                type_counts[mt] = type_counts.get(mt, 0) + 1
                all_rows.append(row)

            df = pd.DataFrame(all_rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no rows)"
            return Output(
                value=df,
                metadata={
                    "input_messages":  MetadataValue.int(len(upstream)),
                    "output_rows":     MetadataValue.int(len(df)),
                    "by_msg_type":     MetadataValue.json(type_counts),
                    "errors":          MetadataValue.int(error_count),
                    "preview":         MetadataValue.md(preview or ""),
                },
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
