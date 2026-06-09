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


class FixMessageParserComponent(Component, Model, Resolvable):
    """Parse FIX trading-protocol messages into a flat DataFrame."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    message_column: str = Field(
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
    tags: Optional[Dict[str, str]] = Field(default=None)
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
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
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

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        message_column = self.message_column
        msg_type_filter = {str(x) for x in self.msg_type_filter} if self.msg_type_filter else None

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Parse FIX trading messages → flat DataFrame.",
            group_name=self.group_name,
            kinds={"fix", "fintech", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
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
