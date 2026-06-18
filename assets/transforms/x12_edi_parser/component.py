"""X12EdiParserComponent — parse ASC X12 EDI envelopes into a flat DataFrame.

X12 is the dominant US-domestic EDI standard. Common transaction sets:
  - **270 / 271** — Eligibility inquiry / response (healthcare)
  - **276 / 277** — Claim status inquiry / response (healthcare)
  - **834**       — Benefit enrollment (healthcare)
  - **835**       — Remittance advice / claim payment (healthcare)
  - **837**       — Healthcare claim (P=Professional, I=Institutional, D=Dental)
  - **850 / 855** — Purchase order / acknowledgment (retail/supply chain)
  - **856**       — Advance ship notice (retail)
  - **940 / 945** — Warehouse shipping order / advice (logistics)
  - **990**       — Response to load tender (transportation)

X12 envelopes have a fixed nested structure:
  ISA  — Interchange Control Header
   GS   — Functional Group Header
    ST   — Transaction Set Header  (e.g. ST*837*0001 → 837 claim)
     ...detail segments...
    SE   — Transaction Set Trailer
   GE   — Functional Group Trailer
  IEA  — Interchange Control Trailer

This component emits ONE ROW PER ST/SE transaction inside each ISA
envelope, with the ISA/GS context inherited as columns. Detail segments
(CLM for claim, BPR for payment, BEG for purchase order, etc.) are
parsed selectively based on the transaction set type.

Segment terminator (`~`), element separator (`*`), and component
separator (`:` or `>`) are auto-detected from the ISA header per the
X12 spec — no need to configure them.
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


def _detect_delimiters(raw: str) -> Dict[str, str]:
    """ISA fields are positionally fixed-width; their separators are at
    known offsets. Element sep is char 4 (after 'ISA'); component sep is
    char 105 (ISA-16); segment terminator is char 106 (after IEA tail)."""
    if not raw.startswith("ISA"):
        # Best effort fallback
        return {"element": "*", "component": ":", "segment": "~"}
    # ISA[3] is the element separator
    element_sep = raw[3] if len(raw) > 3 else "*"
    # ISA segment is fixed 106 chars (incl. element_sep at end of fixed fields);
    # component sep at offset 104, segment terminator at offset 105.
    component_sep = raw[104] if len(raw) > 105 else ":"
    segment_term  = raw[105] if len(raw) > 106 else "~"
    return {"element": element_sep, "component": component_sep, "segment": segment_term}


def _parse_isa(seg: List[str]) -> Dict[str, Any]:
    """ISA fields are positional, fixed widths. Common useful fields:
       ISA06 sender_id, ISA08 receiver_id, ISA09 date (YYMMDD),
       ISA10 time (HHMM), ISA13 interchange_control_num,
       ISA14 ack_requested, ISA15 usage_indicator (T/P)."""
    def at(i: int) -> Optional[str]:
        return (seg[i].strip() if i < len(seg) else None) or None
    return {
        "isa_sender_id":       at(6),
        "isa_receiver_id":     at(8),
        "isa_date":            at(9),
        "isa_time":            at(10),
        "isa_control_num":     at(13),
        "isa_usage_indicator": at(15),
    }


def _parse_gs(seg: List[str]) -> Dict[str, Any]:
    """GS01 functional_id_code (HC=health claim, RA=remittance, PO=order),
       GS02 sender, GS03 receiver, GS04 date (CCYYMMDD), GS06 control_num,
       GS08 version_release_industry_id (e.g. 005010X222A1 for 837P)."""
    def at(i: int) -> Optional[str]:
        return (seg[i].strip() if i < len(seg) else None) or None
    return {
        "gs_functional_id":   at(1),
        "gs_sender":          at(2),
        "gs_receiver":        at(3),
        "gs_date":            at(4),
        "gs_control_num":     at(6),
        "gs_version":         at(8),
    }


def _parse_st(seg: List[str]) -> Dict[str, Any]:
    """ST01 transaction_set_identifier (e.g. 837, 835, 850); ST02 control_num."""
    def at(i: int) -> Optional[str]:
        return (seg[i].strip() if i < len(seg) else None) or None
    return {
        "transaction_set":     at(1),
        "st_control_num":      at(2),
    }


def _extract_claim_fields(segments: List[List[str]]) -> Dict[str, Any]:
    """For an 837 transaction, the CLM segment carries the core claim header:
       CLM01 patient_account_number, CLM02 total_claim_charge,
       CLM05 facility_code (composite). NM1 carries names (PR insurer, IL insured)."""
    out: Dict[str, Any] = {}
    for seg in segments:
        if not seg:
            continue
        sid = seg[0]
        if sid == "CLM" and "claim_account_num" not in out:
            out["claim_account_num"] = (seg[1].strip() if len(seg) > 1 else None) or None
            try:
                out["claim_total_charge"] = float(seg[2]) if len(seg) > 2 and seg[2] else None
            except (ValueError, TypeError):
                out["claim_total_charge"] = None
        elif sid == "NM1" and len(seg) > 1:
            entity = seg[1]
            name = (seg[3].strip() if len(seg) > 3 else None) or None
            if entity == "85" and "billing_provider_name" not in out:
                out["billing_provider_name"] = name
            elif entity == "IL" and "subscriber_last_name" not in out:
                out["subscriber_last_name"] = name
                out["subscriber_first_name"] = (seg[4].strip() if len(seg) > 4 else None) or None
            elif entity == "QC" and "patient_last_name" not in out:
                out["patient_last_name"] = name
                out["patient_first_name"] = (seg[4].strip() if len(seg) > 4 else None) or None
            elif entity == "PR" and "payer_name" not in out:
                out["payer_name"] = name
    return out


def _extract_835_fields(segments: List[List[str]]) -> Dict[str, Any]:
    """835 remittance — BPR is the payment header:
       BPR02 total_payment_amount, BPR03 credit_debit (C/D),
       BPR04 payment_method, BPR16 payment_date (CCYYMMDD)."""
    out: Dict[str, Any] = {}
    for seg in segments:
        if not seg or seg[0] != "BPR":
            continue
        try:
            out["payment_amount"] = float(seg[2]) if len(seg) > 2 and seg[2] else None
        except (ValueError, TypeError):
            out["payment_amount"] = None
        out["credit_debit"]    = (seg[3].strip() if len(seg) > 3 else None) or None
        out["payment_method"]  = (seg[4].strip() if len(seg) > 4 else None) or None
        out["payment_date"]    = (seg[16].strip() if len(seg) > 16 else None) or None
        break
    return out


def _extract_850_fields(segments: List[List[str]]) -> Dict[str, Any]:
    """850 purchase order — BEG is the PO header:
       BEG02 po_type, BEG03 po_number, BEG05 po_date (CCYYMMDD)."""
    out: Dict[str, Any] = {}
    for seg in segments:
        if not seg or seg[0] != "BEG":
            continue
        out["po_type"]   = (seg[2].strip() if len(seg) > 2 else None) or None
        out["po_number"] = (seg[3].strip() if len(seg) > 3 else None) or None
        out["po_date"]   = (seg[5].strip() if len(seg) > 5 else None) or None
        break
    return out


def _extract_270_271_fields(segments: List[List[str]]) -> Dict[str, Any]:
    """270/271 eligibility — TRN (trace num) + NM1 names."""
    out: Dict[str, Any] = {}
    for seg in segments:
        if not seg:
            continue
        if seg[0] == "TRN" and "trace_num" not in out and len(seg) > 2:
            out["trace_num"] = (seg[2].strip() if len(seg) > 2 else None) or None
        elif seg[0] == "NM1" and len(seg) > 3:
            ent = seg[1]
            name = (seg[3].strip() if len(seg) > 3 else None) or None
            if ent == "PR" and "payer_name" not in out:
                out["payer_name"] = name
            elif ent == "IL" and "subscriber_last_name" not in out:
                out["subscriber_last_name"] = name
                out["subscriber_first_name"] = (seg[4].strip() if len(seg) > 4 else None) or None
    return out


_TXN_EXTRACTORS = {
    "270": _extract_270_271_fields,
    "271": _extract_270_271_fields,
    "835": _extract_835_fields,
    "837": _extract_claim_fields,
    "850": _extract_850_fields,
    "855": _extract_850_fields,  # ack of an 850 — similar header shape
}


def _parse_message(raw: str) -> List[Dict[str, Any]]:
    """Return one dict per ST/SE transaction inside the message."""
    raw = raw.strip()
    delims = _detect_delimiters(raw)
    seg_term = delims["segment"]
    elem_sep = delims["element"]

    # Strip optional trailing newline whitespace from each segment
    segments_raw = [s.replace("\r", "").replace("\n", "").strip()
                    for s in raw.split(seg_term) if s.strip()]
    segments = [s.split(elem_sep) for s in segments_raw]

    rows: List[Dict[str, Any]] = []
    isa_ctx: Dict[str, Any] = {}
    gs_ctx:  Dict[str, Any] = {}
    current_txn_segs: List[List[str]] = []
    current_st_ctx: Optional[Dict[str, Any]] = None

    for seg in segments:
        if not seg:
            continue
        sid = seg[0]
        if sid == "ISA":
            isa_ctx = _parse_isa(seg)
        elif sid == "GS":
            gs_ctx = _parse_gs(seg)
        elif sid == "ST":
            current_st_ctx = _parse_st(seg)
            current_txn_segs = [seg]
        elif sid == "SE":
            if current_st_ctx is None:
                continue
            current_txn_segs.append(seg)
            txn_set = current_st_ctx.get("transaction_set") or ""
            extractor = _TXN_EXTRACTORS.get(txn_set)
            extra = extractor(current_txn_segs) if extractor else {}
            row: Dict[str, Any] = {**isa_ctx, **gs_ctx, **current_st_ctx, **extra,
                                    "segment_count": len(current_txn_segs)}
            rows.append(row)
            current_st_ctx = None
            current_txn_segs = []
        else:
            if current_st_ctx is not None:
                current_txn_segs.append(seg)
    return rows


class X12EdiParserComponent(Component, Model, Resolvable):
    """Parse a column of ASC X12 EDI messages into one row per transaction set."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    message_column: Union[str, int] = Field(
        default="message",
        description="Column holding the raw X12 message (segment-terminated text).",
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

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Parse ASC X12 EDI → one row per ST/SE transaction.",
            group_name=self.group_name,
            kinds={"x12", "edi", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> Output:
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            if message_column not in upstream.columns:
                raise ValueError(
                    f"message_column={message_column!r} not in upstream: {list(upstream.columns)}"
                )

            all_rows: List[Dict[str, Any]] = []
            txn_counts: Dict[str, int] = {}
            error_count = 0
            for _, src in upstream.iterrows():
                raw = src[message_column]
                if not isinstance(raw, str) or not raw.strip():
                    error_count += 1
                    continue
                try:
                    parsed = _parse_message(raw)
                except Exception as e:
                    error_count += 1
                    all_rows.append({"_error": str(e), "raw_preview": raw[:80]})
                    continue
                for r in parsed:
                    txn = r.get("transaction_set") or "?"
                    txn_counts[txn] = txn_counts.get(txn, 0) + 1
                    # Carry over non-message columns
                    for c in upstream.columns:
                        if c != message_column and c not in r:
                            r[c] = src[c]
                    all_rows.append(r)

            df = pd.DataFrame(all_rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no rows)"
            return Output(
                value=df,
                metadata={
                    "input_messages":     MetadataValue.int(len(upstream)),
                    "output_rows":        MetadataValue.int(len(df)),
                    "by_transaction_set": MetadataValue.json(txn_counts),
                    "errors":             MetadataValue.int(error_count),
                    "preview":            MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
