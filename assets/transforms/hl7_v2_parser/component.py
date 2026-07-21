"""Hl7V2ParserComponent — parse pipe-delimited HL7 v2 messages to a DataFrame.

HL7 v2.x is the dominant healthcare messaging standard for hospital systems
(LIS, RIS, ADT feeds, etc.). Messages are pipe-delimited segments:

    MSH|^~\\&|HOSPITAL|EHR|LAB|...|<msg_type>|MSG00123|P|2.5
    PID|1||PATID12345||Doe^John^A||19800101|M|...
    ORC|RE|123|...
    OBR|1|...|^^^Glucose|...
    OBX|1|NM|GLU^Glucose^L||120|mg/dL|...

This component takes a column of raw HL7 messages and emits ONE ROW PER
SEGMENT-OF-INTEREST. Supported segments with full extractors:

  - **MSH** — Message header (sending/receiving app, datetime, version)
  - **PID** — Patient identification (name, DOB, sex, address)
  - **OBX** — Observation/result (code, value, units, status)
  - **ORC** — Order control (placer/filler order numbers, ordering provider)
  - **OBR** — Observation request (service code, observation/report times)
  - **PV1** — Patient visit (class, location, attending, admit/discharge dt)
  - **EVN** — Event type (event code, recorded/occurred dt, operator)
  - **DG1** — Diagnosis (code, name, codeset, type)
  - **AL1** — Patient allergy (allergen, severity, reaction, onset)

Common use:
  - Hospital LIS / EHR firehose → BQ events table
  - ADT (admit-discharge-transfer) feeds → patient movement analytics
  - LAB result feeds → observation table

Standard delimiters are auto-detected from MSH-1 (field) and MSH-2
(component/subcomponent/repetition/escape) per HL7 spec.

This is a deliberately simple parser — for full schema coverage, use the
`hl7apy` library directly in a custom component. The shipped extractors
cover ~80% of real-world ingest needs.
"""

from typing import Any, Dict, List, Literal, Optional, Union

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


def _parse_msh(seg_fields: List[str]) -> Dict[str, Any]:
    """MSH-1 is the field separator (already used to split). Fields:
       MSH-3=sending app, MSH-4=sending facility, MSH-5=receiving app,
       MSH-6=receiving facility, MSH-7=datetime, MSH-9=message_type,
       MSH-10=msg_control_id, MSH-11=processing_id, MSH-12=version_id."""
    def at(idx: int) -> Optional[str]:
        return seg_fields[idx] if idx < len(seg_fields) else None
    msg_type = at(9) or ""
    return {
        "segment":            "MSH",
        "sending_app":        at(3),
        "sending_facility":   at(4),
        "receiving_app":      at(5),
        "receiving_facility": at(6),
        "message_datetime":   at(7),
        "message_type":       msg_type,
        "msg_control_id":     at(10),
        "processing_id":      at(11),
        "version_id":         at(12),
    }


def _parse_pid(seg_fields: List[str], comp_sep: str) -> Dict[str, Any]:
    """PID-3=patient_id_list, PID-5=patient_name, PID-7=DOB, PID-8=sex,
       PID-11=address."""
    def at(idx: int) -> Optional[str]:
        return seg_fields[idx] if idx < len(seg_fields) else None
    name = (at(5) or "").split(comp_sep)
    addr = (at(11) or "").split(comp_sep)
    patient_id_field = at(3) or ""
    # PID-3 may have repetitions (~) — take first
    patient_id = patient_id_field.split("~")[0].split(comp_sep)[0]
    return {
        "segment":      "PID",
        "patient_id":   patient_id or None,
        "last_name":    name[0] if len(name) > 0 and name[0] else None,
        "first_name":   name[1] if len(name) > 1 and name[1] else None,
        "middle":       name[2] if len(name) > 2 and name[2] else None,
        "birth_date":   at(7),
        "sex":          at(8),
        "address_line1": addr[0] if len(addr) > 0 and addr[0] else None,
        "city":         addr[2] if len(addr) > 2 and addr[2] else None,
        "state":        addr[3] if len(addr) > 3 and addr[3] else None,
        "postal_code":  addr[4] if len(addr) > 4 and addr[4] else None,
    }


def _parse_obx(seg_fields: List[str], comp_sep: str) -> Dict[str, Any]:
    """OBX-2=value_type, OBX-3=identifier (code^name^codeset),
       OBX-5=value, OBX-6=units, OBX-7=ref_range, OBX-8=abnormal_flags,
       OBX-11=status, OBX-14=observation_datetime."""
    def at(idx: int) -> Optional[str]:
        return seg_fields[idx] if idx < len(seg_fields) else None
    ident = (at(3) or "").split(comp_sep)
    return {
        "segment":            "OBX",
        "value_type":         at(2),
        "code":               ident[0] if len(ident) > 0 and ident[0] else None,
        "code_name":          ident[1] if len(ident) > 1 and ident[1] else None,
        "code_system":        ident[2] if len(ident) > 2 and ident[2] else None,
        "value":              at(5),
        "units":              at(6),
        "reference_range":    at(7),
        "abnormal_flags":     at(8),
        "result_status":      at(11),
        "observation_dt":     at(14),
    }


def _parse_orc(seg_fields: List[str], comp_sep: str) -> Dict[str, Any]:
    """Order Control. ORC-1=order_control_code (NW/CA/RE/etc.),
       ORC-2=placer_order_num, ORC-3=filler_order_num, ORC-5=order_status,
       ORC-9=transaction_datetime, ORC-12=ordering_provider (id^last^first),
       ORC-15=order_effective_datetime."""
    def at(idx: int) -> Optional[str]:
        return seg_fields[idx] if idx < len(seg_fields) else None
    op = (at(12) or "").split(comp_sep)
    return {
        "segment":                 "ORC",
        "order_control_code":      at(1),
        "placer_order_num":        (at(2) or "").split(comp_sep)[0] or None,
        "filler_order_num":        (at(3) or "").split(comp_sep)[0] or None,
        "order_status":            at(5),
        "transaction_datetime":    at(9),
        "ordering_provider_id":    op[0] if len(op) > 0 and op[0] else None,
        "ordering_provider_last":  op[1] if len(op) > 1 and op[1] else None,
        "ordering_provider_first": op[2] if len(op) > 2 and op[2] else None,
        "order_effective_dt":      at(15),
    }


def _parse_obr(seg_fields: List[str], comp_sep: str) -> Dict[str, Any]:
    """Observation Request. OBR-1=set_id, OBR-2=placer_order_num,
       OBR-3=filler_order_num, OBR-4=universal_service_id (code^name^codeset),
       OBR-7=observation_datetime, OBR-14=specimen_received_dt,
       OBR-22=results_report_dt, OBR-24=diagnostic_service_section,
       OBR-25=result_status."""
    def at(idx: int) -> Optional[str]:
        return seg_fields[idx] if idx < len(seg_fields) else None
    svc = (at(4) or "").split(comp_sep)
    return {
        "segment":                  "OBR",
        "set_id":                   at(1),
        "placer_order_num":         (at(2) or "").split(comp_sep)[0] or None,
        "filler_order_num":         (at(3) or "").split(comp_sep)[0] or None,
        "service_code":             svc[0] if len(svc) > 0 and svc[0] else None,
        "service_name":             svc[1] if len(svc) > 1 and svc[1] else None,
        "service_code_system":      svc[2] if len(svc) > 2 and svc[2] else None,
        "observation_dt":           at(7),
        "specimen_received_dt":     at(14),
        "results_report_dt":        at(22),
        "diagnostic_service":       at(24),
        "result_status":            at(25),
    }


def _parse_pv1(seg_fields: List[str], comp_sep: str) -> Dict[str, Any]:
    """Patient Visit. PV1-2=patient_class (I/O/E/etc.),
       PV1-3=assigned_location (point_of_care^room^bed^facility),
       PV1-7=attending_doctor (id^last^first), PV1-10=hospital_service,
       PV1-14=admit_source, PV1-19=visit_number, PV1-44=admit_dt,
       PV1-45=discharge_dt."""
    def at(idx: int) -> Optional[str]:
        return seg_fields[idx] if idx < len(seg_fields) else None
    loc = (at(3) or "").split(comp_sep)
    att = (at(7) or "").split(comp_sep)
    return {
        "segment":            "PV1",
        "patient_class":      at(2),
        "point_of_care":      loc[0] if len(loc) > 0 and loc[0] else None,
        "room":               loc[1] if len(loc) > 1 and loc[1] else None,
        "bed":                loc[2] if len(loc) > 2 and loc[2] else None,
        "facility":           loc[3] if len(loc) > 3 and loc[3] else None,
        "attending_id":       att[0] if len(att) > 0 and att[0] else None,
        "attending_last":     att[1] if len(att) > 1 and att[1] else None,
        "attending_first":    att[2] if len(att) > 2 and att[2] else None,
        "hospital_service":   at(10),
        "admit_source":       at(14),
        "visit_number":       (at(19) or "").split(comp_sep)[0] or None,
        "admit_dt":           at(44),
        "discharge_dt":       at(45),
    }


def _parse_evn(seg_fields: List[str], comp_sep: str) -> Dict[str, Any]:
    """Event Type. EVN-1=event_type_code, EVN-2=recorded_dt,
       EVN-4=event_reason_code, EVN-5=operator (id^last^first),
       EVN-6=event_occurred_dt."""
    def at(idx: int) -> Optional[str]:
        return seg_fields[idx] if idx < len(seg_fields) else None
    op = (at(5) or "").split(comp_sep)
    return {
        "segment":           "EVN",
        "event_type_code":   at(1),
        "recorded_dt":       at(2),
        "event_reason":      at(4),
        "operator_id":       op[0] if len(op) > 0 and op[0] else None,
        "operator_last":     op[1] if len(op) > 1 and op[1] else None,
        "operator_first":    op[2] if len(op) > 2 and op[2] else None,
        "event_occurred_dt": at(6),
    }


def _parse_dg1(seg_fields: List[str], comp_sep: str) -> Dict[str, Any]:
    """Diagnosis. DG1-1=set_id, DG1-2=diag_coding_method (e.g. I10/I9/SNM),
       DG1-3=diagnosis_code (code^name^codeset), DG1-5=diagnosis_datetime,
       DG1-6=diagnosis_type (A=admitting, W=working, F=final)."""
    def at(idx: int) -> Optional[str]:
        return seg_fields[idx] if idx < len(seg_fields) else None
    diag = (at(3) or "").split(comp_sep)
    return {
        "segment":            "DG1",
        "set_id":             at(1),
        "coding_method":      at(2),
        "diagnosis_code":     diag[0] if len(diag) > 0 and diag[0] else None,
        "diagnosis_name":     diag[1] if len(diag) > 1 and diag[1] else None,
        "diagnosis_codeset":  diag[2] if len(diag) > 2 and diag[2] else None,
        "diagnosis_dt":       at(5),
        "diagnosis_type":     at(6),
    }


def _parse_al1(seg_fields: List[str], comp_sep: str) -> Dict[str, Any]:
    """Patient Allergy. AL1-1=set_id, AL1-2=allergen_type (DA=drug, FA=food,
       EA=environmental), AL1-3=allergen_code (code^name^codeset),
       AL1-4=severity (SV/MO/MI/U), AL1-5=reaction, AL1-6=onset_dt."""
    def at(idx: int) -> Optional[str]:
        return seg_fields[idx] if idx < len(seg_fields) else None
    allg = (at(3) or "").split(comp_sep)
    return {
        "segment":          "AL1",
        "set_id":           at(1),
        "allergen_type":    at(2),
        "allergen_code":    allg[0] if len(allg) > 0 and allg[0] else None,
        "allergen_name":    allg[1] if len(allg) > 1 and allg[1] else None,
        "allergen_codeset": allg[2] if len(allg) > 2 and allg[2] else None,
        "severity":         at(4),
        "reaction":         at(5),
        "onset_dt":         at(6),
    }


_SEGMENT_PARSERS = {
    "MSH": lambda fields, _comp: _parse_msh(fields),
    "PID": _parse_pid,
    "OBX": _parse_obx,
    "ORC": _parse_orc,
    "OBR": _parse_obr,
    "PV1": _parse_pv1,
    "EVN": _parse_evn,
    "DG1": _parse_dg1,
    "AL1": _parse_al1,
}


def _parse_message(raw: str, *, keep_segments: List[str]) -> List[Dict[str, Any]]:
    """Parse a single HL7 message; return one dict per segment-of-interest."""
    raw = raw.replace("\r\n", "\r").replace("\n", "\r").strip()
    if not raw.startswith("MSH"):
        return [{"_error": "message must start with MSH segment", "raw_preview": raw[:80]}]

    field_sep = raw[3:4] or "|"
    encoding = raw[4:8] if len(raw) >= 8 else "^~\\&"
    comp_sep = encoding[0] if encoding else "^"

    rows: List[Dict[str, Any]] = []
    msh_context: Dict[str, Any] = {}

    for seg_line in raw.split("\r"):
        if not seg_line:
            continue
        # For MSH the first field is the encoding chars; treat field 1 specially.
        seg_id = seg_line[:3]
        fields = seg_line.split(field_sep)
        # MSH parsing: field-sep itself is fields[0]=='MSH' but the SPEC says
        # MSH-1 is the field separator. Most parsers shift PID-style segments
        # so fields[1] is "MSH-1" for everything else. Keep it simple: pass
        # the raw split through; the extractors know their indices.
        # For MSH specifically the encoding chars are stored at fields[1] in
        # this convention — but tools expect MSH-3 to be sending_app. We
        # special-case MSH below to keep numbering consistent with HL7 spec.
        if seg_id == "MSH":
            # Re-shift so that "fields[3]" == MSH-3
            fields = ["MSH", field_sep] + (seg_line[4 + len(encoding) + 1:].split(field_sep)
                                            if len(seg_line) > 9 else [])
            # Easier: just split and let MSH parser index from 0 = MSH literal,
            # 1 = encoding chars, 2 = sending_app, ... — but our extractor
            # uses idx 3 = sending_app, so prepend a dummy at idx 2:
            fields = ["MSH", encoding] + seg_line.split(field_sep)[1:]

        if seg_id not in keep_segments:
            continue

        parser = _SEGMENT_PARSERS.get(seg_id)
        if parser is None:
            continue
        try:
            row = parser(fields, comp_sep)
        except Exception as e:
            row = {"segment": seg_id, "_error": str(e)}

        # Cache MSH on the message — every subsequent segment inherits it
        if seg_id == "MSH":
            msh_context = {
                "msg_control_id": row.get("msg_control_id"),
                "message_type":   row.get("message_type"),
                "sending_app":    row.get("sending_app"),
                "version_id":     row.get("version_id"),
            }
        else:
            for k, v in msh_context.items():
                row.setdefault(k, v)

        rows.append(row)

    return rows


class Hl7V2ParserComponent(Component, Model, Resolvable):
    """Parse a column of pipe-delimited HL7 v2 messages into a flat DataFrame."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    message_column: Union[str, int] = Field(
        default="message",
        description="Column holding the raw HL7 v2 message (\\r-delimited segments).",
    )

    keep_segments: List[
        Literal["MSH", "PID", "OBX", "ORC", "OBR", "PV1", "EVN", "DG1", "AL1"]
    ] = Field(
        default=["MSH", "PID", "OBX"],
        description=(
            "Segments to emit. Full extractors: MSH, PID, OBX, ORC, OBR, "
            "PV1, EVN, DG1, AL1. Other segments are silently skipped."
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
        keep_segments: List[str] = [str(s) for s in self.keep_segments]

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Parse HL7 v2 → DataFrame (segments={keep_segments}).",
            group_name=self.group_name,
            kinds={"hl7", "healthcare", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
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
            seg_counts: Dict[str, int] = {}
            error_count = 0
            for _, src in upstream.iterrows():
                raw = src[message_column]
                if not isinstance(raw, str) or not raw.strip():
                    error_count += 1
                    continue
                parsed = _parse_message(raw, keep_segments=keep_segments)
                for r in parsed:
                    if "_error" in r:
                        error_count += 1
                    seg = r.get("segment") or "?"
                    seg_counts[seg] = seg_counts.get(seg, 0) + 1
                    # Carry over non-message columns from the source row
                    for c in upstream.columns:
                        if c != message_column and c not in r:
                            r[c] = src[c]
                    all_rows.append(r)

            df = pd.DataFrame(all_rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no rows)"
            return Output(
                value=df,
                metadata={
                    "input_messages": MetadataValue.int(len(upstream)),
                    "output_rows":    MetadataValue.int(len(df)),
                    "by_segment":     MetadataValue.json(seg_counts),
                    "errors":         MetadataValue.int(error_count),
                    "preview":        MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
