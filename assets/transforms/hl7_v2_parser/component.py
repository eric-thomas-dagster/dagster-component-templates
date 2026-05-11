"""Hl7V2ParserComponent — parse pipe-delimited HL7 v2 messages to a DataFrame.

HL7 v2.x is the dominant healthcare messaging standard for hospital systems
(LIS, RIS, ADT feeds, etc.). Messages are pipe-delimited segments:

    MSH|^~\\&|HOSPITAL|EHR|LAB|...|<msg_type>|MSG00123|P|2.5
    PID|1||PATID12345||Doe^John^A||19800101|M|...
    ORC|RE|123|...
    OBR|1|...|^^^Glucose|...
    OBX|1|NM|GLU^Glucose^L||120|mg/dL|...

This component takes a column of raw HL7 messages and emits ONE ROW PER
SEGMENT-OF-INTEREST. The default behavior extracts the most useful fields
from MSH (message header), PID (patient), and OBX (observation) segments.

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

from typing import Any, Dict, List, Literal, Optional

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


_SEGMENT_PARSERS = {
    "MSH": lambda fields, _comp: _parse_msh(fields),
    "PID": _parse_pid,
    "OBX": _parse_obx,
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

    message_column: str = Field(
        default="message",
        description="Column holding the raw HL7 v2 message (\\r-delimited segments).",
    )

    keep_segments: List[
        Literal["MSH", "PID", "OBX", "ORC", "OBR", "PV1", "EVN", "DG1", "AL1"]
    ] = Field(
        default=["MSH", "PID", "OBX"],
        description=(
            "Segments to emit. MSH/PID/OBX have full extractors; others are "
            "currently skipped (you can extend the component for more)."
        ),
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        message_column = self.message_column
        keep_segments: List[str] = [str(s) for s in self.keep_segments]

        @asset(
            name=asset_name,
            description=self.description or f"Parse HL7 v2 → DataFrame (segments={keep_segments}).",
            group_name=self.group_name,
            kinds={"hl7", "healthcare", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
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
