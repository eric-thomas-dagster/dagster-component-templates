"""AcordXmlParserComponent — parse ACORD insurance XML messages.

ACORD (Association for Cooperative Operations Research and Development) is
the global insurance industry's data-exchange standard. Carriers, agencies,
brokers, and rating engines all speak ACORD XML for policies, claims,
quotes, applications, certificates, and binders.

Common ACORD message types:
  - **PolicyResponseRq / PolicyResponseRs** — policy-status round-trip
  - **InsurancePolicyAddRq** — new-business submission (P&C and Life)
  - **InsurancePolicyChangeRq** — endorsements / mid-term changes
  - **InsurancePolicyCancelRq** — cancellations
  - **ClaimsNotificationRq / ClaimsResponseRs** — FNOL + status
  - **InsurancePolicyQuoteInqRq** — rating quote requests
  - **CertificateOfInsuranceRq** — certificate of insurance generation
  - **AppraisalRq / AppraisalRs** — auto appraisal flows
  - **MotorVehicleReportRq / Rs** — MVR pull/return

This component flattens any ACORD XML envelope into one row per <Policy>,
<Claim>, <Application>, etc. inside. Carries the message-level metadata
(transaction reference, sender, language) into every row.

Auto-detects namespaces. Common ACORD root namespace:
  http://www.ACORD.org/standards/PC_Surety/ACORD1/xml/
"""
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

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


def _strip_ns(tag: str) -> str:
    """Drop the XML namespace prefix from a tag: '{ns}Policy' -> 'Policy'."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _at(elem: Optional[ET.Element], *path: str) -> Optional[str]:
    """Navigate a chain of child tag names (namespace-agnostic). Returns the
    text of the leaf, or None if any link is missing."""
    if elem is None:
        return None
    cur = elem
    for name in path:
        found = None
        for child in cur:
            if _strip_ns(child.tag) == name:
                found = child
                break
        if found is None:
            return None
        cur = found
    text = (cur.text or "").strip()
    return text or None


def _find(elem: ET.Element, name: str) -> Optional[ET.Element]:
    """Find first direct or descendant child with given (namespace-stripped) tag."""
    for child in elem.iter():
        if _strip_ns(child.tag) == name:
            return child
    return None


def _findall(elem: ET.Element, name: str) -> List[ET.Element]:
    """All descendants with the given tag."""
    return [child for child in elem.iter() if _strip_ns(child.tag) == name]


def _extract_policy(p: ET.Element) -> Dict[str, Any]:
    """Pull canonical fields from a Policy / PolicySummary element.

    ACORD nests *a lot* — there are MANY paths to the same data. The
    extraction here covers the high-traffic fields used by virtually
    every carrier system.
    """
    out: Dict[str, Any] = {
        "policy_number":      _at(p, "PolicyNumber"),
        "policy_status":      _at(p, "PolicyStatusCd"),
        "policy_status_text": _at(p, "PolicyStatusDesc"),
        "line_of_business":   _at(p, "LOBCd") or _at(p, "PolicyTypeCd"),
        "effective_date":     _at(p, "ContractTerm", "EffectiveDt") or _at(p, "EffectiveDt"),
        "expiration_date":    _at(p, "ContractTerm", "ExpirationDt") or _at(p, "ExpirationDt"),
        "premium_amount":     None,
        "premium_currency":   None,
    }
    # Premium can live at several paths
    prem = _find(p, "FullTermAmt") or _find(p, "PremiumAmount")
    if prem is not None:
        amt = _at(prem, "Amt") or (prem.text or "").strip()
        ccy = _at(prem, "@CurCd") or "USD"
        try:
            out["premium_amount"] = float(amt) if amt else None
        except (ValueError, TypeError):
            out["premium_amount"] = None
        out["premium_currency"] = ccy

    # First named insured (PartyInfo with RoleCd 'Insured' or 'NamedInsured')
    for party in _findall(p, "PartyInfo"):
        role = _at(party, "PartyRoleCd") or _at(party, "GeneralPartyInfo", "RoleCd")
        if role and role.lower() in ("insured", "namedinsured"):
            commercial = _find(party, "CommercialName")
            person = _find(party, "PersonName")
            if commercial is not None:
                out["insured_name"] = _at(commercial, "CommercialName") or _at(commercial, "Name")
                out["insured_type"] = "Commercial"
            elif person is not None:
                out["insured_name"] = " ".join(
                    [x for x in [_at(person, "GivenName"), _at(person, "OtherGivenName"), _at(person, "Surname")] if x]
                ) or None
                out["insured_type"] = "Person"
            break

    return out


def _extract_claim(c: ET.Element) -> Dict[str, Any]:
    """Pull canonical fields from a Claim element."""
    out: Dict[str, Any] = {
        "claim_number":   _at(c, "ClaimsOccurrence", "ClaimsOccurrenceNumber") or _at(c, "ClaimNumber"),
        "loss_date":      _at(c, "LossDt") or _at(c, "ClaimsOccurrence", "LossDt"),
        "report_date":    _at(c, "ReportedDt"),
        "loss_cause":     _at(c, "LossCauseCd"),
        "loss_desc":      _at(c, "LossDesc"),
        "claim_status":   _at(c, "ClaimsStatusCd"),
        "policy_number":  _at(c, "PolicyNumber"),
    }
    # Loss amount
    amt = _find(c, "LossAmt") or _find(c, "EstimatedLossAmt")
    if amt is not None:
        a = _at(amt, "Amt") or (amt.text or "").strip()
        ccy = _at(amt, "@CurCd") or "USD"
        try:
            out["loss_amount"] = float(a) if a else None
        except (ValueError, TypeError):
            out["loss_amount"] = None
        out["loss_currency"] = ccy
    return out


def _extract_quote(q: ET.Element) -> Dict[str, Any]:
    return {
        "quote_number":   _at(q, "QuoteInfo", "QuoteNumber") or _at(q, "QuoteNumber"),
        "quote_date":     _at(q, "QuoteInfo", "DateQuoted"),
        "quote_expiration": _at(q, "QuoteInfo", "ExpirationDt"),
        "rating_engine":  _at(q, "RatingResult", "EngineName"),
    }


_TYPE_EXTRACTORS = {
    "Policy":         _extract_policy,
    "PolicySummary":  _extract_policy,
    "Claim":          _extract_claim,
    "Quote":          _extract_quote,
    "Application":    _extract_policy,   # similar shape
}


def _parse_message(raw: str) -> List[Dict[str, Any]]:
    """Parse one ACORD XML envelope. Return one dict per Policy/Claim/Quote inside.

    Always emits at least one row: if no inner objects are found, returns
    a single 'envelope' row with the message-level metadata only.
    """
    raw = raw.strip()
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        return [{"_error": f"xml parse: {e}", "raw_preview": raw[:80]}]

    # Detect the transaction type. ACORD root typically looks like
    #   <ACORD>
    #     <SignonRq>...</SignonRq>     ← skip
    #     <InsurancePolicyAddRq>       ← this is the real envelope
    #       <RqUID>TX00000001</RqUID>
    #       <Policy>...</Policy>
    #     </InsurancePolicyAddRq>
    #   </ACORD>
    msg_type = None
    envelope_elem = None
    for child in root:
        tag = _strip_ns(child.tag)
        if tag == "SignonRq":
            continue
        msg_type = tag
        envelope_elem = child
        break
    if msg_type is None:
        # Fallback: maybe root IS the envelope itself
        msg_type = _strip_ns(root.tag)
        envelope_elem = root

    envelope = {
        "message_type":   msg_type,
        "transaction_id": (_at(envelope_elem, "RqUID")
                           or _at(envelope_elem, "TransactionRequestDt")
                           or _at(root, "RqUID")),
        "sender_id":      _at(root, "SignonRq", "ClientApp", "Name"),
    }

    rows: List[Dict[str, Any]] = []
    # Find every Policy / Claim / Quote / Application inside the message
    for type_tag, extractor in _TYPE_EXTRACTORS.items():
        for el in _findall(root, type_tag):
            row = {**envelope, "entity_type": type_tag, **extractor(el)}
            rows.append(row)

    if not rows:
        # No inner objects — emit a single envelope-only row so the caller
        # at least sees the message arrived.
        rows.append({**envelope, "entity_type": None})

    return rows


class AcordXmlParserComponent(Component, Model, Resolvable):
    """Parse a column of ACORD insurance XML messages into one row per inner Policy / Claim / Quote."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    xml_column: str = Field(
        default="xml",
        description="Column holding the ACORD XML message text (or bytes — decoded as utf-8).",
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
        xml_column = self.xml_column

        @asset(
            name=asset_name,
            description=self.description or "Parse ACORD XML → one row per Policy/Claim/Quote.",
            group_name=self.group_name,
            kinds={"acord", "insurance", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            if xml_column not in upstream.columns:
                raise ValueError(
                    f"xml_column={xml_column!r} not in upstream: {list(upstream.columns)}"
                )

            all_rows: List[Dict[str, Any]] = []
            type_counts: Dict[str, int] = {}
            err_count = 0
            for _, src in upstream.iterrows():
                raw = src[xml_column]
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", errors="replace")
                if not isinstance(raw, str) or not raw.strip():
                    err_count += 1
                    continue
                parsed = _parse_message(raw)
                for r in parsed:
                    if "_error" in r:
                        err_count += 1
                    typ = r.get("message_type") or r.get("entity_type") or "?"
                    type_counts[typ] = type_counts.get(typ, 0) + 1
                    # Carry over non-XML upstream columns
                    for c in upstream.columns:
                        if c != xml_column and c not in r:
                            r[c] = src[c]
                    all_rows.append(r)

            df = pd.DataFrame(all_rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no rows)"
            return Output(
                value=df,
                metadata={
                    "input_messages":  MetadataValue.int(len(upstream)),
                    "output_rows":     MetadataValue.int(len(df)),
                    "by_type":         MetadataValue.json(type_counts),
                    "errors":          MetadataValue.int(err_count),
                    "preview":         MetadataValue.md(preview),
                },
            )

        return Definitions(assets=[_asset])
