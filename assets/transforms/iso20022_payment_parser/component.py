"""Iso20022PaymentParserComponent — parse ISO 20022 payment XML messages to a DataFrame.

ISO 20022 is the global payments / financial messaging standard. It's
replacing legacy SWIFT MT, and the SEPA + Fedwire + CHIPS networks have
all adopted it. Common message families:

  - **pacs.008** — FI-to-FI Customer Credit Transfer (the heavy lifter for cross-border)
  - **pacs.002** — Payment Status Report (was the credit transfer accepted/rejected/pending?)
  - **pacs.004** — Payment Return
  - **pain.001** — Customer Credit Transfer Initiation (corporate → bank)
  - **pain.002** — Payment Status Report to corporate
  - **camt.054** — Bank-to-Customer Debit/Credit Notification
  - **camt.053** — Bank-to-Customer Statement

This component takes a column of ISO 20022 XML messages and emits one row
per payment transaction inside the message. Supports the most common
message families out of the box and falls back to a generic extractor for
others. Strips namespace prefixes so the same XPath works across versions.

Common use:
  - Treasury ingest: nightly pacs.008 dumps → BQ for reconciliation
  - Statement parsing: camt.053 → ledger entries DataFrame
  - Payment status tracking: pacs.002 → success/failure metrics
"""

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
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _strip_ns(elem):
    """Walk the tree once, stripping `{namespace}tag` → `tag` everywhere."""
    for e in elem.iter():
        if isinstance(e.tag, str) and e.tag.startswith("{"):
            e.tag = e.tag.split("}", 1)[1]


def _text(elem, path: str) -> Optional[str]:
    """`./Foo/Bar/Baz` → text, or None if any segment missing."""
    if elem is None:
        return None
    node = elem.find(path)
    if node is None or node.text is None:
        return None
    return node.text.strip() or None


def _amount(elem, path: str) -> Dict[str, Any]:
    """Extract <Amount Ccy="USD">100.00</Amount> shape."""
    if elem is None:
        return {"value": None, "currency": None}
    node = elem.find(path)
    if node is None:
        return {"value": None, "currency": None}
    try:
        v = float(node.text) if node.text else None
    except (ValueError, TypeError):
        v = None
    return {"value": v, "currency": node.attrib.get("Ccy")}


def _parse_pacs008(root) -> List[Dict[str, Any]]:
    """FI-to-FI Customer Credit Transfer — one row per CdtTrfTxInf."""
    rows = []
    grp = root.find(".//GrpHdr")
    msg_id = _text(grp, "MsgId")
    created = _text(grp, "CreDtTm")
    for txn in root.findall(".//CdtTrfTxInf"):
        amt = _amount(txn, "IntrBkSttlmAmt")
        rows.append({
            "message_type":   "pacs.008",
            "msg_id":         msg_id,
            "created_dt":     created,
            "txn_id":         _text(txn, "PmtId/InstrId") or _text(txn, "PmtId/EndToEndId"),
            "end_to_end_id":  _text(txn, "PmtId/EndToEndId"),
            "amount":         amt["value"],
            "currency":       amt["currency"],
            "debtor_name":    _text(txn, "Dbtr/Nm"),
            "debtor_account": _text(txn, "DbtrAcct/Id/IBAN") or _text(txn, "DbtrAcct/Id/Othr/Id"),
            "debtor_bic":     _text(txn, "DbtrAgt/FinInstnId/BICFI"),
            "creditor_name":  _text(txn, "Cdtr/Nm"),
            "creditor_account": _text(txn, "CdtrAcct/Id/IBAN") or _text(txn, "CdtrAcct/Id/Othr/Id"),
            "creditor_bic":   _text(txn, "CdtrAgt/FinInstnId/BICFI"),
            "remittance_info": _text(txn, "RmtInf/Ustrd"),
        })
    return rows


def _parse_pain001(root) -> List[Dict[str, Any]]:
    """Customer Credit Transfer Initiation — one row per CdtTrfTxInf."""
    rows = []
    grp = root.find(".//GrpHdr")
    msg_id = _text(grp, "MsgId")
    initiating = _text(grp, "InitgPty/Nm")
    for pmt_info in root.findall(".//PmtInf"):
        debtor = _text(pmt_info, "Dbtr/Nm")
        debtor_acct = _text(pmt_info, "DbtrAcct/Id/IBAN")
        for txn in pmt_info.findall(".//CdtTrfTxInf"):
            amt = _amount(txn, "Amt/InstdAmt")
            rows.append({
                "message_type":     "pain.001",
                "msg_id":           msg_id,
                "initiating_party": initiating,
                "txn_id":           _text(txn, "PmtId/InstrId") or _text(txn, "PmtId/EndToEndId"),
                "end_to_end_id":    _text(txn, "PmtId/EndToEndId"),
                "amount":           amt["value"],
                "currency":         amt["currency"],
                "debtor_name":      debtor,
                "debtor_account":   debtor_acct,
                "creditor_name":    _text(txn, "Cdtr/Nm"),
                "creditor_account": _text(txn, "CdtrAcct/Id/IBAN"),
                "creditor_bic":     _text(txn, "CdtrAgt/FinInstnId/BICFI"),
                "remittance_info":  _text(txn, "RmtInf/Ustrd"),
            })
    return rows


def _parse_pacs002(root) -> List[Dict[str, Any]]:
    """Payment Status Report — one row per TxInfAndSts."""
    rows = []
    grp = root.find(".//GrpHdr")
    msg_id = _text(grp, "MsgId")
    for txn in root.findall(".//TxInfAndSts"):
        rows.append({
            "message_type":       "pacs.002",
            "msg_id":             msg_id,
            "txn_id":             _text(txn, "OrgnlInstrId") or _text(txn, "OrgnlEndToEndId"),
            "end_to_end_id":      _text(txn, "OrgnlEndToEndId"),
            "status":             _text(txn, "TxSts"),
            "reason_code":        _text(txn, "StsRsnInf/Rsn/Cd"),
            "reason_text":        _text(txn, "StsRsnInf/AddtlInf"),
        })
    return rows


def _parse_camt054(root) -> List[Dict[str, Any]]:
    """Bank-to-Customer Debit/Credit Notification — one row per Ntry."""
    rows = []
    for ntry in root.findall(".//Ntry"):
        amt = _amount(ntry, "Amt")
        rows.append({
            "message_type":      "camt.054",
            "credit_debit_ind":  _text(ntry, "CdtDbtInd"),
            "amount":            amt["value"],
            "currency":          amt["currency"],
            "booking_dt":        _text(ntry, "BookgDt/Dt") or _text(ntry, "BookgDt/DtTm"),
            "value_dt":          _text(ntry, "ValDt/Dt") or _text(ntry, "ValDt/DtTm"),
            "txn_ref":           _text(ntry, "AcctSvcrRef"),
            "remittance_info":   _text(ntry, "NtryDtls/TxDtls/RmtInf/Ustrd"),
        })
    return rows


def _parse_generic(root) -> List[Dict[str, Any]]:
    """Fallback — best-effort top-level extraction."""
    grp = root.find(".//GrpHdr")
    return [{
        "message_type":   "(unknown)",
        "msg_id":         _text(grp, "MsgId"),
        "created_dt":     _text(grp, "CreDtTm"),
        "nb_of_txs":      _text(grp, "NbOfTxs"),
        "ctrl_sum":       _text(grp, "CtrlSum"),
    }]


_PARSERS = {
    "pacs.008": _parse_pacs008,
    "pacs.002": _parse_pacs002,
    "pain.001": _parse_pain001,
    "camt.054": _parse_camt054,
}


_MSG_TYPE_RE = re.compile(r"(pacs|pain|camt|setr|auth)\.\d{3}(?:\.\d+\.\d+)?")



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


class Iso20022PaymentParserComponent(Component, Model, Resolvable):
    """Parse a column of ISO 20022 XML payment messages into a flat DataFrame."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: Optional[str] = Field(
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

    xml_column: Union[str, int] = Field(
        default="xml",
        description="Column holding the raw ISO 20022 XML message (string).",
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
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("Iso20022PaymentParserComponent: set exactly one of `upstream_asset_key` or `source`.")
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
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key) if self.upstream_asset_key else None
        xml_column = self.xml_column

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Parse ISO 20022 XML → flat transactions DataFrame.",
            group_name=self.group_name,
            kinds={"iso20022", "fintech", "pandas"},
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
            try:
                import xml.etree.ElementTree as ET
            except ImportError:
                raise ImportError("xml.etree is stdlib — should always be available.")

            if xml_column not in upstream.columns:
                raise ValueError(
                    f"xml_column={xml_column!r} not in upstream: {list(upstream.columns)}"
                )

            all_rows: List[Dict[str, Any]] = []
            type_counts: Dict[str, int] = {}
            errors = 0
            for _, src in upstream.iterrows():
                raw = src[xml_column]
                if not isinstance(raw, str) or not raw.strip():
                    errors += 1
                    continue
                try:
                    root = ET.fromstring(raw)
                    _strip_ns(root)
                except Exception as e:
                    errors += 1
                    all_rows.append({"_error": f"xml parse: {e}", "raw_preview": raw[:80]})
                    continue

                # Detect message type — first try the root tag, then a namespace declaration in raw
                msg_type = None
                # Root tag e.g. "Document"; child often hints the type via subelement tag like "FIToFICstmrCdtTrf"
                if root.tag != "Document":
                    msg_type_hint = root.tag.lower()
                else:
                    child = next(iter(root), None)
                    msg_type_hint = (child.tag if child is not None else "").lower()
                # Namespaces usually carry "pacs.008.001.08" — search the raw for the canonical id
                m = _MSG_TYPE_RE.search(raw)
                if m:
                    # Take only the short form (e.g. `pacs.008`), not the full versioned id
                    full_match = m.group(0)
                    parts = full_match.split(".")
                    msg_type = ".".join(parts[:2])
                else:
                    # Fall back to tag-shape detection
                    if "fitoficstmrcdttrf" in msg_type_hint:
                        msg_type = "pacs.008"
                    elif "customercreditxfrinitn" in msg_type_hint or "cstmrcdttrfinitn" in msg_type_hint:
                        msg_type = "pain.001"
                    elif "pmtsts" in msg_type_hint or "fitofipmtstsrpt" in msg_type_hint:
                        msg_type = "pacs.002"
                    elif "bktocstmrdbtcdtntfctn" in msg_type_hint or "bktocstmrntfctn" in msg_type_hint:
                        msg_type = "camt.054"

                parser = _PARSERS.get(msg_type or "", _parse_generic)
                try:
                    rows = parser(root)
                except Exception as e:
                    rows = [{"_error": str(e)}]

                for r in rows:
                    if "_error" in r:
                        errors += 1
                    seg_type = r.get("message_type") or "(unknown)"
                    type_counts[seg_type] = type_counts.get(seg_type, 0) + 1
                    # Carry over non-xml columns from the source row
                    for c in upstream.columns:
                        if c != xml_column and c not in r:
                            r[c] = src[c]
                    all_rows.append(r)

            df = pd.DataFrame(all_rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no rows)"
            return Output(
                value=df,
                metadata={
                    "input_messages": MetadataValue.int(len(upstream)),
                    "output_rows":    MetadataValue.int(len(df)),
                    "by_message_type": MetadataValue.json(type_counts),
                    "errors":         MetadataValue.int(errors),
                    "preview":        MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
