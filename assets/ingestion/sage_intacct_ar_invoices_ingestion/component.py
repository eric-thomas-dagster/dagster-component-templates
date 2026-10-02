"""Sage Intacct AR Invoices Ingestion Component.

Fetches AR Invoice records (the Intacct `ARINVOICE` object) via the legacy
XML Web Services `readByQuery` function and materializes the result as a
pandas DataFrame. Uses the ``sage_intacct_resource`` component for
authentication against Intacct's XML gateway (`xmlgw.phtml`) -- NOT a JSON
REST API.

`readByQuery` returns one page per call; this component walks Intacct's
`readMore`/`resultId` pagination until `numremaining == 0` or `limit` is
reached, whichever comes first. Intacct query clauses use `MM/DD/YYYY` date
literals, so ISO-8601 `from_date`/`to_date` config values are converted
before being embedded in the `query` where-clause string.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
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


def _to_intacct_date(iso_date: str) -> str:
    """Convert an ISO-8601 date (or datetime) string to Intacct's MM/DD/YYYY
    query-literal format. Accepts 'YYYY-MM-DD' or a full ISO datetime."""
    s = iso_date.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%m/%d/%Y")
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%m/%d/%Y")
    except ValueError:
        raise ValueError(f"Could not parse date {iso_date!r}; expected ISO-8601 (e.g. '2026-06-01').")


def _build_query_clause(date_field: str, from_date: Optional[str], to_date: Optional[str], extra_query: Optional[str]) -> Optional[str]:
    clauses: List[str] = []
    if from_date:
        clauses.append(f"{date_field} >= '{_to_intacct_date(from_date)}'")
    if to_date:
        clauses.append(f"{date_field} <= '{_to_intacct_date(to_date)}'")
    if extra_query:
        clauses.append(f"({extra_query})")
    if not clauses:
        return None
    return " AND ".join(clauses)


DEFAULT_ARINVOICE_FIELDS = [
    "RECORDNO",
    "INVOICENO",
    "CUSTOMERID",
    "CUSTOMERNAME",
    "WHENCREATED",
    "WHENDUE",
    "WHENPOSTED",
    "TOTALENTERED",
    "TOTALDUE",
    "CURRENCY",
    "STATE",
    "DESCRIPTION",
]


class SageIntacctArInvoicesIngestionComponent(Component, Model, Resolvable):
    """Ingest Sage Intacct AR Invoices (the `ARINVOICE` object) as a pandas DataFrame.

    Example:

        ```yaml
        type: dagster_component_templates.SageIntacctArInvoicesIngestionComponent
        attributes:
          asset_name: sage_intacct_ar_invoices
          resource_name: sage_intacct_resource
          from_date: "2026-06-01"
          to_date: "2026-07-01"
          limit: 5000
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
        default="sage_intacct_resource",
        description="Key of the SageIntacctResource this asset depends on for authentication",
    )

    object_name: str = Field(
        default="ARINVOICE",
        description="Intacct object to query. Defaults to ARINVOICE (AR Invoices).",
    )

    fields: Optional[List[str]] = Field(
        default=None,
        description="Fields to select from the object. Defaults to a standard ARINVOICE field set if unset.",
    )

    date_field: str = Field(
        default="WHENCREATED",
        description="Intacct date field used for from_date/to_date range filtering (e.g. 'WHENCREATED', 'WHENPOSTED', 'WHENDUE').",
    )

    from_date: Optional[str] = Field(
        default=None,
        description="Start of the date window (ISO-8601 date, e.g. '2026-06-01'). Converted to Intacct's MM/DD/YYYY format. Required unless a time-based partition_type is set.",
    )

    to_date: Optional[str] = Field(
        default=None,
        description="End of the date window (ISO-8601 date, e.g. '2026-07-01'). Converted to Intacct's MM/DD/YYYY format. Required unless a time-based partition_type is set.",
    )

    extra_query: Optional[str] = Field(
        default=None,
        description="Additional Intacct query where-clause fragment, ANDed with the date range (e.g. \"STATE = 'Open'\").",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'multi' / 'dynamic' / None for unpartitioned. When time-based, from_date/to_date are derived from the partition window instead of the static config.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    pagesize: int = Field(
        default=100,
        ge=1,
        le=2000,
        description="Records per readByQuery/readMore page (Intacct max is 2000)",
    )

    limit: int = Field(
        default=1000,
        description="Maximum number of records to fetch across all pages",
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="finance",
        description="Asset group for organization",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:finance', 'user@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['sage_intacct', 'python'].",
    )

    include_preview_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata",
    )

    preview_rows: int = Field(
        default=10,
        ge=1,
        le=200,
        description="Rows to include in the preview metadata",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime)",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        resource_name = self.resource_name
        object_name = self.object_name
        select_fields = list(self.fields or DEFAULT_ARINVOICE_FIELDS)
        date_field = self.date_field
        from_date = self.from_date
        to_date = self.to_date
        extra_query = self.extra_query
        pagesize = self.pagesize
        limit = self.limit
        description = self.description or (
            f"Sage Intacct {object_name} between {from_date} and {to_date}"
            if from_date and to_date
            else f"Sage Intacct {object_name} for the materialized partition"
        )
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )

        _kinds = list(self.kinds or ["sage_intacct", "python"])
        _all_tags = dict(self.asset_tags or {})
        for _k in _kinds:
            _all_tags[f"dagster/kind/{_k}"] = ""

        owners = self.owners or []

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=owners,
            tags=_all_tags,
            group_name=group_name,
            required_resource_keys={resource_name},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
        )
        def sage_intacct_ar_invoices_ingestion_asset(context: AssetExecutionContext):
            _from_date, _to_date = from_date, to_date
            if context.has_partition_key:
                # A time-based partition means this run should fetch exactly that
                # slice, not the static from_date/to_date config.
                try:
                    _window = context.partition_time_window
                    _from_date = _window.start.strftime("%Y-%m-%d")
                    _to_date = _window.end.strftime("%Y-%m-%d")
                except Exception:
                    pass  # static/dynamic/multi partition -- no natural date window
            if not _from_date and not _to_date and not extra_query:
                raise ValueError(
                    "Set from_date/to_date (or extra_query), or a time-based partition_type, to define the query window."
                )

            client = getattr(context.resources, resource_name)
            query_clause = _build_query_clause(date_field, _from_date, _to_date, extra_query)
            context.log.info(
                f"Fetching Sage Intacct {object_name}: query={query_clause!r}, limit={limit}"
            )

            # --- Walk readByQuery -> readMore until numremaining == 0 or limit reached ---
            records: List[Dict[str, Any]] = []
            page = client.read_by_query(
                object_name=object_name,
                fields=select_fields,
                query=query_clause,
                pagesize=min(pagesize, limit) if limit else pagesize,
            )
            records.extend(page["records"])
            numremaining = page["numremaining"]
            resultid = page["resultid"]

            while numremaining > 0 and len(records) < limit and resultid:
                page = client.read_more(resultid)
                records.extend(page["records"])
                numremaining = page["numremaining"]
                resultid = page["resultid"]
                if not page["records"]:
                    break  # defensive: avoid infinite loop on a misbehaving resultid

            records = records[:limit]
            context.log.info(f"Fetched {len(records)} {object_name} records")

            if not records:
                empty_df = pd.DataFrame()
                return Output(
                    value=empty_df,
                    metadata={
                        "row_count": MetadataValue.int(0),
                        "object_name": MetadataValue.text(object_name),
                        "query": MetadataValue.text(query_clause or ""),
                    },
                )

            df = pd.DataFrame(records)

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "object_name": MetadataValue.text(object_name),
                "query": MetadataValue.text(query_clause or ""),
                "from_date": MetadataValue.text(_from_date or ""),
                "to_date": MetadataValue.text(_to_date or ""),
            }
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[sage_intacct_ar_invoices_ingestion_asset])
