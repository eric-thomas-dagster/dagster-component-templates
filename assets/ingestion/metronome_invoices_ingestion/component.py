"""Metronome Invoices Ingestion Component.

Pull-side sibling of `metronome_usage_event_send`: fetches customers and
their invoices from Metronome's billing API for finance reporting, and
materializes the result as a pandas DataFrame (one row per invoice).

Verified against docs.metronome.com (2026-10):
  - `GET /v1/customers` -- paginated list of customers. Response shape
    `{"data": [...], "next_page": "<cursor-or-null>"}`; paginate by
    following `next_page` until it's null/absent.
  - `GET /v1/customers/{customer_id}/invoices` -- paginated list of a
    single customer's invoices (same `{"data": [...], "next_page": ...}`
    envelope). Supports `status`, `starting_on`, `ending_before` filters.
  - Invoice objects carry header-level fields (id, status, total, subtotal,
    issued_at, start_timestamp, end_timestamp, contract_id, customer_id)
    plus a `line_items` array of per-line detail (name, type, quantity,
    unit_price, total, product_id, ...).

Uses the ``metronome_resource`` component for authentication (static
Bearer API key).
"""

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


def _list_customers_page(client, base_url: str, limit: int, next_page: Optional[str]) -> Dict[str, Any]:
    """Isolates the real external-API boundary for `GET /customers` so it
    can be monkeypatched wholesale in tests. One page per call -- the
    component's own loop follows `next_page` until exhausted."""
    params: Dict[str, Any] = {"limit": limit}
    if next_page:
        params["next_page"] = next_page
    resp = client.get(f"{base_url}/customers", params=params)
    resp.raise_for_status()
    return resp.json()


def _list_invoices_page(
    client,
    base_url: str,
    customer_id: str,
    limit: int,
    next_page: Optional[str],
    status: Optional[str],
    starting_on: Optional[str],
    ending_before: Optional[str],
) -> Dict[str, Any]:
    """Isolates the real external-API boundary for
    `GET /customers/{customer_id}/invoices` so it can be monkeypatched
    wholesale in tests. One page per call -- the component's own loop
    follows `next_page` until exhausted."""
    params: Dict[str, Any] = {"limit": limit}
    if next_page:
        params["next_page"] = next_page
    if status:
        params["status"] = status
    if starting_on:
        params["starting_on"] = starting_on
    if ending_before:
        params["ending_before"] = ending_before
    resp = client.get(f"{base_url}/customers/{customer_id}/invoices", params=params)
    resp.raise_for_status()
    return resp.json()



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


class MetronomeInvoicesIngestionComponent(Component, Model, Resolvable):
    """Ingest Metronome customers + invoices as a pandas DataFrame (one row
    per invoice) for finance reporting.

    Example:

        ```yaml
        type: dagster_community_components.MetronomeInvoicesIngestionComponent
        attributes:
          asset_name: metronome_invoices
          resource_key: metronome_resource
          invoice_status: FINALIZED
          starting_on: "2026-01-01T00:00:00.000Z"
          ending_before: "2026-07-01T00:00:00.000Z"
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_key: str = Field(
        default="metronome_resource",
        description="Key of the MetronomeResource this asset depends on for authentication",
    )

    customer_ids: Optional[List[str]] = Field(
        default=None,
        description=(
            "Specific Metronome customer IDs to fetch invoices for. If unset, "
            "all customers are listed via GET /customers and invoices are "
            "fetched for each."
        ),
    )

    invoice_status: Optional[str] = Field(
        default=None,
        description="Filter invoices by status (e.g. 'DRAFT', 'FINALIZED', 'VOID'). Unset fetches all statuses.",
    )
    starting_on: Optional[str] = Field(
        default=None,
        description="Only include invoices starting on/after this ISO-8601 timestamp. On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted.",
    )
    ending_before: Optional[str] = Field(
        default=None,
        description="Only include invoices ending before this ISO-8601 timestamp. On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted.",
    )

    customers_page_limit: int = Field(
        default=100,
        description="Page size for the GET /customers listing (only used when customer_ids is unset).",
    )
    invoices_page_limit: int = Field(
        default=100,
        description="Page size for the GET /customers/{id}/invoices listing, per customer.",
    )
    max_customers: Optional[int] = Field(
        default=None,
        description=(
            "Safety cap on the number of distinct customers to fetch invoices "
            "for in a single run (only applies when customer_ids is unset and "
            "the account has many customers)."
        ),
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="metronome",
        description="Asset group for organization",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners -- list of team names or email addresses, e.g. ['team:finance', 'user@company.com']",
    )

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['metronome', 'python'].",
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        asset_name = self.asset_name
        resource_key = self.resource_key
        customer_ids = self.customer_ids
        invoice_status = self.invoice_status
        starting_on = self.starting_on
        ending_before = self.ending_before
        customers_page_limit = self.customers_page_limit
        invoices_page_limit = self.invoices_page_limit
        max_customers = self.max_customers
        description = self.description or "Metronome customer invoices for finance reporting"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        _kinds = list(self.kinds or ["metronome", "python"])
        _all_tags = dict(self.asset_tags or {})
        for _k in _kinds:
            _all_tags[f"dagster/kind/{_k}"] = ""

        owners = self.owners or []

        @asset(partitions_def=partitions_def, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=owners,
            tags=_all_tags,
            group_name=group_name,
            required_resource_keys={resource_key},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def metronome_invoices_ingestion_asset(context: AssetExecutionContext):
            resource = getattr(context.resources, resource_key)
            client = resource.get_client()
            base_url = getattr(resource, "base_url", None) or "https://api.metronome.com/v1"

            # --- 1. Resolve the set of customer IDs to pull invoices for ----
            if customer_ids:
                resolved_customer_ids = list(customer_ids)
                context.log.info(f"Using {len(resolved_customer_ids)} explicitly configured customer_ids")
            else:
                resolved_customer_ids = []
                next_page = None
                while True:
                    page = _list_customers_page(client, base_url, customers_page_limit, next_page)
                    for c in page.get("data", []):
                        cid = c.get("id")
                        if cid:
                            resolved_customer_ids.append(cid)
                    next_page = page.get("next_page")
                    if not next_page:
                        break
                    if max_customers and len(resolved_customer_ids) >= max_customers:
                        break
                if max_customers:
                    resolved_customer_ids = resolved_customer_ids[:max_customers]
                context.log.info(f"Listed {len(resolved_customer_ids)} customers from Metronome")

            # --- 2. Fetch invoices per customer, paginated -------------------
            invoices: List[Dict[str, Any]] = []
            for cid in resolved_customer_ids:
                next_page = None
                while True:
                    page = _list_invoices_page(
                        client, base_url, cid, invoices_page_limit, next_page,
                        invoice_status, _substitute_partition(starting_on, _partition_vars(context)), _substitute_partition(ending_before, _partition_vars(context)),
                    )
                    for inv in page.get("data", []):
                        inv = dict(inv)
                        inv.setdefault("customer_id", cid)
                        invoices.append(inv)
                    next_page = page.get("next_page")
                    if not next_page:
                        break

            context.log.info(f"Fetched {len(invoices)} invoices across {len(resolved_customer_ids)} customers")

            if not invoices:
                empty_df = pd.DataFrame()
                return Output(
                    value=empty_df,
                    metadata={
                        "row_count": MetadataValue.int(0),
                        "customer_count": MetadataValue.int(len(resolved_customer_ids)),
                    },
                )

            df = pd.DataFrame(invoices)

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "customer_count": MetadataValue.int(len(resolved_customer_ids)),
            }
            if invoice_status:
                metadata["invoice_status_filter"] = MetadataValue.text(invoice_status)
            if "total" in df.columns:
                try:
                    metadata["total_amount_sum"] = MetadataValue.float(float(pd.to_numeric(df["total"], errors="coerce").sum()))
                except Exception:  # noqa: BLE001
                    pass
            if include_preview and len(df) > 0:
                _prev = df.head(preview_rows)
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[metronome_invoices_ingestion_asset])


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
# END scaffold partitions-def builder

# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper
