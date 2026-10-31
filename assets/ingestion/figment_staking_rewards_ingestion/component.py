"""Figment Staking Rewards Ingestion Component.

Pulls granular and/or aggregated staking rewards (and balances) per
validator/delegator from Figment's institutional staking Rewards API, for
any supported network, and materializes the result as a pandas DataFrame.

Figment's Rewards API is READ-ONLY reporting -- there is no corresponding
reverse-ETL/write-side component in this repo for Figment, since staking
operations (delegate, create validator, exit validator) are separate
deliberate actions, not a warehouse sync.

Verified against docs.figment.io (2026-10):
  - ETH Rewards: `POST https://api.figment.io/ethereum/rewards` -- returns
    gross rewards as earned by validators created via FigApp or the Create
    Validators endpoint. Granularity via `time_rollup` (`epoch` / `daily` /
    `all_time`); filter by date with `start`/`end` (both inclusive), by
    validator with `pubkeys`, or by withdrawal address with
    `withdrawal_addresses`. Rewards appear ~3 hours after on-chain
    distribution.
  - SOL Rewards: `POST https://api.figment.io/solana/rewards` -- returns
    rewards (net of validator commission) for any wallet staking with any
    validator. Required body field: `accounts` (stake-account or
    system-account addresses), max 50 accounts per request.
  - Both endpoints share the same `https://api.figment.io` host and the
    same `x-api-key` auth as every other Figment endpoint. Other networks
    Figment supports follow the same `POST /{network}/rewards` shape with
    network-specific identifier fields -- `extra_query_params` is the
    escape hatch for any such field this component doesn't model
    explicitly.

Uses the ``figment_resource`` component for authentication.
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


def _build_rewards_body(
    network: str,
    time_rollup: Optional[str],
    start: Optional[str],
    end: Optional[str],
    pubkeys: Optional[List[str]],
    withdrawal_addresses: Optional[List[str]],
    accounts: Optional[List[str]],
    extra_query_params: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Pure request-body builder (no network call) -- kept separate from
    `_fetch_rewards` so the per-network field mapping is exercised directly
    in tests without needing a fake HTTP client."""
    body: Dict[str, Any] = {}
    network_l = (network or "").lower()
    if network_l == "solana":
        if not accounts:
            raise ValueError(
                "FigmentStakingRewardsIngestionComponent: network='solana' requires "
                "a non-empty 'accounts' list (stake-account or system-account addresses)."
            )
        if len(accounts) > 50:
            raise ValueError(
                f"FigmentStakingRewardsIngestionComponent: network='solana' accepts at "
                f"most 50 accounts per request (got {len(accounts)}). Split into multiple "
                f"component instances/batches."
            )
        body["accounts"] = list(accounts)
    else:
        # ethereum, and best-effort for any other Figment-supported network.
        if time_rollup:
            body["time_rollup"] = time_rollup
        if start:
            body["start"] = start
        if end:
            body["end"] = end
        if pubkeys:
            body["pubkeys"] = list(pubkeys)
        if withdrawal_addresses:
            body["withdrawal_addresses"] = list(withdrawal_addresses)
        if accounts:
            body["accounts"] = list(accounts)
    if extra_query_params:
        body.update(extra_query_params)
    return body


def _flatten_rewards_payload(payload: Any, network: str) -> List[Dict[str, Any]]:
    """Normalize a Figment rewards response into a flat list of row dicts.
    The exact response envelope isn't uniformly documented across every
    network, so this is deliberately defensive: a bare list is used as-is;
    a dict is searched for the first list-valued key among the common
    envelope names; otherwise the whole dict becomes a single aggregate
    row (e.g. a `time_rollup=all_time` summary)."""
    if isinstance(payload, list):
        rows = list(payload)
    elif isinstance(payload, dict):
        rows = None
        for key in ("data", "rewards", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                rows = value
                break
        if rows is None:
            rows = [payload]
    else:
        rows = []

    out = []
    for r in rows:
        if isinstance(r, dict):
            r = dict(r)
            r.setdefault("network", network)
            out.append(r)
    return out


def _fetch_rewards(client, base_url: str, network: str, body: Dict[str, Any]) -> Any:
    """Isolates the one real external-API boundary (`POST
    {base_url}/{network}/rewards`) so it can be monkeypatched wholesale in
    tests without the real `requests` network call ever firing."""
    resp = client.post(f"{base_url}/{network}/rewards", json=body)
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


class FigmentStakingRewardsIngestionComponent(Component, Model, Resolvable):
    """Ingest staking rewards/balances from Figment's Rewards API as a
    pandas DataFrame, for any supported network.

    Example:

        ```yaml
        type: dagster_community_components.FigmentStakingRewardsIngestionComponent
        attributes:
          asset_name: figment_eth_staking_rewards
          resource_key: figment_resource
          network: ethereum
          time_rollup: daily
          start: "2026-06-01T00:00:00Z"
          end: "2026-07-01T00:00:00Z"
          include_aggregate_summary: true
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_key: str = Field(
        default="figment_resource",
        description="Key of the FigmentResource this asset depends on for authentication",
    )

    network: str = Field(
        description=(
            "Figment network slug for the rewards endpoint, e.g. 'ethereum' or "
            "'solana'. Verified field mapping is provided for both; other "
            "Figment-supported networks are passed through best-effort via "
            "extra_query_params."
        ),
    )

    time_rollup: Optional[str] = Field(
        default="epoch",
        description=(
            "Granularity for network='ethereum' (and passed through for others): "
            "'epoch' (most granular), 'daily', or 'all_time' (aggregated). Not "
            "used for network='solana'."
        ),
    )
    start: Optional[str] = Field(
        default=None, description="Inclusive start of the reward window (ISO-8601). ethereum-only filter. On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted."
    )
    end: Optional[str] = Field(
        default=None, description="Inclusive end of the reward window (ISO-8601). ethereum-only filter. On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted."
    )
    pubkeys: Optional[List[str]] = Field(
        default=None, description="Filter to specific validator public keys. ethereum-only filter."
    )
    withdrawal_addresses: Optional[List[str]] = Field(
        default=None, description="Filter to validators by withdrawal address. ethereum-only filter."
    )
    accounts: Optional[List[str]] = Field(
        default=None,
        description=(
            "Stake-account or system-account addresses. REQUIRED for "
            "network='solana' (max 50 per request)."
        ),
    )
    extra_query_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Escape hatch: additional fields merged into the request body, for networks/params not modeled explicitly above.",
    )

    include_aggregate_summary: bool = Field(
        default=False,
        description=(
            "When true (network='ethereum' only), issues a second request with "
            "time_rollup='all_time' and attaches the aggregate summary as asset "
            "metadata (not merged into the main granular DataFrame, to avoid a "
            "schema mismatch between per-epoch rows and an account-level total)."
        ),
    )
    max_pages: int = Field(
        default=20,
        description="Safety cap on pagination pages followed via a 'next_page' cursor, if the response includes one.",
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="figment",
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
        description="Asset kinds for the Dagster catalog. Defaults to ['figment', 'python'].",
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
        network = self.network
        time_rollup = self.time_rollup
        start = self.start
        end = self.end
        pubkeys = self.pubkeys
        withdrawal_addresses = self.withdrawal_addresses
        accounts = self.accounts
        extra_query_params = self.extra_query_params
        include_aggregate_summary = self.include_aggregate_summary
        max_pages = self.max_pages
        description = self.description or f"Figment staking rewards for network={network}"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        if not network:
            raise ValueError("FigmentStakingRewardsIngestionComponent: network must be non-empty.")

        # Validate eagerly (config-time) rather than only at materialization
        # time -- a 'solana' network with no/too-many accounts is a config
        # mistake, not a runtime data problem.
        _build_rewards_body(
            network, time_rollup, start, end, pubkeys, withdrawal_addresses, accounts, extra_query_params
        )

        _kinds = list(self.kinds or ["figment", "python"])
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
        def figment_staking_rewards_ingestion_asset(context: AssetExecutionContext):
            resource = getattr(context.resources, resource_key)
            client = resource.get_client()
            base_url = getattr(resource, "base_url", None) or "https://api.figment.io"

            body = _build_rewards_body(
                network, time_rollup, _substitute_partition(start, _partition_vars(context)), _substitute_partition(end, _partition_vars(context)), pubkeys, withdrawal_addresses, accounts, extra_query_params
            )

            all_rows: List[Dict[str, Any]] = []
            next_page = None
            pages_fetched = 0
            while True:
                req_body = dict(body)
                if next_page:
                    req_body["page"] = next_page
                payload = _fetch_rewards(client, base_url, network, req_body)
                all_rows.extend(_flatten_rewards_payload(payload, network))
                pages_fetched += 1
                next_page = payload.get("next_page") if isinstance(payload, dict) else None
                if not next_page or pages_fetched >= max_pages:
                    break

            context.log.info(
                f"Fetched {len(all_rows)} reward record(s) for network={network!r} "
                f"across {pages_fetched} page(s)"
            )

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(all_rows)),
                "network": MetadataValue.text(network),
                "pages_fetched": MetadataValue.int(pages_fetched),
            }

            if include_aggregate_summary:
                if network.lower() == "ethereum":
                    agg_body = dict(body)
                    agg_body["time_rollup"] = "all_time"
                    agg_payload = _fetch_rewards(client, base_url, network, agg_body)
                    metadata["aggregate_summary"] = MetadataValue.json(agg_payload)
                else:
                    context.log.warning(
                        f"include_aggregate_summary (time_rollup=all_time) is ethereum-specific; "
                        f"skipped for network={network!r}."
                    )

            if not all_rows:
                return Output(value=pd.DataFrame(), metadata=metadata)

            df = pd.DataFrame(all_rows)
            if include_preview and len(df) > 0:
                metadata["preview"] = MetadataValue.md(df.head(preview_rows).to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[figment_staking_rewards_ingestion_asset])


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
