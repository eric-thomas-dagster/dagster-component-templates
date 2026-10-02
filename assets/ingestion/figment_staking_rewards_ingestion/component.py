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


class FigmentStakingRewardsIngestionComponent(Component, Model, Resolvable):
    """Ingest staking rewards/balances from Figment's Rewards API as a
    pandas DataFrame, for any supported network.

    Example:

        ```yaml
        type: dagster_component_templates.FigmentStakingRewardsIngestionComponent
        attributes:
          asset_name: figment_eth_staking_rewards
          resource_name: figment_resource
          network: ethereum
          time_rollup: daily
          start: "2026-06-01T00:00:00Z"
          end: "2026-07-01T00:00:00Z"
          include_aggregate_summary: true
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    resource_name: str = Field(
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
        default=None, description="Inclusive start of the reward window (ISO-8601). ethereum-only filter."
    )
    end: Optional[str] = Field(
        default=None, description="Inclusive end of the reward window (ISO-8601). ethereum-only filter."
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

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners -- list of team names or email addresses, e.g. ['team:finance', 'user@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        resource_name = self.resource_name
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

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=owners,
            tags=_all_tags,
            group_name=group_name,
            required_resource_keys={resource_name},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def figment_staking_rewards_ingestion_asset(context: AssetExecutionContext):
            resource = getattr(context.resources, resource_name)
            client = resource.get_client()
            base_url = getattr(resource, "base_url", None) or "https://api.figment.io"

            body = _build_rewards_body(
                network, time_rollup, start, end, pubkeys, withdrawal_addresses, accounts, extra_query_params
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
