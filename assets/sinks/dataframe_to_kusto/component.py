"""DataFrame → Azure Data Explorer (Kusto).

Push a DataFrame to a Kusto table on an Azure Data Explorer cluster.
Uses queued ingestion by default (durable, retried by ADX) — set
`use_streaming_ingestion: true` for low-latency direct streaming
(requires the cluster to have streaming ingestion enabled).

Companion to `dataframe_from_kusto` — together they cover ADX
read/write workloads.
"""

import os
import tempfile
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


class DataframeToKustoComponent(Component, Model, Resolvable):
    """Push a DataFrame to a Kusto table via queued or streaming ingestion."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Asset key of upstream DataFrame")

    cluster_url: str = Field(description="ADX cluster URL")
    database: str = Field(description="ADX database name")
    table: str = Field(description="Target Kusto table (must already exist with matching schema)")

    use_streaming_ingestion: bool = Field(
        default=False,
        description=(
            "If true, use streaming ingestion (low latency). Cluster must have "
            "streaming ingestion enabled. Otherwise uses queued ingestion (durable, "
            "default for batch workloads)."
        ),
    )
    if_exists: str = Field(
        default="append",
        description="'append' (default) | 'replace' (drops table contents first via .clear)",
    )

    tenant_id_env_var: Optional[str] = Field(default=None)
    client_id_env_var: Optional[str] = Field(default=None)
    client_secret_env_var: Optional[str] = Field(default=None)

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

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

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

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

        cfg = self
        kinds = self.kinds or ["azure", "kusto", "data_explorer"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Ingest DataFrame into Kusto table '{cfg.database}.{cfg.table}'",
            owners=self.owners or [],
            tags=tags,
            deps=[AssetKey.from_user_string(d) for d in (self.deps or [])],
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def kusto_writer(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
            try:
                from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
                from azure.kusto.ingest import (
                    QueuedIngestClient,
                    ManagedStreamingIngestClient,
                    IngestionProperties,
                    DataFormat,
                )
            except ImportError as e:
                raise ImportError(
                    "azure-kusto-data + azure-kusto-ingest required: "
                    "pip install azure-kusto-data azure-kusto-ingest"
                ) from e

            tenant = os.environ.get(cfg.tenant_id_env_var) if cfg.tenant_id_env_var else None
            client_id = os.environ.get(cfg.client_id_env_var) if cfg.client_id_env_var else None
            client_secret = os.environ.get(cfg.client_secret_env_var) if cfg.client_secret_env_var else None

            if tenant and client_id and client_secret:
                kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                    cfg.cluster_url, client_id, client_secret, tenant
                )
            else:
                kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cfg.cluster_url)

            # Optionally clear the table first
            if cfg.if_exists == "replace":
                control = KustoClient(kcsb)
                control.execute(cfg.database, f".clear table {cfg.table} data")
                context.log.info(f"Cleared existing data in {cfg.database}.{cfg.table}")

            # Streaming or queued ingestion
            if cfg.use_streaming_ingestion:
                ingest_client = ManagedStreamingIngestClient.from_engine_kcsb(kcsb)
            else:
                # Queued ingestion uses the data-management endpoint (https://ingest-<cluster>...)
                ingest_kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                    cfg.cluster_url.replace("https://", "https://ingest-"),
                    client_id, client_secret, tenant,
                ) if (tenant and client_id and client_secret) else (
                    KustoConnectionStringBuilder.with_az_cli_authentication(
                        cfg.cluster_url.replace("https://", "https://ingest-")
                    )
                )
                ingest_client = QueuedIngestClient(ingest_kcsb)

            ingestion_props = IngestionProperties(
                database=cfg.database,
                table=cfg.table,
                data_format=DataFormat.CSV,
            )

            # Write DataFrame to a temp CSV (Kusto SDK ingests files)
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                upstream.to_csv(tmp.name, index=False, header=False)
                tmp_path = tmp.name

            try:
                ingest_client.ingest_from_file(tmp_path, ingestion_properties=ingestion_props)
                context.log.info(
                    f"Ingested {len(upstream)} rows into {cfg.database}.{cfg.table} "
                    f"({'streaming' if cfg.use_streaming_ingestion else 'queued'})"
                )
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

            return MaterializeResult(
                metadata={
                    "rows_ingested": MetadataValue.int(len(upstream)),
                    "cluster": MetadataValue.text(cfg.cluster_url),
                    "table": MetadataValue.text(f"{cfg.database}.{cfg.table}"),
                    "ingestion_mode": MetadataValue.text(
                        "streaming" if cfg.use_streaming_ingestion else "queued"
                    ),
                    "if_exists": MetadataValue.text(cfg.if_exists),
                }
            )

        return Definitions(assets=[kusto_writer])
