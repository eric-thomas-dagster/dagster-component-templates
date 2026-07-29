"""Trino Query Asset Component.

Execute Trino SQL and materialize the result as a Dagster asset.
"""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import dagster as dg
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Canonical `_build_partitions_def` shared across the registry.

    Both flat-fields and multi-axis shapes are supported. Raises ValueError
    on misconfigured combinations rather than silently picking a default.
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
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
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


class TrinoQueryComponent(dg.Component, dg.Model, dg.Resolvable):
    """Component for executing Trino queries and materializing results.

    Runs a Trino SQL query via the `trino` Python client and returns the
    result as a pandas DataFrame asset.
    """

    asset_name: str = Field(description="Name of the asset")
    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8080, description="Trino coordinator port")
    user: str = Field(default="dagster", description="Trino user name")
    catalog: str = Field(description="Trino catalog, e.g. 'postgres', 'iceberg', 'hive'")
    schema_name: Optional[str] = Field(
        default=None, description="Default schema within the catalog"
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the Trino password (basic auth). Omit for no auth.",
    )
    query: str = Field(description="Trino SQL query to execute")

    # Catalog / governance
    group_name: Optional[str] = Field(default=None, description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys (e.g. ['raw_orders', 'sales/dim_customer'])",
    )
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None, description="Additional key-value tags"
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Defaults to ['trino'] if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns.",
    )

    # Preview metadata
    include_preview_metadata: bool = Field(
        default=False,
        description="Include a `preview` metadata key (markdown table of first rows).",
    )
    preview_rows: int = Field(
        default=25, ge=1, le=500,
        description="Rows to include in the preview when include_preview_metadata=True.",
    )

    # Partitioning
    partition_type: Optional[str] = Field(
        default=None,
        description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'multi' | None",
    )
    partition_start: Optional[str] = Field(
        default=None, description="ISO date for time-based partition types."
    )
    partition_values: Optional[str] = Field(
        default=None, description="Comma-separated values for static/multi partitioning."
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None, description="Name for DynamicPartitionsDefinition when partition_type='dynamic'."
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter output to the current date partition key (for downstream reuse).",
    )
    partition_static_dim: Optional[str] = Field(
        default=None, description="Dimension name for static axis in multi-partitioning."
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None, description="Column used to filter to the current static partition dimension."
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts.",
    )

    # Retry policy
    retry_policy_max_retries: Optional[int] = Field(
        default=None, description="Max retries on asset failure (opt-in — enables a RetryPolicy)."
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None, description="Seconds between retries (default 1)."
    )
    retry_policy_backoff: str = Field(
        default="exponential", description="Backoff strategy: 'linear' or 'exponential'."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        host = self.host
        port = self.port
        user = self.user
        catalog = self.catalog
        schema_name = self.schema_name
        password_env_var = self.password_env_var
        query = self.query
        group_name = self.group_name
        description = self.description or f"Trino query: {query[:60].strip()}..."
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        column_lineage = self.column_lineage

        kinds = self.kinds or ["trino"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )

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

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=description,
            group_name=group_name,
            owners=self.owners or [],
            tags=tags,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=freshness_policy,
            retry_policy=retry_policy,
        )
        def trino_query_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
            import trino.dbapi as trino_dbapi
            from trino.auth import BasicAuthentication
            import os

            password = os.environ.get(password_env_var) if password_env_var else None
            context.log.info(f"Connecting to Trino at {host}:{port} (catalog={catalog})")
            conn = trino_dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
                schema=schema_name,
                auth=BasicAuthentication(user, password) if password else None,
            )
            try:
                context.log.info(f"Executing query: {query[:100]}")
                cur = conn.cursor()
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                df = pd.DataFrame(rows, columns=cols)
                context.log.info(f"Query returned {len(df)} rows, {len(cols)} columns")

                schema = dg.TableSchema(
                    columns=[
                        dg.TableColumn(name=str(c), type=str(df.dtypes[c]))
                        for c in df.columns
                    ]
                )
                metadata: Dict[str, Any] = {
                    "dagster/row_count": dg.MetadataValue.int(len(df)),
                    "dagster/column_schema": dg.MetadataValue.table_schema(schema),
                }
                if column_lineage:
                    lineage_deps: Dict[str, List[dg.TableColumnDep]] = {}
                    upstream_deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
                    for out_col, in_cols in column_lineage.items():
                        for uak in upstream_deps_keys:
                            lineage_deps.setdefault(str(out_col), []).extend(
                                dg.TableColumnDep(asset_key=uak, column_name=str(ic))
                                for ic in in_cols
                            )
                    if lineage_deps:
                        metadata["dagster/column_lineage"] = dg.MetadataValue.column_lineage(
                            dg.TableColumnLineage(lineage_deps)
                        )
                if include_preview and len(df) > 0:
                    try:
                        _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                        metadata["preview"] = dg.MetadataValue.md(_prev.to_markdown(index=False))
                    except Exception as e:  # noqa: BLE001
                        context.log.warning(f"preview emission failed: {e}")
                context.add_output_metadata(metadata)
                return df
            finally:
                conn.close()

        return dg.Definitions(assets=[trino_query_asset])
