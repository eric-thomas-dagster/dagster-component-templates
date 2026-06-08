"""Read a .yxdb file → pandas DataFrame.

.yxdb is the proprietary binary serialization format used by some ETL tools. Most the source ETL tool
users export intermediate data into .yxdb (faster than CSV, preserves
dtypes), so a real-world migration almost always hits one.

This component is a thin wrapper over the `import-yxdb` PyPI package, which
parses the file format and produces a clean pandas DataFrame. Falls back to
the lower-level `yxdb` package if `import-yxdb` isn't available, iterating
row-by-row.
"""
import os
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class DataframeFromYxdbComponent(Component, Model, Resolvable):
    """Materialize a .yxdb file as a pandas DataFrame asset.

    Example:
        ```yaml
        type: dagster_component_templates.DataframeFromYxdbComponent
        attributes:
          asset_name: customer_master_from_alteryx
          file_path: /data/exports/customers.yxdb
          group_name: ingestion
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    file_path: str = Field(
        description=(
            "Path to the .yxdb file. Supports env var substitution "
            "(e.g. ${ALTERYX_DATA_DIR}/customers.yxdb)."
        ),
    )
    nrows: Optional[int] = Field(
        default=None,
        description="Optional row limit; defaults to all rows.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['alteryx', 'yxdb'].",
    )
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="FreshnessPolicy max lag (min).")
    freshness_cron: Optional[str] = Field(default=None, description="FreshnessPolicy deadline cron.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Retry on transient failures.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        file_path = self.file_path
        nrows = self.nrows
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        _inferred_kinds = list(self.kinds or ["alteryx", "yxdb"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from datetime import timedelta
            from dagster import FreshnessPolicy
            _lag = timedelta(minutes=int(self.freshness_max_lag_minutes))
            _freshness_policy = (
                FreshnessPolicy.cron(deadline_cron=self.freshness_cron, lower_bound_delta=_lag)
                if self.freshness_cron
                else FreshnessPolicy.time_window(fail_window=_lag)
            )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(
            name=asset_name,
            group_name=group_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _yxdb_asset(context: AssetExecutionContext):
            resolved_path = os.path.expandvars(file_path)
            if not os.path.exists(resolved_path):
                raise FileNotFoundError(f"yxdb file not found: {resolved_path}")

            df = _read_yxdb(resolved_path, nrows=nrows, log=context.log)

            context.add_output_metadata({
                "yxdb_path": MetadataValue.text(resolved_path),
                "file_size_bytes": MetadataValue.int(os.path.getsize(resolved_path)),
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(
                    dg.TableSchema(columns=[
                        dg.TableColumn(name=str(c), type=str(df.dtypes[c]))
                        for c in df.columns
                    ])
                ),
            })
            return df

        return Definitions(assets=[_yxdb_asset])


def _read_yxdb(path: str, *, nrows: Optional[int], log):
    """Read a .yxdb file → pandas DataFrame.

    Preferred path: `import-yxdb` (whole-file → DataFrame in one shot).
    Fallback: `yxdb` core package, iterating row-by-row (lighter dep, slower).
    """
    import pandas as pd

    # Preferred: import-yxdb's whole-file → DataFrame helper. The package
    # exposes the helper as `yxdb_to_pandas` (newer versions) or
    # `to_dataframe` (older versions) — try both.
    try:
        import import_yxdb  # noqa: F401
        if hasattr(import_yxdb, "yxdb_to_pandas"):
            log.info(f"Reading {path} via import_yxdb.yxdb_to_pandas")
            df = import_yxdb.yxdb_to_pandas(path)
        elif hasattr(import_yxdb, "to_dataframe"):
            log.info(f"Reading {path} via import_yxdb.to_dataframe")
            df = import_yxdb.to_dataframe(path)
        else:
            raise ImportError("import_yxdb has neither yxdb_to_pandas nor to_dataframe")
        if nrows is not None:
            df = df.head(nrows)
        return df
    except ImportError:
        pass

    # Fallback: core `yxdb` package — iterate rows.
    try:
        from yxdb import YxdbReader
    except ImportError as e:
        raise ImportError(
            "Reading .yxdb files needs either `import-yxdb` (preferred — installs "
            "the whole-file → DataFrame helper) or `yxdb` (lower-level, row-iterator). "
            "Install with: pip install import-yxdb"
        ) from e

    log.info(f"Reading {path} via yxdb.YxdbReader (row iterator)")
    reader = YxdbReader(path)
    fields = reader.list_fields()
    columns = [str(f) for f in fields]
    rows = []
    n = 0
    while reader.next():
        rows.append([reader.read_index(i) for i in range(len(fields))])
        n += 1
        if nrows is not None and n >= nrows:
            break
    return pd.DataFrame(rows, columns=columns)
