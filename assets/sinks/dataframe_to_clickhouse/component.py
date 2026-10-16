"""DataFrame → ClickHouse via clickhouse-connect.

Bulk-insert a Pandas DataFrame into a ClickHouse table using the
official ``clickhouse-connect`` client. clickhouse-connect's
``client.insert_df()`` is the most efficient path for Pandas →
ClickHouse — it batches rows + uses the binary RowBinary format under
HTTP.

For very large DataFrames (>10M rows), prefer running multiple
materializations via Dagster partitions over one giant single-asset run.
clickhouse-connect handles ~1M rows/sec on a single client; partitioned
ingestion scales linearly.

Pairs with:
  - ``clickhouse_resource`` — shared connection
  - ``external_clickhouse_table`` — declare-only catalog entry for
    tables ClickHouse owns (e.g. Materialized Views, Distributed)

Docs: https://clickhouse.com/docs/en/integrations/python
"""
import os
from typing import Any, Dict, List, Optional

import dagster as dg
import pandas as pd
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


class DataframeToClickHouseComponent(dg.Component, dg.Model, dg.Resolvable):
    """Insert a Pandas DataFrame into a ClickHouse table.

    Example:
        ```yaml
        type: dagster_community_components.DataframeToClickHouseComponent
        attributes:
          asset_name: clickhouse_events_load
          upstream_asset_key: events_clean
          table: events
          database: analytics
          host_env_var: CLICKHOUSE_HOST
          port: 8443
          secure: true
          username_env_var: CLICKHOUSE_USER
          password_env_var: CLICKHOUSE_PASSWORD
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset providing the DataFrame.")
    table: str = Field(description="Destination ClickHouse table name.")
    database: str = Field(default="default", description="Destination ClickHouse database.")

    host_env_var: str = Field(default="CLICKHOUSE_HOST", description="Env var with ClickHouse hostname.")
    port: int = Field(default=8123, description="ClickHouse HTTP port (8123 plain / 8443 TLS).")
    username_env_var: str = Field(default="CLICKHOUSE_USER", description="Env var with username.")
    password_env_var: str = Field(default="CLICKHOUSE_PASSWORD", description="Env var with password.")
    secure: bool = Field(default=False, description="Enable TLS.")

    column_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional explicit column list (must match destination table order). "
            "If unset, DataFrame column order is used."
        ),
    )
    request_timeout_seconds: int = Field(default=300, ge=10)

    group_name: Optional[str] = Field(default="clickhouse", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'clickhouse').")

    partition_column: Optional[str] = Field(
        default=None,
        description=(
            "When set AND the asset is partitioned, appends this column to every "
            "row with the current partition key as its value. Enables the "
            "single-table + partition_column analytics pattern "
            "(WHERE partition_date = '2025-01-15') without needing per-partition "
            "tables. Prefer this over `{partition_key}` templating in the `table:` "
            "field for warehouse sinks — analytics queries stay clean. "
            "See docs/partition_patterns.md."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds'), csv=(), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("clickhouse")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Insert DataFrame into ClickHouse {_self.database}.{_self.table} via clickhouse-connect."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream: Any) -> dg.MaterializeResult:
            # Partition-aware target table:
            #   table: "events_{partition_key}" → "events_2025-01-15" on 2025-01-15.
            # Templates without {partition_key} pass through unchanged.
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key and "{partition_key}" in _self.table:
                resolved_table = _self.table.replace("{partition_key}", str(partition_key))
                context.log.info(f"partition-aware ClickHouse write: partition_key={partition_key!r} → table={resolved_table}")
            else:
                resolved_table = _self.table


            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            if partition_key and _self.partition_column:
                if _self.partition_column in upstream.columns:
                    context.log.warning(
                        f"partition_column={_self.partition_column!r} already in upstream — overwriting"
                    )
                upstream = upstream.assign(**{_self.partition_column: str(partition_key)})
                context.log.info(f"partition-aware write (Pattern B): added column {_self.partition_column!r}={partition_key!r} to {len(upstream)} rows")

            try:
                import clickhouse_connect
            except ImportError:
                raise ImportError(
                    "dataframe_to_clickhouse requires `clickhouse-connect`: "
                    "pip install clickhouse-connect"
                )

            host = os.environ.get(_self.host_env_var, "")
            user = os.environ.get(_self.username_env_var, "default")
            password = os.environ.get(_self.password_env_var, "")
            if not host:
                raise RuntimeError(f"ClickHouse host missing: set {_self.host_env_var}.")

            client = clickhouse_connect.get_client(
                host=host,
                port=_self.port,
                username=user,
                password=password,
                database=_self.database,
                secure=_self.secure,
                connect_timeout=_self.request_timeout_seconds,
            )

            df = upstream
            if _self.column_names:
                df = df[_self.column_names]

            context.log.info(
                f"ClickHouse insert: {_self.database}.{resolved_table} "
                f"({len(df)} rows, {len(df.columns)} cols)"
            )
            client.insert_df(table=resolved_table, df=df, database=_self.database)

            # Quick row-count check via SELECT count() for the metadata.
            try:
                total_rows = client.query(
                    f"SELECT count() FROM {_self.database}.{resolved_table}"
                ).result_rows[0][0]
            except Exception:
                total_rows = None

            metadata: Dict[str, Any] = {
                "row_count": dg.MetadataValue.int(len(df)),
                "column_count": dg.MetadataValue.int(len(df.columns)),
                "clickhouse_table": f"{_self.database}.{resolved_table}",
            }
            if total_rows is not None:
                metadata["clickhouse/table_total_rows"] = dg.MetadataValue.int(int(total_rows))

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_asset])
