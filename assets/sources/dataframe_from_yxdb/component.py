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
            "Path to the .yxdb file. Supports env var substitution (e.g. ${ALTERYX_DATA_DIR}/customers.yxdb). On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted."
        ),
    )
    nrows: Optional[int] = Field(
        default=None,
        description="Optional row limit; defaults to all rows.",
    )
    if_missing: str = Field(
        default="raise",
        description=(
            "Behavior when the file_path doesn't exist: 'raise' (default — "
            "fail loudly), 'empty' (return an empty DataFrame and log a "
            "warning so downstream assets continue with empty inputs). "
            "Useful for importer-generated demos where the original Alteryx "
            "workflow referenced a Windows-only path that the customer "
            "hasn't restored yet."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
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
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        )

        asset_name = self.asset_name
        file_path = self.file_path
        nrows = self.nrows
        if_missing = self.if_missing
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

        @asset(partitions_def=partitions_def, 
            key=AssetKey.from_user_string(asset_name),
            group_name=group_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _yxdb_asset(context: AssetExecutionContext):
            resolved_path = os.path.expandvars(_substitute_partition(file_path, _partition_vars(context)))
            if not os.path.exists(resolved_path):
                if if_missing == "empty":
                    import pandas as pd
                    context.log.warning(
                        f"yxdb file not found: {resolved_path} — returning "
                        "empty DataFrame (if_missing='empty'). Downstream "
                        "assets will materialize with no rows."
                    )
                    return pd.DataFrame()
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
