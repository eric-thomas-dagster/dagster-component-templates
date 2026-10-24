"""Cube Query Asset Component.

Runs a query against a Cube semantic-layer server (https://cube.dev/) and
materializes the result as a Dagster asset (pandas DataFrame). Perfect for
"put my Cube metrics into the warehouse" workflows, or as the first step of
a downstream AI/reporting pipeline that consumes governed metrics.

API: POST {api_url}/cubejs-api/v1/load
Body: {"query": {"measures": [...], "dimensions": [...], "timeDimensions": [...],
                 "filters": [...], "limit": ..., "order": {...}}}

Auth:
  - Cube Core dev mode: no auth
  - Cube production / Cube Cloud: JWT via `Authorization: Bearer <token>`
    (generated via a shared secret). Pass the JWT env var name in
    `api_token_env_var`.

Docs: https://cube.dev/docs/product/apis-integrations/rest-api
"""
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
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


class CubeQueryAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Cube semantic-layer query and materialize as a Dagster asset.

    Example — daily orders count by status:

        ```yaml
        type: dagster_community_components.CubeQueryAssetComponent
        attributes:
          asset_name: cube_orders_by_status
          api_url_env_var: CUBE_URL
          query:
            measures:
              - Orders.count
              - Orders.totalAmount
            dimensions:
              - Orders.status
            timeDimensions:
              - dimension: Orders.createdAt
                granularity: day
                dateRange: ["2024-01-01", "2024-12-31"]
            order:
              Orders.createdAt: asc
          group_name: cube
        ```

    Cube Cloud (JWT auth):

        ```yaml
        attributes:
          # ...
          api_url_env_var: CUBE_API_URL
          api_token_env_var: CUBE_API_TOKEN
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    query: Dict[str, Any] = Field(
        description=(
            "Cube query JSON. Standard Cube query shape: measures (list), dimensions (list), timeDimensions (list of {dimension, granularity, dateRange}), filters (list of {member, operator, values}), order (dict), limit (int), offset (int). On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted."
        ),
    )

    api_url_env_var: str = Field(
        default="CUBE_URL",
        description="Env var with Cube API base URL (e.g. http://localhost:4000 for local dev).",
    )
    api_token_env_var: Optional[str] = Field(
        default=None,
        description="Optional env var with JWT for Cube Cloud / production auth. Omit for local dev.",
    )
    request_timeout_seconds: int = Field(default=60, ge=1, description="Timeout per Cube API call.")
    strip_cube_prefix: bool = Field(
        default=True,
        description=(
            "When true (default), strip the '<CubeName>.' prefix from column names — "
            "'Orders.customerName' becomes 'customerName'. Makes downstream consumers "
            "(dbt sources, LangChain templates that use {var} syntax, pandas transforms) "
            "friendlier. Set to false to preserve the fully-qualified Cube identifiers."
        ),
    )

    group_name: Optional[str] = Field(default="cube", description="Dagster asset group name.")

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
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (the row-filter "
            "logic below already handles a multi-partition range correctly). "
            "'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with "
            "partition_expr set) -- a generic filesystem/default IO manager "
            "cannot persist a single_run output spanning multiple partitions "
            "and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
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
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'cube', 'semantic-layer').",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        _self = self
        _kinds = set(self.kinds or [])
        _kinds.update({"cube", "semantic-layer"})

        @dg.asset(partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=_kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Cube query: measures={_self.query.get('measures') or []}, "
                f"dimensions={_self.query.get('dimensions') or []}."
            ),
            deps=[dg.AssetKey.from_user_string(k) for k in (_self.deps or [])],
        )
        def _asset(context: dg.AssetExecutionContext):
            import os
            import pandas as pd
            try:
                import requests
            except ImportError:
                raise ImportError("cube_query_asset requires: pip install requests")

            base_url = os.environ.get(_self.api_url_env_var, "").rstrip("/")
            if not base_url:
                raise RuntimeError(f"{_self.api_url_env_var!r} env var not set.")

            headers = {"Content-Type": "application/json"}
            if _self.api_token_env_var:
                token = os.environ.get(_self.api_token_env_var)
                if not token:
                    raise RuntimeError(f"{_self.api_token_env_var!r} env var not set (Cube JWT).")
                headers["Authorization"] = token if token.startswith("Bearer ") else f"Bearer {token}"

            url = f"{base_url}/cubejs-api/v1/load"
            context.log.info(
                f"[cube] POST {url}  measures={_substitute_partition(_self.query, _partition_vars(context)).get('measures')} "
                f"dimensions={_substitute_partition(_self.query, _partition_vars(context)).get('dimensions')}"
            )
            resp = requests.post(
                url,
                headers=headers,
                json={"query": _substitute_partition(_self.query, _partition_vars(context))},
                timeout=_self.request_timeout_seconds,
            )
            if resp.status_code == 400:
                # Cube returns useful error detail on 400 — surface it.
                raise RuntimeError(f"Cube API 400: {resp.text[:1000]}")
            resp.raise_for_status()
            body = resp.json()

            data = body.get("data") or []
            df = pd.DataFrame(data)
            if _self.strip_cube_prefix and len(df) > 0:
                # Strip the '<CubeName>.' prefix from every column that has one.
                # LangChain / dbt / pandas all prefer identifiers without dots.
                df = df.rename(columns={c: (c.split(".", 1)[1] if "." in c else c) for c in df.columns})
            context.log.info(f"[cube] returned {len(df)} rows, columns={list(df.columns)}")

            md: Dict[str, Any] = {
                "cube_url": dg.MetadataValue.url(base_url),
                "cube_measures": dg.MetadataValue.json(_substitute_partition(_self.query, _partition_vars(context)).get("measures") or []),
                "cube_dimensions": dg.MetadataValue.json(_substitute_partition(_self.query, _partition_vars(context)).get("dimensions") or []),
                "cube_row_count": dg.MetadataValue.int(len(df)),
            }
            if len(df) > 0:
                md["preview"] = dg.MetadataValue.md(df.head(10).to_markdown(index=False))
                md["columns"] = dg.MetadataValue.json(list(df.columns))
            # Cube annotates each query with per-measure/dimension metadata —
            # surface it for lineage docs.
            annotation = body.get("annotation") or {}
            if annotation:
                md["cube_annotation"] = dg.MetadataValue.json(annotation)
            context.add_output_metadata(md)
            return df

        return dg.Definitions(assets=[_asset])


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
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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
