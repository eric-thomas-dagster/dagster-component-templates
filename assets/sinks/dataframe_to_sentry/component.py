"""DataFrame → Sentry events sink.

Turns each row of a DataFrame into a Sentry event and sends it via
the ingestion envelope endpoint. Uses the official `sentry-sdk` client
under the hood — supports levels, tags, extras, and grouping fingerprints.

Typical use case: emit "operational events" from a Dagster pipeline into
Sentry so on-call sees data-pipeline health alongside application errors.
"""

import os
from typing import Any, Dict, List, Literal, Optional

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
    Output,
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


class DataframeToSentryComponent(Component, Model, Resolvable):
    """Send each row of an upstream DataFrame to Sentry as an event.

    Requires `sentry-sdk` installed. DSN is provided via env var (default
    SENTRY_DSN). Common shape: DataFrame with a `message` column plus any
    number of tag/extra columns.
    """

    asset_name: str = Field(description="Output asset name (this asset is a sink and returns None).")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream DataFrame asset.")

    dsn_env_var: str = Field(
        default="SENTRY_DSN",
        description="Env var holding the Sentry DSN. Format: 'https://<key>@sentry.io/<project_id>'.",
    )
    environment: Optional[str] = Field(
        default=None,
        description="Sentry environment tag (e.g. 'prod', 'staging'). Defaults to whatever the SDK detects.",
    )
    release: Optional[str] = Field(
        default=None,
        description="Sentry release tag. Common: your app version, git SHA, or Dagster asset partition.",
    )

    message_column: str = Field(
        default="message",
        description="Name of the column whose value becomes the event's message text.",
    )
    level_column: Optional[str] = Field(
        default=None,
        description=(
            "Column supplying the Sentry level per row (one of debug|info|warning|error|fatal). "
            "Rows with an unknown value get default_level."
        ),
    )
    default_level: str = Field(
        default="info",
        description="Level applied when level_column is None or the row's value is not a valid level.",
    )
    tag_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to attach to each event as searchable tags in Sentry.",
    )
    extra_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to attach as extra (non-indexed) context in each event.",
    )
    fingerprint_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Columns to combine into the event fingerprint (drives Sentry grouping). "
            "Default is Sentry's automatic grouping."
        ),
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="sentry")

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
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "sends the whole backfill range's rows in one run (the date-window "
            "filter already handles a multi-partition range correctly). "
            "'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _self = self
        _upstream_key = AssetKey.from_user_string(_self.upstream_asset_key)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @asset(partitions_def=partitions_def,
            key=_self.asset_name,
            ins={"upstream": AssetIn(key=_upstream_key)},
            description=_self.description or (
                f"Send {_self.upstream_asset_key} rows to Sentry as events"
            ),
            group_name=_self.group_name,
            kinds={"python", "sentry"},
            backfill_policy=backfill_policy,
        )
        def _sink_asset(context: AssetExecutionContext, upstream: pd.DataFrame):
            # Filter to current partition if partitioned
            if hasattr(upstream, "columns") and (context.has_partition_key or context.has_partition_key_range):
                import pandas as pd
                # One filter pass per partition key: a normal tick gives exactly one,
                # a single_run backfill gives the whole range. Rows are selected by
                # position so keys that match overlapping rows never duplicate them.
                _pk_base = upstream
                _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                _pk_keep = set()
                _pk_filtered = False
                for _pk_iter in context.partition_keys:
                    upstream = _pk_pos
                    _pk = _pk_iter
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                    if partition_date_column and partition_date_column in upstream.columns and _date_key:
                        _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                        upstream = upstream[_col_dates == _date_key]
                    if partition_static_column and partition_static_column in upstream.columns and _static_key:
                        upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                        upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                    if upstream is not _pk_pos:
                        _pk_filtered = True
                        _pk_keep.update(upstream.index)
                upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
            try:
                import sentry_sdk
            except ImportError as e:
                raise ImportError(
                    "sentry-sdk not installed. Run `uv add sentry-sdk` (or "
                    "`pip install sentry-sdk`) before using dataframe_to_sentry."
                ) from e

            dsn = os.environ.get(_self.dsn_env_var)
            if not dsn:
                raise RuntimeError(
                    f"{_self.dsn_env_var!r} env var not set. Get your DSN at "
                    f"Sentry → Project Settings → Client Keys (DSN)."
                )

            init_kwargs: Dict[str, Any] = {"dsn": dsn}
            if _self.environment:
                init_kwargs["environment"] = _self.environment
            if _self.release:
                init_kwargs["release"] = _self.release
            client = sentry_sdk.Client(**init_kwargs)
            hub = sentry_sdk.Hub(client)

            _valid_levels = {"debug", "info", "warning", "error", "fatal"}
            _default_level = (_self.default_level or "info").lower()
            if _default_level not in _valid_levels:
                _default_level = "info"

            df = upstream.copy()
            sent = 0
            skipped_no_msg = 0

            for _i, row in df.iterrows():
                _msg = row.get(_self.message_column)
                if not _msg or (isinstance(_msg, float) and pd.isna(_msg)):
                    skipped_no_msg += 1
                    continue
                _level = _default_level
                if _self.level_column and _self.level_column in df.columns:
                    _row_lvl = str(row.get(_self.level_column, "")).strip().lower()
                    if _row_lvl in _valid_levels:
                        _level = _row_lvl

                with hub.push_scope() as scope:
                    scope.level = _level
                    if _self.tag_columns:
                        for _t in _self.tag_columns:
                            if _t in df.columns:
                                _v = row.get(_t)
                                if _v is not None and not (isinstance(_v, float) and pd.isna(_v)):
                                    scope.set_tag(_t, str(_v))
                    if _self.extra_columns:
                        for _x in _self.extra_columns:
                            if _x in df.columns:
                                _v = row.get(_x)
                                if _v is not None and not (isinstance(_v, float) and pd.isna(_v)):
                                    scope.set_extra(_x, _v)
                    if _self.fingerprint_columns:
                        _fp = [
                            str(row.get(_c))
                            for _c in _self.fingerprint_columns
                            if _c in df.columns and row.get(_c) is not None
                        ]
                        if _fp:
                            scope.fingerprint = _fp
                    hub.capture_message(str(_msg), level=_level)
                sent += 1

            client.flush(timeout=5)

            metadata = {
                "dagster/row_count":  MetadataValue.int(sent),
                "rows_upstream":      MetadataValue.int(len(df)),
                "skipped_no_message": MetadataValue.int(skipped_no_msg),
                "sentry_dsn_prefix":  MetadataValue.text(dsn.split("@")[-1][:60] if "@" in dsn else "?"),
            }
            if _self.environment:
                metadata["environment"] = MetadataValue.text(_self.environment)
            if _self.release:
                metadata["release"] = MetadataValue.text(_self.release)

            context.log.info(
                f"Sent {sent} events to Sentry (skipped {skipped_no_msg} rows with empty {_self.message_column!r})"
            )
            return Output(None, metadata=metadata)

        return Definitions(assets=[_sink_asset])


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
