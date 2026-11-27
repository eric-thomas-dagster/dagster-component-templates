"""Base64TransformComponent — base64 encode/decode one or more columns.

No registry component does base64 today (`text_codec_convert_asset` converts
between character codecs like ASCII/EBCDIC — a different operation from
base64, which encodes arbitrary bytes as printable ASCII and back). Common
migration target for a StreamSets Base64 stage.

StreamSets' real Base64 processor works on BYTE ARRAYS ONLY — a string-typed
field is a record error there, not silently accepted. This component matches
that by default (`on_string_field: error`) for `mode: encode`, since encoding
already-text data through base64 is almost always a mistake carried over from
a misconfigured upstream; set `on_string_field: encode_utf8_first` to opt into
the more lenient behavior (UTF-8 encode the string to bytes first, then
base64-encode those bytes) for general ETL use where the input genuinely is
text that needs to become a base64 string. `mode: decode` always accepts a
str or bytes input column (a base64 string is text either way) and always
produces `bytes` output — decoding doesn't have the record-error case
StreamSets documents (that's specific to Base64 *encode* seeing a string
field with no byte representation already in hand).

StreamSets 6.x's Base64 stage takes a list of (origin field, result field)
pairs so one stage call can transform several fields — `fields:` mirrors
that shape.
"""

from typing import Any, Dict, List, Literal, Optional, Union

import base64 as _base64
import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
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
from pydantic import Field, model_validator


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


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


class FieldPair(Model, Resolvable):
    origin: str = Field(description="Source column to encode/decode.")
    result: str = Field(
        default=None,
        description="Column to write the result into. Defaults to `origin` (overwrite in place).",
    )

    @model_validator(mode="after")
    def _default_result(self):
        if self.result is None:
            object.__setattr__(self, "result", self.origin)
        return self


class Base64TransformComponent(Component, Model, Resolvable):
    """Base64 encode or decode one or more columns, StreamSets Base64-stage compatible."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream DataFrame asset key.")

    mode: Literal["encode", "decode"] = Field(
        description="`encode`: bytes/str -> base64 text. `decode`: base64 text/bytes -> raw bytes.",
    )
    fields: List[FieldPair] = Field(
        description=(
            "(origin, result) column pairs to transform — mirrors StreamSets' Base64 "
            "stage field-list shape. Each pair processed independently; one call can "
            "cover several fields."
        ),
    )
    url_safe: bool = Field(
        default=False,
        description=(
            "Use the URL-safe alphabet (`-`/`_` instead of `+`/`/`, no padding stripped) "
            "— `base64.urlsafe_b64encode`/`urlsafe_b64decode` instead of the standard "
            "`base64.b64encode`/`b64decode`."
        ),
    )
    on_string_field: Literal["error", "encode_utf8_first"] = Field(
        default="error",
        description=(
            "mode='encode' only, when a field's value is a Python str (not bytes): "
            "'error' (default) matches StreamSets' real Base64 stage, which treats a "
            "string-typed field as a record error — most of the time this means an "
            "upstream step produced text where raw bytes were expected, and silently "
            "base64-encoding the text is the wrong fix. 'encode_utf8_first' opts into "
            "the more lenient behavior for general ETL (UTF-8 encode the string to "
            "bytes, then base64-encode those bytes) when the input genuinely is text "
            "that needs to become a base64 string."
        ),
    )
    on_error: Literal["raise", "null"] = Field(
        default="raise",
        description="On a per-row encode/decode failure (e.g. invalid base64 on decode): raise, or write null for that row's result and continue.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure.",
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
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), csv=('partition_values',), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Base64 Transform', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if not self.fields:
            raise ValueError("'fields' must list at least one {origin, result?} pair.")

        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = (
                [str(v).strip() for v in self.partition_values if str(v).strip()]
                if isinstance(self.partition_values, (list, tuple))
                else [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            )
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
            else:
                raise ValueError(f"unknown partition_type: {_pt!r}")

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        mode = self.mode
        fields = [(f.origin, f.result) for f in self.fields]
        url_safe = self.url_safe
        on_string_field = self.on_string_field
        on_error = self.on_error

        encode_fn = _base64.urlsafe_b64encode if url_safe else _base64.b64encode
        decode_fn = _base64.urlsafe_b64decode if url_safe else _base64.b64decode

        def _encode_one(value: Any) -> Any:
            if value is None or (isinstance(value, float) and value != value):
                return None
            if isinstance(value, (bytes, bytearray)):
                raw = bytes(value)
            elif isinstance(value, str):
                if on_string_field == "error":
                    raise ValueError(
                        "mode='encode' got a str field but on_string_field='error' "
                        "(StreamSets' real Base64 stage treats a string-typed field as a "
                        "record error) — set on_string_field='encode_utf8_first' to encode "
                        "text fields intentionally."
                    )
                raw = value.encode("utf-8")
            else:
                raise ValueError(f"mode='encode' got a non-bytes, non-str value: {value!r}")
            return encode_fn(raw).decode("ascii")

        def _decode_one(value: Any) -> Any:
            if value is None or (isinstance(value, float) and value != value):
                return None
            if isinstance(value, str):
                raw = value.encode("ascii")
            elif isinstance(value, (bytes, bytearray)):
                raw = bytes(value)
            else:
                raise ValueError(f"mode='decode' got a non-bytes, non-str value: {value!r}")
            return decode_fn(raw)

        transform_one = _encode_one if mode == "encode" else _decode_one

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Base64 {mode}: {', '.join(o for o, _ in fields)}.",
            group_name=self.group_name,
            kinds={"base64", "transform"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

            df = upstream.copy().reset_index(drop=True)
            processed = 0
            errored = 0

            for origin, result in fields:
                if origin not in df.columns:
                    raise ValueError(f"field {origin!r} not in upstream: {list(df.columns)}")

                out: List[Any] = []
                for value in df[origin]:
                    try:
                        out.append(transform_one(value))
                        processed += 1
                    except Exception as e:
                        if on_error == "raise":
                            raise ValueError(f"base64 {mode} failed on field {origin!r}: {e}") from e
                        out.append(None)
                        errored += 1
                df[result] = out

            return Output(
                value=df,
                metadata={
                    "rows": MetadataValue.int(len(df)),
                    "mode": MetadataValue.text(mode),
                    "fields": MetadataValue.text(", ".join(o for o, _ in fields)),
                    "values_processed": MetadataValue.int(processed),
                    "values_errored": MetadataValue.int(errored),
                },
            )

        return Definitions(assets=[_asset])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
