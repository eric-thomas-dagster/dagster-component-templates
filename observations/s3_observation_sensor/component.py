"""S3 Observation Sensor Component.

Polls one or more S3 bucket/prefix targets on a schedule and emits an
AssetObservation (or AssetMaterialization) tagged with a DataVersion.
Downstream AutomationCondition.newly_updated() / .eager() fires when the
DataVersion changes -- i.e. when new objects have arrived or existing ones
have been replaced.

A cursor (`{asset_key_str: last_seen_data_version}`, JSON-encoded) tracks the
last observed DataVersion per target, so a tick where nothing changed emits
NOTHING instead of re-materializing every target on every poll -- without
this, a downstream freshness policy on the observed asset is meaningless (the
asset always looks fresh, whether or not anything new actually landed).

Two operating modes:

- resource_key set (declarative / demo-mode / shared client): sensor calls
  `context.resources.<resource_key>.observe(source)` where source is
  `"bucket/prefix"`. The resource returns a dict of the form
  `{"data_version": "...", **metadata}`.

- resource_key unset (real S3): sensor uses boto3 to paginate list_objects_v2,
  counts objects + tracks latest LastModified, and derives a DataVersion from
  `f"{object_count}-{latest_modified_iso}"`.

Single-target mode (`asset_key`/`bucket_name`/`prefix`) still works exactly
as before. `targets:` declares several bucket/prefix feeds from ONE component
instance -- sharing one sensor, one poll tick, and (in the native boto3 path)
one boto3 client -- instead of needing N component instances for N feeds.
Mutually exclusive with the single-target fields (same convention as
`external_sftp_path`'s `paths:`).
"""
import json
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import AssetKey, AssetMaterialization, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field, model_validator


class S3ObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: Optional[str] = Field(
        default=None,
        description="Asset key of the external asset to observe, for a single target. Mutually exclusive with `targets:`.",
    )
    bucket_name: Optional[str] = Field(default=None, description="S3 bucket name, for a single target. Mutually exclusive with `targets:`.")
    prefix: str = Field(default="", description="Key prefix, for a single target. Mutually exclusive with `targets:`.")
    targets: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Declare several bucket/prefix feeds from one component instance, instead of one "
            "S3ObservationSensorComponent YAML block (and one polling sensor, one boto3 client / "
            "resource call per tick) per feed. Each entry: `asset_key` (required), `bucket_name` "
            "(required), `prefix` (optional, default ''). All entries share this component's "
            "resource_key/region_name/check_interval_seconds/emit_materialization and ONE sensor "
            "that polls every target per tick. Mutually exclusive with the top-level "
            "`asset_key`/`bucket_name`/`prefix` fields."
        ),
    )
    region_name: Optional[str] = Field(default=None, description="AWS region")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(source) -> dict` (source is "
            "'bucket/prefix') that returns `{'data_version': str, **metadata}`. "
            "When set, the sensor uses this instead of instantiating boto3 directly."
        ),
    )

    emit_materialization: bool = Field(
        default=True,
        description=(
            "When True (default), emit AssetMaterialization on the target "
            "asset key. External assets show healthy/green in the Dagster UI "
            "and downstream AutomationCondition.eager() fires naturally on "
            "parent updates. When False, emit AssetObservation — free of "
            "Dagster+ credit charges, but the target asset renders as "
            "observed-external (dashed border, gray) and downstream "
            "conditions that gate on ~any_deps_missing() (including "
            "eager()) will not fire. Both event types carry the same "
            "dagster/data_version tag."
        ),
    )

    @model_validator(mode="after")
    def _validate_targets(self):
        has_single = bool(self.asset_key or self.bucket_name)
        has_multi = bool(self.targets)
        if has_single and has_multi:
            raise ValueError("Set either asset_key/bucket_name (single target) or targets: (several targets), not both.")
        if not has_single and not has_multi:
            raise ValueError("Set either asset_key/bucket_name (single target) or targets: (several targets).")
        if has_single and not (self.asset_key and self.bucket_name):
            raise ValueError("Both asset_key and bucket_name are required in single-target mode.")
        return self

    def _resolve_targets(self) -> List[Dict[str, Any]]:
        if self.targets:
            entries = []
            seen = set()
            for i, raw in enumerate(self.targets):
                if not isinstance(raw, dict):
                    raise ValueError(f"targets[{i}] must be a mapping, got {type(raw).__name__}")
                asset_key = raw.get("asset_key")
                bucket_name = raw.get("bucket_name")
                if not asset_key:
                    raise ValueError(f"targets[{i}] is missing required field 'asset_key'")
                if not bucket_name:
                    raise ValueError(f"targets[{i}] ({asset_key!r}) is missing required field 'bucket_name'")
                if asset_key in seen:
                    raise ValueError(f"targets: duplicate asset_key {asset_key!r}")
                seen.add(asset_key)
                entries.append({"asset_key": asset_key, "bucket_name": bucket_name, "prefix": raw.get("prefix", "")})
            return entries
        return [{"asset_key": self.asset_key, "bucket_name": self.bucket_name, "prefix": self.prefix}]

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='S3 Observation Sensor', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()
        entries = self._resolve_targets()

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.assets(
                *(AssetKey.from_user_string(e["asset_key"]) for e in entries)
            ),
        )
        def _s3_obs(context: SensorEvaluationContext, **_resources):
            _event_cls = AssetMaterialization if _self.emit_materialization else AssetObservation

            # Cursor: {asset_key_str: last_seen_data_version}. Without this, every
            # tick re-emits a materialization for every target even when nothing
            # changed, which makes any downstream freshness policy meaningless --
            # the asset always looks fresh, whether or not anything new landed.
            try:
                last_versions: Dict[str, str] = json.loads(context.cursor) if context.cursor else {}
            except (TypeError, ValueError):
                last_versions = {}
            new_versions: Dict[str, str] = dict(last_versions)
            events = []

            # ── Resource-backed path (preferred) ────────────────────────────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                for e in entries:
                    key_str = e["asset_key"]
                    try:
                        source = f"{e['bucket_name']}/{e['prefix']}" if e["prefix"] else e["bucket_name"]
                        observed: dict[str, Any] = dict(client.observe(source))
                    except Exception as ex:
                        context.log.error(f"resource '{resource_key}'.observe({source!r}) failed: {ex}")
                        continue
                    data_version = str(observed.pop("data_version", ""))
                    new_versions[key_str] = data_version
                    if data_version and data_version == last_versions.get(key_str):
                        continue  # unchanged since last tick -- don't re-materialize
                    events.append(_event_cls(
                        asset_key=AssetKey.from_user_string(key_str),
                        metadata=observed,
                        tags={DATA_VERSION_TAG: data_version} if data_version else None,
                    ))
                cursor = json.dumps(new_versions)
                if not events:
                    return SensorResult(cursor=cursor, skip_reason="no target changed since last tick")
                return SensorResult(asset_events=events, cursor=cursor)

            # ── Native boto3 path ───────────────────────────────────────────
            try:
                import boto3
            except ImportError:
                return SensorResult(skip_reason="boto3 not installed")
            try:
                s3 = boto3.client("s3", region_name=_self.region_name)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            for e in entries:
                key_str = e["asset_key"]
                bucket_name = e["bucket_name"]
                prefix = e["prefix"]
                count = 0
                total = 0
                latest = None
                try:
                    for page in s3.get_paginator("list_objects_v2").paginate(
                        Bucket=bucket_name, Prefix=prefix
                    ):
                        for o in page.get("Contents", []):
                            count += 1
                            total += o.get("Size", 0)
                            lm = o.get("LastModified")
                            if lm and (latest is None or lm > latest):
                                latest = lm
                except Exception as ex:
                    context.log.error(f"List failed for {bucket_name}/{prefix}: {ex}")
                    continue

                latest_iso = latest.isoformat() if latest else ""
                data_version = f"{count}-{latest_iso}"
                new_versions[key_str] = data_version
                if data_version == last_versions.get(key_str):
                    continue  # unchanged since last tick -- don't re-materialize
                metadata = {
                    "object_count": count,
                    "total_size_bytes": total,
                    "latest_modified_iso": latest_iso,
                    "bucket": bucket_name,
                    "prefix": prefix,
                }
                events.append(_event_cls(
                    asset_key=AssetKey.from_user_string(key_str),
                    metadata=metadata,
                    tags={DATA_VERSION_TAG: data_version},
                ))

            cursor = json.dumps(new_versions)
            if not events:
                return SensorResult(cursor=cursor, skip_reason="no target changed since last tick")
            return SensorResult(asset_events=events, cursor=cursor)

        return dg.Definitions(sensors=[_s3_obs])
