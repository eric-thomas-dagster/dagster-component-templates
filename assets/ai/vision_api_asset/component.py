"""VisionApiAssetComponent — Cloud Vision API per-row image analysis.

Calls Google Cloud Vision API on each row of an upstream DataFrame
(image referenced by file path, GCS URI, or remote URL) and writes
the resulting analyses back as new columns.

Supported features:
  LABEL_DETECTION, OBJECT_LOCALIZATION, FACE_DETECTION, LANDMARK_DETECTION,
  LOGO_DETECTION, TEXT_DETECTION (basic OCR), DOCUMENT_TEXT_DETECTION
  (richer OCR for documents), SAFE_SEARCH_DETECTION, IMAGE_PROPERTIES,
  CROP_HINTS, WEB_DETECTION.

For form / table / structured-document parsing, prefer the Document AI
component instead.
"""

import json
import os
import time
from typing import Any, Dict, List, Literal, Optional, Union

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
from pydantic import Field


_FEATURE_MAP = {
    "LABEL_DETECTION":           "label_annotations",
    "OBJECT_LOCALIZATION":       "localized_object_annotations",
    "FACE_DETECTION":            "face_annotations",
    "LANDMARK_DETECTION":        "landmark_annotations",
    "LOGO_DETECTION":            "logo_annotations",
    "TEXT_DETECTION":            "text_annotations",
    "DOCUMENT_TEXT_DETECTION":   "full_text_annotation",
    "SAFE_SEARCH_DETECTION":     "safe_search_annotation",
    "IMAGE_PROPERTIES":          "image_properties_annotation",
    "CROP_HINTS":                "crop_hints_annotation",
    "WEB_DETECTION":             "web_detection",
}


class VisionApiAssetComponent(Component, Model, Resolvable):
    """Run Cloud Vision API on a column of image references."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    image_column: Union[str, int] = Field(description="Column with image references (file path, gs:// URI, or http(s) URL).")
    image_source: Literal["path", "gcs", "url", "auto"] = Field(
        default="auto",
        description="`path` reads + base64-encodes; `gcs` / `url` use Vision's source URI input; `auto` picks based on the value's prefix.",
    )

    features: List[str] = Field(
        description="One or more Vision feature types (e.g. ['LABEL_DETECTION', 'OBJECT_LOCALIZATION']).",
    )
    max_results: int = Field(default=10, description="max_results per feature in the Vision request.")
    output_prefix: str = Field(default="vision_", description="Column-name prefix for added analysis columns.")

    rate_limit_delay: float = Field(default=0.0)
    max_retries: int = Field(default=3)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

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

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        unknown = [f for f in self.features if f.upper() not in _FEATURE_MAP]
        if unknown:
            raise ValueError(f"Unknown Vision features: {unknown}. Allowed: {list(_FEATURE_MAP.keys())}")

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        image_column = self.image_column
        image_source = self.image_source
        features = [f.upper() for f in self.features]
        max_results = self.max_results
        output_prefix = self.output_prefix
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Cloud Vision analysis ({', '.join(features)}) on column {image_column}.",
            group_name=self.group_name,
            kinds={"google", "vision", "ai"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            try:
                from google.cloud import vision
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-vision google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = vision.ImageAnnotatorClient(credentials=sa_creds)

            if image_column not in upstream.columns:
                raise ValueError(f"image_column={image_column!r} not in upstream: {list(upstream.columns)}")

            df = upstream.copy().reset_index(drop=True)
            feature_objs = [
                vision.Feature(type_=getattr(vision.Feature.Type, f), max_results=max_results)
                for f in features
            ]

            results: List[Dict[str, Any]] = []
            for idx, row in df.iterrows():
                ref = row[image_column]
                kind = image_source
                if kind == "auto":
                    s = str(ref)
                    if s.startswith("gs://"):
                        kind = "gcs"
                    elif s.startswith(("http://", "https://")):
                        kind = "url"
                    else:
                        kind = "path"

                if kind == "path":
                    try:
                        with open(ref, "rb") as fh:
                            image = vision.Image(content=fh.read())
                    except Exception as e:
                        results.append({"_error": f"could not read {ref}: {e}"})
                        continue
                else:
                    image = vision.Image(source=vision.ImageSource(image_uri=str(ref)))

                attempt = 0
                last_err = None
                resp = None
                while attempt <= max_retries:
                    try:
                        resp = client.annotate_image({"image": image, "features": feature_objs})
                        last_err = None
                        break
                    except Exception as e:
                        last_err = e
                        attempt += 1
                        if attempt > max_retries:
                            break
                        time.sleep((2 ** attempt) * 0.5)

                if last_err is not None or resp is None:
                    err_str = str(last_err) if last_err else "no response"
                    if "PERMISSION_DENIED" in err_str:
                        context.log.error(
                            "Vision API: 403 PERMISSION_DENIED. Service account needs "
                            "roles/serviceusage.serviceUsageConsumer. Vision API must be enabled."
                        )
                    results.append({"_error": err_str})
                    continue

                if getattr(resp, "error", None) and getattr(resp.error, "message", ""):
                    results.append({"_error": resp.error.message})
                    continue

                row_out: Dict[str, Any] = {}
                for f in features:
                    field = _FEATURE_MAP[f]
                    val = getattr(resp, field, None)
                    if f == "LABEL_DETECTION" and val:
                        row_out[f"{output_prefix}labels"] = [
                            {"description": l.description, "score": float(l.score)} for l in val
                        ]
                    elif f == "OBJECT_LOCALIZATION" and val:
                        row_out[f"{output_prefix}objects"] = [
                            {"name": o.name, "score": float(o.score)} for o in val
                        ]
                    elif f == "FACE_DETECTION" and val:
                        row_out[f"{output_prefix}faces"] = len(val)
                        row_out[f"{output_prefix}face_emotions"] = [
                            {
                                "joy":     str(face.joy_likelihood).split(".")[-1],
                                "sorrow":  str(face.sorrow_likelihood).split(".")[-1],
                                "anger":   str(face.anger_likelihood).split(".")[-1],
                                "surprise":str(face.surprise_likelihood).split(".")[-1],
                            } for face in val
                        ]
                    elif f == "LANDMARK_DETECTION" and val:
                        row_out[f"{output_prefix}landmarks"] = [
                            {"name": l.description, "score": float(l.score)} for l in val
                        ]
                    elif f == "LOGO_DETECTION" and val:
                        row_out[f"{output_prefix}logos"] = [
                            {"name": l.description, "score": float(l.score)} for l in val
                        ]
                    elif f == "TEXT_DETECTION" and val:
                        row_out[f"{output_prefix}text"] = val[0].description if val else None
                    elif f == "DOCUMENT_TEXT_DETECTION" and val:
                        row_out[f"{output_prefix}doc_text"] = val.text if val else None
                    elif f == "SAFE_SEARCH_DETECTION" and val:
                        row_out[f"{output_prefix}safesearch"] = {
                            "adult":     str(val.adult).split(".")[-1],
                            "violence":  str(val.violence).split(".")[-1],
                            "racy":      str(val.racy).split(".")[-1],
                            "spoof":     str(val.spoof).split(".")[-1],
                            "medical":   str(val.medical).split(".")[-1],
                        }
                    elif f == "IMAGE_PROPERTIES" and val and val.dominant_colors and val.dominant_colors.colors:
                        top = val.dominant_colors.colors[0]
                        row_out[f"{output_prefix}dominant_color_rgb"] = (
                            int(top.color.red or 0), int(top.color.green or 0), int(top.color.blue or 0),
                        )
                        row_out[f"{output_prefix}dominant_color_score"] = float(top.score or 0.0)
                    elif f == "WEB_DETECTION" and val:
                        row_out[f"{output_prefix}web_entities"] = [
                            {"description": e.description, "score": float(e.score)} for e in (val.web_entities or [])
                        ]
                    elif f == "CROP_HINTS" and val and val.crop_hints:
                        row_out[f"{output_prefix}crop_hints"] = len(val.crop_hints)
                results.append(row_out)
                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)

            results_df = pd.DataFrame(results)
            out = pd.concat([df.reset_index(drop=True), results_df.reset_index(drop=True)], axis=1)
            ok = sum(1 for r in results if "_error" not in r)
            return Output(
                value=out,
                metadata={
                    "rows":           MetadataValue.int(len(out)),
                    "ok":             MetadataValue.int(ok),
                    "features":       MetadataValue.json(features),
                    "preview":        MetadataValue.md(out.head(5).to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
