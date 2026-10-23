"""OpenlineageEmitterComponent.

Emit OpenLineage events when an upstream asset materializes — wires Dagster's
asset lineage into OpenLineage-compatible backends (Marquez, DataHub, OpenMetadata,
Astro Observe, Atlan, etc.).

This is an asset_check that runs after the upstream asset materializes; it
constructs a START/COMPLETE event pair conforming to the OpenLineage v1.x spec
and POSTs to the configured `openlineage_url`.

Authentication
--------------
If your OL backend requires auth, set `auth_token_env` to the env var name
holding the bearer token.

Backends tested against
-----------------------
- Marquez (open-source reference impl)
- DataHub (the OL receiver endpoint)
- OpenMetadata
- Astro Observe / Atlan

The exact field shapes and required producer URI vary slightly by backend —
see backend docs.
"""

import json
import os
import time
import datetime as dt
from typing import Optional
from uuid import uuid4

import dagster as dg
import pandas as pd
from pydantic import Field


def _post_with_retry(url, json_body, headers, timeout, max_attempts=3, backoff_seconds=1.0):
    """POST an OpenLineage event, retrying a connection failure/timeout or a 5xx
    response (both plausibly transient) with exponential backoff. A 4xx fails
    immediately without retrying -- a malformed event or bad auth won't fix
    itself by resending. Returns (ok, error_message_or_None)."""
    import requests

    last_error = None
    for attempt in range(max_attempts):
        try:
            r = requests.post(url, json=json_body, headers=headers, timeout=timeout)
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        else:
            if r.status_code < 300:
                return True, None
            if r.status_code < 500:
                return False, f"HTTP {r.status_code} — {r.text[:200]}"
            last_error = f"HTTP {r.status_code} — {r.text[:200]}"
        if attempt < max_attempts - 1:
            time.sleep(backoff_seconds * (2 ** attempt))
    return False, last_error



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


class OpenlineageEmitterComponent(dg.Component, dg.Model, dg.Resolvable):
    """Asset check that emits OpenLineage START + COMPLETE events for an upstream asset."""

    check_name: str = Field(description="Asset check name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Asset whose materialization triggers an OL event")

    openlineage_url: str = Field(description="OL backend endpoint (e.g. 'https://marquez.acme.com/api/v1/lineage')")
    auth_token_env: Optional[str] = Field(default=None, description="Env var with bearer token (omit for unauthenticated)")

    namespace: str = Field(default="dagster", description="OL namespace for the asset")
    producer_url: str = Field(
        default="https://github.com/eric-thomas-dagster/dagster-component-templates",
        description="`producer` URL field on the event (identifies the emitter)",
    )
    job_namespace: Optional[str] = Field(default=None, description="Override job namespace (defaults to `namespace`)")
    job_name_override: Optional[str] = Field(default=None, description="Override OL job name (default: derived from asset)")

    schema_facet: bool = Field(default=True, description="Include the schema facet inferred from DataFrame columns")
    column_lineage_facet: Optional[dict] = Field(
        default=None,
        description="Optional column-lineage map: {output_col: [input_col, ...]} → emitted as the columnLineage facet",
    )
    upstream_input_assets: Optional[list[str]] = Field(
        default=None,
        description="Asset keys to declare as `inputs` on the OL job (None = no inputs declared)",
    )

    description: Optional[str] = Field(default=None)
    blocking: bool = Field(default=False, description="Block downstream when emit fails")
    timeout_seconds: int = Field(default=30)
    retry_max_attempts: int = Field(
        default=3,
        description=(
            "Max attempts per event POST. Retries a connection failure/timeout or a "
            "5xx response; a 4xx fails immediately without retrying. Set to 1 to "
            "disable retries entirely."
        ),
    )
    retry_backoff_seconds: float = Field(
        default=1.0,
        description="Base delay between retry attempts; doubles each attempt (1s, 2s, 4s, ...).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset_check(
            name=self.check_name,
            asset=dg.AssetKey.from_user_string(self.upstream_asset_key),
            blocking=self.blocking,
            description=self.description or "Emit OpenLineage events for this asset",
        )
        def _check(df: pd.DataFrame) -> dg.AssetCheckResult:
            run_id = str(uuid4())
            now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
            job_namespace = _self.job_namespace or _self.namespace
            job_name = _self.job_name_override or _self.upstream_asset_key.replace("/", ".")

            base_event = {
                "eventTime": now_iso,
                "producer": _self.producer_url,
                "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent",
                "run": {"runId": run_id},
                "job": {"namespace": job_namespace, "name": job_name},
                "inputs": [
                    {"namespace": _self.namespace, "name": k}
                    for k in (_self.upstream_input_assets or [])
                ],
                "outputs": [{
                    "namespace": _self.namespace,
                    "name": _self.upstream_asset_key,
                }],
            }

            # Schema facet on the output
            if _self.schema_facet and len(df) > 0:
                fields = [
                    {"name": str(c), "type": str(df[c].dtype)}
                    for c in df.columns
                ]
                base_event["outputs"][0]["facets"] = {
                    "schema": {
                        "_producer": _self.producer_url,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json",
                        "fields": fields,
                    }
                }

            # Column lineage facet
            if _self.column_lineage_facet:
                facets = base_event["outputs"][0].setdefault("facets", {})
                facets["columnLineage"] = {
                    "_producer": _self.producer_url,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
                    "fields": {
                        out_col: {
                            "inputFields": [
                                {"namespace": _self.namespace, "name": ".".join(in_col.split(".")[:-1]) or _self.upstream_asset_key,
                                 "field": in_col.split(".")[-1]}
                                for in_col in in_cols
                            ]
                        }
                        for out_col, in_cols in _self.column_lineage_facet.items()
                    },
                }

            headers = {"Content-Type": "application/json"}
            if _self.auth_token_env:
                token = os.environ.get(_self.auth_token_env)
                if token:
                    headers["Authorization"] = f"Bearer {token}"

            errors = []
            for event_type in ("START", "COMPLETE"):
                event = {**base_event, "eventType": event_type, "eventTime": dt.datetime.now(dt.timezone.utc).isoformat()}
                ok, err = _post_with_retry(
                    _self.openlineage_url, event, headers, _self.timeout_seconds,
                    _self.retry_max_attempts, _self.retry_backoff_seconds,
                )
                if not ok:
                    errors.append(f"{event_type}: {err}")

            passed = not errors
            return dg.AssetCheckResult(
                passed=passed,
                severity=dg.AssetCheckSeverity.ERROR if not passed else dg.AssetCheckSeverity.WARN,
                metadata={
                    "openlineage_url": dg.MetadataValue.text(_self.openlineage_url),
                    "namespace": dg.MetadataValue.text(_self.namespace),
                    "ol_run_id": dg.MetadataValue.text(run_id),
                    "ol_job_name": dg.MetadataValue.text(job_name),
                    "errors": dg.MetadataValue.json(errors),
                    "row_count": dg.MetadataValue.int(int(len(df))),
                },
            )

        return dg.Definitions(asset_checks=[_check])
