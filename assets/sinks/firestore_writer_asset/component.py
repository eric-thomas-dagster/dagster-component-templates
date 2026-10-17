"""FirestoreWriterAssetComponent — write DataFrame rows to a Firestore collection.

Each row of the upstream DataFrame becomes one Firestore document.
The doc id is taken from `id_column` if set, otherwise auto-generated.
Supports merge / overwrite behavior on existing documents.

Useful as a real-time-readable sink behind ML scoring, customer 360
denormalized views, dynamic config tables.
"""

import json
import os
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


class FirestoreWriterAssetComponent(Component, Model, Resolvable):
    """Write DataFrame rows to a Firestore collection — one doc per row."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream DataFrame asset key.")

    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional key of a registered FirestoreResourceComponent (resources/firestore_resource). When set, connection "
            "credentials are read from that resource at run time instead of the inline "
            "credentials/credentials_path/project_id fields -- lets one credential serve this sink and other components "
            "without configuring it twice. Leave unset to use the inline fields "
            "(existing behavior, unchanged)."
        ),
    )
    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None)
    database: str = Field(default="(default)")
    collection: str = Field(description="Collection path (e.g. 'customers' or 'tenants/acme/customers').")

    id_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column whose value becomes each document's id. If unset, Firestore auto-generates ids.",
    )
    write_mode: Literal["set", "merge", "create"] = Field(
        default="merge",
        description=(
            "set: overwrite the doc; merge: deep-merge keys (preserves untouched fields); "
            "create: fail if doc already exists."
        ),
    )
    drop_id_column_from_body: bool = Field(
        default=True,
        description="If True (default), the id_column is omitted from the doc body when used as id.",
    )
    batch_size: int = Field(default=500, description="Firestore commit batch size (max 500 per batch).")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
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
        return coerce_field_shapes(data, lists=('deps', 'owners'), csv=('partition_values',), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

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

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        creds_dict = self.credentials
        if creds_dict is None and not self.resource_key:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None and not self.resource_key:
            raise ValueError(
                "Provide resource_key (a registered FirestoreResourceComponent), credentials, "
                "credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS."
            )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        project_id = self.project_id or (creds_dict or {}).get("project_id")
        database = self.database
        collection = self.collection
        id_column = self.id_column
        write_mode = self.write_mode
        drop_id = self.drop_id_column_from_body
        batch_size = self.batch_size

        @asset(
            required_resource_keys=({self.resource_key} if self.resource_key else None),
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Firestore write to {collection} in {project_id}/{database}.",
            group_name=self.group_name,
            kinds={"google", "firestore"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            try:
                from google.cloud import firestore
                from google.oauth2 import service_account
                from google.api_core.exceptions import AlreadyExists
            except ImportError:
                raise ImportError("pip install google-cloud-firestore google-auth")

            if self.resource_key:
                client = getattr(context.resources, self.resource_key).get_client()
            else:
                sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
                client = firestore.Client(project=project_id, credentials=sa_creds, database=database)
            col = client.collection(collection)

            df = upstream.copy().reset_index(drop=True)
            if id_column and id_column not in df.columns:
                raise ValueError(f"id_column={id_column!r} not in upstream: {list(df.columns)}")

            written = 0
            errors: List[Dict[str, Any]] = []
            batch = client.batch()
            ops_in_batch = 0

            for i, row in df.iterrows():
                body = row.to_dict()
                if id_column:
                    doc_id = str(body[id_column])
                    if drop_id:
                        body.pop(id_column, None)
                    doc_ref = col.document(doc_id)
                else:
                    doc_ref = col.document()  # auto-id

                # Convert pandas-y NaN/NaT to None for Firestore.
                body = {k: (None if (isinstance(v, float) and pd.isna(v)) else v) for k, v in body.items()}

                if write_mode == "set":
                    batch.set(doc_ref, body, merge=False)
                elif write_mode == "merge":
                    batch.set(doc_ref, body, merge=True)
                else:  # create
                    batch.create(doc_ref, body)
                ops_in_batch += 1

                if ops_in_batch >= batch_size:
                    try:
                        batch.commit()
                        written += ops_in_batch
                    except AlreadyExists as e:
                        errors.append({"row": int(i), "error": str(e)})
                    except Exception as e:
                        errors.append({"row": int(i), "error": str(e)})
                    batch = client.batch()
                    ops_in_batch = 0

            if ops_in_batch > 0:
                try:
                    batch.commit()
                    written += ops_in_batch
                except Exception as e:
                    errors.append({"error": str(e)})

            df_out = df.copy()
            df_out["_firestore_written"] = True
            md = {
                "collection":   MetadataValue.text(collection),
                "rows_written": MetadataValue.int(written),
                "errors":       MetadataValue.int(len(errors)),
                "preview":      MetadataValue.md(df_out.head(10).to_markdown(index=False) or ""),
            }
            if errors:
                md["error_sample"] = MetadataValue.json(errors[:5])
            return Output(value=df_out, metadata=md)

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
