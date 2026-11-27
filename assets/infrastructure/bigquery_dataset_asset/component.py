"""BigQueryDatasetAssetComponent.

Create/ensure/delete a BigQuery dataset as a Dagster asset -- the Airflow
`BigQueryCreateEmptyDatasetOperator` / `BigQueryDeleteDatasetOperator`
equivalent. Real `google-cloud-bigquery` client calls throughout
(`client.create_dataset`, `client.delete_dataset`), verified against the
installed library's `Dataset`/`DatasetReference` API.
"""
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

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
    """Construct a Dagster partitions_def from the canonical partition fields."""
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
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01').")
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
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
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


def _bigquery_client(credentials, credentials_path, project_id, location):
    import json
    import os

    from google.cloud import bigquery
    from google.oauth2 import service_account

    creds_dict = credentials
    if creds_dict is None:
        cred_path = credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if cred_path:
            with open(cred_path, "r") as fh:
                creds_dict = json.load(fh)
    if creds_dict is None:
        raise ValueError("provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")
    sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
    project = project_id or creds_dict.get("project_id")
    return bigquery.Client(credentials=sa_creds, project=project, location=location), project


class BigQueryDatasetAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Create, ensure-exists, recreate, or delete a BigQuery dataset.

    `mode='ensure_exists'` (default) creates the dataset if missing and is a
    no-op otherwise (idempotent, `exists_ok=True`). `mode='recreate'` deletes
    the dataset (with its contents) and creates it fresh. `mode='delete'`
    deletes it -- set `delete_contents: true` to also drop a non-empty
    dataset, matching `google.cloud.bigquery.Client.delete_dataset`'s own
    `delete_contents` safety default (`False`).
    """

    asset_name: str = Field(description="Dagster asset key.")
    credentials: Optional[Dict[str, Any]] = Field(default=None, description="Service-account JSON as a dict.")
    credentials_path: Optional[str] = Field(default=None, description="Path to a service-account JSON file. Falls back to GOOGLE_APPLICATION_CREDENTIALS.")
    project_id: Optional[str] = Field(default=None, description="GCP project. Defaults to the service account's own project.")
    dataset_id: str = Field(description="Dataset id, e.g. 'analytics' (uses project_id) or 'my-project.analytics'.")
    location: Optional[str] = Field(default=None, description="BigQuery dataset location, e.g. 'US' or 'EU'. Required when creating a new dataset.")
    mode: Literal["ensure_exists", "recreate", "delete"] = Field(default="ensure_exists", description="'ensure_exists' creates if missing (idempotent). 'recreate' drops and recreates. 'delete' removes the dataset.")
    delete_contents: bool = Field(default=False, description="For mode='recreate'/'delete': also delete a non-empty dataset's tables. False raises if the dataset still has tables.")
    default_table_expiration_ms: Optional[int] = Field(default=None, description="Default expiration (ms) applied to new tables created in this dataset.")
    default_partition_expiration_ms: Optional[int] = Field(default=None, description="Default partition expiration (ms) for partitioned tables created in this dataset.")
    labels: Optional[Dict[str, str]] = Field(default=None, description="Dataset labels.")
    dataset_description: Optional[str] = Field(default=None, description="BigQuery dataset description (distinct from the Dagster asset's own `description`).")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['gcp', 'bigquery'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'cron' | None.")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d")
    partition_end_offset: int = Field(default=0)
    partition_timezone: Optional[str] = Field(default=None)

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None)
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps"), csv=("partition_values",), tags=("asset_tags", "labels"))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Big Query Dataset Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["gcp", "bigquery"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(int(self.freshness_max_lag_minutes), self.freshness_cron)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = dg.BackfillPolicy.multi_run(max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1)

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=self.description or f"BigQuery dataset ({self.mode}): {self.dataset_id}",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
            backfill_policy=backfill_policy,
        )
        def _bigquery_dataset_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            from google.api_core.exceptions import NotFound
            from google.cloud import bigquery

            client, project = _bigquery_client(self.credentials, self.credentials_path, self.project_id, self.location)
            dataset_id = self.dataset_id if "." in self.dataset_id else f"{project}.{self.dataset_id}"
            dataset_ref = bigquery.DatasetReference.from_string(dataset_id, default_project=project)

            if self.mode in ("recreate", "delete"):
                client.delete_dataset(dataset_ref, delete_contents=self.delete_contents, not_found_ok=True)
                context.log.info(f"bigquery_dataset_asset: deleted {dataset_id!r} (delete_contents={self.delete_contents})")

            result_dataset = None
            if self.mode in ("ensure_exists", "recreate"):
                ds = bigquery.Dataset(dataset_ref)
                if self.location:
                    ds.location = self.location
                if self.default_table_expiration_ms is not None:
                    ds.default_table_expiration_ms = self.default_table_expiration_ms
                if self.default_partition_expiration_ms is not None:
                    ds.default_partition_expiration_ms = self.default_partition_expiration_ms
                if self.labels:
                    ds.labels = dict(self.labels)
                if self.dataset_description:
                    ds.description = self.dataset_description
                result_dataset = client.create_dataset(ds, exists_ok=True)
                context.log.info(f"bigquery_dataset_asset: ensured {dataset_id!r} exists (location={result_dataset.location})")

            metadata: Dict[str, Any] = {
                "dataset_id": dg.MetadataValue.text(dataset_id),
                "mode": dg.MetadataValue.text(self.mode),
            }
            if result_dataset is not None:
                metadata["location"] = dg.MetadataValue.text(result_dataset.location or "")
                if result_dataset.labels:
                    metadata["labels"] = dg.MetadataValue.json(dict(result_dataset.labels))
            else:
                try:
                    client.get_dataset(dataset_ref)
                    metadata["exists_after_delete"] = dg.MetadataValue.bool(True)
                except NotFound:
                    metadata["exists_after_delete"] = dg.MetadataValue.bool(False)

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_bigquery_dataset_asset])
