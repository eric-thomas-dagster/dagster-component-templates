"""PerPartitionBackfillJobComponent.

Schedule a parallel backfill of N partitions of an existing partitioned asset.

Two strategies for picking partitions:
  - last_n_days  : the most recent N daily partitions, ending today UTC
  - explicit     : user-supplied list of partition keys in `partition_keys`
  - callable     : 'module:function() -> list[str]' returns the keys

Each picked partition becomes a DynamicOutput; the process op materializes
that single partition in-process via `dg.materialize`.

This complements (does not replace) Dagster's first-class backfill machinery.
Use this when you want a simple cron-driven recurring backfill that fits in
one job — e.g. "every Monday at 9am, refresh the last 7 days of sales_daily".

For larger / less predictable backfills, prefer the Dagster UI backfill flow
or `dg backfill` CLI.
"""

import importlib
from typing import Optional

import dagster as dg
from pydantic import Field


def _resolve(callable_path: str):
    module_path, fn_name = callable_path.split(":")
    mod = importlib.import_module(module_path)
    return getattr(mod, fn_name)


class PerPartitionBackfillJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Parallel backfill of N partitions of an existing partitioned asset."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule")
    default_status: str = Field(default="STOPPED")

    target_asset_key: str = Field(description="Partitioned asset to materialize per partition (must already be defined elsewhere)")

    partition_strategy: str = Field(
        default="last_n_days",
        description="last_n_days | explicit | callable",
    )
    partition_count: int = Field(default=7, description="Number of partitions to backfill (when strategy=last_n_days)")
    partition_keys: Optional[list[str]] = Field(
        default=None, description="Explicit partition keys (when strategy=explicit)"
    )
    partition_callable_path: Optional[str] = Field(
        default=None,
        description="'module:function() -> list[str]' (when strategy=callable)",
    )

    max_concurrent_tag_value: Optional[str] = Field(default=None)
    retry_max_retries: Optional[int] = Field(default=None)
    retry_delay_seconds: Optional[int] = Field(default=None)
    fail_on_empty: bool = Field(default=False)
    job_tags: Optional[dict] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        retry = None
        if self.retry_max_retries:
            retry = dg.RetryPolicy(
                max_retries=self.retry_max_retries,
                delay=self.retry_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL,
            )
        op_tags = {"dagster/concurrency_key": self.max_concurrent_tag_value} if self.max_concurrent_tag_value else None

        def _resolve_keys():
            import datetime as dt
            if _self.partition_strategy == "last_n_days":
                today = dt.date.today()
                return [(today - dt.timedelta(days=i)).isoformat() for i in range(_self.partition_count)]
            if _self.partition_strategy == "explicit":
                return list(_self.partition_keys or [])
            if _self.partition_strategy == "callable":
                if not _self.partition_callable_path:
                    raise ValueError("partition_callable_path is required when strategy=callable")
                return list(_resolve(_self.partition_callable_path)())
            raise ValueError(f"unknown partition_strategy: {_self.partition_strategy}")

        @dg.op(out=dg.DynamicOut())
        def _discover_partitions(context):
            keys = _resolve_keys()
            context.log.info(f"will backfill {len(keys)} partition(s) of {_self.target_asset_key}")
            if not keys and _self.fail_on_empty:
                raise Exception("no partitions; fail_on_empty=True")
            for k in keys:
                # mapping keys can't contain dots / slashes
                safe_key = k.replace("/", "_").replace(".", "_")
                yield dg.DynamicOutput({"partition_key": k, "asset_key": _self.target_asset_key}, mapping_key=safe_key)

        @dg.op(retry_policy=retry, tags=op_tags)
        def _materialize_partition(context, info):
            from dagster import materialize, AssetKey
            # Resolve the asset definition from the loaded code location.
            instance = context.instance
            try:
                asset_def = instance.get_asset_value_loader  # placeholder check
            except Exception:
                pass
            # In practice, we use dg.materialize against the asset definitions
            # already loaded in this job's repository. The asset must be loadable
            # by AssetKey lookup; this is the standard backfill-in-process path.
            # NOTE: this requires the target asset to live in the same code
            # location as this job. For cross-location backfills, use dg backfill
            # or the Dagster UI.
            ak = AssetKey.from_user_string(info["asset_key"])
            try:
                # Look up the asset def from this code location's repo.
                from dagster._core.definitions.repository_definition import RepositoryDefinition  # noqa
                asset_def = None
                for asset in (context.repository_def.assets_defs_by_key if hasattr(context, "repository_def") else {}).values():
                    if ak in asset.keys:
                        asset_def = asset
                        break
                if not asset_def:
                    raise Exception(f"asset {info['asset_key']} not found in this code location")
                result = materialize(
                    [asset_def],
                    partition_key=info["partition_key"],
                    instance=instance,
                )
                context.log.info(f"materialized {info['asset_key']}/{info['partition_key']} success={result.success}")
                return {"partition_key": info["partition_key"], "success": bool(result.success)}
            except Exception as exc:
                context.log.error(f"failed {info['asset_key']}/{info['partition_key']}: {exc}")
                raise

        @dg.op
        def _summary(context, results: list):
            success = sum(1 for r in results if r.get("success"))
            context.log.info(f"backfill summary: {success}/{len(results)} succeeded")
            return {"total": len(results), "success": success, "failed": len(results) - success}

        @dg.job(name=self.job_name, tags=self.job_tags or None)
        def _the_job():
            parts = _discover_partitions()
            done = parts.map(_materialize_partition)
            _summary(done.collect())

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule, job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
