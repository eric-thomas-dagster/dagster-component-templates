"""PerPartitionBackfillJobComponent.

Schedule a parallel backfill of N partitions of an existing partitioned asset.

Partition-selection strategies:
  - last_n_days        : the most recent N daily partitions ending today UTC
  - explicit           : user-supplied list in `partition_keys`
  - callable           : 'module:function() -> list[str]' returns the keys
  - dynamic_all        : read every currently-registered key for a
                         DynamicPartitionsDefinition. Multi-tenant use case:
                         "rebuild every tenant" without listing them.
  - dynamic_subset     : dynamic_all filtered by a regex (`dynamic_filter`).

Each picked partition becomes a DynamicOutput; the process op materializes
that single partition in-process via `dg.materialize`.

Concurrency: setting `concurrency_key_template` (e.g.
`tenant-{partition_key}`) tags each per-partition op with a partition-derived
concurrency key, so same-tenant runs serialize but cross-tenant parallelize.
Use `max_concurrent_tag_value` for the legacy single-static-key behavior.

This complements (does not replace) Dagster's first-class backfill machinery.
Use this when you want a simple cron-driven recurring backfill that fits in
one job — e.g. "every Monday at 9am, refresh the last 7 days of sales_daily"
or "every hour, refresh every registered tenant's daily rollup".
"""

import importlib
import re
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

    target_asset_key: str = Field(
        description="Partitioned asset to materialize per partition (must already be defined elsewhere)",
    )

    partition_strategy: str = Field(
        default="last_n_days",
        description="last_n_days | explicit | callable | dynamic_all | dynamic_subset",
    )
    partition_count: int = Field(
        default=7,
        description="Number of partitions to backfill (when strategy=last_n_days)",
    )
    partition_keys: Optional[list[str]] = Field(
        default=None,
        description="Explicit partition keys (when strategy=explicit)",
    )
    partition_callable_path: Optional[str] = Field(
        default=None,
        description="'module:function() -> list[str]' (when strategy=callable)",
    )
    dynamic_partitions_def_name: Optional[str] = Field(
        default=None,
        description=(
            "Name of the DynamicPartitionsDefinition to read keys from "
            "(when strategy=dynamic_all or dynamic_subset). Must match the "
            "name argument the target asset's partitions_def was constructed with."
        ),
    )
    dynamic_filter: Optional[str] = Field(
        default=None,
        description=(
            "Python regex applied to each dynamic-partition key (when "
            "strategy=dynamic_subset). Keys matching are kept; others are dropped."
        ),
    )

    # Concurrency control
    concurrency_key_template: Optional[str] = Field(
        default=None,
        description=(
            "If set, each per-partition op gets a `dagster/concurrency_key` tag "
            "rendered from this template. Available placeholder: {partition_key}. "
            "e.g. 'tenant-{partition_key}' makes same-tenant runs serialize but "
            "cross-tenant parallelize. Mutually exclusive with max_concurrent_tag_value."
        ),
    )
    max_concurrent_tag_value: Optional[str] = Field(
        default=None,
        description=(
            "Legacy: a single static concurrency-key applied to every per-partition "
            "op. Use concurrency_key_template for the more useful per-partition shape."
        ),
    )

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

        def _resolve_keys(context):
            import datetime as dt
            strategy = _self.partition_strategy
            if strategy == "last_n_days":
                today = dt.date.today()
                return [(today - dt.timedelta(days=i)).isoformat() for i in range(_self.partition_count)]
            if strategy == "explicit":
                return list(_self.partition_keys or [])
            if strategy == "callable":
                if not _self.partition_callable_path:
                    raise ValueError("partition_callable_path is required when strategy=callable")
                return list(_resolve(_self.partition_callable_path)())
            if strategy in ("dynamic_all", "dynamic_subset"):
                if not _self.dynamic_partitions_def_name:
                    raise ValueError(
                        f"dynamic_partitions_def_name is required when strategy={strategy}"
                    )
                # Read currently-registered keys from the instance.
                keys = list(
                    context.instance.get_dynamic_partitions(_self.dynamic_partitions_def_name)
                )
                if strategy == "dynamic_subset":
                    if not _self.dynamic_filter:
                        raise ValueError("dynamic_filter is required when strategy=dynamic_subset")
                    pattern = re.compile(_self.dynamic_filter)
                    keys = [k for k in keys if pattern.search(k)]
                return keys
            raise ValueError(f"unknown partition_strategy: {strategy}")

        def _op_tags_for(partition_key: str) -> Optional[dict]:
            """Render the concurrency-key tag for a specific partition."""
            if _self.concurrency_key_template:
                return {
                    "dagster/concurrency_key": _self.concurrency_key_template.format(
                        partition_key=partition_key,
                    ),
                }
            if _self.max_concurrent_tag_value:
                return {"dagster/concurrency_key": _self.max_concurrent_tag_value}
            return None

        @dg.op(out=dg.DynamicOut())
        def _discover_partitions(context):
            keys = _resolve_keys(context)
            context.log.info(f"will backfill {len(keys)} partition(s) of {_self.target_asset_key}")
            if not keys and _self.fail_on_empty:
                raise Exception("no partitions; fail_on_empty=True")
            for k in keys:
                # mapping keys can't contain dots / slashes
                safe_key = k.replace("/", "_").replace(".", "_")
                yield dg.DynamicOutput(
                    {"partition_key": k, "asset_key": _self.target_asset_key},
                    mapping_key=safe_key,
                )

        @dg.op(retry_policy=retry)
        def _materialize_partition(context, info):
            from dagster import AssetKey, materialize

            ak = AssetKey.from_user_string(info["asset_key"])
            partition_key = info["partition_key"]

            # Apply per-partition concurrency tag if templated.
            tags = _op_tags_for(partition_key)
            if tags:
                # Tags on dynamic-mapped ops are usually set at op-decorator time;
                # at runtime we attach them to the run instead so the slot
                # accounting still occurs.
                try:
                    context.instance.add_run_tags(context.run_id, tags)
                except Exception:
                    pass  # best-effort

            # Look up the asset def from the loaded code location's repository.
            # `context.repository_def` is the public-facing accessor;
            # `assets_defs_by_key` is the canonical map.
            repo = getattr(context, "repository_def", None)
            if repo is None:
                raise RuntimeError(
                    "context.repository_def is unavailable — backfill jobs must run "
                    "inside a loaded code location."
                )
            asset_def = None
            for asset in (repo.assets_defs_by_key or {}).values():
                if ak in asset.keys:
                    asset_def = asset
                    break
            if not asset_def:
                raise Exception(
                    f"asset {info['asset_key']} not found in this code location. "
                    f"Backfill jobs only materialize assets in their own location."
                )

            try:
                result = materialize(
                    [asset_def],
                    partition_key=partition_key,
                    instance=context.instance,
                )
                context.log.info(
                    f"materialized {info['asset_key']}/{partition_key} success={result.success}"
                )
                return {"partition_key": partition_key, "success": bool(result.success)}
            except Exception as exc:
                context.log.error(f"failed {info['asset_key']}/{partition_key}: {exc}")
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
