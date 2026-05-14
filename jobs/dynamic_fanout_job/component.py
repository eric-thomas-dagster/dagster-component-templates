"""DynamicFanoutJobComponent.

Generic DynamicOut-based op job. Wraps the canonical Dagster fan-out pattern:

    discover()  → returns a list of items (resolved at runtime)
    .map()      → process each item in parallel via the run executor
    .collect()  → optional: aggregate the results

User provides 2-3 Python callables (`module:function` paths). The component
generates the ops, wires them into a `@dg.job`, and optionally attaches a
schedule. No assets — clean lineage graph.

When to use this vs partitions
------------------------------
- Partitions: items are a stable, identifiable thing worth tracking in the
  catalog (a tenant, a date, a region) — you want per-item history.
- DynamicOut: items are ephemeral within a run (URLs that exist right now,
  files currently in a queue) — you want parallel processing without
  persistence overhead.
"""

import importlib
import os
from typing import Optional

import dagster as dg
from pydantic import Field


def _resolve(callable_path: str):
    module_path, fn_name = callable_path.split(":")
    mod = importlib.import_module(module_path)
    return getattr(mod, fn_name)


class DynamicFanoutJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Generic DynamicOut fan-out: discover → map → optionally collect."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    job_tags: Optional[dict] = Field(default=None, description="Tags applied to the job")

    discover_callable_path: str = Field(
        description="'module:function' that returns a list of items (each item is passed to the process callable)"
    )
    discover_kwargs: Optional[dict] = Field(
        default=None, description="kwargs passed to the discover callable"
    )
    process_callable_path: str = Field(
        description="'module:function' that takes one item and returns a result"
    )
    process_kwargs: Optional[dict] = Field(
        default=None, description="extra kwargs merged into each process call"
    )
    collect_callable_path: Optional[str] = Field(
        default=None,
        description="Optional 'module:function' that takes the list of results and returns the final value",
    )

    mapping_key_field: Optional[str] = Field(
        default=None,
        description="If items are dicts, use this field as the DynamicOutput mapping_key (stable per-item retries). None = use the index.",
    )

    max_concurrent_tag_value: Optional[str] = Field(
        default=None,
        description="If set, applies tag {'dagster/concurrency_key': <value>} to the process op so an instance-level concurrency limit governs fan-out parallelism.",
    )

    retry_max_retries: Optional[int] = Field(default=None, description="Per-item retry max")
    retry_delay_seconds: Optional[int] = Field(default=None)
    retry_backoff: str = Field(default="exponential")

    fail_on_empty: bool = Field(default=False, description="Fail the job if discover returns no items")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        retry = None
        if self.retry_max_retries:
            retry = dg.RetryPolicy(
                max_retries=self.retry_max_retries,
                delay=self.retry_delay_seconds or 1,
                backoff=dg.Backoff.EXPONENTIAL if self.retry_backoff == "exponential" else dg.Backoff.LINEAR,
            )

        op_tags = {"dagster/concurrency_key": self.max_concurrent_tag_value} if self.max_concurrent_tag_value else None

        @dg.op(out=dg.DynamicOut())
        def _discover(context):
            fn = _resolve(_self.discover_callable_path)
            items = fn(**(_self.discover_kwargs or {}))
            items = list(items)
            context.log.info(f"discovered {len(items)} item(s) via {_self.discover_callable_path}")
            if not items and _self.fail_on_empty:
                raise Exception("discover returned no items and fail_on_empty=True")
            for i, item in enumerate(items):
                if _self.mapping_key_field and isinstance(item, dict) and _self.mapping_key_field in item:
                    key = str(item[_self.mapping_key_field])
                else:
                    key = str(i)
                # mapping keys must be unique within the run — Dagster enforces this
                yield dg.DynamicOutput(item, mapping_key=key)

        @dg.op(retry_policy=retry, tags=op_tags)
        def _process(context, item):
            fn = _resolve(_self.process_callable_path)
            extra = _self.process_kwargs or {}
            result = fn(item, **extra)
            context.log.info(f"processed item -> {str(result)[:200]}")
            return result

        @dg.op(name=f"{self.job_name}_op")
        def _collect(context, results: list):
            if _self.collect_callable_path:
                fn = _resolve(_self.collect_callable_path)
                final = fn(results)
                context.log.info(f"collected {len(results)} result(s) -> {str(final)[:200]}")
                return final
            return results

        @dg.job(name=self.job_name, tags=self.job_tags or None)
        def _the_job():
            items = _discover()
            processed = items.map(_process)
            _collect(processed.collect())

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
