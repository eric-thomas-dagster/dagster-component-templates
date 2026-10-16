"""MemcachedCacheFlushJobComponent.

Flush the entire Memcached instance (or delete specific keys) as a Dagster job.

Memcached has no SCAN / pattern-match — the wire protocol only supports
`get` / `set` / `delete` / `flush_all`. So this component's `keys:` list
does explicit per-key `delete`, or `flush_all` if no keys given.
"""

from typing import List, Optional

import dagster as dg
from pydantic import Field



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


class MemcachedCacheFlushJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Flush Memcached — either the whole cache (`flush_all`) or a specific key list."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Dagster job tags")

    host: str = Field(default="localhost", description="Memcached host")
    port: int = Field(default=11211, description="Memcached port")
    keys: Optional[List[str]] = Field(
        default=None,
        description=(
            "Explicit list of keys to delete. When None or empty, calls "
            "`flush_all` (nukes everything). Memcached has no SCAN, so "
            "pattern-based deletes aren't supported — enumerate the keys."
        ),
    )
    timeout_seconds: float = Field(default=3.0, description="Connect + read/write timeout")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self
            from pymemcache.client.base import Client
            client = Client(
                (self.host, self.port),
                connect_timeout=self.timeout_seconds,
                timeout=self.timeout_seconds,
            )
            if self.keys:
                deleted = 0
                for k in self.keys:
                    if client.delete(k, noreply=False):
                        deleted += 1
                context.log.info(f"deleted {deleted} of {len(self.keys)} keys")
            else:
                client.flush_all()
                context.log.info("flushed all keys (flush_all)")

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _the_op()

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
