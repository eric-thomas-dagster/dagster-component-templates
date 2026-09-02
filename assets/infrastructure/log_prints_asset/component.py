"""LogPrintsAssetComponent + `@log_prints` — capture print() into Dagster logs.

Stolen from Prefect's `@flow(log_prints=True)`. Redirects `print()` and
`sys.stdout.write()` calls inside the compute function to
`context.log.info`, so print-heavy scripts land in the Dagster event log
naturally without rewriting them.

## Why this belongs in Dagster

Every captured print line becomes a real Dagster log event — searchable
by run_id, visible in the UI's log panel, respected by the run monitor.
No monkey-patching Dagster; just a stdout redirect scoped to one
compute call.

## What it fixes

- **Porting old scripts** — legacy Python often uses `print()` for
  visibility. `@log_prints` makes them Dagster-native without a rewrite.
- **Third-party libraries that print** — pandas warnings, requests
  redirects, tqdm progress bars — captured instead of going to stderr.

## Two shapes

- **`LogPrintsAssetComponent`** (YAML)
- **`@log_prints` decorator** (Python)

## Behavior

- Wraps compute with `contextlib.redirect_stdout(sink)`.
- Each line written to stdout is emitted as `context.log.info(...)`.
- Empty lines are skipped.
- Original stdout is restored after compute (whether success or fail).

## Composes with

- **All other decorators** — `@log_prints` is orthogonal.

## What's not in v1

- **stderr capture** — v1 only redirects stdout. Add `log_stderr=True`
  in a future iteration.
- **Level per line** — v1 emits all captured lines at `info` level.
  A line-prefix like `WARN:` could route to `context.log.warning`.
"""

import contextlib
import functools
import importlib
import io
import sys
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


class _LogPrintSink(io.TextIOBase):
    """Buffer + flush-on-newline sink that routes lines to `context.log.info`."""

    def __init__(self, context: Any, prefix: str = ""):
        self._context = context
        self._prefix = prefix
        self._buffer = ""

    def write(self, s: str) -> int:  # type: ignore[override]
        if not s:
            return 0
        self._buffer += s
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._flush_line(line)
        return len(s)

    def flush(self) -> None:  # type: ignore[override]
        if self._buffer:
            self._flush_line(self._buffer)
            self._buffer = ""

    def _flush_line(self, line: str) -> None:
        line = line.rstrip()
        if not line:
            return
        try:
            self._context.log.info(f"{self._prefix}{line}")
        except Exception:  # noqa: BLE001
            pass


def log_prints(
    *,
    prefix: str = "[print] ",
) -> Callable:
    """Redirect `print()` inside the decorated compute to `context.log.info`.

    ```python
    @dg.asset
    @log_prints()
    def porting_script(context):
        print("Starting job")
        print(f"Processed {n} rows")
        return build()
    ```

    All `print()` output (and anything else that goes to `sys.stdout`)
    gets captured line-by-line and emitted as info-level Dagster log
    events, so they show up in the run's log panel + are searchable
    across runs. Original stdout is restored on completion (success or
    failure).
    """
    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError("@log_prints requires a Dagster context.")
            sink = _LogPrintSink(context, prefix)
            with contextlib.redirect_stdout(sink):
                try:
                    return fn(*args, **kwargs)
                finally:
                    sink.flush()
        return _wrapped
    return _decorator


class LogPrintsAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@log_prints`. Defines a new asset with print-capture
    wrapping the compute.
    """

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="`{kind: python, python: 'mod:fn'}`. Any return type.")
    prefix: str = Field(
        default="[print] ",
        description="Optional prefix prepended to every captured line before it lands in the log.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'logging'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Log Prints Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        prefix = self.prefix

        kinds_set = set(self.kinds or []) | {"python", "logging"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Print-capturing asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs):
            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"LogPrintsAssetComponent supports compute.kind=python only; got {kind!r}")
            ref = compute.get("python")
            if not ref or ":" not in ref:
                raise ValueError("compute.python must be 'module.path:function_name'")
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                raise ValueError(f"compute.python {ref!r} not callable")

            import inspect
            sig = inspect.signature(fn)
            n_positional = sum(1 for p in sig.parameters.values()
                               if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY))

            sink = _LogPrintSink(context, prefix)
            with contextlib.redirect_stdout(sink):
                try:
                    if n_positional == 0:
                        _ = fn()
                    elif n_positional == 1:
                        _ = fn(context)
                    else:
                        _ = fn(context, kwargs.get("upstream"))
                finally:
                    sink.flush()

            return dg.MaterializeResult(
                metadata={"log_prints_prefix": dg.MetadataValue.text(prefix)}
            )

        return dg.Definitions(assets=[_asset])
