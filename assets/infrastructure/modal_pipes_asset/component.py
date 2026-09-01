"""ModalPipesAssetComponent — Pipes-based Modal integration.

Sibling of `ModalAssetComponent` (trigger-and-track). Uses **Dagster
Pipes** to launch a Modal function and stream structured metadata +
logs back into the Dagster event log during the run — richer
observability than the fire-and-track pattern.

## When to use which

| Need | Component |
|---|---|
| Kick off a Modal job and mark the asset materialized when it finishes | `ModalAssetComponent` (trigger-and-track) |
| Stream logs + structured metadata + AssetMaterialization back from inside the Modal function | `ModalPipesAssetComponent` (this) |
| Multiple assets emitted from one Modal function via `context.report_asset_materialization()` | This — Pipes handles multi-output naturally |

## How it works

Under the hood, `PipesSubprocessClient` launches `modal run
<app_file>::<function_name> [args]` as a subprocess. Inside the Modal
function, the user code opens a Pipes context via
`open_dagster_pipes()` and reports back via
`context.log`, `context.report_asset_materialization()`, etc.

The Dagster side captures those messages (over a temp filesystem or
S3 message reader) and materializes the resulting asset(s) with the
reported metadata.

## Required in the Modal function

The user's Modal function must:

1. Import `dagster_pipes` inside the container:

   ```python
   from dagster_pipes import open_dagster_pipes, PipesContext
   ```

2. Open a Pipes context and report results:

   ```python
   @app.function()
   def train(learning_rate: float):
       with open_dagster_pipes() as ctx:
           ctx.log.info(f"training with lr={learning_rate}")
           # ... do the work ...
           ctx.report_asset_materialization(
               metadata={"accuracy": 0.87, "duration_s": 42.0}
           )
   ```

3. `dagster-pipes` must be installed in the Modal image (add it to your
   Modal `Image.debian_slim(...).pip_install('dagster-pipes')`).

## Pairs with

- `ModalAssetComponent` — the trigger-and-track sibling (no Pipes deps,
  no metadata streaming, simpler when observability isn't needed).
"""

import os
import shlex
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class ModalPipesAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Launch a Modal function via Dagster Pipes and materialize the emitted asset(s).

    Requires the user's Modal function to open a `dagster_pipes.open_dagster_pipes()`
    context and report materializations via `ctx.report_asset_materialization()`.
    Everything the function reports lands in the Dagster event log AND the
    materialization metadata.

    Example:
        ```yaml
        type: dagster_community_components.ModalPipesAssetComponent
        attributes:
          asset_name: model_training
          app_file: ml/training_app.py
          function_name: train
          function_args: {learning_rate: 0.001, epochs: 10}
          modal_token_id_env_var: MODAL_TOKEN_ID
          modal_token_secret_env_var: MODAL_TOKEN_SECRET
          group_name: ml
          kinds: [python, ml, modal, pipes]
        ```
    """

    asset_name: str = Field(
        description="Dagster asset key emitted by this component. The Modal function may also emit additional asset materializations via ctx.report_asset_materialization()."
    )

    app_file: str = Field(
        description=(
            "Path (relative to the Dagster process working directory) to the "
            "Python file defining the Modal app + function. Required for the "
            "`modal run` invocation."
        )
    )
    function_name: str = Field(
        description=(
            "Name of the decorated `@app.function()` inside `app_file`. Used "
            "in the CLI invocation: `modal run app_file::function_name`."
        )
    )

    function_args: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Keyword args forwarded to the Modal function via CLI flags "
            "(`modal run app::fn --key=value`). Values are stringified; "
            "for complex args pass a JSON string and parse inside the function."
        ),
    )

    modal_token_id_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var containing the Modal token ID. Overrides `MODAL_TOKEN_ID` "
            "for the subprocess. Omit if the process env already has valid "
            "Modal credentials (Modal CLI stores them in ~/.modal after `modal setup`)."
        ),
    )
    modal_token_secret_env_var: Optional[str] = Field(
        default=None,
        description="Env var containing the Modal token secret. Overrides `MODAL_TOKEN_SECRET` for the subprocess.",
    )

    modal_env: Optional[str] = Field(
        default=None,
        description="Modal environment name (`modal run --env=<value>`). Optional.",
    )
    modal_extra_flags: Optional[List[str]] = Field(
        default=None,
        description=(
            "Extra flags forwarded to `modal run` (e.g. `--detach`, `--tag=x`). "
            "Passed after `run` and before `app_file::function_name`."
        ),
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None, description="Asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['modal', 'pipes', 'external'].",
    )

    # Partitioning
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Modal Pipes Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        app_file = self.app_file
        function_name = self.function_name
        function_args = dict(self.function_args or {})
        modal_env = self.modal_env
        modal_extra_flags = list(self.modal_extra_flags or [])

        modal_token_id_env_var = self.modal_token_id_env_var
        modal_token_secret_env_var = self.modal_token_secret_env_var

        kinds_set = set(self.kinds or []) | {"modal", "pipes", "external"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Modal function {function_name} via Dagster Pipes",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            partitions_def=partitions_def,
        )
        def _modal_pipes_asset(context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient):
            # Build the `modal run` command.
            cmd: List[str] = ["modal", "run"]
            if modal_env:
                cmd.extend(["--env", modal_env])
            cmd.extend(modal_extra_flags)
            cmd.append(f"{app_file}::{function_name}")

            # Partition-aware: expose partition_key as CLI arg + env so the
            # user's Modal function can read it via os.environ or --partition_key.
            partition_key = context.partition_key if context.has_partition_key else None
            effective_args = dict(function_args)
            if partition_key and "partition_key" not in effective_args:
                effective_args["partition_key"] = partition_key

            for key, value in effective_args.items():
                cmd.append(f"--{key}")
                cmd.append(str(value))

            # Env overrides for token creds.
            env_overrides = {}
            if modal_token_id_env_var:
                tok = os.environ.get(modal_token_id_env_var)
                if tok:
                    env_overrides["MODAL_TOKEN_ID"] = tok
            if modal_token_secret_env_var:
                sec = os.environ.get(modal_token_secret_env_var)
                if sec:
                    env_overrides["MODAL_TOKEN_SECRET"] = sec
            if partition_key:
                env_overrides["DAGSTER_PARTITION_KEY"] = partition_key

            context.log.info(f"[modal_pipes] launching: {' '.join(shlex.quote(c) for c in cmd)}")

            # Launch via PipesSubprocessClient. Materializations reported by the
            # Modal function via `ctx.report_asset_materialization()` land in the
            # Dagster event log automatically.
            return pipes_subprocess_client.run(
                command=cmd,
                context=context,
                env=env_overrides or None,
            ).get_materialize_result()

        # Wire the built-in PipesSubprocessClient resource by default. Users
        # can override at project level if they need custom message-reader /
        # context-injector (e.g. an S3-backed reader for large log payloads).
        return dg.Definitions(
            assets=[_modal_pipes_asset],
            resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
        )


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("static partition requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("static requires values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")
