import json
import os
from datetime import date
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
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
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class JupyterNotebookAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Execute a Jupyter notebook as a Dagster asset using Papermill.

    Runs a ``.ipynb`` notebook via ``papermill``, injecting parameters into a
    tagged cell, saving the fully-executed notebook (with all cell outputs
    preserved) as an artifact, and returning asset metadata so the run is
    visible in the Dagster UI.

    The executed notebook is stored at ``output_path`` so you can inspect cell
    outputs, tracebacks, and embedded plots after the fact without re-running
    the notebook.

    Example:
        ```yaml
        type: dagster_component_templates.JupyterNotebookAssetComponent
        attributes:
          asset_name: monthly_revenue_report
          notebook_path: "{{ project_root }}/notebooks/revenue_analysis.ipynb"
          parameters:
            start_date: "2024-01-01"
            end_date: "2024-01-31"
          execution_timeout: 1800
          group_name: notebooks
        ```
    """

    # --- Core notebook config -------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")
    notebook_path: str = Field(
        description=(
            "Path to the input ``.ipynb`` file to execute. "
            "Supports ``{{ project_root }}`` template substitution."
        )
    )
    output_path: Optional[str] = Field(
        default=None,
        description=(
            "Path where the executed notebook (with outputs) will be saved. "
            "When None, defaults to ``{notebook_stem}_executed_{date}.ipynb`` "
            "in the same directory as the input notebook."
        ),
    )

    # --- Parameterization -----------------------------------------------------
    parameters: Optional[dict] = Field(
        default=None,
        description=(
            "Parameters to inject into the notebook's tagged parameters cell. "
            "Keys and values are passed directly to ``papermill.execute_notebook``. "
            "Overrides any defaults set in the cell."
        ),
    )

    # --- Execution environment ------------------------------------------------
    kernel_name: Optional[str] = Field(
        default=None,
        description=(
            "Jupyter kernel to use for execution (e.g. ``'python3'``). "
            "When None, papermill uses the kernel recorded in the notebook metadata."
        ),
    )
    execution_timeout: int = Field(
        default=600,
        description=(
            "Maximum number of seconds allowed per cell. "
            "Cells that exceed this limit raise a ``CellTimeoutError``. "
            "Set to ``-1`` to disable the per-cell timeout."
        ),
    )
    working_dir: Optional[str] = Field(
        default=None,
        description=(
            "Working directory for notebook execution. "
            "When None, the notebook runs in the directory that contains it."
        ),
    )
    env_vars: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "Environment variables made available to the notebook kernel "
            "via ``os.environ`` before execution begins."
        ),
    )

    # --- Output handling ------------------------------------------------------
    store_output_ipynb: bool = Field(
        default=True,
        description=(
            "Whether to persist the executed notebook. "
            "When True the output path is logged to the Dagster event log. "
            "When False the file is still written by papermill (it needs "
            "somewhere to record outputs) but you can treat it as ephemeral."
        ),
    )

    # --- Asset metadata -------------------------------------------------------
    group_name: str = Field(
        default="notebooks",
        description="Dagster asset group name.",
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description surfaced in the Dagster UI.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on.",
    )

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self  # capture for closure

        asset_deps = [dg.AssetKey(d) for d in (component.deps or [])]


        # Build partition definition (auto-generated; supports daily, weekly, monthly,

        # hourly partitions out of the box).
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @dg.asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=component.asset_name,
            group_name=component.group_name,
            description=component.description,
            deps=asset_deps,
            kinds={"jupyter", "notebook"},
        )
        def _notebook_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import papermill as pm

            notebook_path = component.notebook_path

            # --- 1. Resolve output path --------------------------------------
            if component.output_path:
                output_path = component.output_path
            else:
                stem, _ = os.path.splitext(notebook_path)
                today = date.today().strftime("%Y%m%d")
                output_path = f"{stem}_executed_{today}.ipynb"

            context.log.info(
                f"Executing notebook: {notebook_path} -> {output_path}"
            )

            # --- 2. Apply env vars -------------------------------------------
            if component.env_vars:
                for key, value in component.env_vars.items():
                    os.environ[key] = value
                context.log.info(
                    f"Set {len(component.env_vars)} environment variable(s): "
                    f"{list(component.env_vars.keys())}"
                )

            # --- 3. Execute with papermill -----------------------------------
            try:
                pm.execute_notebook(
                    notebook_path,
                    output_path,
                    parameters=component.parameters,
                    kernel_name=component.kernel_name,
                    execution_timeout=component.execution_timeout,
                    cwd=component.working_dir,
                )
            except pm.PapermillExecutionError as exc:
                cell_num = getattr(exc, "exec_count", None)
                cell_output = getattr(exc, "ename", "") + ": " + getattr(exc, "evalue", "")
                raise RuntimeError(
                    f"Papermill execution failed"
                    + (f" at cell [{cell_num}]" if cell_num is not None else "")
                    + f": {cell_output}"
                ) from exc

            # --- 4. Log output path ------------------------------------------
            if component.store_output_ipynb:
                context.log.info(f"Executed notebook saved to: {output_path}")

            # --- 5. Extract dagster_metadata from last cell output (optional convention) ---
            extra_metadata: dict = {}
            try:
                with open(output_path, "r", encoding="utf-8") as fh:
                    nb = json.load(fh)
                cells = nb.get("cells", [])
                if cells:
                    last_cell = cells[-1]
                    for output in last_cell.get("outputs", []):
                        # Convention: last cell prints / assigns a dict called
                        # dagster_metadata — look for it in plain text output.
                        text_chunks = output.get("text", [])
                        text = "".join(text_chunks) if isinstance(text_chunks, list) else text_chunks
                        if "dagster_metadata" in text:
                            # Try to parse the value after the first '=' or ':'
                            for line in text.splitlines():
                                line = line.strip()
                                if line.startswith("dagster_metadata"):
                                    _, _, raw = line.partition("=")
                                    raw = raw.strip()
                                    if not raw:
                                        _, _, raw = line.partition(":")
                                        raw = raw.strip()
                                    try:
                                        extra_metadata = json.loads(raw)
                                    except (json.JSONDecodeError, ValueError):
                                        pass
                                    break
                        # Also check data outputs (e.g. from display() or repr)
                        data = output.get("data", {})
                        plain = data.get("text/plain", "")
                        if "dagster_metadata" in plain:
                            try:
                                # plain might be "{'key': 'value'}" from repr
                                import ast
                                extra_metadata = ast.literal_eval(plain)
                            except (ValueError, SyntaxError):
                                pass
            except Exception as parse_exc:  # noqa: BLE001
                context.log.warning(
                    f"Could not parse dagster_metadata from executed notebook: {parse_exc}"
                )

            # --- 6. Return MaterializeResult ---------------------------------
            metadata: dict = {
                "notebook": dg.MetadataValue.path(notebook_path),
                "output": dg.MetadataValue.path(output_path),
                "parameters": dg.MetadataValue.json(component.parameters or {}),
            }
            metadata.update(
                {k: dg.MetadataValue.json(v) if isinstance(v, (dict, list)) else v
                 for k, v in extra_metadata.items()}
            )

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_notebook_asset])
