import json
import os
from datetime import date
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
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

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self  # capture for closure

        asset_deps = [dg.AssetKey(d) for d in (component.deps or [])]

        @dg.asset(
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
