"""JupyterNotebook.

Execute a Jupyter notebook (`.ipynb`) or an inline Python code snippet
against an upstream DataFrame, return the result as a Dagster asset.

Two execution modes:

  - `notebook_path` — point at a `.ipynb` file on disk; runs via
    papermill. Use this for the "notebook-as-pipeline-step" pattern.
  - `code` — embed Python directly as a string. The wrapper runs it
    in a controlled scope where `df` is the upstream DataFrame and
    `out_df` is the expected output variable.

Both modes return the value of `out_df` (a DataFrame) as the Dagster
asset's materialized value. Optionally, papermill-output notebooks
get persisted to `output_notebook_path` for inspection.
"""
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    Resolvable,
    asset,
)
from pydantic import Field


_VALID_BACKENDS = ("papermill", "exec")


class JupyterNotebookComponent(Component, Model, Resolvable):
    """Execute a Jupyter notebook (.ipynb) or embedded Python code against an upstream DataFrame.

    The notebook / code receives `df` (the upstream DataFrame) and must
    assign the result to `out_df`.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key (DataFrame in)")
    notebook_path: Optional[str] = Field(
        default=None,
        description=(
            "Path to the .ipynb file to execute (mutually exclusive with `code`). "
            "Resolved relative to the Dagster project root."
        ),
    )
    code: Optional[str] = Field(
        default=None,
        description=(
            "Inline Python code body to execute (mutually exclusive with "
            "`notebook_path`). Receives `df`, must assign `out_df`."
        ),
    )
    backend: str = Field(
        default="exec",
        description=f"Execution backend. One of: {list(_VALID_BACKENDS)}. 'papermill' is the canonical notebook-as-pipeline pattern.",
    )
    output_notebook_path: Optional[str] = Field(
        default=None,
        description=(
            "When set + backend=papermill, save the executed notebook (with "
            "outputs) to this path. Useful for inspection / lineage."
        ),
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="papermill parameters dict, injected as the notebook's parameters cell.",
    )
    timeout_seconds: int = Field(
        default=600,
        description="Max notebook execution time (papermill backend).",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        if self.backend not in _VALID_BACKENDS:
            raise ValueError(
                f"jupyter_notebook: backend must be one of {list(_VALID_BACKENDS)}; "
                f"got {self.backend!r}."
            )
        if not (self.notebook_path or self.code):
            raise ValueError("jupyter_notebook: must set either `notebook_path` or `code`.")
        if self.notebook_path and self.code:
            raise ValueError("jupyter_notebook: only one of `notebook_path` / `code` may be set.")

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "jupyter"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Jupyter notebook / code run against {self.upstream_asset_key!r}",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            if _self.notebook_path:
                return _self._run_notebook(upstream, context)
            return _self._run_code(upstream, context)

        return Definitions(assets=[_asset])

    def _run_notebook(self, df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
        nb_path = Path(self.notebook_path)
        if not nb_path.is_absolute():
            nb_path = Path.cwd() / nb_path
        if not nb_path.exists():
            raise FileNotFoundError(f"jupyter_notebook: notebook_path {nb_path} not found.")

        with tempfile.TemporaryDirectory() as tdir:
            in_path = os.path.join(tdir, "input.parquet")
            df.to_parquet(in_path)
            out_path = os.path.join(tdir, "output.parquet")
            params = {
                "input_dataframe_path": in_path,
                "output_dataframe_path": out_path,
                **(self.parameters or {}),
            }
            executed_nb = self.output_notebook_path or os.path.join(tdir, "executed.ipynb")
            try:
                import papermill as pm
            except ImportError as e:
                raise ImportError(
                    "jupyter_notebook backend='papermill' needs `pip install papermill`."
                ) from e
            pm.execute_notebook(
                str(nb_path),
                executed_nb,
                parameters=params,
                progress_bar=False,
                request_save_on_cell_execute=False,
                kernel_name=None,
                start_timeout=self.timeout_seconds,
            )
            if not os.path.exists(out_path):
                raise RuntimeError(
                    f"jupyter_notebook: notebook didn't write `output_dataframe_path` "
                    f"({out_path}). The notebook should df.to_parquet(output_dataframe_path) "
                    f"at the end."
                )
            out_df = pd.read_parquet(out_path)
        context.log.info(f"jupyter_notebook (papermill): in {len(df)} rows → out {len(out_df)} rows.")
        context.add_output_metadata({
            "dagster/row_count": MetadataValue.int(len(out_df)),
            "backend": MetadataValue.text("papermill"),
        })
        return out_df

    def _run_code(self, df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
        import numpy as np
        scope: Dict[str, Any] = {
            "df": df,
            "pd": pd,
            "np": np,
            "context": context,
            "__name__": "__jupyter_code__",
        }
        try:
            exec(self.code, scope)
        except Exception as e:
            raise RuntimeError(f"jupyter_notebook: inline code raised {type(e).__name__}: {e}") from e
        if "out_df" not in scope:
            raise RuntimeError(
                "jupyter_notebook: inline code must assign `out_df` (e.g. "
                "`out_df = df.assign(x=df['y'] * 2)`)."
            )
        out_df = scope["out_df"]
        if not isinstance(out_df, pd.DataFrame):
            out_df = pd.DataFrame(out_df)
        context.log.info(f"jupyter_notebook (exec): in {len(df)} rows → out {len(out_df)} rows.")
        context.add_output_metadata({
            "dagster/row_count": MetadataValue.int(len(out_df)),
            "backend": MetadataValue.text("exec"),
        })
        return out_df

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""
