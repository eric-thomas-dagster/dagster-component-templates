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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



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


class JupyterNotebookComponent(Component, Model, Resolvable):
    """Execute a Jupyter notebook (.ipynb) or embedded Python code against an upstream DataFrame.

    The notebook / code receives `df` (the upstream DataFrame) and must
    assign the result to `out_df`.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key (DataFrame in)")
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
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

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

        # Allow upstream_asset_key="" — standalone notebook with no DataFrame
        # input. Two branches so Dagster's `ins` inference (from the function
        # signature) doesn't try to wire a phantom 'upstream' input when there
        # isn't a real upstream.
        _has_upstream = bool(self.upstream_asset_key)

        def _do_run(df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
            if _self.notebook_path:
                return _self._run_notebook(df, context)
            return _self._run_code(df, context)

        if _has_upstream:
            @asset(
                key=AssetKey.from_user_string(self.asset_name),
                ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
                group_name=self.group_name,
                description=self.description or f"Jupyter notebook / code run against {self.upstream_asset_key!r}",
                tags=tags,
                owners=self.owners or [],
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
                # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
                # Tolerates upstream authors who annotate `-> Output` or
                # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
                if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                    upstream = upstream.value
                # partition bridge dict-concat: when an unpartitioned
                # asset consumes a partitioned upstream, Dagster's IO
                # manager loads ALL partitions as a dict; concat to
                # a single DataFrame before any DataFrame ops.
                if isinstance(upstream, dict):
                    _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                    upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
                return _do_run(upstream, context)
        else:
            @asset(
                key=AssetKey.from_user_string(self.asset_name),
                group_name=self.group_name,
                description=self.description or "Jupyter notebook / code (no upstream)",
                tags=tags,
                owners=self.owners or [],
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def _asset(context: AssetExecutionContext) -> pd.DataFrame:
                return _do_run(pd.DataFrame(), context)

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
        except ModuleNotFoundError as e:
            # Common when an Alteryx JupyterCode tool referenced a domain
            # package (yfinance, sklearn, transformers, …) that isn't on the
            # current env. Surface the missing module clearly and pass the
            # upstream through unchanged so downstream tools still run.
            context.log.warning(
                f"jupyter_notebook: ModuleNotFoundError ({e}). Install the "
                "missing package OR replace this code block with a "
                "pure-pandas / inline equivalent. Passing upstream through unchanged."
            )
            return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
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
