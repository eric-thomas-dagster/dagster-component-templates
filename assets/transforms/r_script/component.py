"""RScript.

Run an R script against an upstream DataFrame and return the result as
a Dagster asset. Execute statsmodels / dplyr / tidyverse code without
porting to Python.

Two execution backends:

  - `rpy2` — in-process R via rpy2 (best for small/medium data,
    no disk roundtrip). Requires `pip install rpy2` + an R install.
  - `rscript` — shells out to `Rscript`, passes the DataFrame as
    a temp parquet (or CSV) file. Survives if rpy2 isn't installable
    in your environment.

The R script reads from `df` and assigns the result to `out_df`.
"""
import os
import shutil
import subprocess
import tempfile
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


_VALID_BACKENDS = ("rpy2", "rscript")



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


class RScriptComponent(Component, Model, Resolvable):
    """Run an R script against an upstream DataFrame; return a new DataFrame.

    The R script receives `df` and must assign to `out_df`.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key (DataFrame in)")
    script: str = Field(
        description=(
            "R script body. Receives `df` (the upstream DataFrame). Must "
            "assign the output to `out_df`."
        ),
    )
    backend: str = Field(
        default="rscript",
        description=f"Execution backend. One of: {list(_VALID_BACKENDS)}.",
    )
    rscript_executable: str = Field(
        default="Rscript",
        description="Path to the `Rscript` binary (only used when backend='rscript').",
    )
    intermediate_format: str = Field(
        default="parquet",
        description="On-disk format for the DataFrame handoff to/from R: 'parquet' (requires arrow R pkg) or 'csv'.",
    )
    r_packages: Optional[List[str]] = Field(
        default=None,
        description=(
            "R packages to ensure are installed before running the script. "
            "Pre-install in your runtime image for production."
        ),
    )
    timeout_seconds: int = Field(
        default=300,
        description="Max seconds the R subprocess may run (rscript backend only).",
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
                f"r_script: backend must be one of {list(_VALID_BACKENDS)}; "
                f"got {self.backend!r}."
            )
        if self.intermediate_format not in ("parquet", "csv"):
            raise ValueError(
                f"r_script: intermediate_format must be 'parquet' or 'csv'; "
                f"got {self.intermediate_format!r}."
            )

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "r"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"R script run against {self.upstream_asset_key!r} (backend={self.backend}).",
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
            if _self.backend == "rpy2":
                return _self._run_rpy2(upstream, context)
            return _self._run_rscript(upstream, context)

        return Definitions(assets=[_asset])

    def _run_rpy2(self, df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
        try:
            import rpy2.robjects as ro
            from rpy2.robjects import pandas2ri
            from rpy2.robjects.conversion import localconverter
        except ImportError as e:
            raise ImportError(
                "r_script backend='rpy2' needs `pip install rpy2` and an "
                "installed R runtime. Or switch backend to 'rscript' which "
                "shells out to Rscript via subprocess."
            ) from e

        for pkg in (self.r_packages or []):
            ro.r(
                f'if (!requireNamespace("{pkg}", quietly=TRUE)) '
                f'install.packages("{pkg}", repos="https://cloud.r-project.org")'
            )

        with localconverter(ro.default_converter + pandas2ri.converter):
            ro.globalenv["df"] = df
            ro.r(self.script)
            if "out_df" not in ro.globalenv:
                raise RuntimeError(
                    "r_script: the R script must assign its result to `out_df` "
                    "(e.g. `out_df <- df`)."
                )
            out_df = ro.globalenv["out_df"]
            if not isinstance(out_df, pd.DataFrame):
                out_df = pd.DataFrame(out_df)
        context.log.info(f"r_script (rpy2): in {len(df)} rows → out {len(out_df)} rows.")
        return out_df

    def _run_rscript(self, df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
        rscript_bin = shutil.which(self.rscript_executable) or self.rscript_executable
        if not shutil.which(rscript_bin) and not os.path.exists(rscript_bin):
            raise OSError(
                f"r_script backend='rscript' needs {self.rscript_executable!r} "
                "on PATH. Install R from https://cran.r-project.org/, or set "
                "`rscript_executable` to the absolute path."
            )

        fmt = self.intermediate_format
        with tempfile.TemporaryDirectory() as tdir:
            in_path = os.path.join(tdir, f"input.{fmt}")
            out_path = os.path.join(tdir, f"output.{fmt}")
            script_path = os.path.join(tdir, "user_script.R")
            wrapper_path = os.path.join(tdir, "wrapper.R")

            if fmt == "parquet":
                df.to_parquet(in_path)
            else:
                df.to_csv(in_path, index=False)

            with open(script_path, "w") as f:
                f.write(self.script)

            install_lines = ""
            for pkg in (self.r_packages or []):
                install_lines += (
                    f'if (!requireNamespace("{pkg}", quietly=TRUE)) '
                    f'install.packages("{pkg}", repos="https://cloud.r-project.org")\n'
                )

            if fmt == "parquet":
                read_call = (
                    f'if (!requireNamespace("arrow", quietly=TRUE)) '
                    f'install.packages("arrow", repos="https://cloud.r-project.org")\n'
                    f'library(arrow)\n'
                    f'df <- read_parquet({in_path!r})\n'
                )
                write_call = f'write_parquet(out_df, {out_path!r})\n'
            else:
                read_call = f'df <- read.csv({in_path!r}, stringsAsFactors=FALSE)\n'
                write_call = f'write.csv(out_df, {out_path!r}, row.names=FALSE)\n'

            wrapper = (
                install_lines
                + read_call
                + f'source({script_path!r})\n'
                + 'if (!exists("out_df")) stop("r_script: user code did not assign `out_df`")\n'
                + write_call
            )
            with open(wrapper_path, "w") as f:
                f.write(wrapper)

            try:
                proc = subprocess.run(
                    [rscript_bin, wrapper_path],
                    capture_output=True,
                    text=True,
                    timeout=self.timeout_seconds,
                    check=False,
                )
            except subprocess.TimeoutExpired as e:
                raise TimeoutError(
                    f"r_script: Rscript run exceeded {self.timeout_seconds}s."
                ) from e
            if proc.returncode != 0:
                context.log.error(f"Rscript stdout:\n{proc.stdout}")
                context.log.error(f"Rscript stderr:\n{proc.stderr}")
                raise RuntimeError(
                    f"r_script: Rscript exited with code {proc.returncode}. "
                    f"Stderr tail: {proc.stderr[-500:]!r}"
                )
            if proc.stdout:
                context.log.info(f"Rscript stdout:\n{proc.stdout[:2000]}")

            if fmt == "parquet":
                out_df = pd.read_parquet(out_path)
            else:
                out_df = pd.read_csv(out_path)
        context.log.info(f"r_script (Rscript): in {len(df)} rows → out {len(out_df)} rows.")
        context.add_output_metadata({
            "dagster/row_count": MetadataValue.int(len(out_df)),
            "backend": MetadataValue.text("rscript"),
            "intermediate_format": MetadataValue.text(fmt),
        })
        return out_df

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""
