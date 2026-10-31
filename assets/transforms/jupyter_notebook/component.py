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
from typing import Any, Dict, List, Literal, Optional

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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (the row-filter "
            "logic below already handles a multi-partition range correctly). "
            "'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with "
            "partition_expr set) -- a generic filesystem/default IO manager "
            "cannot persist a single_run output spanning multiple partitions "
            "and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
    )
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
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim
        if partitions_def is not None and not self.upstream_asset_key:
            raise ValueError("partition_type needs upstream_asset_key: without an upstream frame there are no rows to partition.")

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
            @asset(partitions_def=partitions_def,
            backfill_policy=backfill_policy,
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
                # Filter to current partition if partitioned
                if context.has_partition_key or context.has_partition_key_range:
                    # One filter pass per partition key: a normal tick gives exactly one,
                    # a single_run backfill gives the whole range. Rows are selected by
                    # position so keys that match overlapping rows never duplicate them.
                    _pk_base = upstream
                    _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                    _pk_keep = set()
                    _pk_filtered = False
                    for _pk_iter in context.partition_keys:
                        upstream = _pk_pos
                        _pk = _pk_iter
                        _is_multi = hasattr(_pk, "keys_by_dimension")
                        _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                        _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                        if partition_date_column and partition_date_column in upstream.columns and _date_key:
                            _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                            upstream = upstream[_col_dates == _date_key]
                        if partition_static_column and partition_static_column in upstream.columns and _static_key:
                            upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                        elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                            upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                        if upstream is not _pk_pos:
                            _pk_filtered = True
                            _pk_keep.update(upstream.index)
                    upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
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


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
# END scaffold partitions-def builder
