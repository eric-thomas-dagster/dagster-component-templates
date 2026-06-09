"""DataframeFromCsv Component.

Read a CSV file and output a DataFrame. Supports environment variable substitution
in the file path for flexible deployment configuration.
"""
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    MetadataValue,
)
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


class DataframeFromCsvComponent(Component, Model, Resolvable):
    """Read a CSV file and output a DataFrame."""

    asset_name: str = Field(description="Output Dagster asset name")
    file_path: str = Field(
        description="Path to CSV file. Supports env var substitution, e.g. ${DATA_DIR}/file.csv"
    )
    delimiter: str = Field(
        default="auto",
        description=(
            "Column delimiter character. Default `auto` uses csv.Sniffer on the "
            "first 4 KB to detect , ; | tab. Pass an explicit char to skip the sniff."
        ),
    )
    encoding: str = Field(
        default="auto",
        description=(
            "File encoding. Default `auto` sniffs the first 4 KB with "
            "charset-normalizer and picks the most likely encoding (handles "
            "UTF-8-with-BOM, Windows-1252, ISO-8859-1, UTF-16, etc.). Pass an "
            "explicit name like 'utf-8' / 'cp1252' / 'latin-1' to skip the sniff."
        ),
    )
    parse_dates: Optional[List[str]] = Field(
        default=None, description="Columns to parse as dates"
    )
    dtype: Optional[Dict[str, str]] = Field(
        default=None, description="Column dtype overrides, e.g. {id: str, amount: float}"
    )
    skiprows: Optional[int] = Field(
        default=None, description="Number of rows to skip at the start of the file"
    )
    nrows: Optional[int] = Field(default=None, description="Maximum number of rows to read")
    add_filename_column: bool = Field(
        default=False,
        description=(
            "If True, add a column to the output containing the source file's "
            "basename. Useful when reading a glob pattern that matches multiple "
            "files and downstream needs to identify which file each row came from."
        ),
    )
    filename_column_name: str = Field(
        default="FileName",
        description="Name of the auto-added filename column (only used when add_filename_column=True).",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
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
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 5 rows "
            "as a markdown table). Used by builder UIs to render asset shape "
            "without warehouse access."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Read a CSV file and output a DataFrame."

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


    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        file_path = self.file_path
        delimiter = self.delimiter
        encoding = self.encoding
        parse_dates = self.parse_dates
        dtype = self.dtype
        skiprows = self.skiprows
        nrows = self.nrows
        add_filename_column = self.add_filename_column
        filename_column_name = self.filename_column_name
        group_name = self.group_name

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "dataframe_from_csv"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
group_name=group_name,
            description=DataframeFromCsvComponent.get_description(),
        )
        def _asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            resolved_path = os.path.expandvars(file_path)
            context.log.info(f"Reading CSV from {resolved_path}")

            # Expand glob patterns: file_path may be a wildcard like
            # `\dir\*.csv`. Concatenate matching files;
            # if zero matches, fall back to empty frame so downstream chain
            # still resolves. Try the literal path first (test stubs sometimes
            # have `*` in the filename).
            import glob as _glob
            if os.path.exists(resolved_path):
                _candidates = [resolved_path]
            elif any(_c in resolved_path for _c in ("*", "?", "[")):
                _candidates = _glob.glob(resolved_path)
            else:
                _candidates = [resolved_path]
            # Encoding resolution: `auto` (default) sniffs the file's first
            # 4 KB with charset-normalizer and uses the best match. An
            # explicit encoding string skips sniffing. Either way, fall
            # back through utf-8-sig (strips BOM) → cp1252 → latin-1 so a
            # wrong sniff or surprise encoding still reads.
            def _sniff_encoding(path: str) -> Optional[str]:
                try:
                    from charset_normalizer import from_path
                    _results = from_path(path)
                    best = _results.best()
                    if best is not None:
                        enc = best.encoding
                        # Normalize charset-normalizer's names to pandas-known.
                        if enc and enc.lower() == "utf_8":
                            return "utf-8-sig"
                        return enc
                except Exception:
                    return None
                return None

            def _sniff_delimiter(path: str, enc: str) -> Optional[str]:
                try:
                    import csv as _csv
                    with open(path, "r", encoding=enc, errors="replace") as _fh:
                        sample = _fh.read(4096)
                    if not sample:
                        return None
                    return _csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
                except Exception:
                    return None

            def _read_one(path: str):
                _user_enc = (encoding or "auto").strip()
                if _user_enc.lower() == "auto":
                    _detected = _sniff_encoding(path)
                    _user_enc = _detected or "utf-8-sig"
                    context.log.info(
                        f"dataframe_from_csv: sniffed encoding={_user_enc!r} for {path}"
                    )
                _user_delim = (delimiter or "auto").strip() if isinstance(delimiter, str) else delimiter
                if isinstance(_user_delim, str) and _user_delim.lower() == "auto":
                    _detected_delim = _sniff_delimiter(path, _user_enc)
                    if _detected_delim:
                        _user_delim = _detected_delim
                        context.log.info(
                            f"dataframe_from_csv: sniffed delimiter={_user_delim!r} for {path}"
                        )
                    else:
                        _user_delim = ","
                # Prefer `utf-8-sig` over plain `utf-8` to handle BOM.
                if _user_enc.lower() in ("utf-8", "utf8"):
                    _encodings = ["utf-8-sig", "utf-8"]
                else:
                    _encodings = [_user_enc, "utf-8-sig"]
                for _fallback in ("cp1252", "latin-1"):
                    if _fallback not in _encodings:
                        _encodings.append(_fallback)
                _last_err = None
                for _enc in _encodings:
                    try:
                        _df = pd.read_csv(
                            path,
                            delimiter=_user_delim,
                            encoding=_enc,
                            parse_dates=parse_dates or False,
                            dtype=dtype,
                            skiprows=skiprows,
                            nrows=nrows,
                        )
                        # Belt-and-suspenders: strip any literal BOM that
                        # snuck into a column name (happens when the user
                        # configures `encoding=latin-1` on a file that's
                        # actually UTF-8-BOM — bytes EF BB BF decode to
                        # `ï»¿` in latin-1 and stay on col[0]).
                        def _strip_bom(c):
                            if not isinstance(c, str):
                                return c
                            # UTF-8 BOM as single ﻿ char
                            if c.startswith("﻿"):
                                c = c[1:]
                            # UTF-8 BOM bytes mis-decoded as latin-1 (3 chars)
                            if c.startswith("ï»¿"):
                                c = c[3:]
                            return c
                        _new_cols = [_strip_bom(c) for c in _df.columns]
                        if _new_cols != list(_df.columns):
                            _df.columns = _new_cols
                        return _df
                    except UnicodeDecodeError as _e:
                        _last_err = _e
                        continue
                if _last_err is not None:
                    raise _last_err
                raise RuntimeError("read failed but no exception captured")

            if not _candidates or all(not os.path.exists(p) for p in _candidates):
                context.log.warning(
                    f"dataframe_from_csv: no files matched {resolved_path!r}. "
                    "Returning empty DataFrame so downstream chain resolves."
                )
                df = pd.DataFrame()
            elif len(_candidates) == 1:
                df = _read_one(_candidates[0])
                if add_filename_column:
                    df[filename_column_name] = os.path.basename(_candidates[0])
            else:
                # Glob match — tag each chunk with its own filename so
                # downstream group_by:[FileName] keeps each file's rows separable.
                _chunks = []
                for _p in _candidates:
                    if not os.path.exists(_p):
                        continue
                    _chunk = _read_one(_p)
                    if add_filename_column:
                        _chunk[filename_column_name] = os.path.basename(_p)
                    _chunks.append(_chunk)
                df = pd.concat(_chunks, ignore_index=True) if _chunks else pd.DataFrame()

            context.log.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from CSV")
            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
