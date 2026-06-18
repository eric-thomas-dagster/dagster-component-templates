"""CSV File Ingestion Asset Component."""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
from pathlib import Path
from datetime import datetime
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetExecutionContext,
    ComponentLoadContext,
    AssetKey,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field


_TEMPLATE_FIELD_RE = None  # built lazily inside helper to avoid module-import cost


def _parse_template_fields(template: str) -> List[str]:
    """Pull `{field}` placeholders out of a Python format-string template.

    `"s3://{bucket}/{key}"` → `["bucket", "key"]`. Used by `from_run_config`
    to construct a Pydantic Config class whose fields match the template.
    """
    import re
    global _TEMPLATE_FIELD_RE
    if _TEMPLATE_FIELD_RE is None:
        _TEMPLATE_FIELD_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)(?::[^}]*)?\}")
    seen, ordered = set(), []
    for m in _TEMPLATE_FIELD_RE.finditer(template):
        name = m.group(1)
        if name not in seen:
            seen.add(name)
            ordered.append(name)
    return ordered


def _resolve_paths_from_upstream(
    upstream_value: Any,
    dict_key: Optional[str],
    dict_keys: Optional[List[str]],
    dict_key_pattern: Optional[str],
) -> List[tuple]:
    """Resolve paths from an upstream asset value (a dict[filename, path]).

    Returns a list of (source_key, file_path) tuples — source_key is the
    dict key that produced this path (useful for the `_source_file` column
    in `concat_with_source` mode).

    Validation: upstream must be a dict; the selector must match at least
    one entry; missing keys raise clearly so users find typos fast.
    """
    if not isinstance(upstream_value, dict):
        raise TypeError(
            f"from_upstream expects upstream asset value to be a dict, "
            f"got {type(upstream_value).__name__}. "
            f"Use an upstream like `archive_fetcher` that emits "
            f"{{filename: absolute_path}}."
        )
    if dict_key is not None:
        if dict_key not in upstream_value:
            available = sorted(upstream_value.keys())
            raise KeyError(
                f"from_upstream.dict_key={dict_key!r} not found in upstream. "
                f"Available keys: {available}"
            )
        return [(dict_key, upstream_value[dict_key])]
    if dict_keys is not None:
        missing = [k for k in dict_keys if k not in upstream_value]
        if missing:
            raise KeyError(
                f"from_upstream.dict_keys missing from upstream: {missing}. "
                f"Available keys: {sorted(upstream_value.keys())}"
            )
        return [(k, upstream_value[k]) for k in dict_keys]
    if dict_key_pattern is not None:
        from fnmatch import fnmatch
        matched = [(k, v) for k, v in upstream_value.items() if fnmatch(k, dict_key_pattern)]
        if not matched:
            raise KeyError(
                f"from_upstream.dict_key_pattern={dict_key_pattern!r} matched "
                f"nothing in upstream. Available keys: {sorted(upstream_value.keys())}"
            )
        return matched
    raise ValueError(
        "from_upstream requires one of: dict_key, dict_keys, dict_key_pattern"
    )


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


_SUPPORTED_FORMATS = ("auto", "csv", "tsv", "json", "jsonl", "parquet", "excel", "avro", "orc", "feather", "fixed_width")


def _detect_format_from_uri(uri: str) -> str:
    """Pick a reader from a URI's extension. Strips compound `.gz`/`.bz2`."""
    lower = uri.lower()
    if lower.endswith((".gz", ".bz2", ".xz", ".zip")):
        lower = lower.rsplit(".", 1)[0]
    for ext, fmt in (
        (".csv", "csv"),
        (".tsv", "tsv"),
        (".jsonl", "jsonl"),
        (".ndjson", "jsonl"),
        (".json", "json"),
        (".parquet", "parquet"),
        (".pq", "parquet"),
        (".xlsx", "excel"),
        (".xls", "excel"),
        (".avro", "avro"),
        (".orc", "orc"),
        (".feather", "feather"),
        (".arrow", "feather"),
        (".ipc", "feather"),
        # Fixed-width can't be auto-detected by extension; users must set explicitly.
    ):
        if lower.endswith(ext):
            return fmt
    raise ValueError(
        f"Could not auto-detect format from URI {uri!r}. "
        f"Set `format:` explicitly to one of: {_SUPPORTED_FORMATS[1:]}"
    )


def _read_avro(uri: str, columns_to_read) -> pd.DataFrame:
    """Read an Avro file (local path or fsspec URI) into a pandas DataFrame.

    Requires `fastavro` (and the matching fsspec driver for remote URIs:
    s3fs / gcsfs / adlfs). The file's writer-side schema is whatever the
    producer (e.g., Event Hubs Capture) wrote — we just decode records.
    """
    import fastavro
    import fsspec

    with fsspec.open(uri, "rb") as f:
        reader = fastavro.reader(f)
        rows = list(reader)
    df = pd.DataFrame(rows)
    if columns_to_read:
        # Match pandas behavior: keep only requested columns, error if missing.
        missing = set(columns_to_read) - set(df.columns)
        if missing:
            raise KeyError(f"columns_to_read missing from Avro file: {sorted(missing)}")
        df = df[list(columns_to_read)]
    return df


def _read_file(uri: str, fmt: str, *, delimiter: str, encoding: str,
               skip_rows: int, header_row: Optional[int],
               columns_to_read, dtype_mapping, parse_dates,
               fixed_width_colspecs=None, fixed_width_names=None) -> pd.DataFrame:
    """Dispatch to the right pandas reader based on `fmt`."""
    if fmt == "auto":
        fmt = _detect_format_from_uri(uri)
    if fmt == "csv":
        return pd.read_csv(uri, delimiter=delimiter, encoding=encoding,
                           skiprows=skip_rows, header=header_row,
                           usecols=columns_to_read, dtype=dtype_mapping,
                           parse_dates=parse_dates)
    if fmt == "tsv":
        return pd.read_csv(uri, delimiter="\t", encoding=encoding,
                           skiprows=skip_rows, header=header_row,
                           usecols=columns_to_read, dtype=dtype_mapping,
                           parse_dates=parse_dates)
    if fmt == "json":
        return pd.read_json(uri, encoding=encoding)
    if fmt == "jsonl":
        return pd.read_json(uri, lines=True, encoding=encoding)
    if fmt == "parquet":
        # Parquet doesn't take encoding/delimiter; columns_to_read maps to `columns`.
        return pd.read_parquet(uri, columns=columns_to_read)
    if fmt == "excel":
        return pd.read_excel(uri, skiprows=skip_rows, header=header_row,
                             usecols=columns_to_read, dtype=dtype_mapping,
                             parse_dates=parse_dates)
    if fmt == "avro":
        return _read_avro(uri, columns_to_read)
    if fmt == "orc":
        # ORC = the Hadoop/Hive columnar format. pyarrow.orc is the reader;
        # pandas has had read_orc since 1.5. Use pandas directly so we
        # consistently return a DataFrame.
        return pd.read_orc(uri, columns=columns_to_read)
    if fmt == "feather":
        # Apache Arrow IPC / Feather v2. Fastest format for pandas↔pyarrow
        # round-trips. pandas.read_feather supports remote URIs via pyarrow.
        return pd.read_feather(uri, columns=columns_to_read)
    if fmt == "fixed_width":
        # Mainframe-style fixed-width text. Pass colspecs/names via the
        # component's `fixed_width_colspecs` and `fixed_width_names` fields.
        return _read_fixed_width(uri, encoding=encoding, skip_rows=skip_rows,
                                 columns_to_read=columns_to_read,
                                 colspecs=fixed_width_colspecs,
                                 names=fixed_width_names,
                                 header_row=header_row)
    raise ValueError(f"Unsupported format: {fmt!r} (supported: {_SUPPORTED_FORMATS})")


def _read_fixed_width(uri: str, *, encoding: str, skip_rows: int, columns_to_read,
                      colspecs, names, header_row: Optional[int]) -> pd.DataFrame:
    """Read a fixed-width text file (mainframe-style).

    `colspecs` is a list of `(start, end)` tuples (0-indexed, exclusive `end`)
    or the special string `"infer"`. `names` is the column-name list when
    there's no header row. Configured via the component's
    `fixed_width_colspecs` and `fixed_width_names` fields — passed through
    here from the build_defs closure.
    """
    return pd.read_fwf(
        uri,
        colspecs=colspecs if colspecs is not None else "infer",
        names=names,
        header=header_row,
        skiprows=skip_rows,
        encoding=encoding,
        usecols=columns_to_read,
    )


class FileIngestionComponent(Component, Model, Resolvable):
    """
    Unified tabular-file ingestion. Reads CSV / TSV / JSON / JSONL / Parquet
    / Excel into a pandas DataFrame.

    Three input modes (mutually exclusive — set exactly one):
      - `file_path:` — a fixed local path or fsspec URI (s3://, gs://, …)
      - `from_upstream:` — resolve path(s) from an upstream asset's dict value
      - `from_run_config:` — resolve a URI from sensor-injected RunConfig,
        with optional `{partition_key}` interpolation for dynamic-partition flows.

    Backward-compatible replacement for `csv_file_ingestion` — that component
    is kept as an alias but new projects should use this one.
    """

    asset_name: str = Field(description="Name of the asset")
    format: str = Field(
        default="auto",
        description=(
            "File format: 'auto' (detect from extension), 'csv', 'tsv', "
            "'json' (one object/array per file), 'jsonl' (one JSON object "
            "per line — also accepts .ndjson), 'parquet', 'excel'. "
            "Default 'auto' inspects the URI's extension."
        ),
    )
    file_path: Optional[str] = Field(
        default=None,
        description=(
            "Path to the CSV file to ingest. Mutually exclusive with "
            "`from_upstream` — set exactly one."
        ),
    )
    from_upstream: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Read the file path(s) from an upstream asset's runtime value, "
            "rather than hardcoding `file_path`. The upstream asset must "
            "return a `dict[filename, absolute_path]` — e.g., what "
            "`archive_fetcher` emits.\n\n"
            "Shape:\n"
            "  {asset: <upstream_asset_key>, dict_key: \"movies.csv\"}\n"
            "      — pick one entry by its dict key → single file → one DataFrame.\n"
            "  {asset: <upstream_asset_key>, dict_keys: [\"a.csv\", \"b.csv\"], combine: concat}\n"
            "      — pick several entries → concatenate row-wise into one DataFrame.\n"
            "  {asset: <upstream_asset_key>, dict_key_pattern: \"*.csv\", combine: concat}\n"
            "      — glob (fnmatch) over dict keys → concatenate.\n\n"
            "`combine` accepts: 'concat' (default — pd.concat rows) or "
            "'concat_with_source' (concat + adds a `_source_file` column "
            "naming the originating dict key)."
        ),
    )
    from_run_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Read the file path from RunConfig injected by a sensor or "
            "schedule, rather than hardcoding `file_path`. The sensor "
            "passes a `config` dict whose fields get interpolated into "
            "a URI template.\n\n"
            "Shape:\n"
            "  {uri_template: \"s3://{bucket}/{key}\"}\n"
            "      — `{bucket}` and `{key}` become required RunConfig fields. "
            "      The sensor sets them per RunRequest; the asset reads "
            "      `pd.read_csv(s3://<bucket>/<key>)` at runtime.\n\n"
            "  {uri_template: \"{path}\"}\n"
            "      — single-field shape; sensor passes one `path` string.\n\n"
            "Works with any URI fsspec supports (s3://, gs://, az://, "
            "file://, plus bare local paths). Auth uses fsspec's ambient "
            "credential discovery."
        ),
    )
    description: Optional[str] = Field(
        default="",
        description="Description of the asset"
    )
    group_name: Optional[str] = Field(
        default="",
        description="Asset group name for organization"
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

    # CSV Reading Options
    delimiter: str = Field(
        default=",",
        description="CSV delimiter character"
    )
    encoding: str = Field(
        default="utf-8",
        description="File encoding"
    )
    skip_rows: int = Field(
        default=0,
        description="Number of rows to skip at the start"
    )
    header_row: Optional[int] = Field(
        default=0,
        description="Row number to use as column names (0-indexed, None for no header)"
    )

    # Data Processing Options
    columns_to_read: Optional[Union[str, int]] = Field(
        default="",
        description="Comma-separated list of column names to read (empty = all columns)"
    )
    dtype_mapping: Optional[str] = Field(
        default="",
        description="Column type mappings as JSON string, e.g. {\"col1\": \"int64\", \"col2\": \"float64\"}"
    )
    parse_dates: Optional[str] = Field(
        default="",
        description="Comma-separated list of columns to parse as dates"
    )

    # Output Options
    cache_to_parquet: bool = Field(
        default=False,
        description="Whether to cache the data to a parquet file for better performance"
    )
    parquet_path: Optional[str] = Field(
        default="",
        description="Path where parquet cache will be stored (auto-generated if empty)"
    )

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output data in metadata (first 5 rows as markdown table). Used by builder UIs to render asset shape without warehouse access."
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

    fixed_width_colspecs: Optional[List[List[int]]] = Field(
        default=None,
        description=(
            "For `format: fixed_width` only — list of [start, end] character "
            "positions (0-indexed, exclusive end) per column. Example for a "
            "mainframe COBOL layout:\n"
            "  fixed_width_colspecs:\n"
            "    - [0, 10]      # 10-char customer ID\n"
            "    - [10, 40]     # 30-char name\n"
            "    - [40, 48]     # 8-char date (YYYYMMDD)\n"
            "    - [48, 58]     # 10-char amount with decimals\n"
            "If omitted, pandas attempts to auto-infer column boundaries from "
            "whitespace in the first row (usually unreliable for tight EBCDIC "
            "layouts — set this explicitly)."
        ),
    )

    fixed_width_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "For `format: fixed_width` only — column names list. Required "
            "unless the file has a header row (then set `header_row: 0`)."
        ),
    )

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    partition_type: Optional[str] = Field(

        default=None,

        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', or None for unpartitioned. With a partition type set, the partition key is exposed via context.partition_key for use in filtering / templating.",

    )

    partition_start: Optional[str] = Field(

        default=None,

        description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is set.",

    )


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




    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
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
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the CSV ingestion asset."""

        # Capture fields for closure
        asset_name = self.asset_name
        file_path = self.file_path
        file_format = (self.format or "auto").lower()
        if file_format not in _SUPPORTED_FORMATS:
            raise ValueError(
                f"{asset_name}: format must be one of {_SUPPORTED_FORMATS}, "
                f"got {file_format!r}"
            )

        # Fixed-width passthrough config (only used when format=fixed_width)
        fw_colspecs = (
            [tuple(c) for c in self.fixed_width_colspecs]
            if self.fixed_width_colspecs else None
        )
        fw_names = list(self.fixed_width_names) if self.fixed_width_names else None

        # Validate exactly one of: file_path | from_upstream | from_run_config
        from_upstream_cfg = self.from_upstream or {}
        from_run_config_cfg = self.from_run_config or {}
        upstream_asset_key = from_upstream_cfg.get("asset") if from_upstream_cfg else None
        dict_key = from_upstream_cfg.get("dict_key") if from_upstream_cfg else None
        dict_keys = from_upstream_cfg.get("dict_keys") if from_upstream_cfg else None
        dict_key_pattern = from_upstream_cfg.get("dict_key_pattern") if from_upstream_cfg else None
        combine_mode = (from_upstream_cfg.get("combine") or "concat") if from_upstream_cfg else "concat"
        run_config_uri_template = from_run_config_cfg.get("uri_template") if from_run_config_cfg else None

        _modes_set = sum(bool(x) for x in (file_path, upstream_asset_key, run_config_uri_template))
        if _modes_set != 1:
            raise ValueError(
                f"{asset_name}: must set exactly one of "
                "`file_path` / `from_upstream` / `from_run_config` "
                f"(got {_modes_set} set)."
            )
        if upstream_asset_key:
            _selectors_set = sum(x is not None for x in (dict_key, dict_keys, dict_key_pattern))
            if _selectors_set != 1:
                raise ValueError(
                    "from_upstream requires exactly one of: dict_key, dict_keys, "
                    f"dict_key_pattern (got {_selectors_set} set)"
                )
            if combine_mode not in ("concat", "concat_with_source"):
                raise ValueError(
                    f"from_upstream.combine must be 'concat' or 'concat_with_source', "
                    f"got {combine_mode!r}"
                )
        if run_config_uri_template:
            _tpl_fields = _parse_template_fields(run_config_uri_template)
            if not _tpl_fields:
                raise ValueError(
                    f"from_run_config.uri_template must contain at least one "
                    f"`{{field}}` placeholder, got {run_config_uri_template!r}."
                )

        description = self.description
        group_name = self.group_name or None
        delimiter = self.delimiter
        encoding = self.encoding
        skip_rows = self.skip_rows
        header_row = self.header_row
        cache_to_parquet = self.cache_to_parquet
        parquet_path = self.parquet_path
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        # Parse column list
        columns_to_read = None
        if self.columns_to_read:
            columns_to_read = [c.strip() for c in self.columns_to_read.split(",")]

        # Parse dtype mapping
        dtype_mapping = None
        if self.dtype_mapping:
            import json
            try:
                dtype_mapping = json.loads(self.dtype_mapping)
            except json.JSONDecodeError:
                pass

        # Parse date columns
        parse_dates = None
        if self.parse_dates:
            parse_dates = [c.strip() for c in self.parse_dates.split(",")]

        # Infer kinds from component name if not explicitly set
        _comp_name = "csv_file_ingestion"  # component directory name
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


        # Build partition definition (auto-generated; supports daily, weekly,


        # monthly, hourly partitions out of the box).
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




        # --- Branch A0: from_run_config — read URI from sensor-injected RunConfig ---
        if run_config_uri_template:
            from dagster import Config
            from pydantic import create_model

            _tpl_fields = _parse_template_fields(run_config_uri_template)
            # `partition_key` is sourced from context, not RunConfig — exclude it
            # from the generated Config class.
            _config_fields = [f for f in _tpl_fields if f != "partition_key"]
            _IngestRunConfig = create_model(
                f"{asset_name.title().replace('_', '')}RunConfig",
                __base__=Config,
                **{f: (str, ...) for f in _config_fields},
            )

            @asset(
                retry_policy=_retry_policy,
                partitions_def=partitions_def,
                key=AssetKey.from_user_string(asset_name),
                description=description or f"CSV data from runtime URI (template: {run_config_uri_template})",
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
                group_name=group_name,
                metadata={
                    "uri_template": run_config_uri_template,
                    "config_fields": MetadataValue.json(_tpl_fields),
                    "delimiter": delimiter,
                    "encoding": encoding,
                },
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def csv_ingestion_asset(context: AssetExecutionContext, config: _IngestRunConfig):
                """Asset that reads a file from a runtime-injected URI.

                `{partition_key}` in the template resolves to context.partition_key
                (when the asset is partitioned). Other `{field}` placeholders
                come from the sensor-injected RunConfig.
                """
                _bindings = dict(config.model_dump())
                if "partition_key" in _tpl_fields:
                    if not context.has_partition_key:
                        raise ValueError(
                            "uri_template uses {partition_key} but this asset "
                            "is not partitioned. Set partition_type: dynamic "
                            "(or another partition type) on the asset."
                        )
                    _bindings["partition_key"] = context.partition_key
                uri = run_config_uri_template.format(**_bindings)
                context.log.info(
                    f"Reading file (format={file_format}) from URI {uri} "
                    f"(template: {run_config_uri_template!r}, bindings: {_bindings})"
                )
                df = _read_file(
                    uri, file_format,
                    delimiter=delimiter, encoding=encoding,
                    skip_rows=skip_rows, header_row=header_row,
                    columns_to_read=columns_to_read, dtype_mapping=dtype_mapping,
                    parse_dates=parse_dates,
                    fixed_width_colspecs=fw_colspecs, fixed_width_names=fw_names,
                )
                meta = {
                    "row_count": int(len(df)),
                    "column_count": int(len(df.columns)),
                    "resolved_uri": uri,
                    "format": file_format,
                    "columns": df.columns.tolist(),
                }
                if include_preview and len(df) > 0:
                    meta["preview"] = MetadataValue.md(df.head().to_markdown())
                return Output(value=df, metadata=meta)

            return Definitions(assets=[csv_ingestion_asset])

        # --- Branch A: from_upstream — read paths from an upstream asset's dict ---
        if upstream_asset_key:
            from dagster import AssetIn

            @asset(
                retry_policy=_retry_policy,
                partitions_def=partitions_def,
                key=AssetKey.from_user_string(asset_name),
                description=description or f"CSV data from upstream {upstream_asset_key}",
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
                group_name=group_name,
                metadata={
                    "upstream_asset": upstream_asset_key,
                    "delimiter": delimiter,
                    "encoding": encoding,
                    "from_upstream_selector": (
                        f"dict_key={dict_key!r}" if dict_key is not None
                        else f"dict_keys={dict_keys!r}" if dict_keys is not None
                        else f"dict_key_pattern={dict_key_pattern!r}"
                    ),
                },
                ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def csv_ingestion_asset(context: AssetExecutionContext, upstream):
                """Asset that reads CSV file(s) from paths resolved against an upstream dict."""
                resolved = _resolve_paths_from_upstream(
                    upstream, dict_key, dict_keys, dict_key_pattern
                )
                context.log.info(
                    f"Resolved {len(resolved)} file(s) from {upstream_asset_key}: "
                    f"{[k for k, _ in resolved]}"
                )
                dfs: List[pd.DataFrame] = []
                for source_key, p in resolved:
                    context.log.info(f"  reading {source_key} → {p}")
                    df_part = _read_file(
                        p, file_format,
                        delimiter=delimiter, encoding=encoding,
                        skip_rows=skip_rows, header_row=header_row,
                        columns_to_read=columns_to_read, dtype_mapping=dtype_mapping,
                        parse_dates=parse_dates,
                    )
                    if combine_mode == "concat_with_source":
                        df_part = df_part.assign(_source_file=source_key)
                    dfs.append(df_part)
                df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
                context.log.info(f"Loaded {len(df)} rows × {len(df.columns)} cols total")
                meta = {
                    "row_count": int(len(df)),
                    "column_count": int(len(df.columns)),
                    "source_files": MetadataValue.json(
                        {k: str(p) for k, p in resolved}
                    ),
                    "columns": df.columns.tolist(),
                }
                if include_preview and len(df) > 0:
                    meta["preview"] = MetadataValue.md(df.head().to_markdown())
                return Output(value=df, metadata=meta)

            return Definitions(assets=[csv_ingestion_asset])

        # --- Branch B: file_path — fixed path on disk (existing behavior) ---
        assert file_path is not None  # validated above
        @asset(retry_policy=_retry_policy, partitions_def=partitions_def,
            key=AssetKey.from_user_string(asset_name),
            description=description or f"CSV data from {file_path}",
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            metadata={
                "source_file": file_path,
                "delimiter": delimiter,
                "encoding": encoding,
            },
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def csv_ingestion_asset(context: AssetExecutionContext):
            """Asset that ingests CSV file into a pandas DataFrame."""

            # Check if running in partitioned mode
            partition_date = None
            partitioned_file_path = file_path
            if context.has_partition_key:
                # Parse partition key as date (format: YYYY-MM-DD)
                try:
                    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
                    context.log.info(f"Reading CSV for partition {context.partition_key}")

                    # Support partitioned file paths with {partition_date} placeholder
                    # e.g., /data/sales_{partition_date}.csv -> /data/sales_2024-01-01.csv
                    if "{partition_date}" in file_path:
                        partitioned_file_path = file_path.replace(
                            "{partition_date}",
                            partition_date.strftime("%Y-%m-%d")
                        )
                        context.log.info(f"Using partitioned file path: {partitioned_file_path}")
                    elif "{partition_key}" in file_path:
                        partitioned_file_path = file_path.replace(
                            "{partition_key}",
                            context.partition_key
                        )
                        context.log.info(f"Using partitioned file path: {partitioned_file_path}")
                except ValueError:
                    context.log.warning(
                        f"Could not parse partition key '{context.partition_key}' as date, "
                        "using original file path"
                    )
            else:
                context.log.info("Reading CSV (non-partitioned)")

            # Check if we should use cached parquet
            if cache_to_parquet:
                cache_path = parquet_path or f"{partitioned_file_path}.parquet"
                cache_file = Path(cache_path)
                source_file = Path(partitioned_file_path)

                # Use cache if it exists and is newer than source
                if cache_file.exists() and cache_file.stat().st_mtime > source_file.stat().st_mtime:
                    context.log.info(f"Loading from parquet cache: {cache_path}")
                    df = pd.read_parquet(cache_path)
                    return df

            # Read the file (format dispatched by _read_file based on `file_format`)
            context.log.info(
                f"Reading file (format={file_format}): {partitioned_file_path}"
            )

            try:
                df = _read_file(
                    partitioned_file_path, file_format,
                    delimiter=delimiter, encoding=encoding,
                    skip_rows=skip_rows, header_row=header_row,
                    columns_to_read=columns_to_read, dtype_mapping=dtype_mapping,
                    parse_dates=parse_dates,
                    fixed_width_colspecs=fw_colspecs, fixed_width_names=fw_names,
                )

                context.log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

                # Log basic statistics
                context.log.info(f"Columns: {list(df.columns)}")
                context.log.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

                # Cache to parquet if requested
                if cache_to_parquet:
                    cache_path = parquet_path or f"{partitioned_file_path}.parquet"
                    context.log.info(f"Caching to parquet: {cache_path}")
                    df.to_parquet(cache_path, index=False)

                # Add row count metadata. Cast numpy scalars to native Python
                # types so Dagster's event log serializer can handle them.
                context.add_output_metadata({
                    "num_rows": int(len(df)),
                    "num_columns": int(len(df.columns)),
                    "columns": list(df.columns),
                    "memory_mb": float(df.memory_usage(deep=True).sum()) / 1024 / 1024,
                })

                if include_preview and len(df) > 0:
                    # Return with sample metadata
                    return Output(
                        value=df,
                        metadata={
                            "row_count": len(df),
                            "columns": df.columns.tolist(),
                            "preview": MetadataValue.md(df.head().to_markdown())
                        }
                    )
                else:
                    return df

            except FileNotFoundError:
                context.log.warning(
                    f"file_ingestion: source file not found at {partitioned_file_path!r}. "
                    "Returning empty DataFrame so downstream chain can still materialize."
                )
                return pd.DataFrame()
            except pd.errors.EmptyDataError:
                context.log.error(f"CSV file is empty: {partitioned_file_path}")
                raise
            except Exception as e:
                context.log.error(f"Error reading CSV file: {str(e)}")
                raise

        return Definitions(assets=[csv_ingestion_asset])
