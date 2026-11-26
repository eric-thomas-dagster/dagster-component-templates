"""PolarsPipelineComponent — single-asset multi-step polars LazyFrame pipeline.

Two YAML shapes are supported, both run inside a single Dagster asset and a
single polars query graph (so the optimizer can fuse + parallelize across
ALL ops, including across steps):

  (a) Flat shape (one source, one ops chain, one sink) — top-level `upstream_asset_key` +
      `operations`. One LazyFrame, one chain of ops:

      upstream_asset_key: raw_orders
      operations:
        - {op: filter, predicate: "status = 'paid'"}
        - {op: group_by, group_by: [region], aggregations: {revenue: {col: amount, agg: sum}}}

  (b) Multi-step `steps:` form — multiple sources (each from a Dagster
      upstream OR from an earlier step's output via `{kind: ref, ref: <id>}`),
      named outputs, an `op: sql` escape hatch, and an optional `sinks:`
      list (writes side-outputs to disk as parquet/csv).

      steps:
        - id: paid_orders
          source: {kind: upstream, upstream_asset_key: raw_orders}
          operations:
            - {op: filter, predicate: "status = 'paid'"}
        - id: gold_customers
          source: {kind: upstream, upstream_asset_key: raw_customers}
          operations:
            - {op: filter, predicate: "tier = 'gold'"}
        - id: enriched
          source: {kind: ref, ref: paid_orders}
          operations:
            - {op: join, right: {ref: gold_customers}, on_columns: [customer_id], how: inner}
            - {op: sql, sql: "SELECT *, amount * 0.15 AS commission FROM self"}
            - {op: group_by, group_by: [region],
               aggregations: {revenue: {col: amount, agg: sum}}}
      sinks:
        - {from: enriched, kind: parquet, path: "./out/enriched.parquet"}
      # The asset's RETURN value is the last step's frame (or `primary_step`
      # if you name one explicitly).

  (c) `source: {kind: encrypted_file, ...}` / `sinks: [{kind: encrypted_file, ...}]`
      — real AWS Encryption SDK envelope encryption (same mechanism as
      `FieldEncryptionComponent`, but file-level: decrypts/encrypts the
      whole file's bytes, not a DataFrame cell). Lets a single asset do
      decrypt -> transform -> re-encrypt -> stage in one materialization:

      steps:
        - id: staged
          source: {kind: encrypted_file, path: "incoming/{partition.bank}/{partition.batch_day}.csv.enc",
                   format: csv, key_source: raw_key, raw_key_env_var: BANK_FILE_KEY}
          operations:
            - {op: rename, mapping: {acct: account_id}}
            - {op: cast, mapping: {amount: Float64}}
      sinks:
        - {from: staged, kind: encrypted_file, path: "staged/{partition.bank}.csv.enc",
           format: csv, key_source: raw_key, raw_key_env_var: BANK_FILE_KEY}
      # key_source: 'raw_key' (env var holds a base64 symmetric key) or 'kms'
      # (kms_key_ids:). encryption_context: is bound per-file via an
      # automatic `path: <resolved path>` entry, so ciphertext can't be
      # silently swapped between files/partitions.
"""
import base64
import os
from typing import Any, Dict, List, Literal, Optional, Tuple

import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    BackfillPolicy,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


_VALID_OPS = {"filter", "with_columns", "select", "drop", "rename",
              "group_by", "sort", "head", "tail", "head_per_group", "limit",
              "unique", "join", "union", "drop_nulls", "fill_null", "cast", "sql",
              "explode", "to_datetime", "coalesce", "route"}


# Java SimpleDateFormat/DateTimeFormatter -> polars (Rust chrono) strptime token
# translation -- confirmed against the real installed polars 2.0's chrono-strftime
# syntax (NOT identical to Java's letter-token grammar, e.g. polars uses %-prefixed
# tokens throughout, Java uses bare letters repeated for width). Longest tokens
# first so e.g. "yyyy" matches before "yy".
_JAVA_TO_POLARS_DATE_TOKENS = [
    ("yyyy", "%Y"), ("yy", "%y"),
    ("MMMM", "%B"), ("MMM", "%b"), ("MM", "%m"),
    ("dd", "%d"),
    ("HH", "%H"), ("hh", "%I"),
    ("mm", "%M"),
    ("ss", "%S"),
    ("SSS", "%3f"),  # millisecond fraction
    ("a", "%p"),
    ("XXX", "%:z"), ("XX", "%z"), ("X", "%z"),
    ("Z", "%z"),
    ("E", "%a"),
]


def _translate_java_date_pattern(java_pattern: str) -> str:
    """Translate a Java SimpleDateFormat/DateTimeFormatter pattern (e.g.
    `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`) into polars' chrono-strftime format string.
    Literal text quoted with single quotes (Java's escape mechanism, e.g. 'T')
    is passed through unquoted/unchanged. Unrecognized letter-runs are left
    as-is (better to surface a clearly-wrong-looking format than silently drop
    characters)."""
    out = []
    i = 0
    n = len(java_pattern)
    while i < n:
        ch = java_pattern[i]
        if ch == "'":
            # Quoted literal text — Java's escape for literal characters
            # (e.g. 'T' in yyyy-MM-dd'T'HH:mm:ss). A doubled '' is a literal quote.
            j = i + 1
            lit = []
            while j < n:
                if java_pattern[j] == "'":
                    if j + 1 < n and java_pattern[j + 1] == "'":
                        lit.append("'")
                        j += 2
                        continue
                    j += 1
                    break
                lit.append(java_pattern[j])
                j += 1
            out.append("".join(lit))
            i = j
            continue
        matched = False
        for tok, repl in _JAVA_TO_POLARS_DATE_TOKENS:
            if java_pattern.startswith(tok, i):
                out.append(repl)
                i += len(tok)
                matched = True
                break
        if not matched:
            out.append(ch)
            i += 1
    return "".join(out)


def _resolve_struct_path(pl_module, path: str):
    """StreamSets-style nested field path (`/a/b` or `a.b`) -> a polars Expr
    drilling into struct columns via chained `.struct.field(...)` calls. A
    bare name with no separator is just `pl.col(name)`."""
    parts = [p for p in path.replace("/", ".").split(".") if p]
    if not parts:
        raise ValueError(f"empty field path: {path!r}")
    expr = pl_module.col(parts[0])
    for p in parts[1:]:
        expr = expr.struct.field(p)
    return expr


def _apply_partition_template(s: str, partition_key: Optional[str], partition: Optional[Dict[str, str]] = None) -> str:
    """Substitute `{partition_key}` and `{partition.<name>}` into a template string."""
    if not s or "{" not in s:
        return s
    out = s.replace("{partition_key}", str(partition_key or ""))
    for k, v in (partition or {}).items():
        out = out.replace("{" + f"partition.{k}" + "}", str(v))
    return out


def _is_uri(path: str) -> bool:
    """True if `path` looks like a URI (has a `scheme://` prefix). Catches
    s3 / gs / gcs / az / abfs / abfss / http / https / ftp / file / any
    future scheme without maintaining a hardcoded list. Doesn't confuse
    Windows drive letters (`C:\\`) since those don't contain `://`."""
    return "://" in path


def _read_file_source(pl_module, src: Dict[str, Any], partition_key: Optional[str], partition_map: Optional[Dict[str, str]]):
    """Read `kind: file` or `kind: url` into a polars LazyFrame.

    Supported formats: json | ndjson | csv | parquet | ipc | avro.
    Auto-detects from the file extension when `format:` is unset.
    Path/url is `{partition_key}` / `{partition.<name>}` templated.

    CSV-only options -- matching an Azure Data Factory Copy activity's declared-type source read,
    which this polars CSV scan's own default schema INFERENCE cannot reproduce on its own (e.g. a
    column of zero-padded codes like "001" silently becomes the integer 1): `has_header` (bool,
    default true), `quote_char` (default polars' own '"'), `schema_overrides` ({column: polars
    dtype name as a string, e.g. "Utf8"/"String"/"Int64"/"Float64"} -- pins specific columns'
    types instead of letting inference guess; every standard polars dtype name is looked up
    directly as a pl.<Name> attribute, so any dtype polars itself exposes works here).

    Cloud URLs (s3:// / gs:// / az:// / abfs:// / http(s)://): pre-fetched
    via fsspec into a temp file, then read locally. This is format-agnostic
    and works for every polars reader without depending on version-specific
    cloud plugins. For very large files consider using
    `pl.scan_parquet(s3://...)` directly in a separate asset instead.
    """
    kind = src.get("kind", "file")
    raw_path = src.get("path") if kind == "file" else src.get("url")
    if not raw_path:
        raise ValueError(f"polars_pipeline source kind={kind!r} requires {'path' if kind == 'file' else 'url'}")
    path = _apply_partition_template(raw_path, partition_key, partition_map)
    fmt = (src.get("format") or "").lower()
    if not fmt:
        low = path.lower()
        if low.endswith(".ndjson") or low.endswith(".jsonl"):
            fmt = "ndjson"
        elif low.endswith(".json"):
            fmt = "json"
        elif low.endswith(".csv") or low.endswith(".tsv"):
            fmt = "csv"
        elif low.endswith(".parquet") or low.endswith(".pq"):
            fmt = "parquet"
        elif low.endswith(".ipc") or low.endswith(".arrow"):
            fmt = "ipc"
        elif low.endswith(".avro"):
            fmt = "avro"
        else:
            raise ValueError(
                f"polars_pipeline: can't infer format from {path!r} — set `format:` explicitly."
            )
    delimiter = src.get("delimiter", "\t" if path.lower().endswith(".tsv") else ",")
    has_header = src.get("has_header", True)
    quote_char = src.get("quote_char")
    schema_overrides_raw = src.get("schema_overrides")
    schema_overrides = None
    if schema_overrides_raw:
        # YAML can't express a polars DataType object directly -- values are plain dtype-name
        # strings (e.g. "Utf8"/"String", "Int64", "Float64"), looked up as real attributes on the
        # polars module itself (works across versions: every standard dtype -- old or renamed
        # alias alike -- is always a direct pl.<Name> attribute, confirmed against the installed
        # polars 2.0.0; no hardcoded name->dtype table to drift out of sync with a polars release).
        schema_overrides = {}
        for col, dtype_name in schema_overrides_raw.items():
            if not hasattr(pl_module, dtype_name):
                raise ValueError(
                    f"polars_pipeline source kind={kind!r}: schema_overrides[{col!r}] names an "
                    f"unknown polars dtype {dtype_name!r} (not a pl.{dtype_name} attribute)"
                )
            schema_overrides[col] = getattr(pl_module, dtype_name)

    # Cloud URL pre-fetch — universal for any format. `storage_options:`
    # in the source spec is forwarded to fsspec.open (e.g. anon: true,
    # endpoint_url: '...', key/secret pairs). `_is_uri` catches any scheme
    # (s3 / gs / az / abfs / http / https / ftp / …) without a hardcoded list.
    local_path = path
    if _is_uri(path):
        try:
            import fsspec
        except ImportError:
            raise ImportError(
                "polars_pipeline: reading cloud URLs requires fsspec + a backend "
                "(pip install 's3fs' | 'gcsfs' | 'adlfs')."
            )
        import tempfile as _tempfile
        storage_options = src.get("storage_options") or {}
        _tmp = _tempfile.NamedTemporaryFile(suffix=_suffix_for(fmt), delete=False)
        try:
            with fsspec.open(path, "rb", **storage_options) as fin:
                # Stream in chunks — file could be large.
                while True:
                    chunk = fin.read(8 * 1024 * 1024)  # 8 MiB
                    if not chunk:
                        break
                    _tmp.write(chunk)
        finally:
            _tmp.close()
        local_path = _tmp.name

    # scan_* returns LazyFrame (streaming-friendly); read_json is eager only.
    if fmt == "csv":
        csv_kwargs: Dict[str, Any] = {"separator": delimiter, "has_header": has_header}
        if quote_char is not None:
            csv_kwargs["quote_char"] = quote_char
        if schema_overrides:
            csv_kwargs["schema_overrides"] = schema_overrides
        return pl_module.scan_csv(local_path, **csv_kwargs)
    if fmt == "parquet":
        return pl_module.scan_parquet(local_path)
    if fmt == "ndjson":
        return pl_module.scan_ndjson(local_path)
    if fmt == "json":
        return pl_module.read_json(local_path).lazy()
    if fmt == "ipc":
        return pl_module.scan_ipc(local_path)
    if fmt == "avro":
        return pl_module.read_avro(local_path).lazy()
    raise ValueError(f"polars_pipeline: format {fmt!r} not supported")


def _suffix_for(fmt: str) -> str:
    return {
        "csv": ".csv", "parquet": ".parquet", "ndjson": ".ndjson",
        "json": ".json", "ipc": ".ipc", "avro": ".avro",
    }.get(fmt, ".dat")


def _read_inline_source(pl_module, src: Dict[str, Any]):
    """`kind: inline` — rows or raw delimited text embedded directly in the
    YAML (for StreamSets raw-data/dev-origin-style pipelines with no external
    file to read). Two shapes: `rows: [{...}, ...]` (list of records, JSON-like)
    or `text: "..."` + `format: csv|ndjson` (raw delimited text)."""
    if "rows" in src:
        rows = src["rows"]
        if not isinstance(rows, list):
            raise ValueError("polars_pipeline source kind=inline: 'rows' must be a list of records")
        return pl_module.DataFrame(rows).lazy()
    if "text" in src:
        text = src["text"]
        fmt = (src.get("format") or "csv").lower()
        import io as _io
        if fmt == "csv":
            delimiter = src.get("delimiter", ",")
            return pl_module.read_csv(_io.StringIO(text), separator=delimiter).lazy()
        if fmt == "ndjson":
            return pl_module.read_ndjson(_io.StringIO(text)).lazy()
        raise ValueError(f"polars_pipeline source kind=inline: format {fmt!r} not supported for 'text' (use 'csv' or 'ndjson')")
    raise ValueError("polars_pipeline source kind=inline requires 'rows' (list of records) or 'text' (+ format)")


def _resolve_sql_engine(src: Dict[str, Any], context):
    """Build a SQLAlchemy engine from `resource_key` (a Dagster resource
    exposing .get_engine()/.get_connection(), same convention as every other
    SQL-connecting component in this repo) or `url_env_var` (a raw SQLAlchemy
    URL in an env var) -- mirrors sql_to_database_asset's own resolution
    pattern, inlined fresh per this repo's house rule (no shared helpers)."""
    from sqlalchemy import create_engine

    resource_key = src.get("resource_key")
    url_env_var = src.get("url_env_var")
    if resource_key:
        resource = getattr(context.resources, resource_key, None)
        if resource is None:
            raise ValueError(f"polars_pipeline source kind=sql: resource {resource_key!r} is not registered in this project's Definitions.")
        if hasattr(resource, "get_engine"):
            return resource.get_engine()
        if hasattr(resource, "get_connection"):
            return resource.get_connection()
        raise ValueError(f"polars_pipeline source kind=sql: resource {resource_key!r} ({type(resource).__name__}) does not expose get_engine()/get_connection().")
    if url_env_var:
        url = os.environ.get(url_env_var)
        if not url:
            raise ValueError(f"polars_pipeline source kind=sql: env var {url_env_var!r} is unset or empty")
        return create_engine(url)
    raise ValueError("polars_pipeline source kind=sql requires 'resource_key' or 'url_env_var'")


def _read_sql_source(pl_module, src: Dict[str, Any], context, asset_name: str, step_id: str, offset_sink: Dict[str, Any]):
    """`kind: sql` (alias `jdbc`) — read from a SQL database. Plain mode:
    `query:` runs once per materialization. `incremental: true` mode:
    `offset_column` + `initial_offset` give a resumable `WHERE offset_column >
    <last stored value> ORDER BY offset_column` read, with the new high-water
    mark computed via a cheap bounded MAX() query (not by scanning the
    collected result, which may never be materialized if this step feeds a
    lazily-fused later step) and written into `offset_sink[step_id]` for the
    caller to persist as output metadata -- same
    'dagster_community_components/last_offset' mechanism
    mongodb_reader/elasticsearch_reader already use, no separate state file."""
    import sqlalchemy

    engine_or_conn = _resolve_sql_engine(src, context)
    incremental = bool(src.get("incremental"))
    if not incremental:
        query = src.get("query")
        if not query:
            raise ValueError("polars_pipeline source kind=sql requires 'query' (or incremental: true + 'table'/'offset_column')")
        return pl_module.read_database(query, engine_or_conn).lazy()

    offset_column = src.get("offset_column")
    if not offset_column:
        raise ValueError("polars_pipeline source kind=sql incremental: true requires 'offset_column'")
    table = src.get("table")
    base_query = src.get("query")
    if not table and not base_query:
        raise ValueError("polars_pipeline source kind=sql incremental: true requires 'table' or a base 'query'")

    from dagster import AssetKey
    event = context.instance.get_latest_materialization_event(AssetKey.from_user_string(asset_name))
    prev_offset = None
    if event is not None and event.asset_materialization is not None:
        md = event.asset_materialization.metadata.get(f"dagster_community_components/last_offset/{step_id}")
        prev_offset = getattr(md, "value", None) if md is not None else None
    current_offset = prev_offset if prev_offset is not None else src.get("initial_offset")

    source_expr = f"({base_query}) AS _t" if base_query else table
    predicate = f"WHERE {offset_column} > :offset" if current_offset is not None else ""
    full_query = f"SELECT * FROM {source_expr} {predicate} ORDER BY {offset_column}"
    max_query = f"SELECT MAX({offset_column}) AS _max_offset FROM {source_expr} {predicate}"
    params = {"offset": current_offset} if current_offset is not None else {}

    conn = engine_or_conn.connect() if hasattr(engine_or_conn, "connect") else engine_or_conn
    max_row = conn.execute(sqlalchemy.text(max_query), params).fetchone()
    new_offset = max_row[0] if max_row else None
    offset_sink[step_id] = new_offset if new_offset is not None else current_offset

    df = pl_module.read_database(sqlalchemy.text(full_query), engine_or_conn, execute_options={"parameters": params} if params else None)
    return df.lazy()


# BEGIN file-level AWS Encryption SDK I/O (encrypted_file source/sink kinds) --
# Same real AWS Encryption SDK (envelope encryption, not a toy cipher),
# raw-key or KMS-backed, as FieldEncryptionComponent (transforms/field_encryption)
# -- but operating on a WHOLE FILE's raw bytes instead of one DataFrame cell.
# Per this repo's house rule (no shared helper files), this is NOT imported
# from field_encryption's module -- it's a fresh copy of the same mechanism
# at a different granularity.
def _build_esdk_key_provider(cfg: Dict[str, Any]):
    """Build an AWS Encryption SDK master key provider from a source/sink
    config dict's key_source/raw_key_env_var/kms_key_ids/key_id fields --
    same raw_key/kms modes and provider construction as FieldEncryptionComponent."""
    from aws_encryption_sdk.identifiers import WrappingAlgorithm, EncryptionKeyType
    from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey
    from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
    import aws_encryption_sdk as _esdk

    key_source = (cfg.get("key_source") or "raw_key").lower()
    key_id = cfg.get("key_id") or "polars-pipeline-encrypted-file-key"
    if key_source == "raw_key":
        raw_key_env_var = cfg.get("raw_key_env_var")
        if not raw_key_env_var:
            raise ValueError("encrypted_file: key_source='raw_key' requires raw_key_env_var")
        raw_key_b64 = os.environ.get(raw_key_env_var, "")
        if not raw_key_b64:
            raise ValueError(f"encrypted_file: raw_key_env_var {raw_key_env_var!r} is unset or empty")
        raw_key_bytes = base64.b64decode(raw_key_b64)

        class _StaticRawMasterKeyProvider(RawMasterKeyProvider):
            provider_id = "polars-pipeline-encrypted-file"

            def __new__(cls, *args, **kwargs):
                return super(RawMasterKeyProvider, cls).__new__(cls)

            def __init__(self_mk, **kwargs):
                RawMasterKeyProvider.__init__(self_mk, **kwargs)

            def _get_raw_key(self_mk, key_id):
                return WrappingKey(
                    wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
                    wrapping_key=raw_key_bytes,
                    wrapping_key_type=EncryptionKeyType.SYMMETRIC,
                )

        provider = _StaticRawMasterKeyProvider()
        provider.add_master_key(key_id)
        return provider
    if key_source == "kms":
        kms_key_ids = cfg.get("kms_key_ids")
        if not kms_key_ids:
            raise ValueError("encrypted_file: key_source='kms' requires kms_key_ids")
        ids = kms_key_ids if isinstance(kms_key_ids, list) else [kms_key_ids]
        return _esdk.StrictAwsKmsMasterKeyProvider(key_ids=ids)
    raise ValueError(f"encrypted_file: key_source must be 'raw_key' or 'kms', got {key_source!r}")


def _infer_format_stripping_enc_suffix(path: str) -> Optional[str]:
    """Same extension-sniffing _read_file_source uses, but strips a trailing
    .enc/.encrypted suffix first (e.g. 'orders.csv.enc' -> infer from 'orders.csv')."""
    low = path.lower()
    for suf in (".enc", ".encrypted"):
        if low.endswith(suf):
            low = low[: -len(suf)]
            break
    if low.endswith(".ndjson") or low.endswith(".jsonl"):
        return "ndjson"
    if low.endswith(".json"):
        return "json"
    if low.endswith(".csv") or low.endswith(".tsv"):
        return "csv"
    if low.endswith(".parquet") or low.endswith(".pq"):
        return "parquet"
    if low.endswith(".ipc") or low.endswith(".arrow"):
        return "ipc"
    if low.endswith(".avro"):
        return "avro"
    return None


def _read_encrypted_file_source(pl_module, src: Dict[str, Any], partition_key: Optional[str], partition_map: Optional[Dict[str, str]]):
    """Read `kind: encrypted_file` -- decrypt the file's raw bytes via the
    real AWS Encryption SDK (file-level, same mechanism as
    FieldEncryptionComponent but whole-file instead of per-cell), then parse
    the plaintext bytes with the same format dispatch `_read_file_source`
    uses once bytes are on disk.

    Encryption-context binding: always adds a `path: <resolved path>` entry
    (the ACTUAL post-partition-template path, not the raw template string)
    on top of whatever `encryption_context:` is configured, so ciphertext
    can't be silently swapped between files/partitions -- same discipline
    FieldEncryptionComponent applies per-column.
    """
    try:
        import aws_encryption_sdk
    except ImportError as e:
        raise ImportError(
            "polars_pipeline: source kind=encrypted_file requires aws-encryption-sdk. "
            "Run: pip install aws-encryption-sdk"
        ) from e

    raw_path = src.get("path")
    if not raw_path:
        raise ValueError("polars_pipeline source kind=encrypted_file requires 'path'")
    path = _apply_partition_template(raw_path, partition_key, partition_map)

    fmt = (src.get("format") or "").lower()
    if not fmt:
        fmt = _infer_format_stripping_enc_suffix(path) or ""
        if not fmt:
            raise ValueError(
                f"polars_pipeline: can't infer format from {path!r} — set `format:` explicitly."
            )

    if _is_uri(path):
        try:
            import fsspec
        except ImportError:
            raise ImportError(
                "polars_pipeline: reading encrypted cloud URLs requires fsspec + a backend "
                "(pip install 's3fs' | 'gcsfs' | 'adlfs')."
            )
        storage_options = src.get("storage_options") or {}
        with fsspec.open(path, "rb", **storage_options) as fin:
            ciphertext = fin.read()
    else:
        with open(path, "rb") as fin:
            ciphertext = fin.read()

    key_provider = _build_esdk_key_provider(src)
    esdk_client = aws_encryption_sdk.EncryptionSDKClient(
        commitment_policy=aws_encryption_sdk.CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
    )
    expected_context = {**(src.get("encryption_context") or {}), "path": path}
    plaintext, header = esdk_client.decrypt(source=ciphertext, key_provider=key_provider)
    for k, v in expected_context.items():
        if header.encryption_context.get(k) != v:
            raise ValueError(
                f"polars_pipeline encrypted_file source: encryption_context mismatch for "
                f"path {path!r}: expected {k}={v!r}, got {header.encryption_context.get(k)!r}"
            )

    import tempfile as _tempfile
    _tmp = _tempfile.NamedTemporaryFile(suffix=_suffix_for(fmt), delete=False)
    try:
        _tmp.write(plaintext)
    finally:
        _tmp.close()

    delimiter = src.get("delimiter", ",")
    if fmt == "csv":
        return pl_module.scan_csv(_tmp.name, separator=delimiter)
    if fmt == "parquet":
        return pl_module.scan_parquet(_tmp.name)
    if fmt == "ndjson":
        return pl_module.scan_ndjson(_tmp.name)
    if fmt == "json":
        return pl_module.read_json(_tmp.name).lazy()
    if fmt == "ipc":
        return pl_module.scan_ipc(_tmp.name)
    if fmt == "avro":
        return pl_module.read_avro(_tmp.name).lazy()
    raise ValueError(f"polars_pipeline: format {fmt!r} not supported")


def _write_encrypted_file_sink(df, sink: Dict[str, Any], partition_key: Optional[str], partition_map: Optional[Dict[str, str]]) -> str:
    """Write `kind: encrypted_file` -- serialize the DataFrame to bytes in
    the given format, then encrypt those bytes via the real AWS Encryption
    SDK (file-level, same config shape as the encrypted_file source kind)
    before writing to `path`. Returns the resolved path for metadata."""
    try:
        import aws_encryption_sdk
    except ImportError as e:
        raise ImportError(
            "polars_pipeline: sink kind=encrypted_file requires aws-encryption-sdk. "
            "Run: pip install aws-encryption-sdk"
        ) from e

    raw_path = sink.get("path")
    if not raw_path:
        raise ValueError("polars_pipeline sink kind=encrypted_file requires 'path'")
    path = _apply_partition_template(raw_path, partition_key, partition_map)

    fmt = (sink.get("format") or "").lower()
    if not fmt:
        fmt = _infer_format_stripping_enc_suffix(path) or ""
        if not fmt:
            raise ValueError(
                f"polars_pipeline sink kind=encrypted_file: can't infer format from {path!r} — "
                "set `format:` explicitly."
            )
    if fmt not in ("csv", "parquet", "ndjson", "json"):
        raise ValueError(
            f"polars_pipeline sink kind=encrypted_file: format {fmt!r} not supported. "
            "Use 'csv' | 'parquet' | 'ndjson' | 'json'."
        )

    import io as _io
    buf = _io.BytesIO()
    if fmt == "csv":
        df.write_csv(buf)
    elif fmt == "parquet":
        df.write_parquet(buf)
    else:  # ndjson / json
        df.write_ndjson(buf)
    plaintext = buf.getvalue()

    key_provider = _build_esdk_key_provider(sink)
    esdk_client = aws_encryption_sdk.EncryptionSDKClient(
        commitment_policy=aws_encryption_sdk.CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
    )
    ctx = {**(sink.get("encryption_context") or {}), "path": path}
    ciphertext, _header = esdk_client.encrypt(source=plaintext, key_provider=key_provider, encryption_context=ctx)

    if _is_uri(path):
        try:
            import fsspec
        except ImportError:
            raise ImportError(
                "polars_pipeline: writing encrypted cloud URLs requires fsspec + a backend "
                "(pip install 's3fs' | 'gcsfs' | 'adlfs')."
            )
        storage_options = sink.get("storage_options") or {}
        with fsspec.open(path, "wb", **storage_options) as fout:
            fout.write(ciphertext)
    else:
        with open(path, "wb") as fout:
            fout.write(ciphertext)
    return path
# END file-level AWS Encryption SDK I/O


def _build_partitions_def(
    partition_type: Optional[str], partition_start: Optional[str],
    partition_values: Optional[Any], dynamic_partition_name: Optional[str],
    partition_dimensions: Optional[List[Dict[str, Any]]],
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Strict partition-def builder — matches ml_pipeline's shape."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError(
            "polars_pipeline: set either partition_type OR partition_dimensions, not both."
        )
    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":  return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
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
                raise ValueError("partition dimension type='static' requires 'values'")
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
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})
    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")

_PL_AGG_FUNCS = {"sum", "mean", "avg", "min", "max", "count", "median",
                 "std", "var", "first", "last", "nunique", "n_unique"}


def _pl_agg(pl_module, src_col: str, out_col: str, func: str):
    col = pl_module.col(src_col)
    f = func.lower()
    if f == "sum":     expr = col.sum()
    elif f in ("mean", "avg"): expr = col.mean()
    elif f == "min":   expr = col.min()
    elif f == "max":   expr = col.max()
    elif f == "count": expr = col.count()
    elif f == "median": expr = col.median()
    elif f == "std":   expr = col.std()
    elif f == "var":   expr = col.var()
    elif f == "first": expr = col.first()
    elif f == "last":  expr = col.last()
    elif f in ("nunique", "n_unique"): expr = col.n_unique()
    else:
        raise ValueError(f"agg func {func!r} not supported. Use one of {sorted(_PL_AGG_FUNCS)}")
    return expr.alias(out_col)


def _apply_op(pl, lf, op: Dict[str, Any], step_outputs: Dict[str, Any]):
    """Apply one op to a polars LazyFrame; returns the updated LazyFrame.

    step_outputs is the map of prior-step ids → their final LazyFrame, used
    by ops that reference other steps (join, union, sql).
    """
    kind = op["op"].lower()
    if kind == "filter":
        ctx = pl.SQLContext({"self": lf, **step_outputs})
        return ctx.execute(f"SELECT * FROM self WHERE {op['predicate']}", eager=False)
    if kind == "with_columns":
        # Two escape hatches:
        #   expressions: {col: "SQL EXPR"}      # via pl.sql_expr — SQL subset
        #   python_expressions: {col: "PY EXPR"} # via eval — full pl.* API
        # Both may appear in the same op; SQL first, then Python (Python can
        # reference columns that SQL just created).
        exprs = op.get("expressions") or {}
        py_exprs = op.get("python_expressions") or {}
        if not exprs and not py_exprs:
            raise ValueError(
                "with_columns needs either `expressions:` (SQL — parsed by "
                "polars.sql_expr) or `python_expressions:` (Python — eval'd "
                "with pl bound). See the polars SQL reference at "
                "https://docs.pola.rs/api/python/stable/reference/sql.html"
            )
        try:
            if exprs:
                lf = lf.with_columns([pl.sql_expr(e).alias(name) for name, e in exprs.items()])
        except Exception as e:  # noqa: BLE001
            raise ValueError(
                f"with_columns.expressions failed at SQL parse. Expressions "
                f"are polars-SQL (NOT the Python pl.* API). Common gotchas: "
                f"NOW() isn't in polars-SQL — use `python_expressions:` with "
                f"pl.lit(datetime.utcnow()) instead. Original error: {e}"
            )
        if py_exprs:
            # Restricted eval — namespace exposes polars + safe stdlib bits.
            # Users write pl.lit(datetime.utcnow()) style; no builtins.
            from datetime import datetime as _dt, timezone as _tz, date as _date, timedelta as _td
            _ns = {"pl": pl, "datetime": _dt, "timezone": _tz, "date": _date, "timedelta": _td}
            built_exprs = []
            for name, code in py_exprs.items():
                try:
                    e_obj = eval(code, {"__builtins__": {}}, _ns)  # noqa: S307
                except Exception as e:  # noqa: BLE001
                    raise ValueError(
                        f"python_expressions[{name!r}] eval failed: {e}. "
                        f"Namespace has: pl, datetime, timezone, date, timedelta. "
                        f"Example: 'pl.lit(datetime.utcnow())'"
                    )
                built_exprs.append(e_obj.alias(name))
            lf = lf.with_columns(built_exprs)
        return lf
    if kind == "select":
        # Plain names pass straight to lf.select(). A name containing '/' or '.'
        # is a StreamSets-style nested struct path (e.g. '/a/b' or 'a.b') --
        # resolved via struct.field() and aliased to the path with separators
        # replaced by '__' so it lands as a flat top-level column (no collision
        # with a same-named leaf under a different parent).
        cols = op["columns"]
        if any(("/" in c or "." in c) for c in cols if isinstance(c, str)):
            exprs = []
            for c in cols:
                if isinstance(c, str) and ("/" in c or "." in c):
                    exprs.append(_resolve_struct_path(pl, c).alias(c.replace("/", "__").replace(".", "__").strip("_")))
                else:
                    exprs.append(pl.col(c))
            return lf.select(exprs)
        return lf.select(cols)
    if kind == "drop":
        # Nested-struct-path drop isn't supported (would require rebuilding the
        # parent struct) -- top-level column names only. Use `select` to pick
        # the nested fields you want instead.
        return lf.drop(op["columns"])
    if kind == "rename":
        # Nested-struct-path rename isn't supported for the same reason as
        # drop -- top-level column names only. Use `select` (which can extract
        # + alias a nested path to a new top-level name) instead.
        return lf.rename(op["mapping"])
    if kind == "explode":
        return lf.explode(op["columns"] if isinstance(op.get("columns"), list) else op["column"])
    if kind == "to_datetime":
        # `pattern` is a JAVA SimpleDateFormat/DateTimeFormatter string (StreamSets'
        # convention, e.g. yyyy-MM-dd'T'HH:mm:ss.SSSXXX) -- translated to polars'
        # chrono-strftime format. Pass `pattern_style: polars` to use a native
        # polars/chrono format string as-is instead.
        column = op["column"]
        out_col = op.get("output_column", column)
        pattern = op["pattern"]
        if (op.get("pattern_style") or "java").lower() != "polars":
            pattern = _translate_java_date_pattern(pattern)
        dtype = pl.Datetime if (op.get("type") or "datetime").lower() == "datetime" else pl.Date
        return lf.with_columns(
            pl.col(column).str.strptime(dtype, format=pattern, strict=op.get("strict", False)).alias(out_col)
        )
    if kind == "coalesce":
        return lf.with_columns(
            pl.coalesce([pl.col(c) for c in op["columns"]]).alias(op["output_column"])
        )
    if kind == "route":
        # Multi-output routing: one input frame -> several named lanes by
        # predicate (polars-SQL, same dialect `filter` uses), with an optional
        # default/catch-all lane for rows matching none. Each lane becomes a
        # referenceable step output under step_outputs[f"{this_step_id}.{lane}"]
        # -- wired up by the caller (build_defs), not here, since this function
        # only has access to one LazyFrame's worth of context. Returning a dict
        # of lane -> LazyFrame signals the caller to fan it out instead of
        # treating the return value as a single next-lf.
        routes = op["routes"]  # [{name, predicate}, ...]
        default_name = op.get("default")
        ctx = pl.SQLContext({"self": lf, **step_outputs})
        lanes: Dict[str, Any] = {}
        matched_any = None
        for r in routes:
            name, predicate = r["name"], r["predicate"]
            lane_lf = ctx.execute(f"SELECT * FROM self WHERE {predicate}", eager=False)
            lanes[name] = lane_lf
        if default_name:
            # Catch-all: rows matching none of the predicates. Built as a SQL
            # NOT (p1) AND NOT (p2) AND ... over the same dialect.
            negated = " AND ".join(f"NOT ({r['predicate']})" for r in routes)
            lanes[default_name] = ctx.execute(f"SELECT * FROM self WHERE {negated}", eager=False) if negated else lf
        return {"__route__": lanes}
    if kind == "group_by":
        group_by = op["group_by"]
        aggregations = op["aggregations"]
        aggs = []
        for out_col, spec in aggregations.items():
            if isinstance(spec, dict) and "col" in spec and "agg" in spec:
                aggs.append(_pl_agg(pl, spec["col"], out_col, spec["agg"]))
            else:
                aggs.append(_pl_agg(pl, out_col, out_col, spec))
        # maintain_order=True: polars' streaming engine (default since 2.0) doesn't
        # guarantee group_by output order otherwise -- a silent reordering, not an
        # error, and this pipeline's own `head`/`limit`/`sort` steps can run right
        # after a group_by in the same config-driven chain (sort doesn't need the
        # order kept, but an unsorted head/limit right after group_by absolutely does).
        return lf.group_by(group_by, maintain_order=True).agg(aggs)
    if kind == "sort":
        return lf.sort(by=op["by"], descending=op.get("descending", False))
    if kind == "head" or kind == "limit":
        return lf.head(op["n"])
    if kind == "tail":
        return lf.tail(op["n"])
    if kind == "head_per_group":
        return lf.group_by(op["group_by"], maintain_order=True).head(op["n"])
    if kind == "unique":
        return lf.unique(
            subset=op.get("subset"),
            keep=op.get("keep", "first"),
            maintain_order=op.get("maintain_order", True),
        )
    if kind == "drop_nulls":
        return lf.drop_nulls(subset=op.get("subset"))
    if kind == "fill_null":
        return lf.fill_null(op.get("value", 0))
    if kind == "cast":
        mapping = op["mapping"]
        type_map = {name: getattr(pl, t) for name, t in mapping.items()}
        return lf.cast(type_map)
    if kind == "join":
        # Right side can be: {ref: <step_id>} (preferred) or a column name string isn't meaningful here.
        right_spec = op["right"]
        if not isinstance(right_spec, dict) or "ref" not in right_spec:
            raise ValueError("polars_pipeline join.right must be {ref: <step_id>}")
        right_id = right_spec["ref"]
        if right_id not in step_outputs:
            raise ValueError(f"join.right.ref={right_id!r} doesn't match any earlier step id")
        right_lf = step_outputs[right_id]
        how = op.get("how", "inner").lower()
        # maintain_order: polars' streaming engine (default since 2.0) doesn't
        # guarantee join output row order otherwise -- a silent reordering, not
        # an error. Preserve whichever side a pandas-trained user would still
        # expect stable (left's order for inner/left/semi/anti, right's for a
        # right join, both concatenated for a full join); not applicable to cross.
        _maintain_order_by_how = {
            "inner": "left", "left": "left", "semi": "left", "anti": "left",
            "right": "right", "full": "left_right", "outer": "left_right",
        }
        _mo = {"maintain_order": _maintain_order_by_how[how]} if how in _maintain_order_by_how else {}
        # suffix: polars' own default is '_right' on any overlapping non-join
        # column name; expose it so a converted Spark/StreamSets join that
        # expects a different disambiguation suffix doesn't have to rename
        # columns afterward.
        suffix = op.get("suffix", "_right")
        on_cols = op.get("on_columns") or op.get("on")
        if on_cols:
            return lf.join(right_lf, on=on_cols, how=how, suffix=suffix, **_mo)
        left_on, right_on = op.get("left_on"), op.get("right_on")
        if left_on and right_on:
            return lf.join(right_lf, left_on=left_on, right_on=right_on, how=how, suffix=suffix, **_mo)
        if how == "cross":
            return lf.join(right_lf, how="cross", suffix=suffix)
        raise ValueError("join op: provide 'on_columns' OR 'left_on' + 'right_on'")
    if kind == "union":
        other = op.get("other")
        if not isinstance(other, dict) or "ref" not in other:
            raise ValueError("polars_pipeline union.other must be {ref: <step_id>}")
        other_id = other["ref"]
        if other_id not in step_outputs:
            raise ValueError(f"union.other.ref={other_id!r} doesn't match any earlier step id")
        other_lf = step_outputs[other_id]
        mode = (op.get("mode") or "by_name").lower()
        if mode not in ("by_name", "by_position"):
            raise ValueError(f"union op: mode must be 'by_name' or 'by_position', got {mode!r}")
        # by_name: match columns by name across inputs, filling any missing ones with
        # null (dataframe_union's own default). by_position: take column order from the
        # left side and stack the right side positionally, tolerating mismatched dtypes.
        how = "diagonal_relaxed" if mode == "by_name" else "vertical_relaxed"
        out = pl.concat([lf, other_lf], how=how)
        if op.get("distinct", False):
            out = out.unique(maintain_order=True)
        return out
    if kind == "sql":
        sql = op.get("sql")
        if not sql or not isinstance(sql, str):
            raise ValueError("op='sql' requires a non-empty 'sql' string")
        # `self` is the current chain; prior step ids are usable by name.
        ctx = pl.SQLContext({"self": lf, **step_outputs})
        return ctx.execute(sql, eager=False)
    raise ValueError(f"polars_pipeline op={kind!r} not supported. Valid: {sorted(_VALID_OPS)}")



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


class PolarsPipelineComponent(Component, Model, Resolvable):
    """Multi-step polars LazyFrame pipeline in a single Dagster asset.

    Two shapes:
      * Flat shape: `upstream_asset_key` + `operations`.
      * Multi-step: `steps:` (each with `source`/`operations`) plus optional
        `sinks:` for side-output parquet/csv writes. The asset's return
        value is the `primary_step`'s frame (default: last step).

    Supported ops: filter, with_columns, select, drop, rename, group_by,
    sort, head/limit, tail, head_per_group, unique, drop_nulls, fill_null,
    cast, join, union, sql, explode, to_datetime, coalesce, route.

    **Converting a Spark/StreamSets Transformer pipeline:**
      - `select`/`rename` accept a StreamSets-style nested field path
        (`/a/b` or `a.b`) in addition to plain column names — `select`
        extracts + flattens a nested struct field to a new top-level column;
        `drop`/`rename` only work on top-level columns (select the fields
        you want instead for a nested rename/drop).
      - `explode: {columns: [...]}` explodes a list/array column into rows.
      - `to_datetime: {column, pattern, output_column?, pattern_style?}`
        parses a string column into a datetime. `pattern` is a JAVA
        SimpleDateFormat/DateTimeFormatter string by default (e.g.
        `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`) — translated to polars' own format
        automatically. Pass `pattern_style: polars` to use a native
        polars/chrono format string as-is. **Known limitation:** chrono's
        `%z` (what Java's X/Z tokens translate to) requires an explicit
        numeric offset (`+0000`) — a bare ISO-8601 `Z` (Zulu/UTC) literal in
        the input string will fail to parse; strip/replace a trailing `Z`
        with `+0000` upstream (e.g. in a `with_columns.python_expressions:`
        step) if your source data uses it.
      - `coalesce: {columns: [...], output_column}` — first non-null across
        several columns (`pl.coalesce`).
      - `route: {routes: [{name, predicate}, ...], default?}` fans one step
        out into several named lanes by predicate (same polars-SQL dialect
        as `filter`), with an optional `default` catch-all lane for rows
        matching no predicate. Each lane is referenceable as
        `source: {kind: ref, ref: "<step_id>.<lane_name>"}` from a later
        step, or as a sink's `from:`; if `default` is set, the bare step id
        also resolves to that lane.
      - `join` accepts `suffix:` (default `_right`, polars' own default) for
        disambiguating overlapping non-join column names.
      - Spark-SQL-function compatibility, for converting `with_columns`/
        `filter` expressions: most string/math/date functions have a direct
        polars-SQL equivalent (see the SQL reference link below). Functions
        with NO equivalent — use `python_expressions:` instead, or note the
        gap: `monotonically_increasing_id()` has no polars equivalent (no
        built-in monotonic-row-id concept); emulate it with `pl.int_range()`
        in a `python_expressions:` block if row numbering is all you need.

    **Expression syntax is SQL, not Python.** Every string value under
    `filter.predicate`, `with_columns.expressions.<col>`, and `sort.by`
    is parsed via `polars.sql_expr()` — you write `"amount - tax"` or
    `"COALESCE(status, 'unknown')"`, NOT `pl.col('amount') - pl.col('tax')`.
    See the polars SQL reference:
      https://docs.pola.rs/api/python/stable/reference/sql.html
    for the supported function catalog. `CAST(x AS INT)`, `CASE WHEN`,
    `LIKE`, string / substring / concat / regex functions all work.

    **Escape hatch: `python_expressions:` on `with_columns`.** When the
    polars-SQL subset is too narrow — e.g. `NOW()` isn't in polars-SQL —
    use the Python API instead:

        operations:
          - op: with_columns
            expressions:                        # SQL — polars.sql_expr
              total: "amount * quantity"
            python_expressions:                 # Python — pl.* API
              ingest_ts: "pl.lit(datetime.utcnow())"
              partition_col: "pl.lit(context.partition_key)"

    Namespace exposes `pl` + `datetime` / `timezone` / `date` /
    `timedelta`. Both blocks may co-exist; SQL runs first, then Python
    (so Python expressions can reference SQL-created columns).

    `op: sql` uses polars's SQLContext where the current chain is available
    as `self` and earlier step outputs are available by id.

    **Encrypted file I/O.** `source: {kind: encrypted_file, ...}` and
    `sinks: [{kind: encrypted_file, ...}]` decrypt/encrypt a whole file's
    bytes via the real AWS Encryption SDK (same mechanism as
    `FieldEncryptionComponent`, file-level instead of per-cell) — lets one
    asset do decrypt -> transform (via the ops above) -> re-encrypt ->
    stage in a single materialization. See the module docstring for a
    worked example.
    """

    asset_name: str = Field(description="Output Dagster asset name")

    # Flat-shape single-source shape ------------------------------------------
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Top-level single-source shape: Dagster upstream asset key (pandas or polars DataFrame). Mutually exclusive with `source:`.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "File / URL source shape. `{kind: file, path, format?, delimiter?, has_header?, "
            "quote_char?, schema_overrides?}` or `{kind: url, url, format?, delimiter?, "
            "has_header?, quote_char?, schema_overrides?}` (CSV only: has_header default true, "
            "quote_char default '\"', schema_overrides: {column: polars dtype name e.g. "
            "'Utf8'/'Int64'/'Float64'} -- pins a column's type instead of letting polars' own "
            "schema inference guess, e.g. a zero-padded code column like '001' silently becoming "
            "the integer 1) or `{kind: upstream_asset, "
            "upstream_asset_key}` or `{kind: encrypted_file, path, format?, key_source?, "
            "raw_key_env_var?, kms_key_ids?, key_id?, encryption_context?}` (decrypts the "
            "file's raw bytes via the real AWS Encryption SDK before parsing — same "
            "mechanism as FieldEncryptionComponent, file-level instead of per-cell), or "
            "`{kind: inline, rows: [...]}` / `{kind: inline, text: '...', format: csv|ndjson}` "
            "(rows or raw text embedded directly in the YAML — for StreamSets raw-data/dev-"
            "origin-style pipelines with no external file), or `{kind: sql, resource_key?, "
            "url_env_var?, query?, table?, offset_column?, incremental?, initial_offset?}` "
            "(reads via a SQLAlchemy-exposing resource or a raw connection URL env var; "
            "`incremental: true` gives a resumable `WHERE offset_column > <last stored "
            "value>` read, high-water mark persisted via output metadata — same mechanism "
            "mongodb_reader/elasticsearch_reader use). `path`/`url` support `{partition_key}` "
            "and `{partition.<name>}` templating. Format is inferred from extension (.json / "
            ".ndjson / .csv / .parquet / .ipc / avro — a trailing .enc/.encrypted is stripped "
            "first) when unset. Mutually exclusive with `upstream_asset_key`."
        ),
    )
    operations: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Flat shape: ordered list of ops applied to upstream_asset_key OR source. Compiles to one anonymous step.",
    )

    # Multi-step shape -----------------------------------------------------
    steps: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named steps. Each: {id, source: {kind: upstream|ref, upstream_asset_key|ref}, "
            "operations: [...]}."
        ),
    )
    sinks: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Optional side-output writes. Each: {from: <step_id>, kind: parquet|csv|avro|"
            "encrypted_file|table, path: '...'}. `kind: encrypted_file` additionally takes "
            "`format?, key_source?, raw_key_env_var?, kms_key_ids?, key_id?, "
            "encryption_context?` — encrypts the serialized bytes via the real AWS "
            "Encryption SDK before writing (same mechanism as FieldEncryptionComponent, "
            "file-level instead of per-cell). `parquet`/`csv`/`avro` additionally take "
            "`mode: overwrite|append` (append reads-concats-rewrites; single-writer, not "
            "concurrent-safe) and, for `kind: parquet` only, `partition_by: [col, ...]` to "
            "write a Hive-style directory of part-files instead of one file (polars' native "
            "`write_parquet(partition_by=...)`). These run after the chain finishes; the "
            "asset's return value comes from primary_step."
        ),
    )
    primary_step: Optional[str] = Field(
        default=None,
        description="Step id whose frame is returned as the asset's output (default: last step).",
    )

    # Partitions ---------------------------------------------------------
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'multi' | None for unpartitioned.",
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
            "materializes an entire backfill range in one run -- the partition_key "
            "token threaded through this pipeline's step templating becomes a "
            "comma-joined string spanning the whole range (not a single key) when "
            "the run covers a range. 'multi_run' launches one run per partition "
            "(optionally capped via backfill_policy_max_partitions_per_run). Leave "
            "unset for Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with partition_expr "
            "set) -- a generic filesystem/default IO manager cannot persist a "
            "single_run output spanning multiple partitions and will raise a clear "
            "error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO date for time-based partition types (daily/weekly/monthly/hourly).",
    )
    partition_values: Optional[Any] = Field(
        default=None,
        description="Comma-separated string OR list — the fixed partition keys for static/multi partitioning.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'. Must match the sensor's `dynamic_partitions_name`.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec (list of {name, type, start, values, dynamic_partition_name} dicts). Set INSTEAD of partition_type for multi-dimensional partitioning.",
    )

    # Asset metadata + execution -----------------------------------------
    output_type: str = Field(
        default="polars",
        description="'polars' (default) or 'pandas' — what the asset returns.",
    )
    streaming: bool = Field(
        default=False,
        description="Use polars streaming engine for the final collect.",
    )
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Asset-level metadata dict — passed straight through to "
            "`@asset(metadata=...)`. Useful for keys the IO manager reads at "
            "handle_output time (e.g. `{partition_expr: created_at}` for "
            "`DuckDBPolarsIOManagerComponent`, or "
            "`{dagster/column_schema: <schema>}` for a hand-authored schema). "
            "Kept separate from `asset_tags` — tags are for catalog filtering, "
            "metadata is arbitrary key-value data consumed by tooling."
        ),
    )
    include_preview_metadata: bool = Field(default=False)
    preview_rows: int = Field(default=25, ge=1, le=500)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('kinds', 'owners', 'deps'), csv=(), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    @classmethod
    def get_description(cls) -> str:
        return "Multi-step polars LazyFrame pipeline in a single Dagster asset (query-plan fusion across all steps)."

    def _normalize(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
        """Return (steps, sinks, upstream_keys).

        upstream_keys is the deduplicated list of Dagster upstream asset
        keys this asset depends on (collected from `upstream_asset_key`
        flat-shape field + any kind=upstream / kind=upstream_asset sources
        across steps). File / url source kinds don't produce upstream keys.
        """
        flat_upstream = bool(self.upstream_asset_key)
        flat_source = bool(self.source)
        flat_ops = self.operations is not None
        multi_present = bool(self.steps)

        if flat_upstream and flat_source:
            raise ValueError("polars_pipeline: set either `upstream_asset_key` OR `source:`, not both.")
        if multi_present and (flat_upstream or flat_source or flat_ops):
            raise ValueError(
                "polars_pipeline: choose ONE shape — top-level "
                "`upstream_asset_key`/`source:` + `operations`, OR `steps:`."
            )
        if multi_present:
            steps = list(self.steps or [])
            sinks = list(self.sinks or [])
            upstream_keys: List[str] = []
            for s in steps:
                src = s.get("source") or {}
                # Accept both 'upstream' (legacy) and 'upstream_asset' (matches ml_pipeline).
                if src.get("kind") in ("upstream", "upstream_asset"):
                    k = src.get("upstream_asset_key")
                    if not k:
                        raise ValueError(f"step {s.get('id')!r}: source kind=upstream needs 'upstream_asset_key'")
                    if k not in upstream_keys:
                        upstream_keys.append(k)
            return steps, sinks, upstream_keys

        if not flat_ops:
            raise ValueError(
                "polars_pipeline: provide either `steps:` OR top-level "
                "`operations:` (with `upstream_asset_key` OR `source:`)."
            )
        flat_sinks = list(self.sinks or [])
        if flat_upstream:
            flat_step = {
                "id": "_default",
                "source": {"kind": "upstream", "upstream_asset_key": self.upstream_asset_key},
                "operations": list(self.operations),
            }
            return [flat_step], flat_sinks, [self.upstream_asset_key]
        # flat_source path (file / url / upstream_asset)
        src = dict(self.source or {})
        if src.get("kind") in ("upstream", "upstream_asset") and src.get("upstream_asset_key"):
            upstream_key = src["upstream_asset_key"]
            flat_step = {
                "id": "_default",
                "source": {"kind": "upstream", "upstream_asset_key": upstream_key},
                "operations": list(self.operations),
            }
            return [flat_step], flat_sinks, [upstream_key]
        # file / url — no upstream dependency
        flat_step = {
            "id": "_default",
            "source": src,
            "operations": list(self.operations),
        }
        return [flat_step], flat_sinks, []

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Polars Pipeline', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        steps, sinks, upstream_keys = self._normalize()
        # Validate each step's ops up front.
        for s in steps:
            for i, op in enumerate(s.get("operations") or []):
                if not isinstance(op, dict) or "op" not in op:
                    raise ValueError(f"step {s.get('id')!r} op #{i + 1}: each op must be a dict with an 'op' key")
                if op["op"].lower() not in _VALID_OPS:
                    raise ValueError(
                        f"step {s.get('id')!r} op #{i + 1}: op={op['op']!r} not supported. "
                        f"Valid: {sorted(_VALID_OPS)}"
                    )

        asset_name = self.asset_name
        output_type = self.output_type.lower()
        if output_type not in ("polars", "pandas"):
            raise ValueError(f"output_type must be 'polars' or 'pandas', got {self.output_type!r}")
        streaming = self.streaming
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        if not steps:
            raise ValueError("polars_pipeline: at least one step is required.")
        primary_step: str = str(self.primary_step or steps[-1].get("id") or "")
        if not primary_step:
            raise ValueError("polars_pipeline: primary_step could not be determined; every step must have an 'id'.")

        kinds = list(self.kinds or []) or ["polars"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        # One AssetIn per distinct upstream — input arg names get sanitized.
        ins = {
            f"upstream_{j}": AssetIn(key=AssetKey.from_user_string(k))
            for j, k in enumerate(upstream_keys)
        }
        upstream_arg_names = {k: f"upstream_{j}" for j, k in enumerate(upstream_keys)}

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        # Auto-detect required Dagster resource keys from table-kind sinks.
        required_resource_keys: set = set()
        for sink in sinks:
            if (sink.get("kind") or "parquet").lower() == "table":
                rk = sink.get("resource_key")
                if rk:
                    required_resource_keys.add(rk)

        @asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            ins=ins,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            required_resource_keys=required_resource_keys or None,
            metadata=self.metadata or {},
        )
        def _pipeline_asset(context: AssetExecutionContext, **upstreams: Any) -> Any:
            import polars as pl

            # Partition-key substitution values for file/url path templating.
            if context.has_partition_key:
                partition_key = context.partition_key
            elif context.has_partition_key_range:
                # single_run backfill: represent the whole range as one joined token.
                partition_key = ",".join(str(k) for k in context.partition_keys)
            else:
                partition_key = None
            partition_map: Dict[str, str] = {}
            if context.has_partition_key:
                pk = context.partition_key
                if hasattr(pk, "keys_by_dimension"):
                    partition_map = dict(pk.keys_by_dimension)

            def _to_lazy(obj: Any):
                if isinstance(obj, pl.LazyFrame):
                    return obj
                if isinstance(obj, pl.DataFrame):
                    return obj.lazy()
                return pl.from_pandas(obj).lazy()

            # step_outputs accumulates the LazyFrame at the END of each step.
            step_outputs: Dict[str, Any] = {}

            import time as _time
            step_timings: Dict[str, float] = {}
            step_op_counts: Dict[str, int] = {}
            incremental_offsets: Dict[str, Any] = {}
            for s_idx, step in enumerate(steps):
                sid = step["id"]
                _t0 = _time.time()
                src = step.get("source") or {}
                src_kind = src.get("kind", "upstream")
                if src_kind in ("upstream", "upstream_asset"):
                    uk = src.get("upstream_asset_key") or ""
                    arg = upstream_arg_names[uk]
                    lf = _to_lazy(upstreams[arg])
                elif src_kind in ("file", "url"):
                    lf = _read_file_source(pl, src, partition_key, partition_map)
                    context.log.info(
                        f"step {sid} source: read {src_kind} → {(src.get('path') or src.get('url'))!r}"
                    )
                elif src_kind == "encrypted_file":
                    lf = _read_encrypted_file_source(pl, src, partition_key, partition_map)
                    context.log.info(
                        f"step {sid} source: decrypted encrypted_file → {src.get('path')!r}"
                    )
                elif src_kind == "inline":
                    lf = _read_inline_source(pl, src)
                    context.log.info(f"step {sid} source: inline ({len(src.get('rows') or [])} row(s) or raw text)")
                elif src_kind in ("sql", "jdbc"):
                    lf = _read_sql_source(pl, src, context, asset_name, sid, incremental_offsets)
                    context.log.info(
                        f"step {sid} source: sql"
                        + (f" (incremental on {src.get('offset_column')})" if src.get("incremental") else "")
                    )
                elif src_kind == "ref":
                    ref = src.get("ref") or ""
                    if ref not in step_outputs:
                        raise ValueError(f"step {sid!r}: source ref={ref!r} not yet defined")
                    lf = step_outputs[ref]
                else:
                    raise ValueError(f"step {sid!r}: source.kind={src_kind!r} not supported. Use 'upstream' | 'file' | 'url' | 'encrypted_file' | 'inline' | 'sql' | 'ref'.")

                ops = step.get("operations") or []
                context.log.info(f"step {sid} ({s_idx + 1}/{len(steps)}): {len(ops)} op(s)")
                for op in ops:
                    lf = _apply_op(pl, lf, op, step_outputs)
                # `op: route` fans one step out into several named lanes instead
                # of a single next LazyFrame -- each lane becomes referenceable
                # as step_outputs["<step_id>.<lane>"] (reachable via
                # source: {kind: ref, ref: "<step_id>.<lane>"} from a later
                # step, or as a sink's `from:`). If a `default:` catch-all lane
                # was named, the bare step id also resolves to that lane, so
                # `primary_step` / an unqualified ref still works.
                if isinstance(lf, dict) and "__route__" in lf:
                    lanes = lf["__route__"]
                    default_name = ops[-1].get("default") if ops and ops[-1].get("op", "").lower() == "route" else None
                    for lane_name, lane_lf in lanes.items():
                        step_outputs[f"{sid}.{lane_name}"] = lane_lf
                    if default_name and default_name in lanes:
                        step_outputs[sid] = lanes[default_name]
                else:
                    step_outputs[sid] = lf
                # Lazy-plan build time — the actual work happens at .collect()
                # on the primary step. This measures "how long did it take to
                # construct the query graph up through this step".
                step_timings[sid] = round(_time.time() - _t0, 4)
                step_op_counts[sid] = len(ops)

            # Collect the primary step. The polars planner fuses everything
            # touched by that final LazyFrame — including ops in earlier
            # steps it referenced.
            primary_lf = step_outputs[primary_step]
            # Capture the optimized plan BEFORE collect so we can surface it
            # as metadata even if collect fails. .explain() is cheap.
            try:
                explain_plan = str(primary_lf.explain(optimized=True))
            except Exception:  # noqa: BLE001
                try:
                    explain_plan = str(primary_lf.explain())
                except Exception:  # noqa: BLE001
                    explain_plan = "(explain unavailable)"
            _collect_t0 = _time.time()
            try:
                result_pl = primary_lf.collect(engine="streaming" if streaming else "auto")
            except TypeError:
                result_pl = primary_lf.collect(streaming=streaming) if streaming else primary_lf.collect()
            collect_seconds = round(_time.time() - _collect_t0, 4)

            # Run any side-output sinks.
            sink_metadata: Dict[str, Any] = {}
            for sink in sinks:
                from_id = sink.get("from") or ""
                if from_id not in step_outputs:
                    raise ValueError(f"sink.from={from_id!r} doesn't match any step id")
                kind = (sink.get("kind") or "parquet").lower()
                df = step_outputs[from_id].collect()  # materialize side-output

                if kind in ("parquet", "csv", "avro"):
                    path = sink.get("path")
                    if not path:
                        raise ValueError(f"sink from={from_id!r}: 'path' is required")
                    path = _apply_partition_template(path, partition_key, partition_map)
                    mode = (sink.get("mode") or "overwrite").lower()
                    partition_by = sink.get("partition_by")
                    if mode not in ("overwrite", "append"):
                        raise ValueError(f"sink from={from_id!r}: mode must be 'overwrite' or 'append', got {mode!r}")
                    if partition_by and kind != "parquet":
                        raise ValueError(f"sink from={from_id!r}: 'partition_by' (directory/partitioned output) is only supported for kind=parquet")
                    if mode == "append" and os.path.exists(path):
                        # Polars has no native append for parquet/csv -- read the
                        # existing file, concat, rewrite. Single-writer assumption
                        # (same as every other file sink in this component); not
                        # concurrent-write-safe.
                        if kind == "parquet":
                            existing = pl.read_parquet(path)
                        else:
                            existing = pl.read_csv(path)
                        df = pl.concat([existing, df], how="diagonal_relaxed")
                    if kind == "parquet":
                        if partition_by:
                            df.write_parquet(path, partition_by=partition_by, mkdir=True)
                        else:
                            df.write_parquet(path)
                    elif kind == "csv":
                        df.write_csv(path)
                    else:  # avro
                        df.write_avro(path)
                    sink_metadata[f"polars/sink/{from_id}/path"] = MetadataValue.path(path)
                    if mode == "append":
                        sink_metadata[f"polars/sink/{from_id}/mode"] = MetadataValue.text("append")
                elif kind == "encrypted_file":
                    written_path = _write_encrypted_file_sink(df, sink, partition_key, partition_map)
                    sink_metadata[f"polars/sink/{from_id}/path"] = MetadataValue.path(written_path)
                    sink_metadata[f"polars/sink/{from_id}/encrypted"] = MetadataValue.bool(True)
                elif kind == "table":
                    # Write to any Dagster resource that exposes .get_engine() /
                    # .get_connection() (e.g. postgres_resource, duckdb_resource,
                    # snowflake_resource, ...). Mirrors ml_pipeline.table_sinks
                    # and streaming_consumer._write_sink.
                    #
                    # Auto-detects DuckDB's `.register()` fast path: if the
                    # yielded connection has .register/.execute/.unregister, we
                    # register the polars frame as a view and INSERT ... SELECT
                    # * FROM view — 10-100x faster than SQLAlchemy to_sql on
                    # large batches and zero-copy for polars. Otherwise falls
                    # back to df.to_pandas().to_sql (works with every
                    # SQLAlchemy resource: postgres / snowflake / bigquery /
                    # mysql / mssql / oracle / db2 / …). Users always write
                    # `kind: table` — no vendor-specific YAML flags.
                    resource_key = sink.get("resource_key")
                    if not resource_key:
                        raise ValueError(f"sink from={from_id!r} kind=table: 'resource_key' is required")
                    table = _apply_partition_template(sink.get("table") or "", partition_key, partition_map)
                    if not table:
                        raise ValueError(f"sink from={from_id!r} kind=table: 'table' is required")
                    schema = sink.get("schema")
                    if_exists = sink.get("if_exists", "append")
                    partition_col = sink.get("partition_column")
                    # `mode: upsert_on_match` + `match: [col, ...]` gives partition-
                    # rewrite idempotency: DELETE existing rows keyed by match, then
                    # INSERT the new batch. Safe to re-run the same partition without
                    # duplicating rows. When absent, falls back to `if_exists` semantics.
                    mode = (sink.get("mode") or "").lower() or None
                    match_cols: List[str] = list(sink.get("match") or [])
                    if mode == "upsert_on_match" and not match_cols:
                        raise ValueError(
                            f"sink from={from_id!r} mode=upsert_on_match requires 'match: [col, ...]'"
                        )

                    # Add partition_column with the partition_key value — analytics-friendly
                    # single-table pattern (WHERE partition_date = ...).
                    if partition_key and partition_col:
                        df = df.with_columns(pl.lit(str(partition_key)).alias(partition_col))

                    resource = getattr(context.resources, resource_key)
                    from contextlib import nullcontext

                    def _acquire():
                        if hasattr(resource, "get_connection"):
                            gc = resource.get_connection()
                            return gc if hasattr(gc, "__enter__") else nullcontext(gc)
                        if hasattr(resource, "get_engine"):
                            eng = resource.get_engine()
                            return eng if hasattr(eng, "__enter__") else nullcontext(eng)
                        raise ValueError(
                            f"resource {resource_key!r} must expose .get_connection() or .get_engine()"
                        )

                    qualified = f"{schema}.{table}" if schema else table
                    with _acquire() as conn:
                        # Fast path: DuckDB-style .register() / .execute() / .unregister().
                        if (
                            hasattr(conn, "register")
                            and hasattr(conn, "execute")
                            and hasattr(conn, "unregister")
                        ):
                            conn.register("_polars_sink_batch", df)
                            try:
                                if mode == "upsert_on_match":
                                    # DELETE rows whose match tuple exists in the batch,
                                    # then INSERT the batch. Idempotent per-partition.
                                    conn.execute(
                                        f"CREATE TABLE IF NOT EXISTS {qualified} AS "
                                        f"SELECT * FROM _polars_sink_batch WHERE 1=0"
                                    )
                                    match_tuple = ", ".join(match_cols)
                                    conn.execute("BEGIN TRANSACTION")
                                    try:
                                        conn.execute(
                                            f"DELETE FROM {qualified} WHERE ({match_tuple}) IN "
                                            f"(SELECT DISTINCT {match_tuple} FROM _polars_sink_batch)"
                                        )
                                        conn.execute(
                                            f"INSERT INTO {qualified} "
                                            f"SELECT * FROM _polars_sink_batch"
                                        )
                                        conn.execute("COMMIT")
                                    except Exception:
                                        conn.execute("ROLLBACK")
                                        raise
                                    sink_metadata[f"polars/sink/{from_id}/mode"] = MetadataValue.text(
                                        f"upsert_on_match({','.join(match_cols)})"
                                    )
                                elif if_exists == "replace":
                                    conn.execute(
                                        f"CREATE OR REPLACE TABLE {qualified} AS "
                                        f"SELECT * FROM _polars_sink_batch"
                                    )
                                else:
                                    conn.execute(
                                        f"CREATE TABLE IF NOT EXISTS {qualified} AS "
                                        f"SELECT * FROM _polars_sink_batch WHERE 1=0"
                                    )
                                    conn.execute("BEGIN TRANSACTION")
                                    try:
                                        conn.execute(
                                            f"INSERT INTO {qualified} "
                                            f"SELECT * FROM _polars_sink_batch"
                                        )
                                        conn.execute("COMMIT")
                                    except Exception:
                                        conn.execute("ROLLBACK")
                                        raise
                                sink_metadata[f"polars/sink/{from_id}/fast_path"] = MetadataValue.text(
                                    "duckdb-register"
                                )
                            finally:
                                try:
                                    conn.unregister("_polars_sink_batch")
                                except Exception:  # noqa: BLE001
                                    pass
                        else:
                            # Fallback: SQLAlchemy path. `mode: upsert_on_match` runs
                            # DELETE + to_sql(append) in a single transaction.
                            if mode == "upsert_on_match":
                                pdf = df.to_pandas()
                                match_tuple = ", ".join(match_cols)
                                # Build a values-list for the DELETE. Small batches only —
                                # for huge fanouts, prefer the DuckDB fast path or a
                                # temp-table strategy (future work).
                                distinct = pdf[match_cols].drop_duplicates()
                                if len(distinct) > 0:
                                    from sqlalchemy import text as _sa_text
                                    placeholders = ", ".join(
                                        "(" + ", ".join(f":v{i}_{j}" for j in range(len(match_cols))) + ")"
                                        for i in range(len(distinct))
                                    )
                                    params = {}
                                    for i, row in enumerate(distinct.itertuples(index=False)):
                                        for j, v in enumerate(row):
                                            params[f"v{i}_{j}"] = v
                                    tx = conn.begin() if hasattr(conn, "begin") else nullcontext()
                                    with tx:
                                        conn.execute(
                                            _sa_text(
                                                f"DELETE FROM {qualified} "
                                                f"WHERE ({match_tuple}) IN ({placeholders})"
                                            ),
                                            params,
                                        )
                                        pdf.to_sql(
                                            table, conn, schema=schema,
                                            if_exists="append", index=False,
                                        )
                                else:
                                    pdf.to_sql(
                                        table, conn, schema=schema,
                                        if_exists="append", index=False,
                                    )
                                sink_metadata[f"polars/sink/{from_id}/mode"] = MetadataValue.text(
                                    f"upsert_on_match({','.join(match_cols)})"
                                )
                            else:
                                df.to_pandas().to_sql(
                                    table, conn, schema=schema, if_exists=if_exists, index=False,
                                )
                            sink_metadata[f"polars/sink/{from_id}/fast_path"] = MetadataValue.text(
                                "sqlalchemy-to_sql"
                            )

                    sink_metadata[f"polars/sink/{from_id}/table"] = MetadataValue.text(qualified)
                    sink_metadata[f"polars/sink/{from_id}/resource_key"] = MetadataValue.text(resource_key)
                    context.log.info(
                        f"table sink {from_id!r} → {qualified} "
                        f"(via {resource_key}, {if_exists}, {df.height} rows)"
                    )
                else:
                    raise ValueError(
                        f"sink.kind={kind!r} not supported. Use 'parquet' | 'csv' | 'avro' | 'encrypted_file' | 'table'."
                    )
                sink_metadata[f"polars/sink/{from_id}/row_count"] = MetadataValue.int(df.height)

            row_count = result_pl.height
            _meta_df = result_pl.to_pandas()
            from dagster import TableSchema, TableColumn
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(_meta_df.dtypes[col]))
                for col in _meta_df.columns
            ])
            # Estimated memory footprint of the materialized DataFrame.
            try:
                mem_bytes = int(result_pl.estimated_size())
            except Exception:  # noqa: BLE001
                mem_bytes = 0
            metadata: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(row_count),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "polars/step_count": MetadataValue.int(len(steps)),
                "polars/primary_step": MetadataValue.text(primary_step or ""),
                "polars/streaming": MetadataValue.bool(streaming),
                "polars/collect_seconds": MetadataValue.float(collect_seconds),
                "polars/estimated_bytes": MetadataValue.int(mem_bytes),
                "polars/estimated_mb": MetadataValue.float(round(mem_bytes / (1024 * 1024), 3)),
                "polars/step_timings_seconds": MetadataValue.json(step_timings),
                "polars/step_op_counts": MetadataValue.json(step_op_counts),
                "polars/explain": MetadataValue.md(f"```\n{explain_plan}\n```"),
            }
            for step_id, new_offset in incremental_offsets.items():
                if new_offset is not None:
                    metadata[f"dagster_community_components/last_offset/{step_id}"] = MetadataValue.text(str(new_offset))
            metadata.update(sink_metadata)
            if include_preview and row_count > 0:
                try:
                    _prev = _meta_df.sample(min(preview_rows, row_count)) if row_count > preview_rows * 10 else _meta_df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            context.add_output_metadata(metadata)

            return result_pl if output_type == "polars" else result_pl.to_pandas()

        return Definitions(assets=[_pipeline_asset])
