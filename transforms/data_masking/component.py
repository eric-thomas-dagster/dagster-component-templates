"""DataMaskingComponent.

Rule-based PII masking — hash, partial-mask (last 4 only), full-redact, or character-substitute. Per-column policies.
"""

import os
from typing import Any, Optional, Dict

import dagster as dg
import pandas as pd
from pydantic import Field



def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine()
    (SQLAlchemy), .get_connection() (DBAPI), or .get_client() (vendor
    client -- dispatched by the client's own shape since "get_client" means
    something different per vendor: BigQuery's .query(sql).to_dataframe(),
    Redshift's .execute_query(sql, fetch_results=True, cursor_factory=
    RealDictCursor)), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no Dagster resource is registered. Same
    helper, same contract, as every other dual-ingestion component in this
    repo (e.g. automl_asset, logistic_regression_model, churn_prediction)."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        if hasattr(resource, "get_client"):
            client = resource.get_client()
            if hasattr(client, "query"):
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(sql, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this helper doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found). Add a dispatch branch for it."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        import os
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


_STREAMSETS_FIELD_TYPES = (
    "boolean", "byte", "short", "integer", "long", "float", "double",
    "string", "byte_array", "datetime",
)


def streamsets_field_bytes(value: Any, field_type: Optional[str] = None) -> bytes:
    """Serialize `value` exactly the way StreamSets Data Collector's real Field Hasher does, confirmed by
    reading its own open-source implementation directly (com.streamsets.pipeline.lib.hashing.HashingUtil.
    RecordFunnel.funnel(), Guava's PrimitiveSink): a null field is a single sentinel byte (Guava's own
    putBoolean(true) call for a null value), BOOLEAN is 1 byte (0/1), and every fixed-width numeric type is
    written LITTLE-ENDIAN at its own native width (Guava's AbstractStreamingHasher convention, confirmed by
    independently reproducing a reported real-engine MD5 of the integer 42: int32 little-endian 42 ->
    md5 9824a7030ce67cf3f0efe7529f0c6ecc, matching byte-for-byte) -- NOT the text representation this
    component's default (encoding unset) hash mode uses. Only STRING/DECIMAL fields are UTF-8 text bytes;
    BYTE_ARRAY is passed through unchanged; DATE/TIME/DATETIME are all an 8-byte little-endian epoch-millis
    LONG (Guava's putLong(field.getValueAsDate().getTime())).

    `field_type` pins the exact StreamSets Field.Type this value had when StreamSets hashed it (one of
    boolean/byte/short/integer/long/float/double/string/byte_array/datetime) -- set it when you know it, e.g.
    from the source schema. Left unset, this infers the narrowest StreamSets numeric type Data Collector's own
    data-driven parsers (JSON, CSV with inferred types, Avro-less text) would have picked for that value: a
    bool is BOOLEAN; an int fitting in a signed 32-bit range is INTEGER (4 bytes), otherwise LONG (8 bytes);
    a float is always DOUBLE (8 bytes); anything with a `.timestamp()` method (`datetime.datetime`,
    `pandas.Timestamp`) is DATETIME -- Data Collector's own JSON/text parsers never infer the narrower
    FLOAT/SHORT/BYTE types on their own, only an explicit schema or a Field Type Converter stage does, which
    this best-effort inference cannot see. Pin `field_type` explicitly if your source pipeline used one of
    those narrower types (or BOOLEAN-as-integer-flag, etc.) -- the hash will otherwise come out wrong for
    that specific field, silently (a different valid StreamSets digest, not an error)."""
    import struct

    if value is None or (not isinstance(value, (bytes, bytearray)) and pd.isna(value)):
        return b"\x01"  # Guava's putBoolean(true) null-field sentinel

    ft = field_type
    if ft is None:
        if isinstance(value, bool):
            ft = "boolean"
        elif isinstance(value, int):
            ft = "integer" if -(2**31) <= value <= 2**31 - 1 else "long"
        elif isinstance(value, float):
            ft = "double"
        elif isinstance(value, (bytes, bytearray)):
            ft = "byte_array"
        elif hasattr(value, "timestamp") and callable(value.timestamp):  # datetime.datetime / pd.Timestamp
            ft = "datetime"
        else:
            ft = "string"
    elif ft not in _STREAMSETS_FIELD_TYPES:
        raise ValueError(f"field_type must be one of {_STREAMSETS_FIELD_TYPES}, got {field_type!r}")

    if ft == "boolean":
        return b"\x01" if bool(value) else b"\x00"
    if ft == "byte":
        return struct.pack("<b", int(value))
    if ft == "short":
        return struct.pack("<h", int(value))
    if ft == "integer":
        return struct.pack("<i", int(value))
    if ft == "long":
        return struct.pack("<q", int(value))
    if ft == "float":
        return struct.pack("<f", float(value))
    if ft == "double":
        return struct.pack("<d", float(value))
    if ft == "datetime":
        # StreamSets' own getTime() is epoch MILLISECONDS; .timestamp() is epoch SECONDS (a float), so
        # this must multiply by 1000 before truncating to an int. `value` itself may already be a plain
        # epoch-seconds number (field_type explicitly pinned) rather than a datetime/Timestamp object.
        seconds = value.timestamp() if hasattr(value, "timestamp") else float(value)
        return struct.pack("<q", int(round(seconds * 1000)))
    if ft == "byte_array":
        return bytes(value)
    return str(value).encode("utf-8")


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


class DataMaskingComponent(dg.Component, dg.Model, dg.Resolvable):
    """Rule-based PII masking — hash, partial-mask (last 4 only), full-redact, or character-substitute. Per-column policies."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream DataFrame asset key. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )

    rules: list = Field(
        description=(
            "List of {column, method, ...} dicts. method: 'hash' | 'partial' | 'redact' | "
            "'substitute' | 'pseudonymize' | 'mask'. For method='hash', optional StreamSets-Field-Hasher-"
            "parity fields: `algorithm` ('sha256' default | 'md5' | 'sha1' | 'sha512' | "
            "'murmur3_128'; 'sha256' truncates+salts ONLY when `hash_mode` is also the default "
            "'in_place' -- the original, unchanged behavior -- every other algorithm/hash_mode "
            "combination is a full, unsalted hex digest, matching StreamSets' own output), "
            "`encoding` (unset default: hash the value's TEXT representation, this component's "
            "original behavior; 'streamsets': hash the value as StreamSets' real Field Hasher "
            "does -- each value's own native per-type bytes, little-endian, not its text form -- "
            "this is a REAL behavior difference confirmed against StreamSets' own open-source "
            "Field Hasher code, not a cosmetic option: an INTEGER/LONG/DOUBLE/BOOLEAN column "
            "hashed with encoding unset will NOT match a hash StreamSets itself produced for the "
            "same values), `field_type` (encoding='streamsets' only; pins the exact StreamSets "
            "Field.Type a value had -- one of boolean/byte/short/integer/long/float/double/string/"
            "byte_array/datetime -- when left unset this infers the narrowest type Data "
            "Collector's own JSON/text parsers would pick, which cannot see a narrower type an "
            "explicit schema or Field Type Converter stage assigned upstream; see "
            "streamsets_field_bytes()'s own docstring for exactly what is and isn't inferred), "
            "`hash_mode` ('in_place' default | 'target_field' | 'record'), `output_field` "
            "(required for target_field/record), `columns` (list, required for 'record' -- "
            "fields concatenated together before hashing), `field_separator` (appended after "
            "each field's value in 'record' mode, including the last -- matches StreamSets' own "
            "Hash Record behavior). For method='mask' (StreamSets Field Masker parity), "
            "`mask_mode`: 'fixed_length' (always outputs the literal 10-char string 'xxxxxxxxxx', "
            "regardless of input), 'variable_length' (one 'x' per input character), 'custom' "
            "(requires `mask`: a pattern string where '#' copies the next input character through, "
            "'x' outputs a literal mask char, and any other character is literal output -- consuming "
            "an input character only if the input already has that same literal character at that "
            "position, so pre-formatted input like '123-45-6789' against mask '###-##-####' both "
            "shows digits and preserves the dashes without doubling them), or 'regex' (requires "
            "`regex` and `groups_to_show`: a list of 1-based capture-group numbers -- the regex must "
            "match the ENTIRE value, Python `re.fullmatch` semantics matching StreamSets' own "
            "`Matcher.matches()`; a non-matching value is returned completely unmasked, not an error; "
            "on a match, every character becomes 'x' except the listed groups' original characters at "
            "their original positions). All four mask_mode behaviors are reproduced from "
            "StreamSets' own open-source FieldMaskProcessor.java (verified by reading that file "
            "directly, not inferred) -- see README for the exact source commit referenced. "
            "See README for exact parity semantics and caveats."
        )
    )
    salt_env: Optional[str] = Field(default="MASKING_SALT", description="Env var with hash salt (improves resistance to rainbow attacks)")
    redacted_value: str = Field(default="***", description="Replacement value for 'redact' method")

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="transforms")
    deps: Optional[list[str]] = Field(default=None)
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=20)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Data Masking', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("DataMaskingComponent: set exactly one of `upstream_asset_key` or `source`.")
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _self = self

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or "Rule-based PII masking — hash, partial-mask (last 4 only), full-redact, or character-substitute. Per-column policies.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['pii', 'masking', 'anonymize']),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins=({"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))} if self.upstream_asset_key else None),
            required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs) -> pd.DataFrame:
            df = kwargs.get("df")
            if df is None:
                df = _ingest_warehouse_query(_self.source, context)
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(df, dict):
                _frames = [v for v in df.values() if isinstance(v, pd.DataFrame)]
                df = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            self = _self
            import hashlib, os, re
            salt = os.environ.get(self.salt_env, "") if self.salt_env else ""
            df = df.copy()
            for rule in self.rules:
                col = rule["column"]
                method = rule["method"]
                if col not in df.columns:
                    continue
                if method == "hash":
                    algorithm = rule.get("algorithm", "sha256")
                    hash_mode = rule.get("hash_mode", "in_place")
                    field_separator = rule.get("field_separator", "")
                    encoding = rule.get("encoding")
                    field_type = rule.get("field_type")
                    if encoding not in (None, "streamsets"):
                        raise ValueError(f"unknown encoding: {encoding!r}; the only supported value is 'streamsets'")
                    if field_type is not None and hash_mode == "record":
                        raise ValueError(
                            "field_type is not supported with hash_mode='record' (it spans several columns, "
                            "each with its own type) -- leave field_type unset; each column's type is inferred "
                            "independently per value"
                        )

                    def _hash_bytes(data: bytes, _algorithm=algorithm) -> str:
                        # The encoding='streamsets' path: `data` is ALREADY the exact bytes StreamSets'
                        # own Field Hasher would hash (see streamsets_field_bytes) -- a full, unsalted
                        # hex digest, same as the text path's non-default combinations below.
                        if _algorithm in ("sha256", "md5", "sha1", "sha512"):
                            return hashlib.new(_algorithm, data).hexdigest()
                        if _algorithm == "murmur3_128":
                            try:
                                import mmh3
                            except ImportError as e:
                                raise ImportError(
                                    "algorithm='murmur3_128' requires the 'mmh3' package. "
                                    "Run: pip install mmh3"
                                ) from e
                            return mmh3.mmh3_x64_128_digest(data, 0).hex()
                        raise ValueError(f"unknown hash algorithm: {_algorithm!r}")

                    def _digest(value: str, _algorithm=algorithm, _hash_mode=hash_mode, _salt=salt) -> str:
                        # Only the ORIGINAL code path -- default algorithm 'sha256' in the
                        # default 'in_place' mode -- applies the configured salt and truncates
                        # to 16 hex chars; that's this component's pre-existing behavior,
                        # preserved unchanged for backward compat. Every other combination (a
                        # non-default algorithm, or sha256 used in target_field/record mode --
                        # both new surfaces nothing used before this change) produces a full,
                        # unsalted hex digest: these exist specifically to reproduce StreamSets
                        # Field Hasher's own output (unsalted, untruncated) for joins against
                        # hashes StreamSets already wrote in production.
                        if _algorithm == "sha256" and _hash_mode == "in_place":
                            return hashlib.sha256((_salt + value).encode()).hexdigest()[:16]
                        if _algorithm == "sha256":
                            return hashlib.sha256(value.encode()).hexdigest()
                        if _algorithm == "md5":
                            return hashlib.md5(value.encode()).hexdigest()
                        if _algorithm == "sha1":
                            return hashlib.sha1(value.encode()).hexdigest()
                        if _algorithm == "sha512":
                            return hashlib.sha512(value.encode()).hexdigest()
                        if _algorithm == "murmur3_128":
                            try:
                                import mmh3
                            except ImportError as e:
                                raise ImportError(
                                    "algorithm='murmur3_128' requires the 'mmh3' package. "
                                    "Run: pip install mmh3"
                                ) from e
                            # mmh3_x64_128_digest returns the raw 16-byte MurmurHash3_x64_128
                            # digest directly (confirmed byte-identical to hash128()'s unsigned
                            # integer form) -- hex-encoded the same way this component already
                            # hex-encodes every other algorithm, and the same way StreamSets'
                            # own (MessageDigest-based) hash output is hex-encoded. StreamSets'
                            # current murmur3_128 support post-dates the last public StreamSets
                            # OSS snapshot (which only has MD5/SHA via java.security.MessageDigest
                            # -- confirmed by reading that source directly), so exact byte-for-byte
                            # parity with a live StreamSets instance is NOT independently verified
                            # here -- only mmh3's own correctness is (see tests: the empty-string,
                            # seed-0 case is provably all-zero bytes for MurmurHash3_x64_128,
                            # independent of this repo). Compare against a real StreamSets-produced
                            # hash before relying on this for a production join.
                            return mmh3.mmh3_x64_128_digest(value.encode(), 0).hex()
                        raise ValueError(f"unknown hash algorithm: {_algorithm!r}")

                    if encoding == "streamsets":
                        # Raw typed values reach streamsets_field_bytes -- do NOT .astype(str) first,
                        # that's exactly the bug this mode exists to avoid (see its docstring).
                        def _digest_streamsets(value, _field_type=field_type):
                            return _hash_bytes(streamsets_field_bytes(value, _field_type))

                    if hash_mode == "in_place":
                        if encoding == "streamsets":
                            df[col] = df[col].apply(_digest_streamsets)
                        else:
                            df[col] = df[col].astype(str).apply(_digest)
                    elif hash_mode == "target_field":
                        output_field = rule.get("output_field")
                        if not output_field:
                            raise ValueError("hash_mode='target_field' requires 'output_field'")
                        if encoding == "streamsets":
                            df[output_field] = df[col].apply(_digest_streamsets)
                        else:
                            df[output_field] = df[col].astype(str).apply(_digest)
                    elif hash_mode == "record":
                        output_field = rule.get("output_field")
                        if not output_field:
                            raise ValueError("hash_mode='record' requires 'output_field'")
                        record_columns = rule.get("columns") or [col]
                        missing_cols = [c for c in record_columns if c not in df.columns]
                        if missing_cols:
                            raise ValueError(f"hash_mode='record' columns not found: {missing_cols}")

                        # StreamSets' Hash Record mode appends the field separator after EVERY
                        # field's value, including the last one -- it is not a between-fields
                        # join. Field names are never included, only values (confirmed from
                        # StreamSets' own current documentation for the Field Hasher processor).
                        if encoding == "streamsets":
                            # Native-byte parity: each field's own typed bytes (streamsets_field_bytes),
                            # followed by the separator's UTF-8 bytes -- matching Guava's RecordFunnel,
                            # which writes sink.putString(separator, UTF-8) after every field.
                            sep_bytes = field_separator.encode("utf-8")

                            def _record_bytes(row, _cols=record_columns, _sep=sep_bytes, _field_type=field_type):
                                data = b""
                                for c in _cols:
                                    data += streamsets_field_bytes(row[c], _field_type) + _sep
                                return data

                            df[output_field] = df.apply(lambda row: _hash_bytes(_record_bytes(row)), axis=1)
                        else:
                            def _record_concat(row, _cols=record_columns, _sep=field_separator):
                                parts = []
                                for c in _cols:
                                    v = row[c]
                                    parts.append(("" if pd.isna(v) else str(v)) + _sep)
                                return "".join(parts)

                            df[output_field] = df.apply(lambda row: _digest(_record_concat(row)), axis=1)
                    else:
                        raise ValueError(f"unknown hash_mode: {hash_mode!r}")
                elif method == "partial":
                    # keep last N chars
                    keep = rule.get("keep_last", 4)
                    df[col] = df[col].astype(str).apply(lambda v: ("*" * max(0, len(v) - keep)) + v[-keep:] if len(v) > keep else "*" * len(v))
                elif method == "redact":
                    df[col] = self.redacted_value
                elif method == "substitute":
                    pattern = rule.get("pattern", r".")
                    replacement = rule.get("replacement", "*")
                    df[col] = df[col].astype(str).apply(lambda v: re.sub(pattern, replacement, v))
                elif method == "pseudonymize":
                    # consistent random-looking value per input — useful for analytics where uniqueness matters but identity doesn't
                    df[col] = df[col].astype(str).apply(lambda v: f"p_{hashlib.md5((salt + v).encode()).hexdigest()[:12]}")
                elif method == "mask":
                    mask_mode = rule.get("mask_mode")
                    if not mask_mode:
                        raise ValueError("method='mask' requires 'mask_mode'")

                    if mask_mode == "fixed_length":
                        # StreamSets' own FIXED_LENGTH_MASK constant -- always exactly this
                        # 10-char string, independent of input length or value.
                        df[col] = "xxxxxxxxxx"
                    elif mask_mode == "variable_length":
                        df[col] = df[col].astype(str).apply(lambda v: "x" * len(v))
                    elif mask_mode == "custom":
                        pattern = rule.get("mask")
                        if not pattern:
                            raise ValueError("mask_mode='custom' requires 'mask'")

                        def _custom_mask(value: str, _pattern=pattern) -> str:
                            # Ported directly from StreamSets' own FieldMaskProcessor.mask():
                            # '#' copies the next input char through; 'x' emits a literal mask
                            # char; any other pattern char is emitted literally, and ALSO
                            # consumes an input char only if the input already has that exact
                            # same character at the current position (so pre-formatted input
                            # isn't double-punctuated, but unformatted input gets the
                            # formatting inserted for free). Stops at the end of whichever of
                            # pattern/value runs out first -- matching the Java loop's
                            # `i < mask.length() && index < toMask.length()` bound exactly.
                            masked = []
                            index = 0
                            for ch in _pattern:
                                if index >= len(value):
                                    break
                                if ch == "#":
                                    masked.append(value[index])
                                    index += 1
                                elif ch == "x":
                                    masked.append("x")
                                    index += 1
                                else:
                                    masked.append(ch)
                                    if value[index] == ch:
                                        index += 1
                            return "".join(masked)

                        df[col] = df[col].astype(str).apply(_custom_mask)
                    elif mask_mode == "regex":
                        regex = rule.get("regex")
                        groups_to_show = rule.get("groups_to_show")
                        if not regex or not groups_to_show:
                            raise ValueError("mask_mode='regex' requires 'regex' and 'groups_to_show'")
                        compiled = re.compile(regex)

                        def _regex_mask(value: str, _compiled=compiled, _groups=groups_to_show) -> str:
                            # Ported from StreamSets' own FieldMaskProcessor.regExMask(): the
                            # WHOLE value must match (Java's Matcher.matches(), i.e. Python's
                            # re.fullmatch -- NOT re.search/re.match, which only anchor the
                            # start). A non-matching value is returned completely unmasked, not
                            # an error. On a match, every character becomes 'x' except the
                            # listed groups' original characters, placed back at their
                            # original positions in the full-length masked buffer.
                            m = _compiled.fullmatch(value)
                            if not m:
                                return value
                            result = list("x" * len(value))
                            for g in _groups:
                                start, end = m.start(g), m.end(g)
                                result[start:end] = list(m.group(g))
                            return "".join(result)

                        df[col] = df[col].astype(str).apply(_regex_mask)
                    else:
                        raise ValueError(f"unknown mask_mode: {mask_mode!r}")
                else:
                    raise ValueError(f"unknown masking method: {method}")
            context.add_output_metadata({
                "dagster/row_count": dg.MetadataValue.int(len(df)),
            })
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    sample = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                    context.add_output_metadata({"preview": dg.MetadataValue.md(sample.to_markdown(index=False))})
                except Exception:
                    pass
            return df

        return dg.Definitions(assets=[_asset])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
