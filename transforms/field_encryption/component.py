"""FieldEncryptionComponent.

Field-level encrypt/decrypt using the real AWS Encryption SDK (envelope
encryption, not a toy cipher) -- raw-key or KMS-backed, type-preserving,
per-field encryption-context binding.

IMPORTANT -- read before pointing this at existing StreamSets ciphertext:
StreamSets' "Encrypt and Decrypt Fields" processor also uses the AWS
Encryption SDK, but this component's interop with ciphertext StreamSets
already wrote is UNVERIFIED. The AWS Encryption SDK's wire format is a
standard envelope (so the ciphertext ITSELF is readable by any correct
ESDK client), but what gets encrypted -- the plaintext payload layout this
component wraps around each value for type-preservation -- is this
component's own scheme, not a published StreamSets format. Round-tripping
THIS component's own ciphertext is fully verified (see tests/). Reading a
value StreamSets already encrypted requires knowing StreamSets' own
plaintext layout, which is not publicly documented. To verify real interop
you would need: (1) a real ciphertext sample StreamSets produced, and (2)
the exact "Encrypt and Decrypt Fields" stage config used to produce it
(master key mode -- KMS alias/ARN or a raw key, and any encryption context
set on the stage). Without both of those, do not assume this decrypts
real StreamSets production data.
"""

import base64
import json
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

import dagster as dg
import pandas as pd
from pydantic import Field, model_validator


def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine()
    (SQLAlchemy), .get_connection() (DBAPI), or .get_client() (vendor
    client -- dispatched by the client's own shape since "get_client" means
    something different per vendor: BigQuery's .query(sql).to_dataframe(),
    Redshift's .execute_query(sql, fetch_results=True, cursor_factory=
    RealDictCursor)), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no Dagster resource is registered. Same
    helper, same contract, as every other dual-ingestion component in this
    repo (e.g. data_masking, automl_asset, logistic_regression_model)."""
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
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


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


# Type-preservation wrapper around each plaintext value, so a decrypted value
# round-trips to its original Python type instead of coming back as a bare
# string. This is THIS component's own scheme (see module docstring) --
# not a StreamSets-compatible layout.
def _serialize_plaintext(value: Any) -> Optional[bytes]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        tag, v = "bool", value
    elif isinstance(value, int):
        tag, v = "int", value
    elif isinstance(value, float):
        tag, v = "float", value
    elif isinstance(value, (pd.Timestamp, datetime, date)):
        tag, v = "iso_datetime", pd.Timestamp(value).isoformat()
    else:
        tag, v = "str", str(value)
    return json.dumps({"t": tag, "v": v}).encode("utf-8")


def _deserialize_plaintext(payload: bytes) -> Any:
    obj = json.loads(payload.decode("utf-8"))
    tag, v = obj["t"], obj["v"]
    if tag == "bool":
        return bool(v)
    if tag == "int":
        return int(v)
    if tag == "float":
        return float(v)
    if tag == "iso_datetime":
        return pd.Timestamp(v)
    return v


# ---------------------------------------------------------------------------
# `compatibility: streamsets` -- matches StreamSets' "Encrypt and Decrypt
# Fields" processor's real AWS Encryption SDK plaintext/context layout,
# verified against a real Data Collector 6.4.1 and 3.18.1 (both directions)
# by the migrating-streamsets-to-dagster skill's own test suite
# (tests/test_sdc_crypto_parity.py) and reference implementation
# (streamsets_to_dagster.runtime.encrypt_value/decrypt_value). This is a
# DIFFERENT plaintext/context layout from this component's own `native`
# mode above -- native mode is NOT StreamSets-compatible, and this mode is
# NOT what native mode produces. Do not mix ciphertext between the two.
#
# Verified behavior:
#   - plaintext is the value's text in UTF-8 ("42", "2.5", "true"); a
#     byte-array field stays raw bytes, not UTF-8 text of anything;
#   - the type is NOT in the plaintext (no JSON wrapper) -- it's in the
#     encryption context under SDC_FIELD_TYPE (STRING/INTEGER/LONG/DOUBLE/
#     BOOLEAN/DECIMAL/BYTE_ARRAY), alongside the user's own context entries
#     -- and ONLY those; this component's native-mode habit of adding a
#     `field: <column>` context entry is NOT done here, because StreamSets
#     never adds that entry either, and adding it would make ciphertext
#     this component writes undecryptable by a real engine (and vice versa);
#   - Data Collector uses the AWS Encryption SDK's older, non-committing
#     algorithm suites, so CommitmentPolicy.FORBID_ENCRYPT_ALLOW_DECRYPT is
#     required (the SDK's default policy refuses to decrypt them);
#   - output is raw bytes, not base64 text, matching the real stage;
#   - the raw-key provider id is "JceMasterKeyProvider" (StreamSets' "User
#     Supplied Key" option), with wrapping algorithm selected by actual key
#     length (16/24/32 bytes), not hardcoded to AES-256 the way native
#     mode's raw-key provider is;
#   - AWS KMS uses StrictAwsKmsMasterKeyProvider with the same plaintext/
#     context shape -- NOT independently exercised against a real AWS
#     account by the skill either; treat it as corroborated, not verified.
def _sdc_serialize_plaintext(value: Any):
    """Returns (kind, plaintext_bytes) per StreamSets' real type->bytes mapping, or
    (None, None) for a null value (the caller skips encryption for nulls)."""
    import decimal

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None, None
    if isinstance(value, bool):
        return "BOOLEAN", (b"true" if value else b"false")
    if isinstance(value, int):
        kind = "INTEGER" if -(2**31) <= value < 2**31 else "LONG"
        return kind, str(value).encode()
    if isinstance(value, float):
        return "DOUBLE", repr(value).encode()
    if isinstance(value, decimal.Decimal):
        return "DECIMAL", str(value).encode()
    if isinstance(value, (bytes, bytearray)):
        return "BYTE_ARRAY", bytes(value)
    if isinstance(value, str):
        return "STRING", value.encode()
    raise TypeError(
        f"cannot encrypt a value of type {type(value).__name__} in compatibility='streamsets' "
        "mode (dates and times are not verified against a real engine yet)"
    )


def _sdc_deserialize_plaintext(kind: str, plain: bytes) -> Any:
    import decimal

    text = plain.decode() if kind != "BYTE_ARRAY" else ""
    conv = {
        "STRING": lambda: text,
        "INTEGER": lambda: int(text),
        "LONG": lambda: int(text),
        "SHORT": lambda: int(text),
        "BYTE": lambda: int(text),
        "DOUBLE": lambda: float(text),
        "FLOAT": lambda: float(text),
        "BOOLEAN": lambda: text == "true",
        "DECIMAL": lambda: decimal.Decimal(text),
        "BYTE_ARRAY": lambda: plain,
    }
    if kind not in conv:
        raise ValueError(f"field type {kind!r} is not supported yet")
    return conv[kind]()


class FieldEncryptionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Field-level encrypt/decrypt using the real AWS Encryption SDK — raw-key
    or KMS-backed, type-preserving, per-field encryption-context binding."""

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

    mode: str = Field(description="'encrypt' or 'decrypt'.")
    fields: Union[str, List[str]] = Field(
        description="DataFrame column(s) to encrypt (mode='encrypt') or decrypt (mode='decrypt'). "
                     "Comma-separated string or a YAML list."
    )

    compatibility: str = Field(
        default="native",
        description=(
            "'native' (default) -- this component's own type-preserving scheme: plaintext is a "
            "JSON {\"t\": type, \"v\": value} wrapper, ciphertext is base64-encoded text, and an "
            "auto-added `field: <column>` encryption-context entry stops ciphertext being silently "
            "swapped between columns. NOT StreamSets-compatible. "
            "'streamsets' -- matches StreamSets' \"Encrypt and Decrypt Fields\" processor's real AWS "
            "Encryption SDK layout byte-for-byte (verified against a real Data Collector 6.4.1 and "
            "3.18.1, both directions, by the migrating-streamsets-to-dagster skill): plaintext is the "
            "value's UTF-8 text (byte-array fields stay raw bytes), the type goes in the encryption "
            "context as SDC_FIELD_TYPE instead of a plaintext wrapper, output is raw bytes not base64, "
            "and no `field` context entry is auto-added (StreamSets doesn't add one either). Use this "
            "mode to read ciphertext StreamSets wrote, or write ciphertext StreamSets can read."
        ),
    )

    key_source: str = Field(
        default="raw_key",
        description=(
            "'raw_key' (a user-supplied symmetric key via AWS ESDK's RawMasterKeyProvider) "
            "or 'kms' (an AWS KMS CMK via AWS ESDK's StrictAwsKmsMasterKeyProvider)."
        ),
    )
    raw_key_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding a base64-encoded 256-bit (32-byte) symmetric key. "
            "Required when key_source='raw_key'. Generate one with: "
            "python3 -c \"import os,base64; print(base64.b64encode(os.urandom(32)).decode())\""
        ),
    )
    kms_key_ids: Optional[Union[str, List[str]]] = Field(
        default=None,
        description=(
            "KMS key ARN(s) for StrictAwsKmsMasterKeyProvider. Required when key_source='kms'. "
            "For mode='encrypt' any valid KMS key identifier works (ARN, key id, alias name/ARN); "
            "for mode='decrypt' AWS requires the full key ARN -- key ids and alias "
            "names/ARNs are not accepted for decryption."
        ),
    )
    key_id: str = Field(
        default="field-encryption-key",
        description=(
            "Logical wrapping-key identifier passed to the master key provider. Must match "
            "between the encrypt run and the decrypt run for raw_key mode (it's how the "
            "RawMasterKeyProvider looks up which key material to use)."
        ),
    )
    encryption_context: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Additional authenticated data (AAD) passed to the AWS Encryption SDK. Not secret, "
            "but must match exactly between encrypt and decrypt of the same value or decryption "
            "fails. This component automatically adds a `field: <column name>` entry per column "
            "on top of whatever you set here, so ciphertext can't silently be swapped between "
            "columns."
        ),
    )
    on_decrypt_error: str = Field(
        default="raise",
        description=(
            "'raise' (default -- stop the asset on the first value that fails to decrypt: wrong "
            "key, corrupted ciphertext, or mismatched encryption context) or 'null' (replace that "
            "value with null and keep going; count of failures is reported in output metadata)."
        ),
    )

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
        return coerce_field_shapes(
            data,
            lists=('deps', 'owners', 'kinds', 'fields', 'kms_key_ids'),
            csv=('partition_values',),
            tags=('asset_tags', 'encryption_context'),
        )

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Field Encryption', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("FieldEncryptionComponent: set exactly one of `upstream_asset_key` or `source`.")
        if self.mode not in ("encrypt", "decrypt"):
            raise ValueError(f"mode must be 'encrypt' or 'decrypt', got {self.mode!r}")
        if self.key_source not in ("raw_key", "kms"):
            raise ValueError(f"key_source must be 'raw_key' or 'kms', got {self.key_source!r}")
        if self.key_source == "raw_key" and not self.raw_key_env_var:
            raise ValueError("key_source='raw_key' requires raw_key_env_var.")
        if self.key_source == "kms" and not self.kms_key_ids:
            raise ValueError("key_source='kms' requires kms_key_ids.")
        if self.compatibility not in ("native", "streamsets"):
            raise ValueError(f"compatibility must be 'native' or 'streamsets', got {self.compatibility!r}")

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
            description=self.description or "Field-level encrypt/decrypt using the real AWS Encryption SDK.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['encryption', 'security', 'pii']),
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
            try:
                import aws_encryption_sdk
                from aws_encryption_sdk.identifiers import WrappingAlgorithm, EncryptionKeyType
                from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey
                from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
            except ImportError as e:
                raise ImportError(
                    "aws-encryption-sdk is not installed. Run: pip install aws-encryption-sdk"
                ) from e

            self = _self
            df = kwargs.get("df")
            if df is None:
                df = _ingest_warehouse_query(self.source, context)
            # partition bridge dict-concat: when an unpartitioned asset
            # consumes a partitioned upstream, Dagster's IO manager loads
            # ALL partitions as a dict; concat to a single DataFrame before
            # any DataFrame ops.
            if isinstance(df, dict):
                _frames = [v for v in df.values() if isinstance(v, pd.DataFrame)]
                df = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            df = df.copy()

            field_list = self.fields if isinstance(self.fields, list) else [self.fields]
            base_encryption_context = dict(self.encryption_context or {})

            sdc_mode = self.compatibility == "streamsets"
            esdk_client = aws_encryption_sdk.EncryptionSDKClient(
                commitment_policy=(
                    aws_encryption_sdk.CommitmentPolicy.FORBID_ENCRYPT_ALLOW_DECRYPT
                    if sdc_mode
                    # Data Collector writes (and expects) the AWS Encryption SDK's older,
                    # non-committing algorithm suites -- the SDK's default policy refuses
                    # to decrypt them, so compatibility='streamsets' needs the weaker
                    # policy. Verified against a real Data Collector 6.4.1 and 3.18.1.
                    else aws_encryption_sdk.CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
                )
            )

            if self.key_source == "raw_key":
                raw_key_b64 = os.environ.get(self.raw_key_env_var or "", "")
                if not raw_key_b64:
                    raise ValueError(f"raw_key_env_var {self.raw_key_env_var!r} is unset or empty.")
                raw_key_bytes = base64.b64decode(raw_key_b64)

                if sdc_mode:
                    _sdc_wrap_alg = {
                        16: WrappingAlgorithm.AES_128_GCM_IV12_TAG16_NO_PADDING,
                        24: WrappingAlgorithm.AES_192_GCM_IV12_TAG16_NO_PADDING,
                        32: WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
                    }.get(len(raw_key_bytes))
                    if _sdc_wrap_alg is None:
                        raise ValueError(
                            f"compatibility='streamsets' requires a 16, 24, or 32-byte raw key, "
                            f"got {len(raw_key_bytes)} bytes."
                        )

                    class _JceMasterKeyProvider(RawMasterKeyProvider):
                        # Provider id StreamSets' "User Supplied Key" option uses -- must
                        # match exactly for cross-engine ciphertext interop.
                        provider_id = "JceMasterKeyProvider"

                        def __new__(cls, *args, **kwargs):
                            return super(RawMasterKeyProvider, cls).__new__(cls)

                        def __init__(self_mk, **kwargs):
                            RawMasterKeyProvider.__init__(self_mk, **kwargs)

                        def _get_raw_key(self_mk, key_id):
                            return WrappingKey(
                                wrapping_algorithm=_sdc_wrap_alg,
                                wrapping_key=raw_key_bytes,
                                wrapping_key_type=EncryptionKeyType.SYMMETRIC,
                            )

                    key_provider = _JceMasterKeyProvider()
                    key_provider.add_master_key(self.key_id)
                else:
                    class _StaticRawMasterKeyProvider(RawMasterKeyProvider):
                        provider_id = "field-encryption-raw"

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

                    key_provider = _StaticRawMasterKeyProvider()
                    key_provider.add_master_key(self.key_id)
            else:
                kms_ids = self.kms_key_ids if isinstance(self.kms_key_ids, list) else [self.kms_key_ids]
                key_provider = aws_encryption_sdk.StrictAwsKmsMasterKeyProvider(key_ids=kms_ids)

            rows_processed = 0
            rows_decrypt_failed = 0

            def _encrypt_cell(value, column_name):
                nonlocal rows_processed
                if sdc_mode:
                    kind, plaintext = _sdc_serialize_plaintext(value)
                    if plaintext is None:
                        return None
                    # StreamSets never adds a `field` context entry -- adding one here
                    # would make this component's own streamsets-mode ciphertext
                    # undecryptable by a real engine, defeating the point of this mode.
                    ctx = {"SDC_FIELD_TYPE": kind, **base_encryption_context}
                else:
                    plaintext = _serialize_plaintext(value)
                    if plaintext is None:
                        return None
                    ctx = {**base_encryption_context, "field": column_name}
                ciphertext, _header = esdk_client.encrypt(
                    source=plaintext, key_provider=key_provider, encryption_context=ctx,
                )
                rows_processed += 1
                return ciphertext if sdc_mode else base64.b64encode(ciphertext).decode("ascii")

            def _decrypt_cell(value, column_name):
                nonlocal rows_processed, rows_decrypt_failed
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return None
                try:
                    ciphertext = value if sdc_mode else base64.b64decode(value)
                    plaintext, header = esdk_client.decrypt(source=ciphertext, key_provider=key_provider)
                    expected = (
                        dict(base_encryption_context)
                        if sdc_mode
                        else {**base_encryption_context, "field": column_name}
                    )
                    for k, v in expected.items():
                        if header.encryption_context.get(k) != v:
                            raise ValueError(
                                f"encryption_context mismatch for column {column_name!r}: "
                                f"expected {k}={v!r}, got {header.encryption_context.get(k)!r}"
                            )
                    rows_processed += 1
                    if sdc_mode:
                        kind = header.encryption_context.get("SDC_FIELD_TYPE", "STRING")
                        return _sdc_deserialize_plaintext(kind, plaintext)
                    return _deserialize_plaintext(plaintext)
                except Exception as e:  # noqa: BLE001 — real crypto/format failures, not a bug in this component
                    rows_decrypt_failed += 1
                    if self.on_decrypt_error == "raise":
                        raise ValueError(
                            f"failed to decrypt column {column_name!r}: {e}"
                        ) from e
                    return None

            for col in field_list:
                if col not in df.columns:
                    continue
                if self.mode == "encrypt":
                    df[col] = df[col].apply(lambda v, c=col: _encrypt_cell(v, c))
                else:
                    df[col] = df[col].apply(lambda v, c=col: _decrypt_cell(v, c))

            context.add_output_metadata({
                "dagster/row_count": dg.MetadataValue.int(len(df)),
                "mode": dg.MetadataValue.text(self.mode),
                "fields": dg.MetadataValue.text(",".join(field_list)),
                "rows_processed": dg.MetadataValue.int(rows_processed),
                "rows_decrypt_failed": dg.MetadataValue.int(rows_decrypt_failed),
            })
            if self.include_preview_metadata and len(df) > 0:
                try:
                    sample = df.sample(min(self.preview_rows, len(df))) if len(df) > self.preview_rows * 10 else df.head(self.preview_rows)
                    context.add_output_metadata({"preview": dg.MetadataValue.md(sample.to_markdown(index=False))})
                except Exception:
                    pass
            return df

        return dg.Definitions(assets=[_asset])
