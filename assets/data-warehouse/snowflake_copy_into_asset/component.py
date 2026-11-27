"""Snowflake COPY INTO asset.

Bulk-loads files from a stage into a table via `COPY INTO <table> FROM <stage> ...` -- mirrors
Airflow's `CopyFromExternalStageToSnowflakeOperator` (and the broader stage-to-table load pattern
behind `S3ToSnowflakeOperator`/`SnowflakeToSnowflakeOperator`). Sibling to `snowflake_sql_asset`
(arbitrary SQL) and `snowflake_stored_procedure_call_asset` (named proc call): this one is for
the specific, very common "load staged files into a table" case, with the actual COPY INTO
grammar (FILES/PATTERN/FILE_FORMAT/copy options) as typed fields instead of a hand-written SQL
string.

Connection fields/dispatch are the same shape as snowflake_sql_asset and
snowflake_stored_procedure_call_asset (duplicated, not imported -- components in this repo are
self-contained).
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


# BEGIN generic timeout wrapper (inlined per component; keep copies identical — see tests/test_generic_timeout_drift.py)
def _run_with_timeout(fn, timeout_seconds, *args, **kwargs):
    """Bounds how long this step waits for `fn`, raising dg.Failure past timeout_seconds.
    Python threads can't be forcibly killed -- fn keeps running in the background past the
    deadline (same documented trade-off as this repo's own @timeout decorator); this bounds
    the STEP's wait, not the underlying call itself. Deliberately NOT `with ThreadPoolExecutor()`
    -- that form's __exit__ calls shutdown(wait=True), which blocks until the background call
    finishes anyway, silently defeating the timeout for a call that hangs indefinitely."""
    if not timeout_seconds:
        return fn(*args, **kwargs)
    import concurrent.futures
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn, *args, **kwargs)
    try:
        result = future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError:
        executor.shutdown(wait=False)
        raise dg.Failure(description=f"Exceeded timeout_seconds={timeout_seconds}.") from None
    executor.shutdown(wait=False)
    return result
# END generic timeout wrapper


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset. Same
    helper as snowflake_sql_asset / snowflake_stored_procedure_call_asset (duplicated, not
    imported -- components are self-contained)."""
    value = dg.EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


def _format_option_value(v: Any) -> str:
    """Render a FILE_FORMAT option (or a non-enum COPY option like PURGE/FORCE/SIZE_LIMIT)
    value using Snowflake's own grammar: bare TRUE/FALSE for booleans, bare numbers,
    single-quoted (escaped) strings for everything else -- Snowflake's CREATE FILE FORMAT docs
    document FILE_FORMAT's own TYPE as accepting a quoted string (TYPE = 'CSV'). NOT used for
    ON_ERROR/MATCH_BY_COLUMN_NAME/VALIDATION_MODE, whose keyword-enum values COPY INTO's own
    docs show unquoted -- those are rendered directly in _build_copy_sql instead."""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("'", "''")
    return f"'{s}'"


def _render_options(options: Dict[str, Any]) -> str:
    return " ".join(f"{k.upper()} = {_format_option_value(v)}" for k, v in options.items())


# ───────────────────────────────────────────────────────────────────────────
# Partition helper (canonical shape, copied from external_*_asset / sql_transform
# -- self-contained per the registry's distribution model)
# ───────────────────────────────────────────────────────────────────────────

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
            f"partition_type={partition_type!r} requires partition_start (ISO date)."
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
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class SnowflakeCopyIntoAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Bulk-load files from a Snowflake stage into a table via COPY INTO -- mirrors Airflow's
    `CopyFromExternalStageToSnowflakeOperator`.

    One component instance, N COPY INTO assets: set `loads:` (a list of per-table overrides)
    instead of the single-instance fields (asset_key/database/schema/table/stage) -- mutually
    exclusive, same convention as `sql_transform`'s `transforms:` / `external_sftp_path`'s `paths:`.
    """

    asset_key: Optional[str] = Field(default=None, description="Dagster asset key (e.g. 'snowflake/loads/orders'). Required unless `loads:` is set (mutually exclusive with it).")
    database: Optional[str] = Field(default=None, description="Target Snowflake database. Required unless `loads:` is set.")
    schema_name: Optional[str] = Field(default=None, description="Target Snowflake schema.", alias="schema")
    table: Optional[str] = Field(default=None, description="Target table name. Required unless `loads:` is set.")

    loads: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "One component instance, N COPY INTO assets: a list of per-table overrides, each "
            "applied on top of this instance's own field values (connection fields, group_name, "
            "kinds, etc. are shared defaults unless an entry overrides them). Each entry requires "
            "at least `asset_key`, `table`, and `stage_path`; any other field on this component "
            "(database, schema, file_format_options, partition_type, replace_partition_column, "
            "...) can be set per-entry too. Mutually exclusive with the top-level "
            "asset_key/database/schema/table/stage (single-instance mode)."
        ),
    )

    stage: Optional[str] = Field(default=None, description="Stage to copy from, e.g. 'my_stage', '@my_stage', or '@~' for the user stage. The leading '@' is added automatically if omitted. Required unless `loads:` is set.")
    stage_path: Optional[str] = Field(default=None, description="Subpath/prefix within the stage to copy from, e.g. '2026/01/'. Supports {partition_key} substitution when a partitions_def is configured.")
    replace_partition_column: Optional[str] = Field(
        default=None,
        description=(
            "When set (and a partitions_def is configured), runs "
            "`DELETE FROM <table> WHERE <replace_partition_column> = '<partition_key>'` before "
            "the COPY INTO, so re-running a partition is idempotent instead of appending "
            "duplicate rows on top of a prior load."
        ),
    )

    # Partitions — canonical shape used across the registry.
    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'cron' | 'static' | 'dynamic'.")
    partition_start: Optional[str] = Field(default=None, description="Required for daily/weekly/monthly/hourly/cron -- ISO start date.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for partition_type='static'.")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required for partition_type='cron', e.g. '0 0 * * 1-5'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition_type='cron' keys.")
    partition_end_offset: int = Field(default=0, description="Passed through to the underlying time-window partitions definition.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the partitions definition (defaults to UTC).")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Required for partition_type='dynamic'.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partitions -- set instead of partition_type.")
    files: Optional[List[str]] = Field(default=None, description="Explicit list of filenames (relative to the stage) to load, rendered as FILES = ('a.csv', 'b.csv'). Leave unset to load every file matching `pattern` (or every file in the stage/path if `pattern` is also unset).")
    pattern: Optional[str] = Field(default=None, description="Regex pattern (Snowflake's PATTERN clause) to filter which staged files are loaded.")

    file_format_name: Optional[str] = Field(default=None, description="Name of an existing named file format object, rendered as FILE_FORMAT = (FORMAT_NAME = '...'). Set this or file_format_options, not both.")
    file_format_options: Optional[Dict[str, Any]] = Field(default=None, description="Inline file format options, e.g. {TYPE: CSV, FIELD_DELIMITER: '|', SKIP_HEADER: 1}, rendered as FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1). Set this or file_format_name, not both.")

    on_error: Optional[str] = Field(default=None, description="COPY INTO's ON_ERROR option, e.g. 'CONTINUE', 'SKIP_FILE', 'ABORT_STATEMENT' (the default when unset).")
    purge: Optional[bool] = Field(default=None, description="If true, staged files are deleted after a successful load (COPY INTO's PURGE option).")
    force: Optional[bool] = Field(default=None, description="If true, files are reloaded even if already loaded previously (COPY INTO's FORCE option; by default Snowflake skips files it has already loaded).")
    match_by_column_name: Optional[str] = Field(default=None, description="For semi-structured formats: 'CASE_SENSITIVE', 'CASE_INSENSITIVE', or 'NONE' (the default when unset).")
    size_limit: Optional[int] = Field(default=None, description="Maximum bytes loaded per COPY statement (COPY INTO's SIZE_LIMIT option).")
    truncatecolumns: Optional[bool] = Field(default=None, description="If true, string values exceeding a column's length are truncated rather than erroring.")
    validation_mode: Optional[str] = Field(default=None, description="If set (e.g. 'RETURN_ERRORS', 'RETURN_10_ROWS'), validates the load without inserting data and returns the validation result instead.")

    account_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake account. Required unless resource_key is set.")
    user_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake username. Required unless resource_key is set.")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake password.")
    pat_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake PAT.")
    private_key_path_env_var: Optional[str] = Field(default=None, description="Env var with RSA private key path.")
    private_key_passphrase_env_var: Optional[str] = Field(default=None, description="Env var with private-key passphrase.")
    warehouse_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake warehouse.")
    role_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake role.")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional key of a registered SnowflakeResourceComponent (resources/snowflake_resource). When set, connection "
            "credentials are read from that resource at run time instead of the inline "
            "account_env_var/user_env_var/password_env_var/pat_env_var/private_key_path_env_var/warehouse_env_var/role_env_var fields."
        ),
    )

    group_name: str = Field(default="snowflake", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'snowflake').")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient connection failures, warehouse resume delays, etc.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    pool: Optional[str] = Field(
        default=None,
        description=(
            "Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed "
            "number of slots shared across every asset/op using the same pool name, e.g. "
            "warehouse concurrency limits). The pool's own slot count is configured at the "
            "deployment level (dagster.yaml's concurrency: pools:, or the Dagster+ UI), not here."
        ),
    )
    timeout_seconds: Optional[float] = Field(
        default=None,
        description="Kill the wait and fail the run if the COPY INTO hasn't finished after this many seconds. Enforced via a background-thread wrapper -- the underlying Snowflake call may continue running past the deadline (Python threads can't be forcibly killed, and this does not cancel the query server-side); this bounds the step's wait, not the call itself.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds', 'files'), csv=(), tags=('asset_tags',))

    @model_validator(mode="after")
    def _validate_file_format_and_connection(self):
        if self.file_format_name and self.file_format_options:
            raise ValueError("SnowflakeCopyIntoAssetComponent: set at most one of `file_format_name`, `file_format_options`.")
        if not self.resource_key and not (self.account_env_var and self.user_env_var):
            raise ValueError("SnowflakeCopyIntoAssetComponent: set `resource_key`, or both `account_env_var` and `user_env_var`.")
        return self

    def _connection_kwargs(self) -> Dict[str, Any]:
        import os
        kwargs: Dict[str, Any] = {
            "account": _require_env(self.account_env_var),
            "user": _require_env(self.user_env_var),
        }
        if self.warehouse_env_var:
            kwargs["warehouse"] = os.environ.get(self.warehouse_env_var, "")
        if self.role_env_var:
            kwargs["role"] = os.environ.get(self.role_env_var, "")
        if self.pat_env_var and os.environ.get(self.pat_env_var):
            kwargs["password"] = _require_env(self.pat_env_var)
        elif self.private_key_path_env_var and os.environ.get(self.private_key_path_env_var):
            kwargs["private_key_file"] = _require_env(self.private_key_path_env_var)
            if self.private_key_passphrase_env_var:
                kwargs["private_key_file_pwd"] = os.environ.get(self.private_key_passphrase_env_var, "")
            kwargs["authenticator"] = "snowflake_jwt"
        elif self.password_env_var:
            kwargs["password"] = os.environ.get(self.password_env_var, "")
        return kwargs

    def _build_copy_sql(self, stage_path_override: Optional[str] = None) -> str:
        fqn = f"{self.database}.{self.schema_name}.{self.table}"
        stage_ref = self.stage if self.stage.startswith("@") else f"@{self.stage}"
        stage_path = self.stage_path if stage_path_override is None else stage_path_override
        if stage_path:
            stage_ref = f"{stage_ref.rstrip('/')}/{stage_path.lstrip('/')}"

        parts = [f"COPY INTO {fqn}", f"FROM {stage_ref}"]
        if self.files:
            parts.append("FILES = (" + ", ".join(f"'{f}'" for f in self.files) + ")")
        if self.pattern:
            escaped_pattern = self.pattern.replace("'", "''")
            parts.append(f"PATTERN = '{escaped_pattern}'")
        if self.file_format_name:
            parts.append(f"FILE_FORMAT = (FORMAT_NAME = '{self.file_format_name}')")
        elif self.file_format_options:
            parts.append(f"FILE_FORMAT = ({_render_options(self.file_format_options)})")

        # ON_ERROR and MATCH_BY_COLUMN_NAME (like VALIDATION_MODE below) are rendered UNQUOTED,
        # not through _render_options -- Snowflake's own reference docs show these keyword-enum
        # options' values bare (ON_ERROR = CONTINUE, not ON_ERROR = 'CONTINUE'; the one quoted
        # ON_ERROR form, 'SKIP_FILE_<num>%', is a string literal because of the trailing '%', not
        # evidence that quoting is a general alternative syntax here). Quoting them anyway isn't
        # a hard SQL error, but it's not the documented form either, so this avoids depending on
        # Snowflake's parser being lenient about it.
        if self.on_error is not None:
            parts.append(f"ON_ERROR = {self.on_error}")
        copy_opts: Dict[str, Any] = {}
        if self.purge is not None:
            copy_opts["PURGE"] = self.purge
        if self.force is not None:
            copy_opts["FORCE"] = self.force
        if self.size_limit is not None:
            copy_opts["SIZE_LIMIT"] = self.size_limit
        if self.truncatecolumns is not None:
            copy_opts["TRUNCATECOLUMNS"] = self.truncatecolumns
        if copy_opts:
            parts.append(_render_options(copy_opts))
        if self.match_by_column_name is not None:
            parts.append(f"MATCH_BY_COLUMN_NAME = {self.match_by_column_name}")
        if self.validation_mode:
            parts.append(f"VALIDATION_MODE = {self.validation_mode}")

        return "\n".join(parts)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Snowflake Copy Into Asset", editable=True)

    def _missing_single_fields(self) -> List[str]:
        return [f for f in ("asset_key", "database", "schema_name", "table", "stage") if not getattr(self, f)]

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Dispatch to single-load or collection mode -- mutually exclusive, same convention as
        sql_transform's asset_name/sql vs transforms: / external_sftp_path's asset_key/remote_path
        vs paths:."""
        has_single = bool(self.asset_key or self.table)
        has_collection = bool(self.loads)
        if has_single and has_collection:
            raise ValueError(
                "SnowflakeCopyIntoAssetComponent: set either the single-instance fields "
                "(asset_key/database/schema/table/stage) or loads: (several COPY INTO assets), not both."
            )
        if not has_single and not has_collection:
            raise ValueError(
                "SnowflakeCopyIntoAssetComponent: set either the single-instance fields "
                "(asset_key/database/schema/table/stage) or loads:."
            )

        if not has_collection:
            missing = self._missing_single_fields()
            if missing:
                raise ValueError(f"SnowflakeCopyIntoAssetComponent: missing required field(s) {missing} for a single COPY INTO asset.")
            return self._build_single_defs(context)

        defs_list = []
        for i, entry in enumerate(self.loads):
            if not isinstance(entry, dict):
                raise ValueError(f"loads[{i}] must be a mapping, got {type(entry).__name__}")
            merged = {**self.model_dump(by_alias=True), **entry, "loads": None}
            try:
                instance = type(self)(**merged)
            except Exception as e:
                raise ValueError(f"loads[{i}] ({entry.get('asset_key', '<no asset_key>')!r}): {e}") from e
            missing = instance._missing_single_fields()
            if missing:
                raise ValueError(f"loads[{i}]: missing required field(s) {missing}.")
            defs_list.append(instance._build_single_defs(context))
        return dg.Definitions.merge(*defs_list)

    def _build_single_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else set()
        kinds.add("snowflake")
        resource_key = component.resource_key
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        partitions_def = _build_partitions_def(
            component.partition_type, component.partition_start, component.partition_values,
            component.dynamic_partition_name, component.partition_dimensions,
            cron_schedule=component.partition_cron_schedule,
            cron_fmt=component.partition_cron_fmt or "%Y-%m-%d",
            end_offset=component.partition_end_offset,
            timezone=component.partition_timezone,
        )

        @dg.asset(
            key=dg.AssetKey(component.asset_key.split("/")),
            description=component.description or f"Snowflake COPY INTO: {component.database}.{component.schema_name}.{component.table}",
            group_name=component.group_name,
            kinds=kinds,
            owners=component.owners,
            tags=component.asset_tags,
            deps=deps_keys,
            required_resource_keys=({resource_key} if resource_key else None),
            retry_policy=_retry_policy,
            pool=component.pool,
            partitions_def=partitions_def,
        )
        def _snowflake_copy_into(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            partition_key = context.partition_key if context.has_partition_key else None
            stage_path_rendered = component.stage_path
            if partition_key is not None and stage_path_rendered:
                rendered = stage_path_rendered.replace("{partition_key}", partition_key).replace("{date}", partition_key)
                keys_by_dimension = getattr(partition_key, "keys_by_dimension", None)
                if keys_by_dimension:
                    for dim_name, dim_val in keys_by_dimension.items():
                        rendered = rendered.replace("{partition." + dim_name + "}", dim_val)
                stage_path_rendered = rendered

            def _do_copy_into():
                import contextlib
                import snowflake.connector

                copy_sql = component._build_copy_sql(stage_path_override=stage_path_rendered)
                delete_sql = None
                if component.replace_partition_column and partition_key is not None:
                    fqn = f"{component.database}.{component.schema_name}.{component.table}"
                    escaped_pk = str(partition_key).replace("'", "''")
                    delete_sql = f"DELETE FROM {fqn} WHERE {component.replace_partition_column} = '{escaped_pk}'"

                stack = contextlib.ExitStack()
                if resource_key:
                    conn = stack.enter_context(getattr(context.resources, resource_key).get_connection())
                else:
                    conn = snowflake.connector.connect(**component._connection_kwargs())
                cursor = conn.cursor()
                try:
                    deleted_rows = None
                    if delete_sql:
                        context.log.info(delete_sql)
                        cursor.execute(delete_sql)
                        deleted_rows = cursor.rowcount
                    context.log.info(copy_sql)
                    cursor.execute(copy_sql)
                    columns = [c[0].lower() for c in (cursor.description or [])]
                    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    conn.commit()
                    return copy_sql, rows, delete_sql, deleted_rows
                finally:
                    cursor.close()
                    if not resource_key:
                        conn.close()
                    stack.close()

            copy_sql, rows, delete_sql, deleted_rows = _run_with_timeout(_do_copy_into, component.timeout_seconds)

            rows_loaded = sum(int(r.get("rows_loaded") or 0) for r in rows)
            rows_parsed = sum(int(r.get("rows_parsed") or 0) for r in rows)
            errors_seen = sum(int(r.get("errors_seen") or 0) for r in rows)

            metadata: Dict[str, Any] = {
                "copy_sql": dg.MetadataValue.text(copy_sql),
                "file_count": dg.MetadataValue.int(len(rows)),
                "rows_loaded": dg.MetadataValue.int(rows_loaded),
                "rows_parsed": dg.MetadataValue.int(rows_parsed),
                "errors_seen": dg.MetadataValue.int(errors_seen),
                "files": dg.MetadataValue.json(rows),
            }
            if delete_sql:
                metadata["replace_partition_delete_sql"] = dg.MetadataValue.text(delete_sql)
                metadata["replace_partition_rows_deleted"] = dg.MetadataValue.int(deleted_rows if deleted_rows is not None else -1)

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_snowflake_copy_into])
