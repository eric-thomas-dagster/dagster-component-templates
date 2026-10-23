"""LakeFS Asset Component.

A control-plane Dagster asset that runs git-like operations on a lakeFS
repository: `commit`, `merge`, or `create_branch`. Does NOT read or write
data — pair with `io_managers/lakefs_io_manager` for the data plane.
"""
from typing import Dict, List, Optional, Union

import dagster as dg
from pydantic import Field



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


class LakeFSAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a lakeFS control-plane operation as a Dagster asset.

    Three operation modes:

    - ``commit``         — commit staged changes on a branch with a message
                           and optional metadata. Use after data assets have
                           materialized via ``lakefs_io_manager`` to seal
                           those staged writes into a versioned snapshot.
    - ``merge``          — merge a source branch into a destination branch.
    - ``create_branch``  — create a new branch from a source ref.

    This asset performs lakeFS API calls only — it does not read or write
    data files. Declare upstream data assets via ``deps`` so the lakeFS
    operation runs after they've materialized; pair with the
    ``lakefs_io_manager`` IO manager component for the data plane.

    All lakeFS operations use the official `lakefs` Python SDK.
    """

    # --- lakeFS connection config ---------------------------------------------
    endpoint: str = Field(
        description="lakeFS endpoint URL, e.g. 'http://localhost:8000' or 'https://lakefs.my-company.com'."
    )
    access_key_env_var: str = Field(
        default="LAKEFS_ACCESS_KEY_ID",
        description="Environment variable holding the lakeFS access key ID.",
    )
    secret_key_env_var: str = Field(
        default="LAKEFS_SECRET_ACCESS_KEY",
        description="Environment variable holding the lakeFS secret access key.",
    )

    # --- Repository and operation config -------------------------------------
    repository: str = Field(description="lakeFS repository name.")
    mode: str = Field(
        default="commit",
        description=(
            "Operation: 'commit' (commit staged changes on a branch), "
            "'merge' (merge source branch into destination), "
            "'create_branch' (create a new branch from a source ref)."
        ),
    )
    branch: str = Field(
        default="main",
        description="Branch to commit to (commit mode), or the source branch (merge / create_branch mode).",
    )
    destination_branch: Optional[str] = Field(
        default=None,
        description="Target branch for merge mode.",
    )
    source_ref: Optional[str] = Field(
        default=None,
        description="Source ref (branch or commit SHA) for create_branch mode.",
    )
    commit_message: str = Field(
        default="Dagster materialization commit",
        description="Commit message used in commit mode.",
    )
    metadata: Optional[dict[str, str]] = Field(
        default=None,
        description="Key-value metadata attached to the commit (commit mode only).",
    )

    # --- Asset metadata -------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")
    group_name: Optional[str] = Field(
        default="data_versioning",
        description="Dagster asset group name.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on. Use to declare ordering — typically the data assets whose writes this commit/merge should seal.",
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



    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
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

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `branch`, `destination_branch`, `source_ref`, `commit_message` and `metadata` instead.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `branch`, `destination_branch`, `source_ref`, `commit_message` and `metadata` instead.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        _column_lineage_md = _column_lineage_metadata(self)
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            None,  # dynamic_partition_name: not a field of this component
            None,  # partition_dimensions: not a field of this component
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        endpoint = self.endpoint
        access_key = dg.EnvVar(self.access_key_env_var)
        secret_key = dg.EnvVar(self.secret_key_env_var)
        repository = self.repository
        mode = self.mode
        branch = self.branch
        destination_branch = self.destination_branch
        source_ref = self.source_ref
        commit_message = self.commit_message
        commit_metadata = self.metadata
        asset_name = self.asset_name
        group_name = self.group_name
        dep_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        if mode == "merge" and not destination_branch:
            raise ValueError("destination_branch must be set when mode is 'merge'.")
        if mode == "create_branch" and not source_ref:
            raise ValueError("source_ref must be set when mode is 'create_branch'.")
        if mode not in ("commit", "merge", "create_branch"):
            raise ValueError(f"Unknown mode '{mode}'. Expected: commit, merge, create_branch.")

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).

        _retry_policy = None

        if self.retry_policy_max_retries is not None:

            from dagster import Backoff, RetryPolicy

            _retry_policy = RetryPolicy(

                max_retries=self.retry_policy_max_retries,

                delay=self.retry_policy_delay_seconds or 1,

                backoff=Backoff[self.retry_policy_backoff.upper()],

            )


        @dg.asset(description=self.description, partitions_def=partitions_def, freshness_policy=_freshness_policy_obj, retry_policy=_retry_policy, 
            key=dg.AssetKey.from_user_string(asset_name),
            group_name=group_name,
            deps=dep_keys,
            kinds={"lakefs", "storage"},
        )
        def _lakefs_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            _tokens = _partition_vars(context)
            _branch = _substitute_partition(branch, _tokens)
            _destination_branch = _substitute_partition(destination_branch, _tokens)
            _source_ref = _substitute_partition(source_ref, _tokens)
            _commit_message = _substitute_partition(commit_message, _tokens)
            _commit_metadata = _substitute_partition(commit_metadata, _tokens)
            import lakefs
            from lakefs.client import Client

            client = Client(host=endpoint, username=access_key.get_value(), password=secret_key.get_value())
            repo = lakefs.Repository(repository, client=client)

            if mode == "commit":
                context.log.info(
                    f"lakeFS commit: repo={repository} branch={_branch} message='{_commit_message}'"
                )
                ref = repo.branch(_branch).commit(
                    message=_commit_message,
                    metadata=_commit_metadata or {},
                )
                commit_id = ref.get_commit().id
                context.log.info(f"Committed: {commit_id}")
                return dg.MaterializeResult(
                    metadata={
                        "mode": dg.MetadataValue.text(mode),
                        "repository": dg.MetadataValue.text(repository),
                        "branch": dg.MetadataValue.text(_branch),
                        "commit_id": dg.MetadataValue.text(commit_id),
                        "commit_message": dg.MetadataValue.text(_commit_message),
                    }
                )

            if mode == "merge":
                context.log.info(
                    f"lakeFS merge: {_branch} -> {_destination_branch} (repo={repository})"
                )
                merge_ref = repo.branch(_branch).merge_into(_destination_branch)
                ref_id = getattr(merge_ref, "id", str(merge_ref))
                context.log.info(f"Merge complete: {ref_id}")
                return dg.MaterializeResult(
                    metadata={
                        "mode": dg.MetadataValue.text(mode),
                        "repository": dg.MetadataValue.text(repository),
                        "source_branch": dg.MetadataValue.text(_branch),
                        "destination_branch": dg.MetadataValue.text(_destination_branch),
                        "reference": dg.MetadataValue.text(ref_id),
                    }
                )

            # mode == "create_branch"
            context.log.info(
                f"lakeFS create_branch: '{_branch}' from '{_source_ref}' (repo={repository})"
            )
            new_branch = repo.branch(_branch).create(source_reference=_source_ref)
            context.log.info(f"Branch created: {new_branch.id}")
            return dg.MaterializeResult(
                metadata={
                    "mode": dg.MetadataValue.text(mode),
                    "repository": dg.MetadataValue.text(repository),
                    "new_branch": dg.MetadataValue.text(_branch),
                    "source_ref": dg.MetadataValue.text(_source_ref),
                }
            )

        return dg.Definitions(assets=[_lakefs_asset])


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


# BEGIN partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port.py)
def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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
# END partitions-def builder


# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
