import os
import subprocess
import tempfile
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


class AnsibleAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run an Ansible playbook as a Dagster asset.

    Executes ``ansible-playbook`` in a subprocess, streams stdout line-by-line
    to the Dagster event log, parses the PLAY RECAP to surface per-host
    ok/changed/unreachable/failed counts as asset metadata, and raises on
    non-zero exit codes so Dagster marks the run as failed.
    """

    # --- Core playbook config -------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")
    playbook: str = Field(description="Path to the Ansible playbook YAML file.")
    inventory: Optional[str] = Field(
        default=None,
        description=(
            "Inventory file path, directory, or comma-separated host list "
            "(e.g. 'host1,host2,'). When None, ansible-playbook falls back to "
            "the inventory configured in ansible.cfg."
        ),
    )

    # --- Variable injection ---------------------------------------------------
    extra_vars: Optional[dict[str, str]] = Field(
        default=None,
        description="Key/value pairs passed as --extra-vars 'k=v k2=v2 ...'.",
    )
    extra_vars_file: Optional[str] = Field(
        default=None,
        description="Path to a vars file passed as --extra-vars @<file>.",
    )

    # --- Task filtering -------------------------------------------------------
    tags: Optional[list[str]] = Field(
        default=None,
        description="Limit execution to tasks with these Ansible tags (--tags).",
    )
    skip_tags: Optional[list[str]] = Field(
        default=None,
        description="Skip tasks with these Ansible tags (--skip-tags).",
    )
    limit: Optional[str] = Field(
        default=None,
        description="Further limit the hosts targeted by this play (--limit).",
    )

    # --- Privilege escalation -------------------------------------------------
    become: bool = Field(
        default=False,
        description="Enable privilege escalation (--become / sudo).",
    )
    become_user: Optional[str] = Field(
        default=None,
        description="User to become when privilege escalation is active (--become-user).",
    )

    # --- Vault / secrets ------------------------------------------------------
    vault_password_file: Optional[str] = Field(
        default=None,
        description="Path to a file containing the Ansible Vault password (--vault-password-file).",
    )
    vault_password_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of an environment variable whose value is the Vault password. "
            "The component writes the value to a secure temporary file and passes "
            "it as --vault-password-file, then deletes it after the run."
        ),
    )

    # --- SSH key --------------------------------------------------------------
    ssh_private_key_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of an environment variable containing an SSH private key (PEM). "
            "The component writes it to a temp file with 0600 permissions and sets "
            "ANSIBLE_PRIVATE_KEY_FILE for the subprocess."
        ),
    )

    # --- Run mode -------------------------------------------------------------
    check_mode: bool = Field(
        default=False,
        description="Run in check (dry-run) mode — no changes are made (--check).",
    )
    diff_mode: bool = Field(
        default=False,
        description="Show diffs for files that are changed (--diff).",
    )
    verbosity: int = Field(
        default=0,
        ge=0,
        le=4,
        description="Verbosity level 0-4. Translates to -v / -vv / -vvv / -vvvv.",
    )

    # --- Execution environment ------------------------------------------------
    ansible_bin: str = Field(
        default="ansible-playbook",
        description="Path to the ansible-playbook binary.",
    )
    working_dir: Optional[str] = Field(
        default=None,
        description="Working directory for the subprocess. Defaults to the current process cwd.",
    )
    env_vars: Optional[dict[str, str]] = Field(
        default=None,
        description="Additional environment variables merged into the subprocess environment.",
    )

    # --- Asset metadata -------------------------------------------------------
    group_name: str = Field(
        default="infrastructure",
        description="Dagster asset group name.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description surfaced in the Dagster UI.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _build_command(self, partition_tokens=None) -> list[str]:
        cmd: list[str] = [self.ansible_bin, self.playbook]

        if self.inventory:
            cmd += ["-i", self.inventory]

        if self.extra_vars:
            pairs = " ".join(f"{k}={v}" for k, v in _substitute_partition(self.extra_vars, partition_tokens).items())
            cmd += ["--extra-vars", pairs]

        if self.extra_vars_file:
            cmd += ["--extra-vars", f"@{self.extra_vars_file}"]

        if self.tags:
            cmd += ["--tags", ",".join(self.tags)]

        if self.skip_tags:
            cmd += ["--skip-tags", ",".join(self.skip_tags)]

        if self.limit:
            cmd += ["--limit", self.limit]

        if self.become:
            cmd.append("--become")

        if self.become_user:
            cmd += ["--become-user", self.become_user]

        if self.check_mode:
            cmd.append("--check")

        if self.diff_mode:
            cmd.append("--diff")

        if self.verbosity > 0:
            cmd.append("-" + "v" * self.verbosity)

        return cmd

    @staticmethod
    def _parse_play_recap(output: str) -> dict[str, dict[str, int]]:
        """Parse the PLAY RECAP block and return per-host counters.

        Returns a dict keyed by hostname, each value being a dict with keys
        ok, changed, unreachable, failed, skipped, rescued, ignored.
        """
        results: dict[str, dict[str, int]] = {}
        in_recap = False
        for line in output.splitlines():
            if "PLAY RECAP" in line:
                in_recap = True
                continue
            if not in_recap:
                continue
            line = line.strip()
            if not line:
                break
            # Format: hostname   : ok=N  changed=N  unreachable=N  failed=N ...
            if ":" not in line:
                continue
            host, _, counters_str = line.partition(":")
            host = host.strip()
            host_counters: dict[str, int] = {}
            for token in counters_str.split():
                if "=" in token:
                    k, _, v = token.partition("=")
                    try:
                        host_counters[k] = int(v)
                    except ValueError:
                        pass
            if host:
                results[host] = host_counters
        return results

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------


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
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `extra_vars` instead.",
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
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `extra_vars` instead.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        # retry_policy_* settings -> RetryPolicy (applied to the asset(s) below)
        _column_lineage_md = _column_lineage_metadata(self)
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            None,  # dynamic_partition_name: not a field of this component
            None,  # partition_dimensions: not a field of this component
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _retry_policy_obj = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
        
            _retry_policy_obj = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        component = self  # capture for closure

        asset_deps = [dg.AssetKey(d) for d in (component.deps or [])]

        @dg.asset(partitions_def=partitions_def, retry_policy=_retry_policy_obj, freshness_policy=_freshness_policy_obj, 
            name=component.asset_name,
            group_name=component.group_name,
            description=component.description,
            deps=asset_deps,
        )
        def _ansible_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            _tokens = _partition_vars(context)
            cmd = component._build_command(_tokens)
            env = {**os.environ}

            if component.env_vars:
                env.update(component.env_vars)
            env.update(_partition_env(_tokens))

            tmp_files: list[str] = []
            try:
                # -- Vault password from env var ------------------------------
                vault_password_file = component.vault_password_file
                if component.vault_password_env_var:
                    vault_password = os.environ.get(component.vault_password_env_var)
                    if not vault_password:
                        raise ValueError(
                            f"Environment variable '{component.vault_password_env_var}' "
                            "is not set or is empty."
                        )
                    with tempfile.NamedTemporaryFile(
                        mode="w", suffix=".vault_pass", delete=False
                    ) as vf:
                        vf.write(vault_password)
                        vault_password_file = vf.name
                    os.chmod(vault_password_file, 0o600)
                    tmp_files.append(vault_password_file)

                if vault_password_file:
                    cmd += ["--vault-password-file", vault_password_file]

                # -- SSH private key from env var -----------------------------
                if component.ssh_private_key_env_var:
                    ssh_key = os.environ.get(component.ssh_private_key_env_var)
                    if not ssh_key:
                        raise ValueError(
                            f"Environment variable '{component.ssh_private_key_env_var}' "
                            "is not set or is empty."
                        )
                    with tempfile.NamedTemporaryFile(
                        mode="w", suffix=".pem", delete=False
                    ) as kf:
                        kf.write(ssh_key)
                        ssh_key_file = kf.name
                    os.chmod(ssh_key_file, 0o600)
                    tmp_files.append(ssh_key_file)
                    env["ANSIBLE_PRIVATE_KEY_FILE"] = ssh_key_file

                # -- Run playbook --------------------------------------------
                context.log.info(f"Running command: {' '.join(cmd)}")

                output_lines: list[str] = []
                stderr_lines: list[str] = []

                with subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env,
                    cwd=component.working_dir,
                    text=True,
                ) as proc:
                    assert proc.stdout is not None
                    for line in iter(proc.stdout.readline, ""):
                        line = line.rstrip("\n")
                        context.log.info(line)
                        output_lines.append(line)

                    _, stderr_output = proc.communicate()
                    if stderr_output:
                        for line in stderr_output.splitlines():
                            context.log.warning(line)
                            stderr_lines.append(line)

                    returncode = proc.returncode

                if returncode != 0:
                    stderr_text = "\n".join(stderr_lines)
                    raise RuntimeError(
                        f"ansible-playbook exited with code {returncode}.\n"
                        f"stderr:\n{stderr_text}"
                    )

                # -- Parse PLAY RECAP ----------------------------------------
                full_output = "\n".join(output_lines)
                recap = component._parse_play_recap(full_output)

                hosts_ok = sum(h.get("ok", 0) for h in recap.values())
                hosts_changed = sum(h.get("changed", 0) for h in recap.values())
                hosts_unreachable = sum(h.get("unreachable", 0) for h in recap.values())
                hosts_failed = sum(h.get("failed", 0) for h in recap.values())

                return dg.MaterializeResult(
                    metadata={
                        "playbook": component.playbook,
                        "hosts_ok": hosts_ok,
                        "hosts_changed": hosts_changed,
                        "hosts_unreachable": hosts_unreachable,
                        "hosts_failed": hosts_failed,
                        "host_recap": str(recap),
                    }
                )
            finally:
                for tmp in tmp_files:
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass

        return dg.Definitions(assets=[_ansible_asset])


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
