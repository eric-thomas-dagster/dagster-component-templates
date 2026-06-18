import io
import os
import tempfile
from typing import Any, Dict, List, Optional, Union

import dagster as dg
import paramiko
from pydantic import Field


@dg.definitions
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


class SSHAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run shell commands on a remote host via SSH as a Dagster asset.

    Uses ``paramiko`` to open an SSH session, optionally transfers files via
    SFTP before or after command execution, streams stdout line-by-line to the
    Dagster event log, and surfaces per-command exit codes as asset metadata.
    Designed for legacy systems, HPC clusters, on-premise servers, and
    pre-pipeline configuration steps that live outside your Dagster worker.
    """

    # --- Identity -------------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")

    # --- Connection -----------------------------------------------------------
    host_env_var: str = Field(
        description="Name of the environment variable containing the hostname or IP address of the remote host."
    )
    username_env_var: str = Field(
        description="Name of the environment variable containing the SSH username."
    )
    private_key_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable containing the PEM-encoded SSH private key "
            "content (not a file path). The component writes it to a secure temporary file "
            "with mode 0600 and deletes it after the run."
        ),
    )
    private_key_path_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable containing the path to an SSH private key file "
            "already present on disk. Takes precedence over ``private_key_env_var`` when both "
            "are set."
        ),
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable containing the SSH password. Used as a fallback "
            "when no private key option is provided."
        ),
    )
    port: int = Field(default=22, description="SSH port on the remote host.")
    known_hosts_check: bool = Field(
        default=False,
        description=(
            "When False (default), ``AutoAddPolicy`` is applied so the client accepts any "
            "host key without prompting. Set to True to use the system known_hosts file and "
            "reject unknown host keys — recommended for security-sensitive environments."
        ),
    )

    # --- Commands -------------------------------------------------------------
    commands: list[str] = Field(
        description="Ordered list of shell commands to execute on the remote host."
    )
    working_dir: Optional[str] = Field(
        default=None,
        description=(
            "Remote directory to change into before running commands. "
            "Prepended as ``cd <working_dir> && `` to each command."
        ),
    )
    timeout_seconds: int = Field(
        default=300,
        description="Per-command execution timeout in seconds.",
    )
    fail_on_non_zero: bool = Field(
        default=True,
        description="Raise an exception when any command exits with a non-zero status code.",
    )
    capture_output: bool = Field(
        default=True,
        description="Stream each command's stdout to the Dagster run log as INFO messages.",
    )

    # --- SFTP -----------------------------------------------------------------
    sftp_upload: Optional[dict] = Field(
        default=None,
        description=(
            "If provided, upload a local file to the remote host via SFTP before running "
            "commands. Expected keys: ``local_path`` (str) and ``remote_path`` (str)."
        ),
    )
    sftp_download: Optional[dict] = Field(
        default=None,
        description=(
            "If provided, download a remote file to the local filesystem via SFTP after all "
            "commands complete. Expected keys: ``remote_path`` (str) and ``local_path`` (str)."
        ),
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

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

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




    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
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

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self  # capture for closure

        asset_deps = [dg.AssetKey(d) for d in (component.deps or [])]

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



        @dg.asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=component.asset_name,
            group_name=component.group_name,
            description=component.description,
            deps=asset_deps,
        )
        def _ssh_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            # -- Resolve required env vars ------------------------------------
            host = os.environ.get(component.host_env_var)
            if not host:
                raise ValueError(
                    f"Environment variable '{component.host_env_var}' is not set or is empty."
                )
            username = os.environ.get(component.username_env_var)
            if not username:
                raise ValueError(
                    f"Environment variable '{component.username_env_var}' is not set or is empty."
                )

            # -- Resolve auth -------------------------------------------------
            password: Optional[str] = None
            pkey: Optional[paramiko.PKey] = None
            key_path: Optional[str] = None
            tmp_key_file: Optional[str] = None

            try:
                if component.private_key_path_env_var:
                    key_path = os.environ.get(component.private_key_path_env_var)
                    if not key_path:
                        raise ValueError(
                            f"Environment variable '{component.private_key_path_env_var}' "
                            "is not set or is empty."
                        )
                    pkey = paramiko.RSAKey.from_private_key_file(key_path)
                elif component.private_key_env_var:
                    key_content = os.environ.get(component.private_key_env_var)
                    if not key_content:
                        raise ValueError(
                            f"Environment variable '{component.private_key_env_var}' "
                            "is not set or is empty."
                        )
                    # Write to temp file so paramiko can read it reliably for
                    # encrypted keys, then also try the in-memory path first.
                    try:
                        pkey = paramiko.RSAKey.from_private_key(io.StringIO(key_content))
                    except paramiko.SSHException:
                        # Fall back to Ed25519 / ECDSA
                        try:
                            pkey = paramiko.Ed25519Key.from_private_key(io.StringIO(key_content))
                        except paramiko.SSHException:
                            pkey = paramiko.ECDSAKey.from_private_key(io.StringIO(key_content))

                    # Also write to temp file so the path is available if needed
                    with tempfile.NamedTemporaryFile(
                        mode="w", suffix=".pem", delete=False
                    ) as kf:
                        kf.write(key_content)
                        tmp_key_file = kf.name
                    os.chmod(tmp_key_file, 0o600)

                elif component.password_env_var:
                    password = os.environ.get(component.password_env_var)
                    if not password:
                        raise ValueError(
                            f"Environment variable '{component.password_env_var}' "
                            "is not set or is empty."
                        )

                # -- Connect ---------------------------------------------------
                client = paramiko.SSHClient()
                if not component.known_hosts_check:
                    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                else:
                    client.load_system_host_keys()

                context.log.info(
                    f"Connecting to {host}:{component.port} as {username} ..."
                )
                client.connect(
                    hostname=host,
                    port=component.port,
                    username=username,
                    pkey=pkey,
                    password=password,
                    timeout=component.timeout_seconds,
                )
                context.log.info("SSH connection established.")

                # -- SFTP upload (before commands) -----------------------------
                if component.sftp_upload:
                    local_path = component.sftp_upload["local_path"]
                    remote_path = component.sftp_upload["remote_path"]
                    context.log.info(
                        f"SFTP upload: {local_path} -> {host}:{remote_path}"
                    )
                    sftp = client.open_sftp()
                    sftp.put(local_path, remote_path)
                    sftp.close()
                    context.log.info("SFTP upload complete.")

                # -- Execute commands -----------------------------------------
                exit_codes: list[int] = []
                for cmd in component.commands:
                    full_cmd = cmd
                    if component.working_dir:
                        full_cmd = f"cd {component.working_dir} && {cmd}"

                    context.log.info(f"[SSH] Running: {full_cmd}")
                    stdin, stdout, stderr = client.exec_command(
                        full_cmd, timeout=component.timeout_seconds
                    )
                    stdin.close()

                    if component.capture_output:
                        for line in iter(stdout.readline, ""):
                            context.log.info(line.rstrip("\n"))

                    stderr_output = stderr.read().decode("utf-8", errors="replace")
                    if stderr_output.strip():
                        for line in stderr_output.splitlines():
                            context.log.warning(line)

                    exit_code = stdout.channel.recv_exit_status()
                    exit_codes.append(exit_code)
                    context.log.info(f"[SSH] Exit code: {exit_code}")

                    if component.fail_on_non_zero and exit_code != 0:
                        raise RuntimeError(
                            f"Command exited with non-zero status {exit_code}: {full_cmd}"
                        )

                # -- SFTP download (after commands) ----------------------------
                if component.sftp_download:
                    remote_path = component.sftp_download["remote_path"]
                    local_path = component.sftp_download["local_path"]
                    context.log.info(
                        f"SFTP download: {host}:{remote_path} -> {local_path}"
                    )
                    sftp = client.open_sftp()
                    sftp.get(remote_path, local_path)
                    sftp.close()
                    context.log.info("SFTP download complete.")

                client.close()

                return dg.MaterializeResult(
                    metadata={
                        "host": host,
                        "commands_run": len(component.commands),
                        "exit_codes": str(exit_codes),
                    }
                )

            finally:
                if tmp_key_file:
                    try:
                        os.unlink(tmp_key_file)
                    except OSError:
                        pass

        return dg.Definitions(assets=[_ssh_asset])
