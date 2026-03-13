import os
import subprocess
import tempfile
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
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

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _build_command(self) -> list[str]:
        cmd: list[str] = [self.ansible_bin, self.playbook]

        if self.inventory:
            cmd += ["-i", self.inventory]

        if self.extra_vars:
            pairs = " ".join(f"{k}={v}" for k, v in self.extra_vars.items())
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

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self  # capture for closure

        asset_deps = [dg.AssetKey(d) for d in (component.deps or [])]

        @dg.asset(
            name=component.asset_name,
            group_name=component.group_name,
            description=component.description,
            deps=asset_deps,
        )
        def _ansible_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            cmd = component._build_command()
            env = {**os.environ}

            if component.env_vars:
                env.update(component.env_vars)

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
