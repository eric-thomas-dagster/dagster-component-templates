import os
import subprocess
from typing import Any, Optional

import dagster as dg
import requests
from pydantic import Field


class VaultResource(dg.ConfigurableResource):
    """Dagster resource for reading secrets from a HashiCorp Vault cluster.

    Supports both token-based and AppRole authentication. The Vault URL and all
    credentials are read from environment variables at runtime so that no secret
    values appear in component YAML or Dagster configuration files.
    """

    vault_url_env_var: str = Field(
        description=(
            "Name of the environment variable that holds the Vault base URL "
            "(e.g. https://vault.company.com). Must not have a trailing slash."
        )
    )
    token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable that holds a Vault token. "
            "Used for token-based authentication. "
            "Mutually exclusive with AppRole (role_id_env_var / secret_id_env_var)."
        ),
    )
    role_id_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable that holds the AppRole role_id. "
            "Both role_id_env_var and secret_id_env_var must be set for AppRole auth."
        ),
    )
    secret_id_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable that holds the AppRole secret_id. "
            "Both role_id_env_var and secret_id_env_var must be set for AppRole auth."
        ),
    )
    namespace: Optional[str] = Field(
        default=None,
        description=(
            "Vault Enterprise namespace (e.g. 'admin' or 'admin/team-a'). "
            "Set this when your Vault cluster uses namespaces. "
            "Ignored for Vault OSS."
        ),
    )
    verify_ssl: bool = Field(
        default=True,
        description=(
            "Verify the Vault server's TLS certificate. Set to False only in "
            "development environments with self-signed certificates."
        ),
    )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _vault_url(self) -> str:
        url = os.environ.get(self.vault_url_env_var, "").rstrip("/")
        if not url:
            raise ValueError(
                f"Environment variable '{self.vault_url_env_var}' is not set or empty. "
                "It must contain the Vault base URL (e.g. https://vault.company.com)."
            )
        return url

    def _base_headers(self, token: str) -> dict[str, str]:
        headers: dict[str, str] = {
            "X-Vault-Token": token,
            "Content-Type": "application/json",
        }
        if self.namespace:
            headers["X-Vault-Namespace"] = self.namespace
        return headers

    def _authenticate(self) -> str:
        """Return a Vault token, performing AppRole login if necessary."""
        # -- Token auth -------------------------------------------------------
        if self.token_env_var:
            token = os.environ.get(self.token_env_var, "")
            if not token:
                raise ValueError(
                    f"Environment variable '{self.token_env_var}' is not set or empty."
                )
            return token

        # -- AppRole auth -----------------------------------------------------
        if self.role_id_env_var and self.secret_id_env_var:
            role_id = os.environ.get(self.role_id_env_var, "")
            secret_id = os.environ.get(self.secret_id_env_var, "")
            if not role_id:
                raise ValueError(
                    f"Environment variable '{self.role_id_env_var}' is not set or empty."
                )
            if not secret_id:
                raise ValueError(
                    f"Environment variable '{self.secret_id_env_var}' is not set or empty."
                )

            vault_url = self._vault_url()
            login_url = f"{vault_url}/v1/auth/approle/login"
            login_headers: dict[str, str] = {"Content-Type": "application/json"}
            if self.namespace:
                login_headers["X-Vault-Namespace"] = self.namespace

            resp = requests.post(
                login_url,
                json={"role_id": role_id, "secret_id": secret_id},
                headers=login_headers,
                verify=self.verify_ssl,
                timeout=30,
            )
            if not resp.ok:
                raise RuntimeError(
                    f"AppRole login to Vault failed (HTTP {resp.status_code}): {resp.text}"
                )
            token = resp.json()["auth"]["client_token"]
            return token

        raise ValueError(
            "VaultResource requires either 'token_env_var' or both "
            "'role_id_env_var' and 'secret_id_env_var' to be configured."
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_secret(self, path: str, key: Optional[str] = None) -> Any:
        """Read a secret from Vault KV v2 and return its data.

        Args:
            path: Full KV v2 path including the ``data`` segment, e.g.
                  ``secret/data/myapp/db``.
            key:  Optional key within the secret data dict. When provided the
                  value for that key is returned directly. When None the entire
                  ``data`` dict is returned.

        Returns:
            Either a single secret value (str, int, …) or a dict of all keys
            stored at *path*.

        Raises:
            RuntimeError: If Vault returns a non-2xx response.
            KeyError: If *key* is specified but not present in the secret data.
        """
        vault_url = self._vault_url()
        token = self._authenticate()
        headers = self._base_headers(token)

        url = f"{vault_url}/v1/{path.lstrip('/')}"
        resp = requests.get(url, headers=headers, verify=self.verify_ssl, timeout=30)

        if not resp.ok:
            raise RuntimeError(
                f"Vault GET {url} failed (HTTP {resp.status_code}): {resp.text}"
            )

        # KV v2 wraps data under resp["data"]["data"]
        payload = resp.json()
        data: dict = payload.get("data", {}).get("data", payload.get("data", {}))

        if key is not None:
            if key not in data:
                raise KeyError(
                    f"Key '{key}' not found in Vault secret at path '{path}'. "
                    f"Available keys: {sorted(data.keys())}"
                )
            return data[key]

        return data


# ---------------------------------------------------------------------------
# Component
# ---------------------------------------------------------------------------


@dg.component_type(name="VaultAssetComponent")
class VaultAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Read secrets from HashiCorp Vault at pipeline runtime.

    Fetches one or more secrets from a Vault KV v2 engine, optionally injects
    them as environment variables into the current process, and optionally
    executes shell commands with those secrets available in the subprocess
    environment. The asset records which secret *paths* were accessed in its
    materialization metadata — never the secret values themselves.

    Supports both token-based and AppRole authentication. All credentials are
    read from environment variables so that no secrets appear in YAML config.
    """

    # --- Identity ------------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")

    # --- Vault connection / auth --------------------------------------------
    vault_url_env_var: str = Field(
        description=(
            "Name of the environment variable that holds the Vault base URL "
            "(e.g. https://vault.company.com)."
        )
    )
    token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable holding a Vault token. "
            "Use this for token-based authentication."
        ),
    )
    role_id_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable holding an AppRole role_id. "
            "Must be paired with secret_id_env_var."
        ),
    )
    secret_id_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable holding an AppRole secret_id. "
            "Must be paired with role_id_env_var."
        ),
    )
    namespace: Optional[str] = Field(
        default=None,
        description="Vault Enterprise namespace. Omit for Vault OSS.",
    )

    # --- Secret mappings -----------------------------------------------------
    secrets: list[dict] = Field(
        description=(
            "List of secrets to fetch from Vault. Each entry must have: "
            "``path`` (KV v2 path including 'data/' segment), "
            "``key`` (key within the secret dict), and "
            "``env_var`` (name of the environment variable to set)."
        )
    )

    # --- Execution -----------------------------------------------------------
    commands: Optional[list[str]] = Field(
        default=None,
        description=(
            "Optional shell commands to run after secrets are fetched. "
            "Secrets are injected as environment variables into each subprocess."
        ),
    )
    export_to_env: bool = Field(
        default=True,
        description=(
            "When True, set the resolved secrets as environment variables on the "
            "current process via os.environ. This makes them available to any "
            "code running in the same process after this asset executes."
        ),
    )

    # --- Asset metadata ------------------------------------------------------
    group_name: str = Field(
        default="secrets",
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

    # ------------------------------------------------------------------
    # build_defs
    # ------------------------------------------------------------------

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self  # capture for closure

        asset_deps = [dg.AssetKey(d) for d in (component.deps or [])]

        @dg.asset(
            name=component.asset_name,
            group_name=component.group_name,
            description=component.description,
            deps=asset_deps,
        )
        def _vault_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            # Build a VaultResource from component config
            vault = VaultResource(
                vault_url_env_var=component.vault_url_env_var,
                token_env_var=component.token_env_var,
                role_id_env_var=component.role_id_env_var,
                secret_id_env_var=component.secret_id_env_var,
                namespace=component.namespace,
            )

            resolved_env: dict[str, str] = {}
            paths_accessed: list[str] = []

            for secret_cfg in component.secrets:
                path: str = secret_cfg["path"]
                key: str = secret_cfg["key"]
                env_var_name: str = secret_cfg["env_var"]

                context.log.info(
                    f"Fetching secret path='{path}' key='{key}' -> env_var='{env_var_name}'"
                )

                value = vault.get_secret(path=path, key=key)
                resolved_env[env_var_name] = str(value)

                if path not in paths_accessed:
                    paths_accessed.append(path)

            # Optionally inject into the current process environment
            if component.export_to_env:
                for env_name, env_val in resolved_env.items():
                    os.environ[env_name] = env_val
                context.log.info(
                    f"Exported {len(resolved_env)} secret(s) to process environment."
                )

            # Optionally run shell commands with secrets injected
            command_results: list[dict] = []
            if component.commands:
                subprocess_env = {**os.environ, **resolved_env}
                for cmd in component.commands:
                    context.log.info(f"Running command: {cmd}")
                    result = subprocess.run(
                        cmd,
                        shell=True,
                        env=subprocess_env,
                        capture_output=True,
                        text=True,
                    )
                    if result.stdout:
                        for line in result.stdout.splitlines():
                            context.log.info(line)
                    if result.stderr:
                        for line in result.stderr.splitlines():
                            context.log.warning(line)
                    if result.returncode != 0:
                        raise RuntimeError(
                            f"Command '{cmd}' exited with code {result.returncode}. "
                            "See run log for stderr output."
                        )
                    command_results.append(
                        {"command": cmd, "exit_code": result.returncode}
                    )

            # Build metadata — paths accessed, never values
            metadata: dict = {
                "vault_paths_accessed": dg.MetadataValue.json(paths_accessed),
                "secrets_injected": dg.MetadataValue.int(len(resolved_env)),
                "env_vars_set": dg.MetadataValue.json(
                    sorted(resolved_env.keys())
                ),
                "export_to_env": dg.MetadataValue.bool(component.export_to_env),
            }
            if command_results:
                metadata["commands_run"] = dg.MetadataValue.json(command_results)

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_vault_asset])
