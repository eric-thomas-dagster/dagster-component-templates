import json
import os
import subprocess
from typing import Optional

import dagster as dg
from pydantic import Field


@dg.definitions
class ModalAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Modal function as a Dagster asset.

    Runs a function on Modal's serverless infrastructure (CPU, GPU, custom
    containers) and tracks the invocation as a Dagster asset materialization.
    Supports both the Modal Python SDK (when ``modal`` is installed in the
    Dagster worker environment) and a subprocess fallback using the Modal CLI.

    Typical use cases include ML model training, GPU batch inference, large-scale
    data processing, and any compute-heavy workload you want to offload to Modal
    while keeping it observable inside Dagster.
    """

    # --- Identity -------------------------------------------------------------
    asset_name: str = Field(description="Dagster asset key for this component.")

    # --- Modal function reference ---------------------------------------------
    modal_function: str = Field(
        description=(
            "Fully-qualified reference to the Modal function in the form "
            "``app_name::function_name`` (SDK lookup) or ``module.path::function_name`` "
            "(CLI run). Used as a display label in asset metadata."
        )
    )
    app_file: Optional[str] = Field(
        default=None,
        description=(
            "Path to the Python file that defines the Modal app "
            "(e.g. ``ml/training_app.py``). Required for the CLI fallback path. "
            "Also used with the SDK when ``modal_function`` does not embed the app name."
        ),
    )
    function_name: Optional[str] = Field(
        default=None,
        description=(
            "Name of the decorated ``@modal.function`` within ``app_file``. "
            "Required when using the CLI fallback (``modal run app_file::function_name``)."
        ),
    )

    # --- Invocation parameters ------------------------------------------------
    args: Optional[list] = Field(
        default=None,
        description="Positional arguments forwarded to the Modal function.",
    )
    kwargs: Optional[dict] = Field(
        default=None,
        description="Keyword arguments forwarded to the Modal function.",
    )

    # --- Authentication -------------------------------------------------------
    modal_token_id_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable containing the Modal token ID. "
            "When set, the component overrides the ``MODAL_TOKEN_ID`` subprocess "
            "environment variable or configures the SDK client accordingly."
        ),
    )
    modal_token_secret_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable containing the Modal token secret. "
            "Paired with ``modal_token_id_env_var`` for non-default credential injection."
        ),
    )

    # --- Run behaviour --------------------------------------------------------
    detach: bool = Field(
        default=False,
        description=(
            "When True, fire-and-forget: the asset completes as soon as the function "
            "is dispatched without waiting for it to finish. Metadata will not include "
            "return values. Use with care — Dagster will mark the asset as materialised "
            "even if the Modal function subsequently fails."
        ),
    )
    environment_name: Optional[str] = Field(
        default=None,
        description=(
            "Modal environment (workspace namespace) to target. Defaults to the "
            "environment configured in the active Modal profile."
        ),
    )

    # --- Asset metadata -------------------------------------------------------
    group_name: str = Field(
        default="compute",
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

    def _build_env(self) -> dict:
        """Return a subprocess environment with optional credential overrides."""
        env = {**os.environ}
        if self.modal_token_id_env_var:
            token_id = os.environ.get(self.modal_token_id_env_var)
            if not token_id:
                raise ValueError(
                    f"Environment variable '{self.modal_token_id_env_var}' "
                    "is not set or is empty."
                )
            env["MODAL_TOKEN_ID"] = token_id
        if self.modal_token_secret_env_var:
            token_secret = os.environ.get(self.modal_token_secret_env_var)
            if not token_secret:
                raise ValueError(
                    f"Environment variable '{self.modal_token_secret_env_var}' "
                    "is not set or is empty."
                )
            env["MODAL_TOKEN_SECRET"] = token_secret
        if self.environment_name:
            env["MODAL_ENVIRONMENT"] = self.environment_name
        return env

    def _run_via_cli(self, context: dg.AssetExecutionContext) -> Optional[str]:
        """Invoke the Modal function via the ``modal run`` CLI.

        Returns combined stdout as a string, or None when ``detach`` is True.
        Raises RuntimeError on non-zero exit.
        """
        if not self.app_file or not self.function_name:
            raise ValueError(
                "Both 'app_file' and 'function_name' must be set to use the "
                "CLI fallback path (modal SDK not available)."
            )

        cmd: list[str] = ["modal", "run", f"{self.app_file}::{self.function_name}"]

        if self.detach:
            cmd.append("--detach")

        # Pass kwargs as JSON after the '--' separator so Modal can pick them up
        # via a custom argument parser in the app file.
        if self.kwargs:
            cmd += ["--", json.dumps(self.kwargs)]

        env = self._build_env()
        context.log.info(f"[Modal CLI] Running: {' '.join(cmd)}")

        output_lines: list[str] = []
        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
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

            returncode = proc.returncode

        if returncode != 0:
            raise RuntimeError(
                f"'modal run' exited with code {returncode}. "
                f"See logs above for details."
            )

        return "\n".join(output_lines) if not self.detach else None

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
        def _modal_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            result_value = None
            used_sdk = False

            try:
                import modal  # type: ignore[import]

                used_sdk = True

                # Resolve app name and function name from modal_function field
                # Expected format: "app_name::function_name"
                if "::" in component.modal_function:
                    app_name, fn_name = component.modal_function.split("::", 1)
                else:
                    # Fall back to explicit fields
                    app_name = component.modal_function
                    fn_name = component.function_name

                if not fn_name:
                    raise ValueError(
                        "Cannot determine function name: set 'function_name' or "
                        "use 'app_name::function_name' format in 'modal_function'."
                    )

                context.log.info(
                    f"[Modal SDK] Looking up function '{fn_name}' in app '{app_name}' ..."
                )

                # Configure credentials if overrides are requested
                env = component._build_env()
                token_id = env.get("MODAL_TOKEN_ID")
                token_secret = env.get("MODAL_TOKEN_SECRET")

                if token_id and token_secret:
                    modal.config._override_config(
                        token_id=token_id, token_secret=token_secret
                    )

                remote_fn = modal.Function.lookup(
                    app_name,
                    fn_name,
                    environment_name=component.environment_name,
                )

                call_args = component.args or []
                call_kwargs = component.kwargs or {}

                if component.detach:
                    context.log.info("[Modal SDK] Dispatching (detach=True) ...")
                    remote_fn.spawn(*call_args, **call_kwargs)
                    context.log.info("[Modal SDK] Function dispatched (fire-and-forget).")
                else:
                    context.log.info("[Modal SDK] Calling function (waiting for result) ...")
                    result_value = remote_fn.remote(*call_args, **call_kwargs)
                    context.log.info(f"[Modal SDK] Function returned: {result_value!r}")

            except ImportError:
                context.log.warning(
                    "Modal Python SDK not found; falling back to 'modal run' CLI. "
                    "Install 'modal' to use the SDK path."
                )
                component._run_via_cli(context)

            metadata: dict = {
                "modal_function": component.modal_function,
                "kwargs": json.dumps(component.kwargs or {}),
                "detach": component.detach,
                "sdk_used": used_sdk,
            }
            if result_value is not None:
                # Truncate large return values to keep metadata readable
                result_repr = repr(result_value)
                metadata["return_value"] = (
                    result_repr if len(result_repr) <= 2000 else result_repr[:2000] + "…"
                )

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_modal_asset])
