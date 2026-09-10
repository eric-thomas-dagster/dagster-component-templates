"""Mid-run per-model monitor for dbt Cloud.

Vendored from `eric-thomas-dagster/dbt-cloud-mesh-demo` (which itself mirrors
the upstream ``et/dbt-cloud-monitor-runs`` PR to dagster-io/dagster). Delete
this file when the PR merges + releases; then import
``dagster_dbt.cloud_v2.run_monitor.DbtCloudRunMonitor`` directly.

Streams per-model Dagster events (Output, AssetCheckResult, AssetMaterialization)
as individual models complete during the dbt Cloud run — like dbt Core's
``.stream()`` but for dbt Cloud. Enables mid-run alerting via Dagster+ instead
of waiting for the entire job to finish.

Architecture:

1. Poll dbt Cloud every ``poll_interval`` seconds
2. Fetch debug logs from the steps API for per-model results
3. Map log results (``SCHEMA.model_name``) to Dagster asset keys via manifest
4. Yield ``Output`` events immediately — Dagster processes them (triggers
   alerts, updates UI) then resumes the generator
5. On completion, yield remaining events from ``run_results.json`` for
   anything not already yielded (tests, missed models)

Uses only public dagster-dbt APIs:
    dagster_dbt.cloud_v2.client.DbtCloudWorkspaceClient
    dagster_dbt.cloud_v2.run_handler.DbtCloudJobRunResults
    dagster_dbt.cloud_v2.types.DbtCloudJobRunStatusType, DbtCloudRun
    dagster_dbt.dagster_dbt_translator.DagsterDbtTranslator

Usage:
    invocation = workspace.cli(["build"], context=context)
    monitor = DbtCloudRunMonitor(client=..., run_id=..., ...)
    yield from monitor.stream(
        context=context,
        manifest=invocation.manifest,
        dagster_dbt_translator=translator,
    )
"""

import re
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Union

from dagster import (
    AssetCheckEvaluation,
    AssetCheckResult,
    AssetExecutionContext,
    AssetMaterialization,
    Failure,
    MetadataValue,
    Output,
)

try:
    from dagster_dbt.cloud_v2.client import DbtCloudWorkspaceClient
    from dagster_dbt.cloud_v2.run_handler import DbtCloudJobRunResults
    from dagster_dbt.cloud_v2.types import DbtCloudJobRunStatusType, DbtCloudRun
    from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

    TERMINAL_STATUSES = {
        DbtCloudJobRunStatusType.SUCCESS,
        DbtCloudJobRunStatusType.ERROR,
        DbtCloudJobRunStatusType.CANCELLED,
    }
except ImportError:  # pragma: no cover
    # Stubbed so `import` doesn't fail when dagster-dbt is missing —
    # the component's own build_defs raises a clear install error.
    DbtCloudWorkspaceClient = Any  # type: ignore
    DbtCloudJobRunResults = Any  # type: ignore
    DbtCloudJobRunStatusType = Any  # type: ignore
    DbtCloudRun = Any  # type: ignore
    DagsterDbtTranslator = Any  # type: ignore
    TERMINAL_STATUSES = set()

FAILURE_STATUSES = {"error", "fail"}

# Strip ANSI color codes before parsing debug logs
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")

# Model results in dbt logs: "N of M OK created ... SCHEMA.model_name"
_MODEL_RESULT_PATTERN = re.compile(
    r"(\d+)\s+of\s+(\d+)\s+"
    r"(OK|ERROR)\s+"
    r"(?:created|creating)\s+"
    r"(?:sql\s+)?(?:table|view|incremental)\s+"
    r"(?:model|snapshot|seed)\s+"
    r"(\S+\.\S+)",
)

# Test results: "N of M PASS test_name"
_TEST_RESULT_PATTERN = re.compile(
    r"(\d+)\s+of\s+(\d+)\s+"
    r"(PASS|FAIL|WARN|SKIP)\s+"
    r"(?:\d+\s+)?"
    r"(\S+)",
)

# Explicit error patterns
_RUNTIME_ERROR_PATTERN = re.compile(r"Runtime Error in model (\S+)", re.IGNORECASE)
_COMPILATION_ERROR_PATTERN = re.compile(
    r"Compilation Error in (?:model|test|snapshot|seed) (\S+)", re.IGNORECASE
)
_DATABASE_ERROR_PATTERN = re.compile(r"Database Error in model (\S+)", re.IGNORECASE)


@dataclass
class DbtCloudRunMonitor:
    """Streams per-model Dagster events from a dbt Cloud run mid-execution.

    Args:
        client: The dbt Cloud workspace client.
        run_id: The dbt Cloud run ID to monitor.
        poll_interval: Seconds between polls. Default 5.
        fail_fast: Cancel the dbt Cloud run on first model failure.
    """

    client: Any  # DbtCloudWorkspaceClient
    run_id: int
    poll_interval: float = 5.0
    fail_fast: bool = False
    _seen_log_nodes: set = field(default_factory=set)
    _yielded_output_names: set = field(default_factory=set)
    _failures: list = field(default_factory=list)

    def stream(
        self,
        context: AssetExecutionContext,
        manifest: Mapping[str, Any],
        dagster_dbt_translator: Any = None,  # DagsterDbtTranslator | None
        timeout: float | None = None,
    ) -> Iterator[Union[AssetCheckEvaluation, AssetCheckResult, AssetMaterialization, Output]]:
        """Stream Dagster events as dbt Cloud models complete.

        Yields Output events mid-run from debug log parsing, then yields
        remaining events (tests, missed models) from run_results.json on
        completion. Raises Failure if any models errored.
        """
        translator = dagster_dbt_translator or DagsterDbtTranslator()
        start_time = time.time()
        poll_count = 0
        log_parsing_available = False

        # Build lookup: "SCHEMA.model_name" (lowered) → unique_id
        log_name_to_unique_id = self._build_log_name_lookup(manifest)

        # Track whether the dbt Cloud run reached a terminal state; if not
        # and the generator is closed (Dagster run cancelled), the finally
        # block cancels the dbt Cloud run so it doesn't keep consuming compute.
        run_completed = False
        try:
            while True:
                poll_count += 1
                if timeout and (time.time() - start_time) > timeout:
                    raise TimeoutError(f"dbt Cloud run {self.run_id} timed out after {timeout}s")

                # 1. Get run status
                run_details = self._get_run_details_safe(context)
                run = DbtCloudRun.from_run_details(run_details)
                status_name = run.status.name if run.status else "UNKNOWN"
                context.log.info(f"Run {self.run_id} status: {status_name}")

                if poll_count == 1:
                    run_url = run.url or run_details.get("href", "")
                    if run_url:
                        context.log.info(f"dbt Cloud run URL: {run_url}")

                # 2. Try to activate debug log parsing
                if not log_parsing_available:
                    run_steps = run_details.get("run_steps", [])
                    if run_steps:
                        latest_step = max(run_steps, key=lambda s: s.get("index", 0))
                        test_logs = self._fetch_step_debug_logs(latest_step["id"])
                        if test_logs:
                            log_parsing_available = True
                            context.log.info("Per-model log streaming active.")

                # 3. Parse debug logs and YIELD events for completed models
                if log_parsing_available:
                    yield from self._stream_from_debug_logs(
                        run_details, context, manifest, translator,
                        log_name_to_unique_id,
                    )

                # 4. Fail fast?
                if self.fail_fast and self._failures:
                    failed = ", ".join(self._failures)
                    context.log.error(
                        f"Fail-fast triggered. Cancelling dbt Cloud run {self.run_id}. "
                        f"Failed: {failed}"
                    )
                    self.cancel_run(context)
                    run_completed = True
                    yield from self._stream_remaining_from_artifacts(
                        context, manifest, translator
                    )
                    raise Failure(
                        f"dbt Cloud run '{self.run_id}' cancelled — failures: {failed}",
                        metadata={"run_id": MetadataValue.int(self.run_id)},
                    )

                # 5. Terminal state?
                if run.status in TERMINAL_STATUSES:
                    run_completed = True
                    yield from self._stream_remaining_from_artifacts(
                        context, manifest, translator
                    )

                    if run.status in {DbtCloudJobRunStatusType.ERROR, DbtCloudJobRunStatusType.CANCELLED}:
                        failed = ", ".join(self._failures) if self._failures else "see dbt Cloud logs"
                        raise Failure(
                            f"dbt Cloud run '{self.run_id}' finished with {status_name}. "
                            f"Failures: {failed}",
                            metadata={"run_id": MetadataValue.int(self.run_id)},
                        )
                    return

                time.sleep(self.poll_interval)
        finally:
            if not run_completed:
                context.log.warning(
                    f"Dagster run terminated. Cancelling dbt Cloud run {self.run_id}."
                )
                self.cancel_run(context)

    def _stream_from_debug_logs(
        self,
        run_details: dict,
        context: AssetExecutionContext,
        manifest: Mapping[str, Any],
        translator: Any,
        log_name_to_unique_id: dict,
    ) -> Iterator[Output]:
        """Parse debug logs and yield Output events for completed models."""
        try:
            log_text = self._get_logs_from_steps(run_details)
            if not log_text:
                return
            log_text = _ANSI_ESCAPE.sub("", log_text)
        except Exception:
            return

        # Parse model results
        for match in _MODEL_RESULT_PATTERN.finditer(log_text):
            _seq, _total, status_str, log_name = match.groups()
            if log_name in self._seen_log_nodes:
                continue
            self._seen_log_nodes.add(log_name)

            is_error = status_str.upper() == "ERROR"
            if is_error:
                context.log.error(f"MODEL FAILED (logs): {log_name}")
                self._failures.append(log_name)
                continue

            unique_id = log_name_to_unique_id.get(log_name.lower())
            if not unique_id:
                context.log.info(f"Model OK (logs): {log_name} (no manifest mapping, will yield from artifacts)")
                continue

            node = manifest.get("nodes", {}).get(unique_id)
            if not node:
                continue

            asset_key = translator.get_asset_key(node)
            output_name = asset_key.to_python_identifier()

            if output_name in self._yielded_output_names:
                continue
            self._yielded_output_names.add(output_name)

            context.log.info(f"Model OK (streaming): {log_name} → {asset_key}")
            yield Output(
                value=None,
                output_name=output_name,
                metadata={
                    "unique_id": unique_id,
                    "streamed_from": "debug_logs",
                },
            )

        # Parse test results (log only; tests yielded from artifacts with full metadata)
        for match in _TEST_RESULT_PATTERN.finditer(log_text):
            _seq, _total, status_str, test_name = match.groups()
            if "." in test_name:
                continue
            if test_name in self._seen_log_nodes:
                continue
            self._seen_log_nodes.add(test_name)

            if status_str.upper() == "FAIL":
                context.log.error(f"TEST FAILED (logs): {test_name}")
                self._failures.append(test_name)
            elif status_str.upper() == "WARN":
                context.log.warning(f"Test warning (logs): {test_name}")
            else:
                context.log.info(f"Test {status_str.lower()} (logs): {test_name}")

        # Catch explicit error patterns
        for pattern in (_RUNTIME_ERROR_PATTERN, _COMPILATION_ERROR_PATTERN, _DATABASE_ERROR_PATTERN):
            for match in pattern.finditer(log_text):
                node_name = match.group(1)
                if node_name in self._seen_log_nodes:
                    continue
                self._seen_log_nodes.add(node_name)
                context.log.error(f"ERROR (logs): {match.group(0).strip()}")
                self._failures.append(node_name)

    def _stream_remaining_from_artifacts(
        self,
        context: AssetExecutionContext,
        manifest: Mapping[str, Any],
        translator: Any,
    ) -> Iterator[Union[AssetCheckEvaluation, AssetCheckResult, AssetMaterialization, Output]]:
        """Yield events from run_results.json for anything not already streamed."""
        try:
            run_results_json = self.client.get_run_results_json(self.run_id)
        except Exception:
            context.log.warning("run_results.json not available — no remaining events to yield")
            return

        run_results = DbtCloudJobRunResults.from_run_results_json(run_results_json)

        for event in run_results.to_default_asset_events(
            client=self.client,
            manifest=manifest,
            dagster_dbt_translator=translator,
            context=context,
        ):
            if isinstance(event, Output) and event.output_name in self._yielded_output_names:
                continue
            if isinstance(event, Output):
                self._yielded_output_names.add(event.output_name)
                unique_id = (event.metadata or {}).get("unique_id", "")
                context.log.info(f"OK (artifacts): {unique_id}")
            yield event

        # Track failures from run_results for error reporting
        for result in run_results_json.get("results", []):
            status = result.get("status", "")
            if status in FAILURE_STATUSES:
                unique_id = result.get("unique_id", "")
                if unique_id not in self._failures:
                    self._failures.append(unique_id)
                    context.log.error(
                        f"FAILED (artifacts): {unique_id} — {result.get('message', '')}"
                    )

    def _build_log_name_lookup(self, manifest: Mapping[str, Any]) -> dict:
        """Map ``schema.name`` (lowered) → unique_id for model/snapshot/seed nodes."""
        lookup: dict = {}
        for unique_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") not in ("model", "snapshot", "seed"):
                continue
            schema = node.get("schema", "")
            name = node.get("alias") or node.get("name", "")
            if schema and name:
                key = f"{schema}.{name}".lower()
                lookup[key] = unique_id
        return lookup

    def _dbt_cloud_get(self, endpoint: str, params: dict | None = None) -> dict:
        """GET request to dbt Cloud API using requests directly."""
        import requests as req
        url = f"{self.client.api_v2_url}/{endpoint}"
        resp = req.get(
            url,
            headers={
                "Authorization": f"Token {self.client.token}",
                "Content-Type": "application/json",
            },
            params=params,
            timeout=self.client.request_timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def _get_run_details_safe(self, context: AssetExecutionContext) -> dict:
        """Fetch run details with run_steps included."""
        try:
            return self._dbt_cloud_get(
                f"runs/{self.run_id}",
                params={"include_related": '["run_steps"]'},
            )["data"]
        except Exception:
            try:
                return self.client.get_run_details(self.run_id)
            except Exception as e:
                context.log.warning(f"Failed to fetch run details: {e}")
                raise

    def _fetch_step_debug_logs(self, step_id: int) -> str:
        """Fetch debug logs for a specific run step."""
        try:
            data = self._dbt_cloud_get(
                f"steps/{step_id}",
                params={"include_related": '["debug_logs"]'},
            )
            step_data = data.get("data", {})
            return step_data.get("debug_logs", "") or step_data.get("logs", "") or ""
        except Exception:
            return ""

    def _get_logs_from_steps(self, run_details: dict) -> str:
        """Fetch and concatenate debug logs from all run steps."""
        run_steps = run_details.get("run_steps", [])
        if not run_steps:
            return ""
        parts: list = []
        for step in sorted(run_steps, key=lambda s: s.get("index", 0)):
            step_id = step.get("id")
            if step_id:
                logs = self._fetch_step_debug_logs(step_id)
                if logs:
                    parts.append(logs)
        return "\n".join(parts)

    def cancel_run(self, context: AssetExecutionContext) -> None:
        """Cancel the dbt Cloud run."""
        import requests as req
        try:
            url = f"{self.client.api_v2_url}/runs/{self.run_id}/cancel/"
            resp = req.post(
                url,
                headers={
                    "Authorization": f"Token {self.client.token}",
                    "Content-Type": "application/json",
                },
                timeout=self.client.request_timeout,
            )
            resp.raise_for_status()
            context.log.warning(f"Cancelled dbt Cloud run {self.run_id}")
        except Exception as e:
            context.log.error(f"Failed to cancel run {self.run_id}: {e}")
