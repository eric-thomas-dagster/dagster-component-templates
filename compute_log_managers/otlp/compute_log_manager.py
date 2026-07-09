"""OtlpComputeLogManager.

Send Dagster op stdout/stderr to any OpenTelemetry-compatible logs
backend via OTLP/HTTP (JSON). One wire protocol, many destinations:

  - Splunk via Splunk Distribution of OpenTelemetry Collector
  - Datadog OTel ingest
  - Honeycomb
  - Sumo Logic OTel
  - Grafana Loki via OTel Collector
  - AWS CloudWatch via OTel Collector
  - Any OTel Collector → forwards to anything

Configured at the Dagster *instance* layer (``dagster.yaml``), not the
project layer:

    compute_logs:
      module: dagster_community_components.compute_log_managers.otlp
      class: OtlpComputeLogManager
      config:
        otlp_endpoint: http://otel-collector.svc:4318
        otlp_headers:
          x-honeycomb-team: "{env: HONEYCOMB_API_KEY}"
        service_name: dagster
        location_label: prod-east

What gets shipped per step:

  - One OTLP LogRecord per line of captured stdout/stderr
  - Resource attributes: service.name, service.instance.id
  - LogRecord attributes:
      dagster.run_id, dagster.step_key, dagster.io_type (stdout|stderr),
      dagster.partial
  - severityNumber: INFO (9) for stdout, WARN (13) for stderr — Dagster
    doesn't distinguish app-level errors from operational stderr, so
    stderr stays WARN rather than ERROR (visitors can override with
    severity_stderr / severity_stdout config).

UI behavior:

  - ``display_path_for_type`` returns the URL from ``display_url_template``
    with ``{run_id}`` / ``{step_key}`` / ``{io_type}`` substituted. Set
    this to your destination's search URL pattern (Splunk Web, Datadog,
    Honeycomb, etc.) so the Dagster UI shows a "View logs →" deep-link
    per step.
  - ``download_from_cloud_storage`` writes a stub (logs are at <url>)
    rather than re-fetching from the OTel backend.

Compose with TeeComputeLogManager to write to OTLP AND Dagster+ (or
S3 archive, etc.) simultaneously.
"""
import logging
import os
import time
from typing import IO, Optional, Sequence

from dagster import Field, Noneable, StringSource, _check as check
from dagster._config import IntSource
from dagster._core.storage.cloud_storage_compute_log_manager import (
    TruncatingCloudStorageComputeLogManager,
)
from dagster._core.storage.compute_log_manager import ComputeIOType
from dagster._core.storage.cloud_storage_compute_log_manager import (
    PollingComputeLogSubscriptionManager,
)
from dagster._core.storage.local_compute_log_manager import (
    IO_TYPE_EXTENSION,
    LocalComputeLogManager,
)
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster._utils import ensure_dir
from dagster_shared import seven

_logger = logging.getLogger(__name__)

# OTel severity number scheme (1-24). 9 = INFO, 13 = WARN, 17 = ERROR.
_SEVERITY_INFO = 9
_SEVERITY_WARN = 13

_IO_TYPE_NAMES = {
    ComputeIOType.STDOUT: "stdout",
    ComputeIOType.STDERR: "stderr",
}


class OtlpComputeLogManager(TruncatingCloudStorageComputeLogManager, ConfigurableClass):
    """Logs compute function stdout and stderr via OTLP/HTTP to any OTel-compatible backend.

    Configured in ``dagster.yaml``:

    .. code-block:: yaml

        compute_logs:
          module: dagster_community_components.compute_log_managers.otlp
          class: OtlpComputeLogManager
          config:
            otlp_endpoint: http://otel-collector.svc:4318
            otlp_headers:
              x-honeycomb-team: "{env: HONEYCOMB_API_KEY}"
              x-honeycomb-dataset: dagster_compute_logs
            service_name: dagster
            location_label: prod-east
            display_url_template: "https://ui.honeycomb.io/acme/datasets/dagster_compute_logs?query=dagster.run_id%3D{run_id}"
            batch_size: 100
            local_dir: /tmp/dagster_compute_logs
            upload_interval: 30

    Args:
        otlp_endpoint (str): OTLP/HTTP base URL. /v1/logs is appended.
            Examples:
              http://otel-collector.svc:4318
              https://api.honeycomb.io
              https://otlp.nr-data.net  (New Relic)
        otlp_headers (Optional[dict[str, str]]): Headers added to every
            OTLP POST. Convention varies per vendor:
              Honeycomb: ``x-honeycomb-team``, ``x-honeycomb-dataset``
              Datadog:   ``DD-API-KEY``
              Splunk OTC: usually no auth (intra-cluster)
        service_name (Optional[str]): OTel ``service.name`` resource
            attribute. Default ``dagster``.
        location_label (Optional[str]): OTel ``service.instance.id``
            resource attribute — usually the Dagster deployment / region.
            Default ``default``.
        display_url_template (Optional[str]): URL template for the
            Dagster UI's "View logs →" button. Supports ``{run_id}``,
            ``{step_key}``, ``{io_type}`` placeholders. If unset, no
            deep-link is rendered.
        severity_stdout (Optional[int]): OTel severityNumber for stdout
            lines. Default 9 (INFO).
        severity_stderr (Optional[int]): OTel severityNumber for stderr
            lines. Default 13 (WARN) — not ERROR, because Dagster doesn't
            distinguish application errors from operational stderr.
        verify_ssl (Optional[bool]): TLS cert verification. Default True.
        batch_size (Optional[int]): LogRecords per OTLP POST. Default 100.
        request_timeout_seconds (Optional[int]): HTTP timeout. Default 30.
        skip_empty_files (Optional[bool]): Skip uploads when the captured
            file is empty. Default False.
        local_dir (Optional[str]): Local capture directory. Defaults to
            system temp.
        upload_interval (Optional[int]): Partial-upload interval in
            seconds. None = only on step finish.
        inst_data (Optional[ConfigurableClassData]): Serdes plumbing.
    """

    def __init__(
        self,
        otlp_endpoint: str,
        otlp_headers: Optional[dict] = None,
        service_name: str = "dagster",
        location_label: str = "default",
        display_url_template: Optional[str] = None,
        severity_stdout: int = _SEVERITY_INFO,
        severity_stderr: int = _SEVERITY_WARN,
        verify_ssl: bool = True,
        batch_size: int = 100,
        request_timeout_seconds: int = 30,
        skip_empty_files: bool = False,
        local_dir: Optional[str] = None,
        upload_interval: Optional[int] = None,
        inst_data: Optional[ConfigurableClassData] = None,
    ):
        self._otlp_endpoint = check.str_param(otlp_endpoint, "otlp_endpoint").rstrip("/")
        self._otlp_headers = dict(check.opt_dict_param(otlp_headers, "otlp_headers"))
        self._service_name = check.str_param(service_name, "service_name")
        self._location_label = check.str_param(location_label, "location_label")
        self._display_url_template = check.opt_str_param(
            display_url_template, "display_url_template"
        )
        self._severity_stdout = check.int_param(severity_stdout, "severity_stdout")
        self._severity_stderr = check.int_param(severity_stderr, "severity_stderr")
        self._verify_ssl = check.bool_param(verify_ssl, "verify_ssl")
        self._batch_size = check.int_param(batch_size, "batch_size")
        self._request_timeout = check.int_param(request_timeout_seconds, "request_timeout_seconds")
        self._skip_empty_files = check.bool_param(skip_empty_files, "skip_empty_files")

        if not local_dir:
            local_dir = seven.get_system_temp_directory()
        self._local_manager = LocalComputeLogManager(local_dir)
        self._upload_interval = check.opt_int_param(upload_interval, "upload_interval")
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

        self._uploaded_keys: set = set()
        self._session = None  # lazy import of requests

        self._subscription_manager = PollingComputeLogSubscriptionManager(self)
        super().__init__()

    # ──────────────────────────── ConfigurableClass plumbing ────────────────

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "otlp_endpoint": StringSource,
            "otlp_headers": Field(dict, is_required=False, default_value={}),
            "service_name": Field(StringSource, is_required=False, default_value="dagster"),
            "location_label": Field(StringSource, is_required=False, default_value="default"),
            "display_url_template": Field(StringSource, is_required=False),
            "severity_stdout": Field(IntSource, is_required=False, default_value=_SEVERITY_INFO),
            "severity_stderr": Field(IntSource, is_required=False, default_value=_SEVERITY_WARN),
            "verify_ssl": Field(bool, is_required=False, default_value=True),
            "batch_size": Field(IntSource, is_required=False, default_value=100),
            "request_timeout_seconds": Field(IntSource, is_required=False, default_value=30),
            "skip_empty_files": Field(bool, is_required=False, default_value=False),
            "local_dir": Field(StringSource, is_required=False),
            "upload_interval": Field(Noneable(int), is_required=False, default_value=None),
        }

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    # ──────────────────────────── Required abstract API ─────────────────────

    @property
    def local_manager(self) -> LocalComputeLogManager:
        return self._local_manager

    @property
    def upload_interval(self) -> Optional[int]:
        return self._upload_interval

    def delete_logs(
        self,
        log_key: Optional[Sequence[str]] = None,
        prefix: Optional[Sequence[str]] = None,
    ) -> None:
        """Drop local files. OTLP backends are append-only — their own
        retention policies handle server-side lifecycle."""
        self._local_manager.delete_logs(log_key=log_key, prefix=prefix)
        if log_key:
            for io_type in (ComputeIOType.STDOUT, ComputeIOType.STDERR):
                for partial in (False, True):
                    self._uploaded_keys.discard(self._key_tuple(log_key, io_type, partial))

    def download_url_for_type(
        self, log_key: Sequence[str], io_type: ComputeIOType
    ) -> Optional[str]:
        """OTLP backends don't expose presigned downloads — surface
        display_path_for_type instead."""
        return None

    def display_path_for_type(
        self, log_key: Sequence[str], io_type: ComputeIOType
    ) -> Optional[str]:
        """Render display_url_template with run_id / step_key / io_type
        substituted. None if no template configured."""
        if not self.is_capture_complete(log_key):
            return None
        if not self._display_url_template:
            return None
        run_id, step_key = _split_log_key(log_key)
        return self._display_url_template.format(
            run_id=run_id,
            step_key=step_key,
            io_type=_IO_TYPE_NAMES[io_type],
            location=self._location_label,
            service=self._service_name,
        )

    def cloud_storage_has_logs(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial: bool = False
    ) -> bool:
        return self._key_tuple(log_key, io_type, partial) in self._uploaded_keys

    # ──────────────────────────── Upload path (OTLP/HTTP) ───────────────────

    def _upload_file_obj(
        self,
        data: IO[bytes],
        log_key: Sequence[str],
        io_type: ComputeIOType,
        partial: bool = False,
    ) -> None:
        path = self._local_manager.get_captured_local_path(log_key, IO_TYPE_EXTENSION[io_type])
        if not os.path.exists(path):
            return
        if (self._skip_empty_files or partial) and os.stat(path).st_size == 0:
            return

        run_id, step_key = _split_log_key(log_key)
        io_name = _IO_TYPE_NAMES[io_type]
        severity = (
            self._severity_stdout if io_type == ComputeIOType.STDOUT else self._severity_stderr
        )
        severity_text = "INFO" if severity == _SEVERITY_INFO else (
            "WARN" if severity == _SEVERITY_WARN else "ERROR"
        )
        attrs = [
            _otlp_attr("dagster.run_id", run_id),
            _otlp_attr("dagster.step_key", step_key or ""),
            _otlp_attr("dagster.io_type", io_name),
            _otlp_attr("dagster.partial", "true" if partial else "false"),
        ]
        # Enrich with step timing (start_time, end_time, duration_ms, status).
        # Belt-and-suspenders try/except so enrichment CANNOT break log delivery.
        try:
            _instance = _resolve_instance(self)
            _timing_attrs = _step_timing_attrs(_instance, run_id, step_key)
            attrs.extend(_timing_attrs)
            # INFO-level so it's visible without --log-level=DEBUG. Comment out
            # once you've confirmed enrichment is landing.
            if _timing_attrs:
                _logger.info(
                    f"OTLP CLM: enriched with {len(_timing_attrs)} step-timing attrs "
                    f"for run={run_id[:8]} step={step_key} — keys="
                    f"{[a['key'] for a in _timing_attrs]}"
                )
            else:
                _logger.info(
                    f"OTLP CLM: step-timing enrichment returned no attrs for "
                    f"run={run_id[:8] if run_id else '?'} step={step_key} — "
                    f"instance_resolved={_instance is not None}"
                )
        except Exception as _e:  # noqa: BLE001
            _logger.warning(f"OTLP CLM: step-timing enrichment skipped: {_e}")

        sent = 0
        for batch in _chunked(_iter_log_lines(path), self._batch_size):
            envelope = self._build_envelope(batch, attrs, severity, severity_text)
            self._post_otlp(envelope)
            sent += len(batch)

        if sent:
            self._uploaded_keys.add(self._key_tuple(log_key, io_type, partial))
            _logger.debug(
                f"OTLP CLM: shipped {sent} log records for run={run_id} step={step_key} io={io_name}"
            )

    def download_from_cloud_storage(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial: bool = False
    ) -> None:
        """Write a stub local file pointing at the OTLP destination —
        don't re-fetch from the backend on every UI viewer hit."""
        path = self._local_manager.get_captured_local_path(
            log_key, IO_TYPE_EXTENSION[io_type], partial=partial
        )
        ensure_dir(os.path.dirname(path))
        if os.path.exists(path):
            return
        display = self.display_path_for_type(log_key, io_type) or ""
        msg = (
            "Compute logs for this step were shipped via OTLP and are not "
            "cached locally.\n\n"
            + (f"View them at: {display}\n" if display else "(display_url_template not set)\n")
        )
        with open(path, "w") as f:
            f.write(msg)

    def get_log_keys_for_log_key_prefix(
        self, log_key_prefix: Sequence[str], io_type: ComputeIOType
    ) -> Sequence[Sequence[str]]:
        return self._local_manager.get_log_keys_for_log_key_prefix(
            log_key_prefix, io_type=io_type
        )

    # ──────────────────────────── Subscription / lifecycle ──────────────────

    def on_subscribe(self, subscription):
        self._subscription_manager.add_subscription(subscription)

    def on_unsubscribe(self, subscription):
        self._subscription_manager.remove_subscription(subscription)

    def dispose(self):
        self._subscription_manager.dispose()
        self._local_manager.dispose()

    # ──────────────────────────── Internals ─────────────────────────────────

    @staticmethod
    def _key_tuple(log_key: Sequence[str], io_type: ComputeIOType, partial: bool) -> tuple:
        # ComputeIOType is an Enum (.value is "stdout"/"stderr") — use the
        # value, not int(), since it's not an IntEnum.
        return ("/".join(log_key), io_type.value, bool(partial))

    def _build_envelope(self, lines, attrs, severity, severity_text):
        """Build one OTLP/HTTP logs envelope for a batch of lines.

        Per OTLP spec: resourceLogs → scopeLogs → logRecords. All lines
        in this batch share the same resource (service.name + instance.id)
        and the same scope; each gets its own LogRecord with the line as
        body and the dagster.* attributes.
        """
        now_nanos = str(int(time.time() * 1_000_000_000))
        log_records = []
        for line in lines:
            log_records.append({
                "timeUnixNano": now_nanos,
                "observedTimeUnixNano": now_nanos,
                "severityNumber": severity,
                "severityText": severity_text,
                "body": {"stringValue": line},
                "attributes": attrs,
            })
        return {
            "resourceLogs": [{
                "resource": {
                    "attributes": [
                        _otlp_attr("service.name", self._service_name),
                        _otlp_attr("service.instance.id", self._location_label),
                    ],
                },
                "scopeLogs": [{
                    "scope": {
                        "name": "dagster_community_components.compute_log_managers.otlp"
                    },
                    "logRecords": log_records,
                }],
            }],
        }

    def _post_otlp(self, envelope: dict) -> None:
        import requests
        if self._session is None:
            self._session = requests.Session()
        headers = {"Content-Type": "application/json", **self._otlp_headers}
        resp = self._session.post(
            f"{self._otlp_endpoint}/v1/logs",
            json=envelope,
            headers=headers,
            timeout=self._request_timeout,
            verify=self._verify_ssl,
        )
        resp.raise_for_status()


# ────────────────────────────── helpers ───────────────────────────────────


def _split_log_key(log_key: Sequence[str]) -> tuple:
    parts = list(log_key)
    if not parts:
        return "", ""
    return parts[0], (parts[-1] if len(parts) > 1 else "")


def _iter_log_lines(path: str):
    with open(path, "r", errors="replace") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line:
                continue
            yield line


def _chunked(it, n):
    buf: list = []
    for item in it:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def _otlp_attr(key: str, value) -> dict:
    """Build one OTLP KeyValue attribute — string, int, or double supported."""
    if isinstance(value, bool):
        # bool is a subclass of int in Python; special-case it.
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": int(value)}}
    if isinstance(value, float):
        return {"key": key, "value": {"doubleValue": float(value)}}
    return {"key": key, "value": {"stringValue": str(value)}}


def _resolve_instance(manager):
    """See splunk sibling — safe DagsterInstance resolution across
    multiprocess execution contexts. Returns None on any failure."""
    try:
        if getattr(manager, "has_instance", False):
            return manager._instance
    except Exception:  # noqa: BLE001
        pass
    try:
        from dagster import DagsterInstance
        return DagsterInstance.get()
    except Exception:  # noqa: BLE001
        return None


def _step_timing_attrs(instance, run_id, step_key):
    """Return a list of OTLP attributes for step start/end/duration/status.

    Best-effort — any exception silently returns an empty list so log
    delivery never fails because of enrichment.
    """
    try:
        if instance is None or not run_id or not step_key:
            return []
        try:
            _stats = instance.get_run_step_stats(run_id, step_keys=[step_key])
        except Exception:  # noqa: BLE001
            return []
        if not _stats:
            return []
        _s = _stats[0]
        _start = _coerce_epoch(getattr(_s, "start_time", None))
        _end = _coerce_epoch(getattr(_s, "end_time", None))
        _status = getattr(_s, "status", None)

        out = []
        if _start is not None:
            out.append(_otlp_attr("dagster.step_start_epoch", _start))
            _iso = _epoch_iso(_start)
            if _iso:
                out.append(_otlp_attr("dagster.step_start_iso", _iso))
        if _end is not None:
            out.append(_otlp_attr("dagster.step_end_epoch", _end))
            _iso = _epoch_iso(_end)
            if _iso:
                out.append(_otlp_attr("dagster.step_end_iso", _iso))
        if _start is not None and _end is not None:
            out.append(_otlp_attr("dagster.step_duration_ms", int(round((_end - _start) * 1000))))
        if _status is not None:
            out.append(_otlp_attr("dagster.step_status", getattr(_status, "value", str(_status))))
        return out
    except Exception:  # noqa: BLE001 — outer guard, never break upload
        return []


def _coerce_epoch(v):
    """Coerce a start_time/end_time to unix-epoch float or None."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if hasattr(v, "timestamp"):  # datetime.datetime
        try:
            return float(v.timestamp())
        except Exception:  # noqa: BLE001
            return None
    try:
        return float(v)
    except Exception:  # noqa: BLE001
        return None


def _epoch_iso(epoch):
    try:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
    except Exception:  # noqa: BLE001
        return None
