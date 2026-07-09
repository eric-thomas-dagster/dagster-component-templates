"""SplunkComputeLogManager.

Send Dagster op stdout/stderr to Splunk via the HTTP Event Collector
(HEC). Sits at the Dagster *instance* layer (configured in
``dagster.yaml``), not the project layer.

Configured like any other cloud-storage compute log manager:

    compute_logs:
      module: dagster_community_components.compute_log_managers.splunk
      class: SplunkComputeLogManager
      config:
        hec_url: https://splunk.acme.com:8088/services/collector
        hec_token: "{env: SPLUNK_HEC_TOKEN}"
        splunk_web_url: https://splunk.acme.com:8000
        index: dagster
        sourcetype: dagster:compute_log
        local_dir: /tmp/dagster_compute_logs
        upload_interval: 30

What gets shipped per step:

  - One Splunk HEC event per line of captured stdout/stderr
  - Structured fields on every event:
      dagster_run_id, dagster_step_key, dagster_io_type (stdout|stderr),
      dagster_partial (true|false)
  - Splunk receives logs as text events; query with
      index=dagster dagster_run_id="<run>" | sort _time

UI behavior:

  - ``display_path_for_type`` returns a Splunk Web search URL pre-filtered
    to the run + step + io_type, so the Dagster UI shows a
    "View logs in Splunk →" button per step.
  - ``download_from_cloud_storage`` writes a small stub file noting that
    logs live in Splunk. The UI's inline log viewer shows that stub
    instead of trying to fetch from cloud storage Dagster doesn't manage.

To dual-write to both Splunk AND Dagster+ (or any other CLM), wrap this
in ``TeeComputeLogManager`` — see the tee/ sibling.
"""
import json
import logging
import os
import urllib.parse
from typing import IO, Optional, Sequence

from dagster import (
    Field,
    Noneable,
    StringSource,
    _check as check,
)
from dagster._config import IntSource
from dagster._core.storage.cloud_storage_compute_log_manager import (
    TruncatingCloudStorageComputeLogManager,
)
from dagster._core.storage.compute_log_manager import ComputeIOType
from dagster._core.storage.local_compute_log_manager import (
    IO_TYPE_EXTENSION,
    LocalComputeLogManager,
)
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster._utils import ensure_dir
from dagster_shared import seven

_logger = logging.getLogger(__name__)

# Tag identifying the io_type on a Splunk event.
_IO_TYPE_NAMES = {
    ComputeIOType.STDOUT: "stdout",
    ComputeIOType.STDERR: "stderr",
}


class SplunkComputeLogManager(TruncatingCloudStorageComputeLogManager, ConfigurableClass):
    """Logs compute function stdout and stderr to Splunk via the HTTP Event Collector (HEC).

    Configured in ``dagster.yaml``:

    .. code-block:: yaml

        compute_logs:
          module: dagster_community_components.compute_log_managers.splunk
          class: SplunkComputeLogManager
          config:
            hec_url: https://splunk.acme.com:8088/services/collector
            hec_token: "{env: SPLUNK_HEC_TOKEN}"
            splunk_web_url: https://splunk.acme.com:8000
            index: dagster
            sourcetype: dagster:compute_log
            source: dagster
            host: prod-east
            verify_ssl: true
            batch_size: 100
            local_dir: /tmp/dagster_compute_logs
            upload_interval: 30

    Args:
        hec_url (str): Splunk HEC endpoint, e.g.
            ``https://splunk.acme.com:8088/services/collector``.
        hec_token (str): HEC token. Read from env via ``{env: SPLUNK_HEC_TOKEN}``.
        splunk_web_url (Optional[str]): Splunk Web UI base URL for
            ``display_path_for_type`` deep-links. If unset, the display
            path is the HEC URL with a placeholder query — useful for
            grep but not clickable.
        index (Optional[str]): Splunk index to write into. Defaults to
            the HEC token's default index.
        sourcetype (Optional[str]): Splunk sourcetype field. Default
            ``dagster:compute_log``.
        source (Optional[str]): Splunk source field. Default ``dagster``.
        host (Optional[str]): Splunk host field. Default = local hostname.
        verify_ssl (Optional[bool]): Verify Splunk TLS cert. Set False
            for self-signed dev splunks. Default True.
        batch_size (Optional[int]): Max events per HEC POST. Default 100.
        request_timeout_seconds (Optional[int]): HTTP timeout. Default 30.
        skip_empty_files (Optional[bool]): Skip uploading empty log
            files. Default False.
        local_dir (Optional[str]): Local capture directory. Defaults to
            ``dagster_shared.seven.get_system_temp_directory()``.
        upload_interval (Optional[int]): Interval in seconds to upload
            partial log files. By default, only uploads on capture
            complete.
        inst_data (Optional[ConfigurableClassData]): Serdes plumbing.
    """

    def __init__(
        self,
        hec_url: str,
        hec_token: str,
        splunk_web_url: Optional[str] = None,
        index: Optional[str] = None,
        sourcetype: str = "dagster:compute_log",
        source: str = "dagster",
        host: Optional[str] = None,
        verify_ssl: bool = True,
        batch_size: int = 100,
        request_timeout_seconds: int = 30,
        skip_empty_files: bool = False,
        local_dir: Optional[str] = None,
        upload_interval: Optional[int] = None,
        inst_data: Optional[ConfigurableClassData] = None,
    ):
        self._hec_url = check.str_param(hec_url, "hec_url").rstrip("/")
        self._hec_token = check.str_param(hec_token, "hec_token")
        self._splunk_web_url = (
            check.opt_str_param(splunk_web_url, "splunk_web_url", "").rstrip("/") or None
        )
        self._index = check.opt_str_param(index, "index")
        self._sourcetype = check.str_param(sourcetype, "sourcetype")
        self._source = check.str_param(source, "source")
        self._host = check.opt_str_param(host, "host") or _hostname()
        self._verify_ssl = check.bool_param(verify_ssl, "verify_ssl")
        self._batch_size = check.int_param(batch_size, "batch_size")
        self._request_timeout = check.int_param(request_timeout_seconds, "request_timeout_seconds")
        self._skip_empty_files = check.bool_param(skip_empty_files, "skip_empty_files")

        if not local_dir:
            local_dir = seven.get_system_temp_directory()
        self._local_manager = LocalComputeLogManager(local_dir)
        self._upload_interval = check.opt_int_param(upload_interval, "upload_interval")
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

        # Track which (log_key, io_type, partial) tuples we've successfully
        # uploaded so cloud_storage_has_logs returns truthful answers and
        # the UI knows to surface the "View in Splunk" link.
        self._uploaded_keys: set = set()

        # Lazy-imported on first use so this module loads even without
        # `requests` installed (e.g. during config validation).
        self._session = None

        # Subscription manager — polls local files for live UI streaming.
        from dagster._core.storage.cloud_storage_compute_log_manager import (
            PollingComputeLogSubscriptionManager,
        )
        self._subscription_manager = PollingComputeLogSubscriptionManager(self)

        super().__init__()

    # ──────────────────────────── ConfigurableClass plumbing ────────────────

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "hec_url": StringSource,
            "hec_token": StringSource,
            "splunk_web_url": Field(StringSource, is_required=False),
            "index": Field(StringSource, is_required=False),
            "sourcetype": Field(StringSource, is_required=False, default_value="dagster:compute_log"),
            "source": Field(StringSource, is_required=False, default_value="dagster"),
            "host": Field(StringSource, is_required=False),
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
        """Drop local files. Splunk side is append-only — let Splunk's index
        retention policy handle the server-side lifecycle."""
        self._local_manager.delete_logs(log_key=log_key, prefix=prefix)
        if log_key:
            for io_type in (ComputeIOType.STDOUT, ComputeIOType.STDERR):
                for partial in (False, True):
                    self._uploaded_keys.discard(self._key_tuple(log_key, io_type, partial))

    def download_url_for_type(
        self, log_key: Sequence[str], io_type: ComputeIOType
    ) -> Optional[str]:
        """Splunk doesn't expose presigned download URLs — we surface the
        display path (a Splunk search URL) via display_path_for_type instead."""
        return None

    def display_path_for_type(
        self, log_key: Sequence[str], io_type: ComputeIOType
    ) -> Optional[str]:
        """Build a Splunk Web search URL that pre-filters to this run/step/io_type.

        Example output (when splunk_web_url=https://splunk.acme.com:8000):

            https://splunk.acme.com:8000/en-US/app/search/search?q=search%20index%3Ddagster%20dagster_run_id%3D%22abc-123%22%20dagster_step_key%3D%22step.foo%22%20dagster_io_type%3D%22stdout%22

        The Dagster UI shows this as a "View logs in Splunk →" button.
        """
        if not self.is_capture_complete(log_key):
            return None
        if not self._splunk_web_url:
            return None

        run_id, step_key = _split_log_key(log_key)
        io_name = _IO_TYPE_NAMES[io_type]

        clauses = [f'index={self._index}'] if self._index else []
        clauses.append(f'dagster_run_id="{run_id}"')
        if step_key:
            clauses.append(f'dagster_step_key="{step_key}"')
        clauses.append(f'dagster_io_type="{io_name}"')
        spl = "search " + " ".join(clauses) + " | sort _time"

        return (
            f"{self._splunk_web_url}/en-US/app/search/search?q="
            + urllib.parse.quote(spl, safe="")
        )

    def cloud_storage_has_logs(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial: bool = False
    ) -> bool:
        return self._key_tuple(log_key, io_type, partial) in self._uploaded_keys

    # ──────────────────────────── Upload path (Splunk HEC) ──────────────────

    def _upload_file_obj(
        self,
        data: IO[bytes],
        log_key: Sequence[str],
        io_type: ComputeIOType,
        partial: bool = False,
    ) -> None:
        """Stream the local capture file to Splunk HEC."""
        path = self._local_manager.get_captured_local_path(log_key, IO_TYPE_EXTENSION[io_type])
        if not os.path.exists(path):
            return
        if (self._skip_empty_files or partial) and os.stat(path).st_size == 0:
            return

        run_id, step_key = _split_log_key(log_key)
        io_name = _IO_TYPE_NAMES[io_type]
        _fields = {
            "dagster_run_id": run_id,
            "dagster_step_key": step_key or "",
            "dagster_io_type": io_name,
            "dagster_partial": "true" if partial else "false",
        }
        # Enrich with step-level timing when the step is finished.
        # Belt-and-suspenders try/except so enrichment CANNOT break log delivery.
        try:
            _timing = _step_timing(getattr(self, "_instance", None), run_id, step_key)
            _fields.update(_timing)
            if _timing:
                _logger.info(
                    f"Splunk CLM: enriched with {len(_timing)} step-timing fields "
                    f"for run={run_id[:8]} step={step_key} — keys={list(_timing.keys())}"
                )
            else:
                _logger.info(
                    f"Splunk CLM: step-timing enrichment returned no fields for "
                    f"run={run_id[:8] if run_id else '?'} step={step_key} — "
                    f"instance={type(getattr(self, '_instance', None)).__name__ if getattr(self, '_instance', None) else 'None'}"
                )
        except Exception as _e:  # noqa: BLE001
            _logger.warning(f"Splunk CLM: step-timing enrichment skipped: {_e}")

        events_iter = _iter_hec_events(
            path,
            index=self._index,
            sourcetype=self._sourcetype,
            source=self._source,
            host=self._host,
            fields=_fields,
        )

        # Batch into HEC POSTs.
        sent = 0
        for batch in _chunked(events_iter, self._batch_size):
            body = "".join(json.dumps(e) + "\n" for e in batch)
            self._post_hec(body)
            sent += len(batch)

        if sent:
            self._uploaded_keys.add(self._key_tuple(log_key, io_type, partial))
            _logger.debug(
                f"Splunk CLM: shipped {sent} events for run={run_id} step={step_key} io={io_name}"
            )

    def download_from_cloud_storage(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial: bool = False
    ) -> None:
        """Write a stub local file telling the UI to check Splunk.

        Dagster's UI calls this when an inline log viewer requests a file
        that isn't cached locally. We don't (and shouldn't) pull logs back
        from Splunk — that would round-trip every log view. Instead we
        write a small placeholder so the UI sees *something*, and the
        display path link takes the user to Splunk Web.
        """
        path = self._local_manager.get_captured_local_path(
            log_key, IO_TYPE_EXTENSION[io_type], partial=partial
        )
        ensure_dir(os.path.dirname(path))
        if os.path.exists(path):
            return
        display = self.display_path_for_type(log_key, io_type) or ""
        msg = (
            "Compute logs for this step were shipped to Splunk and are not "
            "cached locally. View them in Splunk:\n\n"
            + (display + "\n" if display else "(splunk_web_url not configured)\n")
        )
        with open(path, "w") as f:
            f.write(msg)

    def get_log_keys_for_log_key_prefix(
        self, log_key_prefix: Sequence[str], io_type: ComputeIOType
    ) -> Sequence[Sequence[str]]:
        """Splunk is full-text search, not directory listing. We don't
        attempt to enumerate stored keys — return what's locally cached."""
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

    def _post_hec(self, body: str) -> None:
        """POST a batch of HEC events. Raises on non-200 — caller's caller
        handles upload errors (Dagster's CLM machinery logs them)."""
        import requests
        if self._session is None:
            self._session = requests.Session()
        headers = {
            "Authorization": f"Splunk {self._hec_token}",
            "Content-Type": "application/json",
        }
        resp = self._session.post(
            self._hec_url,
            data=body.encode("utf-8"),
            headers=headers,
            timeout=self._request_timeout,
            verify=self._verify_ssl,
        )
        resp.raise_for_status()


# ────────────────────────────── helpers ───────────────────────────────────


def _hostname() -> str:
    try:
        import socket
        return socket.gethostname()
    except Exception:
        return "dagster"


def _split_log_key(log_key: Sequence[str]) -> tuple:
    """Pull (run_id, step_key) out of a Dagster log_key. Layout varies a
    bit across Dagster versions; the run_id is consistently the first
    segment and the step_key is the last."""
    parts = list(log_key)
    if not parts:
        return "", ""
    run_id = parts[0]
    step_key = parts[-1] if len(parts) > 1 else ""
    return run_id, step_key


def _iter_hec_events(path: str, *, index, sourcetype, source, host, fields):
    """Yield one HEC event per line of the captured file."""
    with open(path, "r", errors="replace") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line:
                continue
            evt: dict = {
                "event": line,
                "sourcetype": sourcetype,
                "source": source,
                "host": host,
                "fields": fields,
            }
            if index:
                evt["index"] = index
            yield evt


def _chunked(it, n):
    buf: list = []
    for item in it:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def _step_timing(instance, run_id: Optional[str], step_key: Optional[str]) -> dict:
    """Return step start/end/duration as HEC-friendly fields.

    Best-effort — any exception silently returns an empty dict so log
    delivery never fails because of enrichment. Handles both epoch-float
    and datetime-object shapes for start_time/end_time (varies across
    Dagster versions).
    """
    try:
        if instance is None or not run_id or not step_key:
            return {}
        try:
            _stats = instance.get_run_step_stats(run_id, step_keys=[step_key])
        except Exception:  # noqa: BLE001
            return {}
        if not _stats:
            return {}
        _s = _stats[0]

        _start = _coerce_epoch(getattr(_s, "start_time", None))
        _end = _coerce_epoch(getattr(_s, "end_time", None))
        _status = getattr(_s, "status", None)

        _out: dict = {}
        if _start is not None:
            _out["dagster_step_start_epoch"] = _start
            _iso = _epoch_iso(_start)
            if _iso:
                _out["dagster_step_start_iso"] = _iso
        if _end is not None:
            _out["dagster_step_end_epoch"] = _end
            _iso = _epoch_iso(_end)
            if _iso:
                _out["dagster_step_end_iso"] = _iso
        if _start is not None and _end is not None:
            _out["dagster_step_duration_ms"] = int(round((_end - _start) * 1000))
        if _status is not None:
            _out["dagster_step_status"] = getattr(_status, "value", str(_status))
        return _out
    except Exception:  # noqa: BLE001 — outer guard so enrichment never breaks upload
        return {}


def _coerce_epoch(v) -> Optional[float]:
    """Coerce a start_time/end_time value to unix-epoch float or None.

    Dagster's step stats have historically returned either a raw float
    (unix seconds) OR a datetime object. Handle both, plus ints and
    strings that look like floats.
    """
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


def _epoch_iso(epoch: float) -> Optional[str]:
    try:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
    except Exception:  # noqa: BLE001
        return None
