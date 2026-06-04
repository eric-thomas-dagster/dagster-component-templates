"""TeeComputeLogManager — write compute logs to N destinations in parallel.

A Dagster instance has exactly one compute log manager. This one
composes others — fan out writes, first-success reads, per-config
display URL. Use it when you want logs in Splunk AND Dagster+, or any
other multi-destination combination.

Configured in ``dagster.yaml``:

    compute_logs:
      module: dagster_community_components.compute_log_managers.tee
      class: TeeComputeLogManager
      config:
        local_dir: /tmp/dagster_compute_logs
        display_manager_index: 0
        managers:
          - module: dagster_community_components.compute_log_managers.splunk
            class: SplunkComputeLogManager
            config:
              hec_url: https://splunk.acme.com:8088/services/collector
              hec_token: {env: SPLUNK_HEC_TOKEN}
              splunk_web_url: https://splunk.acme.com:8000
          - module: dagster_cloud.storage.compute_logs
            class: CloudComputeLogManager
            config: {}

Semantics:
  - upload_to_cloud_storage    fan out to all inner managers; warn on
                               individual failure, don't fail the run
                               (unless fail_on_partial_upload=True).
  - cloud_storage_has_logs     True if ANY inner has logs.
  - download_from_cloud_storage try inner managers in order, first success wins.
  - display_path_for_type      returns the inner manager at
                               display_manager_index — that's the URL
                               the Dagster UI surfaces per step.
  - delete_logs                fan out to all.
  - upload_interval            min of all inner intervals (or None if
                               none set one).

Inner managers share Tee's local_manager — Tee patches each inner's
``_local_manager`` attribute at construction so there's a single
source of truth on disk. Each inner manager's own ``local_dir`` config
(if any) is ignored.
"""
import logging
from typing import IO, List, Optional, Sequence

import yaml
from dagster import Field, StringSource, _check as check
from dagster._core.storage.cloud_storage_compute_log_manager import (
    CloudStorageComputeLogManager,
    PollingComputeLogSubscriptionManager,
    TruncatingCloudStorageComputeLogManager,
)
from dagster._core.storage.compute_log_manager import ComputeIOType
from dagster._core.storage.local_compute_log_manager import (
    LocalComputeLogManager,
)
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster_shared import seven

_logger = logging.getLogger(__name__)


class TeeComputeLogManager(TruncatingCloudStorageComputeLogManager, ConfigurableClass):
    """Tee compute logs to N inner compute log managers.

    Args:
        managers (list[dict]): List of inner CLM configs. Each dict has:
            - module (str): Python module containing the inner CLM class.
            - class (str): Class name.
            - config (dict): Config kwargs passed to the inner CLM.
        local_dir (Optional[str]): Local capture directory. Shared with
            every inner manager (Tee patches each inner's _local_manager
            to this one). Defaults to a system temp dir.
        display_manager_index (Optional[int]): Which inner manager's
            display_path_for_type to surface in the Dagster UI. Default
            0 (the first manager).
        fail_on_partial_upload (Optional[bool]): If True, raise when ANY
            inner upload fails. Default False — failures are logged and
            the upload moves on (Splunk being down shouldn't fail your
            Dagster run).
        inst_data (Optional[ConfigurableClassData]): Serdes plumbing.
    """

    def __init__(
        self,
        managers: List[dict],
        local_dir: Optional[str] = None,
        display_manager_index: int = 0,
        fail_on_partial_upload: bool = False,
        inst_data: Optional[ConfigurableClassData] = None,
    ):
        check.list_param(managers, "managers", of_type=dict)
        if not managers:
            raise ValueError("TeeComputeLogManager requires at least one inner manager.")

        if not local_dir:
            local_dir = seven.get_system_temp_directory()
        self._local_dir = local_dir
        self._local_manager = LocalComputeLogManager(local_dir)
        self._display_manager_index = check.int_param(
            display_manager_index, "display_manager_index"
        )
        self._fail_on_partial_upload = check.bool_param(
            fail_on_partial_upload, "fail_on_partial_upload"
        )
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

        # Rehydrate each inner CLM. Tee owns the local_manager — overwrite
        # each inner's `_local_manager` so they all read/write to the same
        # files. (Without this, each inner would expect its own copy of
        # the captured logs at its own configured local_dir.)
        self._inner_managers: List[CloudStorageComputeLogManager] = []
        for m_cfg in managers:
            inner = ConfigurableClassData(
                module_name=m_cfg["module"],
                class_name=m_cfg["class"],
                config_yaml=yaml.dump(m_cfg.get("config") or {}),
            ).rehydrate(as_type=CloudStorageComputeLogManager)
            inner._local_manager = self._local_manager  # type: ignore[attr-defined]
            self._inner_managers.append(inner)

        if not (0 <= self._display_manager_index < len(self._inner_managers)):
            raise ValueError(
                f"display_manager_index={self._display_manager_index} out of range "
                f"for {len(self._inner_managers)} managers."
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
            "managers": Field(
                [
                    {
                        "module": StringSource,
                        "class": StringSource,
                        "config": Field(dict, is_required=False, default_value={}),
                    }
                ],
                is_required=True,
            ),
            "local_dir": Field(StringSource, is_required=False),
            "display_manager_index": Field(int, is_required=False, default_value=0),
            "fail_on_partial_upload": Field(bool, is_required=False, default_value=False),
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
        intervals = [m.upload_interval for m in self._inner_managers if m.upload_interval]
        return min(intervals) if intervals else None

    def delete_logs(
        self,
        log_key: Optional[Sequence[str]] = None,
        prefix: Optional[Sequence[str]] = None,
    ) -> None:
        # Delete local first so partial failures don't leave orphans.
        self._local_manager.delete_logs(log_key=log_key, prefix=prefix)
        for m in self._inner_managers:
            try:
                m.delete_logs(log_key=log_key, prefix=prefix)
            except Exception as exc:
                _logger.warning(
                    f"TeeComputeLogManager: inner {type(m).__name__} delete_logs failed: {exc}"
                )

    def download_url_for_type(
        self, log_key: Sequence[str], io_type: ComputeIOType
    ) -> Optional[str]:
        return self._display_manager().download_url_for_type(log_key, io_type)

    def display_path_for_type(
        self, log_key: Sequence[str], io_type: ComputeIOType
    ) -> Optional[str]:
        return self._display_manager().display_path_for_type(log_key, io_type)

    def cloud_storage_has_logs(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial: bool = False
    ) -> bool:
        for m in self._inner_managers:
            try:
                if m.cloud_storage_has_logs(log_key, io_type, partial=partial):
                    return True
            except Exception as exc:
                _logger.warning(
                    f"TeeComputeLogManager: inner {type(m).__name__} cloud_storage_has_logs "
                    f"failed: {exc}"
                )
        return False

    def _upload_file_obj(
        self,
        data: IO[bytes],
        log_key: Sequence[str],
        io_type: ComputeIOType,
        partial: bool = False,
    ) -> None:
        """Fan-out the upload. We can't share `data` (each inner would
        consume the IO stream), so we delegate to each inner's full
        upload_to_cloud_storage which reads from the shared local_manager.
        """
        errors = []
        for m in self._inner_managers:
            try:
                m.upload_to_cloud_storage(log_key, io_type, partial=partial)
            except Exception as exc:
                _logger.warning(
                    f"TeeComputeLogManager: inner {type(m).__name__} upload failed for "
                    f"log_key={log_key} io_type={io_type}: {exc}"
                )
                errors.append((type(m).__name__, exc))
        if errors and self._fail_on_partial_upload:
            raise RuntimeError(
                f"TeeComputeLogManager: {len(errors)} inner upload(s) failed and "
                f"fail_on_partial_upload=True. First error: {errors[0][1]}"
            )

    def download_from_cloud_storage(
        self, log_key: Sequence[str], io_type: ComputeIOType, partial: bool = False
    ) -> None:
        """Try inner managers in order; the first that has logs wins."""
        for m in self._inner_managers:
            try:
                if m.cloud_storage_has_logs(log_key, io_type, partial=partial):
                    m.download_from_cloud_storage(log_key, io_type, partial=partial)
                    return
            except Exception as exc:
                _logger.warning(
                    f"TeeComputeLogManager: inner {type(m).__name__} download attempt "
                    f"failed: {exc}"
                )
        # Fall through: no inner reported logs. Leave the local path
        # untouched — the UI's empty-log path takes over.

    def get_log_keys_for_log_key_prefix(
        self, log_key_prefix: Sequence[str], io_type: ComputeIOType
    ) -> Sequence[Sequence[str]]:
        return self._display_manager().get_log_keys_for_log_key_prefix(
            log_key_prefix, io_type=io_type
        )

    # ──────────────────────────── Subscription / lifecycle ──────────────────

    def on_subscribe(self, subscription):
        self._subscription_manager.add_subscription(subscription)

    def on_unsubscribe(self, subscription):
        self._subscription_manager.remove_subscription(subscription)

    def dispose(self):
        self._subscription_manager.dispose()
        for m in self._inner_managers:
            try:
                m.dispose()
            except Exception as exc:
                _logger.warning(
                    f"TeeComputeLogManager: inner {type(m).__name__} dispose failed: {exc}"
                )
        self._local_manager.dispose()

    # ──────────────────────────── Internals ─────────────────────────────────

    def _display_manager(self) -> CloudStorageComputeLogManager:
        return self._inner_managers[self._display_manager_index]
