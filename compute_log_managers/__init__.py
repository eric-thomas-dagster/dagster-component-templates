"""Community-maintained Dagster compute log managers.

Unlike the components in ``assets/``, ``integrations/``, etc. — which are
defs-folder entities loaded by ``load_from_defs_folder`` at code-location
load time — compute log managers are *instance-level* infrastructure
configured in ``dagster.yaml``:

    compute_logs:
      module: dagster_community_components.compute_log_managers.splunk
      class: SplunkComputeLogManager
      config:
        hec_url: https://splunk.acme.com:8088/services/collector
        hec_token: ...

A Dagster instance has exactly one compute log manager. To send compute
logs to multiple destinations (e.g. Splunk + Dagster+'s CloudComputeLogManager
so OSS users get full coverage), compose them via ``TeeComputeLogManager``.

Available managers in this package:

  - ``splunk.SplunkComputeLogManager`` — POSTs op stdout/stderr to Splunk
    HEC with ``dagster_run_id`` / ``dagster_step_key`` / ``dagster_io_type``
    structured fields. ``display_path_for_type`` returns a deep-link search
    URL so the Dagster UI surfaces "View logs in Splunk →".
  - ``tee.TeeComputeLogManager`` — composes N inner CLMs. Fan-out writes,
    first-success reads. Use this to send to Splunk AND Dagster+ (or any
    other combination).
"""
