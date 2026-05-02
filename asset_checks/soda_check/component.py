"""Soda Check Component.

Runs a Soda scan and surfaces EACH SodaCL check as its own Dagster asset
check. Pre-parses the SodaCL YAML at build_defs time to enumerate checks
so they're individually alertable, retriable, and visible in the catalog UI.

This is a community alternative to the official `dagster-soda` package.
It's a single file you copy into your project — easy to fork, easy to
customize (edit the parser, swap the runtime, change naming, etc.).
For the maintained, packaged path, use `dagster-soda` instead.
"""
from pathlib import Path
from typing import Dict, List, Optional, Union
import re

import dagster as dg
from pydantic import Field


class SodaCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Soda scan as N Dagster asset checks (one per SodaCL check).

    Example:
        ```yaml
        type: dagster_component_templates.SodaCheckComponent
        attributes:
          asset_key: warehouse/orders
          data_source_name: my_postgres
          checks_yaml_paths:
            - ./soda/orders_checks.yaml
            - ./soda/customers_checks.yaml
          soda_configuration_path: ./soda/configuration.yaml
          # Optional — route per-dataset checks to different assets.
          # Keys are SodaCL dataset names; values are slash-separated asset keys.
          # asset_key_map:
          #   orders: warehouse/orders
          #   customers: warehouse/customers
          # severity_override: WARN
        ```

    Per-check severity is read from the SodaCL `attributes.severity` field
    (or `warn:`/`fail:` thresholds in the SodaCL grammar). Pass
    `severity_override` to force the same severity across every check.
    """

    asset_key: str = Field(
        description="Default asset key for checks not routed via asset_key_map"
    )
    data_source_name: str = Field(
        description="Soda data source name (defined in configuration.yaml)"
    )
    checks_yaml_paths: Union[str, List[str]] = Field(
        description="Path(s) to SodaCL YAML files. Accepts a single path or a list."
    )
    soda_configuration_path: str = Field(
        default="./soda/configuration.yaml",
        description="Path to the Soda configuration YAML file",
    )
    asset_key_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Optional map from SodaCL dataset name → slash-separated Dagster asset "
            "key. Lets a single scan attach checks to multiple assets. Datasets "
            "not in the map fall back to the component-level `asset_key`."
        ),
    )
    severity_override: Optional[str] = Field(
        default=None,
        description=(
            "Optional — 'WARN' or 'ERROR'. When unset (default), severity is read "
            "from each SodaCL check's attributes, defaulting to ERROR."
        ),
    )

    @classmethod
    def get_description(cls) -> str:
        return "Run a Soda scan — one Dagster asset check per SodaCL check (community)."

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster import (
            AssetCheckResult,
            AssetCheckSeverity,
            AssetCheckSpec,
            AssetKey,
            asset_check,
            multi_asset_check,
        )

        paths = (
            [self.checks_yaml_paths]
            if isinstance(self.checks_yaml_paths, str)
            else list(self.checks_yaml_paths)
        )
        default_asset_key = AssetKey(self.asset_key.split("/"))
        akmap = {
            ds: AssetKey(k.split("/"))
            for ds, k in (self.asset_key_map or {}).items()
        }
        _self = self

        # Pre-parse every SodaCL file to enumerate checks. If we can't, fall
        # back to one aggregate asset_check.
        parsed = _parse_sodacl_files(paths, default_asset_key, akmap)

        if parsed:
            specs = [p["spec"] for p in parsed]
            check_str_to_name = {p["check_str"]: p["spec"].name for p in parsed}
            name_to_meta_severity = {p["spec"].name: p["meta_severity"] for p in parsed}

            @multi_asset_check(
                specs=specs,
                description=f"Soda scan — {len(specs)} checks across {len(paths)} file(s)",
            )
            def soda_checks(context):
                try:
                    from soda.scan import Scan
                except ImportError:
                    for spec in specs:
                        yield AssetCheckResult(
                            check_name=spec.name,
                            passed=False,
                            severity=AssetCheckSeverity.ERROR,
                            metadata={"error": "soda-core not installed"},
                        )
                    return

                try:
                    scan = Scan()
                    scan.set_data_source_name(_self.data_source_name)
                    scan.add_configuration_yaml_file(_self.soda_configuration_path)
                    for p in paths:
                        scan.add_sodacl_yaml_file(p)
                    scan.execute()
                except Exception as e:
                    for spec in specs:
                        yield AssetCheckResult(
                            check_name=spec.name,
                            passed=False,
                            severity=AssetCheckSeverity.ERROR,
                            metadata={"error": f"Soda scan failed: {e}"},
                        )
                    return

                # Best-effort: detect Soda Cloud configuration so we can deep-link
                # per-check results into the Cloud UI.
                cloud_host, cloud_scan_url = _resolve_soda_cloud(
                    scan, _self.soda_configuration_path
                )

                # Map Soda Check objects → spec names by check string. Soda's
                # Check.name is the SodaCL line as written.
                results_by_name = {}
                try:
                    all_checks = (
                        list(scan.get_checks_pass() or [])
                        + list(scan.get_checks_fail() or [])
                        + list(scan.get_checks_warn() or [])
                    )
                except Exception:
                    all_checks = []

                for ch in all_checks:
                    name = check_str_to_name.get(getattr(ch, "name", "").strip())
                    if name is None:
                        continue
                    outcome = (getattr(ch, "outcome", "") or "").lower()
                    results_by_name[name] = {
                        "passed": outcome == "pass",
                        "outcome": outcome,
                        "check_str": getattr(ch, "name", ""),
                        "check_obj": ch,
                    }

                for spec in specs:
                    res = results_by_name.get(spec.name)
                    if res is None:
                        # Soda didn't return a result for this spec — likely the
                        # scan errored on this individual check.
                        yield AssetCheckResult(
                            check_name=spec.name,
                            passed=False,
                            severity=AssetCheckSeverity.ERROR,
                            metadata={"error": "no result returned by Soda scan"},
                        )
                        continue
                    md = {
                        "check": res["check_str"],
                        "outcome": res["outcome"],
                    }
                    # If Soda Cloud is configured, surface the per-check
                    # incident / scan URL as a clickable link in the Dagster
                    # catalog. Best-effort: prefer a per-check URL if Soda
                    # exposes one on the Check object, otherwise fall back to
                    # the scan-level URL.
                    if cloud_scan_url:
                        from dagster import MetadataValue
                        per_check_url = _per_check_cloud_url(
                            res.get("check_obj"), cloud_host, cloud_scan_url
                        )
                        md["soda_cloud_url"] = MetadataValue.url(per_check_url)
                    yield AssetCheckResult(
                        check_name=spec.name,
                        passed=res["passed"],
                        severity=_resolve_severity(
                            override=_self.severity_override,
                            meta_severity=name_to_meta_severity.get(spec.name),
                            outcome=res["outcome"],
                        ),
                        metadata=md,
                    )

            return dg.Definitions(asset_checks=[soda_checks])

        # Fallback: SodaCL not parseable.
        @asset_check(
            asset=default_asset_key,
            description=f"Run Soda scan (aggregate): {paths}",
        )
        def soda_check_aggregate(context):
            severity = _resolve_severity(
                override=_self.severity_override, meta_severity=None, outcome=None
            )
            try:
                from soda.scan import Scan
            except ImportError:
                return AssetCheckResult(
                    passed=False,
                    severity=severity,
                    metadata={"error": "soda-core not installed"},
                )
            try:
                scan = Scan()
                scan.set_data_source_name(_self.data_source_name)
                scan.add_configuration_yaml_file(_self.soda_configuration_path)
                for p in paths:
                    scan.add_sodacl_yaml_file(p)
                scan.execute()
                has_errors = scan.has_check_fails() or scan.has_errors()
                checks_passed = len(scan.get_checks_pass() or [])
                checks_failed = len(scan.get_checks_fail() or [])
                checks_warned = len(scan.get_checks_warn() or [])
            except Exception as e:
                return AssetCheckResult(
                    passed=False, severity=severity, metadata={"error": str(e)}
                )

            return AssetCheckResult(
                passed=not has_errors,
                severity=severity,
                metadata={
                    "checks_passed": checks_passed,
                    "checks_failed": checks_failed,
                    "checks_warned": checks_warned,
                    "checks_yaml_paths": str(paths),
                    "note": "SodaCL couldn't be pre-parsed at build_defs time — running as one aggregate check.",
                },
            )

        return dg.Definitions(asset_checks=[soda_check_aggregate])


# ── helpers ────────────────────────────────────────────────────────────────


_DATASET_HEADER = re.compile(r"^checks\s+for\s+(\S+)\s*$", re.IGNORECASE)


def _name_for_soda_check(check_str: str) -> str:
    """Build a stable, readable name for one SodaCL check line."""
    # Strip threshold/operator suffix so "missing_count(customer_id) = 0"
    # collapses to "missing_count(customer_id)".
    s = re.split(r"[<>=!]|\sbetween\s|\sin\s|\snot\s", check_str, maxsplit=1)[0].strip()
    s = re.sub(r"[^A-Za-z0-9_]+", "_", s).strip("_")
    return s or "soda_check"


def _parse_sodacl_files(
    paths: List[str],
    default_asset_key,
    asset_key_map: Dict[str, "AssetKey"],  # noqa: F821
) -> List[Dict]:
    """Walk every SodaCL file and emit one parsed entry per check.

    Returns a list of dicts: {spec, check_str, meta_severity}.
    Returns [] if nothing could be parsed (caller falls back to aggregate).
    """
    try:
        import yaml
    except ImportError:
        return []
    from dagster import AssetCheckSpec

    out = []
    seen_names = set()

    for p in paths:
        path = Path(p).expanduser()
        if not path.exists():
            return []
        try:
            data = yaml.safe_load(path.read_text())
        except Exception:
            return []
        if not isinstance(data, dict):
            return []

        for header, items in data.items():
            if not isinstance(header, str):
                continue
            m = _DATASET_HEADER.match(header.strip())
            if not m:
                continue
            dataset = m.group(1)
            asset_key = asset_key_map.get(dataset, default_asset_key)
            if not isinstance(items, list):
                continue

            for item in items:
                check_str, meta_severity = _normalize_check_item(item)
                if not check_str:
                    continue
                base = _name_for_soda_check(check_str)
                # dataset prefix avoids collisions across blocks
                base = f"{dataset}__{base}"
                name = base
                i = 1
                while name in seen_names:
                    i += 1
                    name = f"{base}__{i}"
                seen_names.add(name)
                out.append({
                    "spec": AssetCheckSpec(
                        name=name,
                        asset=asset_key,
                        description=f"Soda: {check_str}",
                    ),
                    "check_str": check_str.strip(),
                    "meta_severity": meta_severity,
                })

    return out


def _normalize_check_item(item) -> tuple:
    """Pull the check string + (optional) severity out of a SodaCL list item.

    SodaCL allows two shapes per check:
      - `missing_count(x) = 0`                 (str)
      - `missing_count(x) = 0:` with attrs     (single-key dict)
    """
    if isinstance(item, str):
        return item.strip(), None
    if isinstance(item, dict) and len(item) == 1:
        check_str, attrs = next(iter(item.items()))
        if not isinstance(check_str, str):
            return None, None
        meta_severity = None
        if isinstance(attrs, dict):
            # Soda allows `attributes.severity` or top-level `warn:` / `fail:` thresholds.
            attributes = attrs.get("attributes") or {}
            meta_severity = attributes.get("severity") if isinstance(attributes, dict) else None
            if meta_severity is None and "warn" in attrs:
                meta_severity = "WARN"
        return check_str.strip(), meta_severity
    return None, None


def _resolve_severity(
    *,
    override: Optional[str],
    meta_severity: Optional[str],
    outcome: Optional[str],
):
    """Resolve severity for a single Soda check.

    Priority:
      1. `severity_override` on the component.
      2. SodaCL `attributes.severity` (or a `warn:` threshold).
      3. If the runtime outcome was "warn", treat as WARN.
      4. Default: ERROR.
    """
    from dagster import AssetCheckSeverity

    raw = override or meta_severity
    if raw and str(raw).upper() == "WARN":
        return AssetCheckSeverity.WARN
    if raw and str(raw).upper() == "ERROR":
        return AssetCheckSeverity.ERROR
    if outcome == "warn":
        return AssetCheckSeverity.WARN
    return AssetCheckSeverity.ERROR


def _resolve_soda_cloud(scan, configuration_path: str) -> tuple:
    """Best-effort detection of Soda Cloud config + scan URL.

    Returns (host, scan_url). Either may be None if Cloud isn't configured
    or the scan didn't post to it.

    Soda's public API for this isn't fully stable — soda-core stores the
    Cloud config under various paths across versions. We try a few common
    spots and fall back gracefully.
    """
    host = None
    scan_id = None

    # 1. Try the scan's own configuration object first (avoids parsing YAML).
    for attr_path in (
        ("_configuration", "soda_cloud", "host"),
        ("configuration", "soda_cloud", "host"),
        ("_data_source_manager", "data_source_properties", "soda_cloud", "host"),
    ):
        try:
            obj = scan
            for a in attr_path:
                obj = getattr(obj, a, None) if not isinstance(obj, dict) else obj.get(a)
                if obj is None:
                    break
            if isinstance(obj, str) and obj:
                host = obj
                break
        except Exception:
            continue

    # 2. Fall back to grepping the configuration.yaml.
    if host is None:
        try:
            import yaml
            with open(configuration_path) as f:
                cfg = yaml.safe_load(f) or {}
            sc = cfg.get("soda_cloud") if isinstance(cfg, dict) else None
            if isinstance(sc, dict):
                host = sc.get("host") or "cloud.soda.io"
        except Exception:
            pass

    if not host:
        return None, None

    # 3. Try to recover the scan ID from the scan object — soda-core sets
    # scan._scan_results['scanId'] or similar after a Cloud-enabled execute().
    for attr_path in (
        ("_scan_results", "scanId"),
        ("scan_results", "scanId"),
        ("_scan_results", "scan_id"),
    ):
        try:
            obj = scan
            for a in attr_path:
                obj = getattr(obj, a, None) if not isinstance(obj, dict) else obj.get(a)
                if obj is None:
                    break
            if isinstance(obj, str) and obj:
                scan_id = obj
                break
        except Exception:
            continue

    if scan_id:
        return host, f"https://{host}/scan/{scan_id}"
    # Without a scan ID we can still link to the host's scans page.
    return host, f"https://{host}/scans"


def _per_check_cloud_url(check_obj, host, scan_url: str) -> str:
    """Build a per-check Cloud URL when possible, else fall back to the scan URL.

    soda-core sometimes attaches a `cloud_dict` or `id` field to its Check
    objects after a Cloud-enabled scan. We try both before falling back.
    """
    if check_obj is None or not host:
        return scan_url
    for attr in ("cloud_dict", "_cloud_dict"):
        d = getattr(check_obj, attr, None)
        if isinstance(d, dict):
            cid = d.get("id") or d.get("checkId")
            if cid:
                return f"https://{host}/check/{cid}"
    cid = getattr(check_obj, "id", None) or getattr(check_obj, "_id", None)
    if cid:
        return f"https://{host}/check/{cid}"
    return scan_url
