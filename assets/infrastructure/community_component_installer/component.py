"""Community Component Installer.

A state-backed component that downloads other community components from the
registry at refresh-state time. Designed for Dagster+ environments where you
want to install community components from the UI (no local git changes
required), but also works locally — useful as a "components.txt"-style
declarative way to pin community components into a project.

Workflow:
  1. Add this component to your project once.
  2. List the community component IDs you want in `components`.
  3. On refresh-state (Dagster+ UI button OR `dg utils refresh-defs-state`),
     this component fetches each listed component's files into the
     state path and runs pip install on their requirements.txt files.
  4. A reporting asset records what was installed / skipped / failed.

Limitation: this downloads files into the component's state directory. To
*use* the installed components in your project today, copy or symlink them
from the state path into your `defs/` tree, OR run the dagster-component CLI
locally (it auto-installs to the right place). When Dagster+ ships full
state-backed UI editing for arbitrary components, this gap will close —
each downloaded component will appear as its own editable node in the UI.

Registry: https://dagster-component-ui.vercel.app/
CLI: https://github.com/eric-thomas-dagster/dagster-community-components-cli
"""
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import dagster as dg
from pydantic import Field

try:
    import requests
except ImportError as _exc:
    raise ImportError(
        "community_component_installer requires `requests`. Install it with "
        "`pip install requests`."
    ) from _exc

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None
    _HAS_STATE_BACKED = False


DEFAULT_REGISTRY_URL = (
    "https://raw.githubusercontent.com/"
    "eric-thomas-dagster/dagster-component-templates/main/manifest.json"
)

# Files that may be present in a community component's directory.
COMPONENT_FILES = (
    "component.py",
    "io_manager.py",
    "__init__.py",
    "README.md",
    "schema.json",
    "example.yaml",
    "requirements.txt",
)


# ── Helpers (used by both StateBackedComponent and the fallback) ───────────────


def _fetch_manifest(registry_url: str) -> dict:
    resp = requests.get(registry_url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _parse_component_ref(spec: str) -> tuple[str, Optional[str]]:
    """Split an `id@ref` spec into (id, ref).

    Examples:
        "postgres_resource"             -> ("postgres_resource", None)
        "s3_parquet_io_manager@v1.2.0"  -> ("s3_parquet_io_manager", "v1.2.0")
        "one_hot_encoding@a1b2c3d"      -> ("one_hot_encoding", "a1b2c3d")
    """
    if "@" in spec:
        cid, ref = spec.split("@", 1)
        return cid.strip(), ref.strip() or None
    return spec.strip(), None


def _file_url_for(entry: dict, filename: str, ref: Optional[str] = None) -> str:
    """Build the raw-content URL for a file inside a component's directory.

    If `ref` is provided, the `/main/` segment in the component_url is replaced
    with `/<ref>/` so files are pulled from the pinned commit / tag / branch.
    """
    base = entry.get("component_url")
    if not base:
        raise RuntimeError(f"manifest entry for '{entry.get('id')}' has no component_url")
    if ref:
        base = base.replace("/main/", f"/{ref}/", 1)
    return f"{base.rsplit('/', 1)[0]}/{filename}"


def _download_component(
    entry: dict, target_dir: Path, *, ref: Optional[str] = None
) -> dict:
    """Download all known files for a component into target_dir.

    If `ref` is provided, files are fetched from that commit / tag / branch
    instead of `main`.

    Returns a dict with `files_written` (list of filenames) and `bytes_total`.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    files_written: list[str] = []
    bytes_total = 0
    for filename in COMPONENT_FILES:
        url = _file_url_for(entry, filename, ref=ref)
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except Exception:
            continue  # file doesn't exist for this component
        (target_dir / filename).write_bytes(resp.content)
        files_written.append(filename)
        bytes_total += len(resp.content)
    if "component.py" not in files_written:
        ref_msg = f" at ref '{ref}'" if ref else ""
        raise RuntimeError(f"failed to fetch component.py for '{entry.get('id')}'{ref_msg}")

    # Drop a marker so other tooling can recognize this as a community install.
    marker = {
        "id": entry.get("id"),
        "name": entry.get("name"),
        "category": entry.get("category"),
        "ref": ref or "main",
        "registry_url": entry.get("component_url"),
        "installed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (target_dir / ".dg-community.json").write_text(json.dumps(marker, indent=2) + "\n")

    return {"files_written": files_written, "bytes_total": bytes_total}


def _install_requirements(target_dir: Path, *, log) -> int:
    """Run `pip install -r requirements.txt` for a downloaded component."""
    req = target_dir / "requirements.txt"
    if not req.exists():
        return 0
    packages: list[str] = []
    for line in req.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            packages.append(line)
    if not packages:
        return 0

    cmd = (
        ["uv", "pip", "install", *packages]
        if shutil.which("uv")
        else [sys.executable, "-m", "pip", "install", *packages]
    )
    log.info(f"Installing {len(packages)} package(s): {' '.join(packages)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        log.warning(f"pip install exited {proc.returncode}: {proc.stderr.strip()[-500:]}")
    return proc.returncode


def _do_install(
    component_specs: List[str],
    target_root: Path,
    *,
    registry_url: str,
    install_pip_requirements: bool,
    log,
) -> dict:
    """Resolve, download, and pip-install every requested component.

    `component_specs` accepts plain `id` or pinned `id@ref` (commit / tag /
    branch). The registry manifest is fetched once at `main`; per-component
    files are fetched at their pinned ref if specified.

    Returns a dict with `installed` (success), `skipped` (not in registry or
    failed), and `timestamp`.
    """
    log.info(f"Fetching registry manifest from {registry_url}")
    manifest = _fetch_manifest(registry_url)
    by_id = {c.get("id"): c for c in manifest.get("components", []) if c.get("id")}

    installed: list[dict] = []
    skipped: list[dict] = []

    for spec in component_specs:
        cid, ref = _parse_component_ref(spec)
        entry = by_id.get(cid)
        if not entry:
            log.warning(f"Component '{cid}' not found in registry — skipping.")
            skipped.append({"id": cid, "ref": ref, "reason": "not found in registry"})
            continue

        category = entry.get("category", "other")
        comp_dir = target_root / category / cid
        log.info(f"Installing {cid}{('@' + ref) if ref else ''} -> {comp_dir}")
        try:
            stats = _download_component(entry, comp_dir, ref=ref)
        except Exception as e:
            log.error(f"Failed to download {cid}: {e}")
            skipped.append({"id": cid, "ref": ref, "reason": f"download failed: {e}"})
            continue

        record = {
            "id": cid,
            "ref": ref or "main",
            "name": entry.get("name"),
            "category": category,
            "path": str(comp_dir.relative_to(target_root)),
            "files": stats["files_written"],
            "bytes": stats["bytes_total"],
            "pip_install_returncode": None,
        }
        if install_pip_requirements:
            record["pip_install_returncode"] = _install_requirements(comp_dir, log=log)
        installed.append(record)

    return {
        "installed": installed,
        "skipped": skipped,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "registry_url": registry_url,
        "target_root": str(target_root),
    }


# ── Reporting asset ────────────────────────────────────────────────────────────


def _build_reporting_defs(
    asset_name: str, group_name: str, manifest: dict
) -> dg.Definitions:
    """Build a single reporting asset that surfaces install state in the catalog."""
    @dg.asset(
        name=asset_name,
        group_name=group_name,
        kinds={"installer", "registry"},
    )
    def _community_installer_status(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        installed = manifest.get("installed", [])
        skipped = manifest.get("skipped", [])
        for r in installed:
            context.log.info(f"Installed: {r['id']} -> {r.get('path')}")
        for r in skipped:
            context.log.warning(f"Skipped: {r['id']} ({r.get('reason')})")
        return dg.MaterializeResult(
            metadata={
                "installed_count": dg.MetadataValue.int(len(installed)),
                "skipped_count": dg.MetadataValue.int(len(skipped)),
                "installed": dg.MetadataValue.json(installed),
                "skipped": dg.MetadataValue.json(skipped),
                "timestamp": dg.MetadataValue.text(manifest.get("timestamp", "")),
                "registry_url": dg.MetadataValue.text(manifest.get("registry_url", "")),
                "target_root": dg.MetadataValue.text(manifest.get("target_root", "")),
            }
        )

    return dg.Definitions(assets=[_community_installer_status])


# ── State-backed implementation ────────────────────────────────────────────────


if _HAS_STATE_BACKED:

    class CommunityComponentInstallerComponent(StateBackedComponent, dg.Resolvable):
        """Install Dagster community components declared via YAML.

        Lists the IDs of components you want in your project; on refresh-state,
        each is downloaded into the component's state directory and its pip
        requirements are installed.

        Pair with the dagster-community-components-cli for local development:
        https://github.com/eric-thomas-dagster/dagster-community-components-cli
        """

        components: List[str] = Field(
            description=(
                "Community component IDs to install. See the registry: "
                "https://dagster-community-ui.vercel.app/"
            )
        )
        registry_url: Optional[str] = Field(
            default=None,
            description=(
                "Override the default registry manifest URL. Useful for forks or "
                "internal mirrors."
            ),
        )
        install_pip_requirements: bool = Field(
            default=True,
            description=(
                "If True, run `pip install -r requirements.txt` for each downloaded "
                "component. Disable in environments where the runtime can't install "
                "packages (e.g. read-only images)."
            ),
        )
        asset_name: str = Field(
            default="community_components_installed",
            description="Name of the reporting asset that surfaces install state in the catalog.",
        )
        group_name: str = Field(
            default="community_components",
            description="Dagster asset group for the reporting asset.",
        )

        defs_state: DefsStateConfigArgs = field(default_factory=DefsStateConfigArgs)  # noqa: F821

        @property
        def defs_state_config(self) -> DefsStateConfig:
            key = "CommunityComponentInstaller[" + ",".join(sorted(self.components)) + "]"
            return DefsStateConfig.from_args(self.defs_state, default_key=key)

        def write_state_to_path(self, state_path: Path) -> None:
            """Refresh state — download each listed community component into state_path."""
            import logging

            log = logging.getLogger("CommunityComponentInstaller")
            log.info(f"Refreshing community components into {state_path}")

            target_root = state_path / "components"
            target_root.mkdir(parents=True, exist_ok=True)

            manifest = _do_install(
                self.components,
                target_root,
                registry_url=self.registry_url or DEFAULT_REGISTRY_URL,
                install_pip_requirements=self.install_pip_requirements,
                log=log,
            )
            (state_path / "install_manifest.json").write_text(
                json.dumps(manifest, indent=2)
            )

        def build_defs_from_state(
            self,
            context: "dg.ComponentLoadContext",
            state_path: Optional[Path],
        ) -> dg.Definitions:
            """Emit a reporting asset describing what's installed."""
            if state_path is None or not (state_path / "install_manifest.json").exists():
                return dg.Definitions()
            manifest = json.loads((state_path / "install_manifest.json").read_text())
            return _build_reporting_defs(self.asset_name, self.group_name, manifest)

else:

    # Fallback: download eagerly at build_defs time. Slow, no caching — but works
    # in environments where StateBackedComponent isn't available.

    class CommunityComponentInstallerComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Eagerly install community components on every code-location reload.

        Falls back to this implementation when StateBackedComponent isn't
        available. Slower than the state-backed version because every reload
        re-downloads (still cheap for small lists; consider upgrading Dagster
        for larger ones).
        """

        components: List[str] = Field(
            description="Community component IDs to install."
        )
        registry_url: Optional[str] = Field(default=None)
        install_pip_requirements: bool = Field(default=True)
        asset_name: str = Field(default="community_components_installed")
        group_name: str = Field(default="community_components")

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            import logging
            import tempfile

            log = logging.getLogger("CommunityComponentInstaller")
            target_root = Path(tempfile.mkdtemp(prefix="dg-community-")) / "components"
            target_root.mkdir(parents=True, exist_ok=True)

            manifest = _do_install(
                self.components,
                target_root,
                registry_url=self.registry_url or DEFAULT_REGISTRY_URL,
                install_pip_requirements=self.install_pip_requirements,
                log=log,
            )
            return _build_reporting_defs(self.asset_name, self.group_name, manifest)
