"""ArchiveFetcherComponent — download + extract a remote archive.

Use this when a dataset ships as a compressed bundle (ZIP, tar.gz, etc.) — e.g.
MovieLens (`ml-latest-small.zip`), IMDb (`title.basics.tsv.gz`), Kaggle
dumps, USGS earthquake CSVs, GTFS transit feeds, government data releases.

The asset materializes the extracted directory and emits a `{filename: path}`
dict so downstream assets can reference individual files by their predictable
absolute paths.

Supported archive types (auto-detected from URL extension or `archive_type`):
  - zip
  - tar / tar.gz / tgz / tar.bz2 / tbz2 / tar.xz / txz
  - gz   (single-file gzip; output written to <extract_to>/<basename-without-.gz>)
  - bz2  (single-file bzip2; output written to <extract_to>/<basename-without-.bz2>)

```yaml
type: dagster_component_templates.ArchiveFetcherComponent
attributes:
  asset_name: movielens_raw
  url: https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
  extract_to: /tmp/movielens
  flatten: true   # strip the top-level "ml-latest-small/" directory
```

Downstream usage:

```yaml
# A CSV ingestion that depends on the archive_fetcher
type: dagster_component_templates.CSVFileIngestionComponent
attributes:
  asset_name: movies
  file_path: /tmp/movielens/movies.csv
  deps: [movielens_raw]
```
"""

from dagster import AssetKey  # auto-added for hierarchical keys

import os
import gzip
import bz2
import shutil
import tarfile
import zipfile
import tempfile
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union
from urllib.parse import urlparse

import requests
from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


# ---- Archive-type detection ---------------------------------------------------

# Order matters — check the longest suffix first.
_KNOWN_SUFFIXES = [
    (".tar.gz", "tar.gz"),
    (".tgz", "tar.gz"),
    (".tar.bz2", "tar.bz2"),
    (".tbz2", "tar.bz2"),
    (".tbz", "tar.bz2"),
    (".tar.xz", "tar.xz"),
    (".txz", "tar.xz"),
    (".tar", "tar"),
    (".zip", "zip"),
    (".gz", "gz"),
    (".bz2", "bz2"),
]


def _infer_archive_type(url: str) -> Optional[str]:
    path = urlparse(url).path.lower()
    for suffix, kind in _KNOWN_SUFFIXES:
        if path.endswith(suffix):
            return kind
    return None


def _strip_archive_suffix(name: str, kind: str) -> str:
    """For single-file gzip/bz2: derive the output filename by dropping the suffix."""
    lower = name.lower()
    if kind == "gz" and lower.endswith(".gz"):
        return name[:-3]
    if kind == "bz2" and lower.endswith(".bz2"):
        return name[:-4]
    return name


# ---- Safe extraction ----------------------------------------------------------

def _is_within(parent: Path, target: Path) -> bool:
    """Defense against path-traversal (zip-slip)."""
    try:
        target.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _extract_zip(archive_path: Path, dest: Path) -> List[Path]:
    extracted: List[Path] = []
    with zipfile.ZipFile(archive_path) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            out_path = dest / member.filename
            if not _is_within(dest, out_path):
                raise ValueError(f"Refusing to extract outside dest: {member.filename!r}")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member) as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
            extracted.append(out_path)
    return extracted


def _extract_tar(
    archive_path: Path, dest: Path, mode: Literal["r:", "r:gz", "r:bz2", "r:xz"]
) -> List[Path]:
    extracted: List[Path] = []
    with tarfile.open(archive_path, mode) as tf:
        for member in tf.getmembers():
            if not member.isfile():
                continue
            out_path = dest / member.name
            if not _is_within(dest, out_path):
                raise ValueError(f"Refusing to extract outside dest: {member.name!r}")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            src = tf.extractfile(member)
            if src is None:
                continue
            with src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
            extracted.append(out_path)
    return extracted


def _extract_single(archive_path: Path, dest: Path, opener, url: str, kind: str) -> List[Path]:
    """Decompress a single-file archive (gz / bz2) to dest."""
    basename = os.path.basename(urlparse(url).path) or archive_path.name
    out_name = _strip_archive_suffix(basename, kind)
    if not out_name or out_name == basename:
        out_name = archive_path.stem  # fallback if no recognized suffix
    out_path = dest / out_name
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with opener(archive_path, "rb") as src, open(out_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    return [out_path]


def _extract(archive_path: Path, dest: Path, kind: str, url: str) -> List[Path]:
    dest.mkdir(parents=True, exist_ok=True)
    if kind == "zip":
        return _extract_zip(archive_path, dest)
    if kind == "tar":
        return _extract_tar(archive_path, dest, "r:")
    if kind == "tar.gz":
        return _extract_tar(archive_path, dest, "r:gz")
    if kind == "tar.bz2":
        return _extract_tar(archive_path, dest, "r:bz2")
    if kind == "tar.xz":
        return _extract_tar(archive_path, dest, "r:xz")
    if kind == "gz":
        return _extract_single(archive_path, dest, gzip.open, url, "gz")
    if kind == "bz2":
        return _extract_single(archive_path, dest, bz2.open, url, "bz2")
    raise ValueError(f"Unsupported archive_type: {kind!r}")


def _flatten_single_root(dest: Path, extracted: List[Path]) -> List[Path]:
    """If every extracted file lives under one common top-level directory,
    move them up one level. Mirrors the way `unzip` users usually want it."""
    if not extracted:
        return extracted
    top_levels = {p.relative_to(dest).parts[0] for p in extracted}
    if len(top_levels) != 1:
        return extracted
    top = dest / next(iter(top_levels))
    if not top.is_dir():
        return extracted
    moved: List[Path] = []
    for src in extracted:
        rel = src.relative_to(top)
        new_path = dest / rel
        new_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(new_path))
        moved.append(new_path)
    # Remove the now-empty top-level dir.
    shutil.rmtree(top, ignore_errors=True)
    return moved


def _matches_any(name: str, patterns: List[str]) -> bool:
    from fnmatch import fnmatch
    return any(fnmatch(name, p) for p in patterns)


# ---- Destination resolution (local path vs remote URI) ------------------------

_REMOTE_SCHEMES = ("s3", "gs", "gcs", "az", "abfs", "abfss", "adl", "ftp", "http", "https")


def _is_remote_uri(s: str) -> bool:
    """True if `s` looks like a remote URI fsspec should handle.

    Local paths like /tmp/foo and file:///tmp/foo return False — those are
    handled directly by pathlib. Remote schemes (s3, gs, az, ...) return True.
    """
    if "://" not in s:
        return False
    scheme = s.split("://", 1)[0].lower()
    return scheme in _REMOTE_SCHEMES


def _local_path_from_extract_to(extract_to: str) -> Path:
    """Strip a `file://` prefix if present and return a pathlib.Path."""
    if extract_to.startswith("file://"):
        return Path(extract_to[len("file://"):])
    return Path(extract_to)


def _upload_to_fsspec(
    local_files: List[Path], local_root: Path, destination_uri: str
) -> Dict[str, str]:
    """Upload each local file to a remote location via fsspec.

    Returns a {relative_path: full_remote_uri} mapping. Auth is whatever
    fsspec's underlying filesystem driver discovers (env vars, ~/.aws/credentials,
    instance roles, etc.) — see the README.
    """
    try:
        import fsspec
    except ImportError as e:
        raise ImportError(
            "Remote destinations (s3://, gs://, etc.) require fsspec. "
            "Install with `pip install fsspec s3fs` (for S3), `gcsfs` (for GCS), "
            "or `adlfs` (for Azure)."
        ) from e

    fs, base_path = fsspec.core.url_to_fs(destination_uri)
    base_path = base_path.rstrip("/")
    scheme = destination_uri.split("://", 1)[0]
    uri_map: Dict[str, str] = {}
    for p in local_files:
        rel = str(p.relative_to(local_root))
        remote_path = f"{base_path}/{rel}" if base_path else rel
        with open(p, "rb") as src, fs.open(remote_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        uri_map[rel] = f"{scheme}://{remote_path}"
    return uri_map


# ---- Component ----------------------------------------------------------------

class ArchiveFetcherComponent(Component, Model, Resolvable):
    """Download a remote archive (zip / tar.gz / tar.bz2 / tar.xz / gz / bz2),
    extract it, and emit a `{filename: absolute_path}` dict so downstream
    assets can reference individual files."""

    asset_name: str = Field(description="Name of the asset")

    url: str = Field(description="URL of the archive to download")

    extract_to: Optional[str] = Field(
        default=None,
        description=(
            "Destination for extracted files. Either a local directory path "
            "(`/tmp/foo`, `file:///tmp/foo`) or a remote URI: `s3://bucket/prefix/`, "
            "`gs://bucket/prefix/`, `abfss://container@account.dfs.core.windows.net/prefix/` "
            "(canonical ADLS Gen2), `abfs://...`, or `az://container/prefix/` "
            "(adlfs alias). For remote URIs the archive is extracted to a "
            "local temp dir, uploaded via fsspec, "
            "then the temp dir is cleaned up. Auth uses fsspec's ambient "
            "credential discovery (env vars, ~/.aws/credentials, IRSA, "
            "instance role, etc.). Install s3fs / gcsfs / adlfs for the "
            "scheme you need. Default: `/tmp/<asset_name>/`."
        ),
    )

    archive_type: Optional[str] = Field(
        default=None,
        description=(
            "Archive type: 'zip', 'tar', 'tar.gz', 'tar.bz2', 'tar.xz', 'gz', 'bz2'. "
            "If omitted, inferred from the URL extension."
        ),
    )

    flatten: bool = Field(
        default=False,
        description=(
            "If True and all archive entries share a single top-level directory "
            "(e.g. `ml-latest-small/movies.csv`), strip that directory so files "
            "land directly under `extract_to`."
        ),
    )

    include_glob: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional list of fnmatch patterns (e.g. `['*.csv', '*.tsv']`) to "
            "restrict which extracted files appear in the asset's output dict. "
            "Files that don't match are still extracted to disk but omitted "
            "from the returned mapping + metadata."
        ),
    )

    timeout: int = Field(
        default=300,
        description="HTTP timeout in seconds for the download",
    )

    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify SSL certificates on the download",
    )

    headers: Optional[Union[str, Dict[str, str]]] = Field(
        default=None,
        description=(
            "Optional HTTP headers for the download — YAML dict or JSON string. "
            "Useful for `Authorization`, `User-Agent`, etc."
        ),
    )

    cleanup_archive: bool = Field(
        default=True,
        description="If True, delete the downloaded archive after extraction",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        url = self.url
        extract_to = self.extract_to or f"/tmp/{asset_name}"
        archive_type_override = self.archive_type
        flatten = self.flatten
        include_glob = list(self.include_glob) if self.include_glob else None
        timeout = self.timeout
        verify_ssl = self.verify_ssl
        headers_raw = self.headers
        cleanup_archive = self.cleanup_archive
        deps_keys = self.deps

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Archive fetched + extracted from {url}",
            group_name=self.group_name,
            kinds=set(self.kinds) if self.kinds else {"http", "archive"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=deps_keys,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            # Resolve archive kind.
            kind = archive_type_override or _infer_archive_type(url)
            if not kind:
                raise ValueError(
                    f"Could not infer archive_type from URL {url!r} — "
                    f"please set `archive_type:` explicitly."
                )
            context.log.info(f"Fetching {url} (archive_type={kind})")

            # Parse headers.
            req_headers: Optional[Dict[str, str]] = None
            if headers_raw:
                if isinstance(headers_raw, str):
                    import json
                    req_headers = json.loads(headers_raw)
                else:
                    req_headers = dict(headers_raw)

            # Resolve destination — local path (pathlib) vs remote URI (fsspec).
            is_remote = _is_remote_uri(extract_to)
            if is_remote:
                # Always extract locally first; upload after.
                local_extract_dir = Path(tempfile.mkdtemp(prefix="archive_fetcher_extract_"))
                context.log.info(
                    f"Destination is remote ({extract_to}) — extracting to "
                    f"local temp {local_extract_dir}, will upload via fsspec."
                )
            else:
                local_extract_dir = _local_path_from_extract_to(extract_to)
                local_extract_dir.mkdir(parents=True, exist_ok=True)

            # Download to a temp file (stream — archives can be large).
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{kind.replace('.', '_')}") as tmp:
                archive_path = Path(tmp.name)
            try:
                with requests.get(url, stream=True, timeout=timeout, verify=verify_ssl, headers=req_headers) as resp:
                    resp.raise_for_status()
                    bytes_downloaded = 0
                    with open(archive_path, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=64 * 1024):
                            if chunk:
                                fh.write(chunk)
                                bytes_downloaded += len(chunk)
                context.log.info(f"Downloaded {bytes_downloaded:,} bytes to {archive_path}")

                # Extract.
                extracted = _extract(archive_path, local_extract_dir, kind, url)
                context.log.info(f"Extracted {len(extracted)} files to {local_extract_dir}")

                if flatten:
                    extracted = _flatten_single_root(local_extract_dir, extracted)

                # Build the {filename: path-or-uri} dict, and optionally upload.
                file_sizes: Dict[str, int] = {
                    str(p.relative_to(local_extract_dir)): p.stat().st_size
                    for p in extracted
                }

                if is_remote:
                    # Upload everything (or only matching), then dict values are remote URIs.
                    to_upload = [
                        p for p in extracted
                        if not include_glob or _matches_any(
                            str(p.relative_to(local_extract_dir)), include_glob
                        )
                    ]
                    context.log.info(
                        f"Uploading {len(to_upload)} file(s) to {extract_to} via fsspec"
                    )
                    file_map = _upload_to_fsspec(to_upload, local_extract_dir, extract_to)
                else:
                    file_map: Dict[str, str] = {}
                    for p in extracted:
                        rel = str(p.relative_to(local_extract_dir))
                        if include_glob and not _matches_any(rel, include_glob):
                            continue
                        file_map[rel] = str(p.resolve())

                # Trim file_sizes to match the keys we actually emit.
                file_sizes = {k: file_sizes[k] for k in file_map if k in file_sizes}
                total_size = sum(file_sizes.values())

                # Build a readable preview table.
                if file_map:
                    rows = "\n".join(
                        f"| `{name}` | {file_sizes[name]:,} | `{file_map[name]}` |"
                        for name in sorted(file_map)
                    )
                    preview_md = (
                        "| File | Size (bytes) | Path |\n"
                        "|---|---:|---|\n"
                        f"{rows}"
                    )
                else:
                    preview_md = "_(no files matched include_glob)_"

                return Output(
                    value=file_map,
                    metadata={
                        "url":         MetadataValue.url(url),
                        "archive_type": MetadataValue.text(kind),
                        "extract_to":  MetadataValue.text(extract_to),
                        "destination": MetadataValue.text("remote (fsspec)" if is_remote else "local"),
                        "file_count":  MetadataValue.int(len(file_map)),
                        "total_files_extracted": MetadataValue.int(len(extracted)),
                        "total_size_bytes": MetadataValue.int(total_size),
                        "bytes_downloaded": MetadataValue.int(bytes_downloaded),
                        "files":       MetadataValue.md(preview_md),
                    },
                )
            finally:
                if cleanup_archive and archive_path.exists():
                    try:
                        archive_path.unlink()
                    except OSError:
                        pass
                if is_remote:
                    # We used a temp dir; clean it up.
                    shutil.rmtree(local_extract_dir, ignore_errors=True)

        return Definitions(assets=[_asset])
